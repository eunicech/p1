package main

import (
	"container/list"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
)

type server struct {
	lspServer   lsp.Server
	pendingReqs *list.List          // requests that still need to be sent to miners
	clientMap   map[int]*clientInfo // active clients and status of their requests
	reqChan     chan *serverReq     // channel for requests to main routine
	freeMiners  *list.List          // queue keeping track of miners that are free receive requests
	busyMiners  map[int]*reqInfo    // miners currently working through requests
	msgSize     uint64
}

type serverReq struct {
	msg    *bitcoin.Message
	connID int
	err    error
}

type reqInfo struct {
	data     string
	lower    uint64
	upper    uint64
	clientID int // keeps track of client/miner ID
}

type clientInfo struct {
	numReqs int    // number of requests left until client can be finished
	hash    uint64 // minimum hash so far
	nonce   uint64 // best nonce so far
}

func startServer(port int) (*server, error) {
	lspServer, err := lsp.NewServer(port, lsp.NewParams())
	if err != nil {
		return nil, err
	}
	newServer := &server{
		lspServer:   lspServer,
		reqChan:     make(chan *serverReq),
		pendingReqs: list.New(),
		clientMap:   make(map[int]*clientInfo),
		freeMiners:  list.New(),
		busyMiners:  make(map[int]*reqInfo),
		msgSize:     10000,
	}

	return newServer, nil
}

var LOGF *log.Logger

// writes the result of a request back to client
func writeResult(hash uint64, nonce uint64, clientID int, srv lsp.Server) {
	msg := bitcoin.NewResult(hash, nonce)
	msgBytes, _ := json.Marshal(msg)
	srv.Write(clientID, msgBytes)
}

// checks if there are any free miners and pending requests;
// if there are, it will send as many requests as possible with
// the given miners
// ** Load Balancing Algorithm **
// Load balancing occurs by taking a request off the queue. If the request is bigger than the
// predetermined maximum size for a request to a miner, only part of the request is sent. The
// remainder of the request is appended to the back of the queue. This ensures efficiency as similarly
// sized requests will take the same average time. Smaller requests will finish quicker (due to requiring
// less removals from the queue to finish the request) therefore this also ensures fairness
// Each miner on the free miners list will get work of about the same size (unless the requests are small)
// if there are sufficient requests to work on. This ensures the work is divided evenly between miners.
func dispatchMiner(srv *server) {
	var elem = srv.pendingReqs.Front()
	var miner = srv.freeMiners.Front()
	for elem != nil && miner != nil {

		// remove the first miner and pending message
		nextElem := elem.Next()
		nextMiner := miner.Next()
		connID := srv.freeMiners.Remove(miner).(int)
		request := srv.pendingReqs.Remove(elem).(*reqInfo)

		var newReq *bitcoin.Message
		var minerReq *reqInfo

		// if remaining range is smaller than the predetermined chunk size,
		// send the entire request and remove request from the queue
		if request.upper-request.lower+1 <= srv.msgSize {
			newReq = bitcoin.NewRequest(request.data, request.lower, request.upper)
			minerReq = &reqInfo{
				clientID: request.clientID,
				data:     request.data,
				lower:    request.lower,
				upper:    request.upper,
			}
		} else {
			// size of request is bigger than predetermined chunk size
			// send part of request, put remainder of request at the back of the queue
			// ** Load Balanacing: remainder of request is added to the back
			// of the queue, smaller requests will be completed first since
			// they require fewer removals from the queue to finish the request **
			newReq = bitcoin.NewRequest(request.data, request.lower, request.lower+srv.msgSize-1)
			minerReq = &reqInfo{
				clientID: request.clientID,
				data:     request.data,
				lower:    request.lower,
				upper:    request.lower + srv.msgSize - 1,
			}
			request.lower = request.lower + srv.msgSize
			srv.pendingReqs.PushBack(request)
		}

		// add any sent requests to the map keeping track of busy miners
		srv.busyMiners[connID] = minerReq

		// write the request to the miner
		byteMsg, _ := json.Marshal(newReq)
		srv.lspServer.Write(connID, byteMsg)

		miner = nextMiner
		elem = nextElem
	}
}

// handles any requests pertaining to the server (read messages are passed into this function)
func handleServer(srv *server, closed chan bool) {
	for {
		select {
		case <-closed:
			return
		case data := <-srv.reqChan:
			if data.err != nil {
				// CASE: DISCONNECTED CLIENT OR MINER

				_, isClient := srv.clientMap[data.connID]
				if isClient {
					// Disconnected client: delete it from clientmap
					// (all results from miners pertaining to this client will be ignored)
					delete(srv.clientMap, data.connID)
				} else {
					req, isBusy := srv.busyMiners[data.connID]
					if isBusy {
						// Disconnected busy miner: retrieve the request they were working on
						// and add to front of queue
						// delete the miner from list of busy miners
						srv.pendingReqs.PushFront(req)
						delete(srv.busyMiners, data.connID)
						// Check if there are any free miners to work on the request of the disconnected miner
						dispatchMiner(srv)
					} else {
						// Disconnected free miner: delete the miner from the queue
						for curr := srv.freeMiners.Front(); curr != nil; curr = curr.Next() {
							if curr.Value.(int) == data.connID {
								srv.freeMiners.Remove(curr)
								break
							}
						}
						dispatchMiner(srv)
					}
				}

			} else {
				switch data.msg.Type {
				case bitcoin.Join:
					// Case: New miner
					// add to free miners queue
					srv.freeMiners.PushFront(data.connID)
					// check if there are any pending messages the free miner can work on
					// ** Load Balancing: idle time of miner is minimized **
					dispatchMiner(srv)
				case bitcoin.Result:
					// Case: Result from miner
					miner, ok := srv.busyMiners[data.connID]
					if !ok {
						// should never reach this case, since only busy miners should return results
						continue
					}
					client, ok := srv.clientMap[miner.clientID]

					// if clientID not in map, client was lost: ignore resule
					if ok {
						// otherwise, this is a valid client, so update the min hash and nonce accordingly
						if data.msg.Hash < client.hash {
							client.hash = data.msg.Hash
							client.nonce = data.msg.Nonce
						}

						// update number of requests left until client finishes (requests are of s.msgSize)
						client.numReqs--
						if client.numReqs == 0 {
							// if no more requests, client request and results can be written back to client
							go writeResult(client.hash, client.nonce, miner.clientID, srv.lspServer)
							//remove client since request is done
							delete(srv.clientMap, miner.clientID)
						}
					}

					// Received a result: busy miner is now free
					// ** Load Balancing: Free miner added to end of list, ensures all miners
					// that weren't working previously will first get work before this miner. Allows
					// for utilization of all miners **
					srv.freeMiners.PushBack(data.connID)
					delete(srv.busyMiners, data.connID)

					// check if there are pending requests for the free miner to work on
					// ** Load Balancing: idle time of miner is minimized **
					dispatchMiner(srv)

				case bitcoin.Request:
					// Case: Request from a client

					// create new client to keep track of best hash values
					// add to client map
					newClientReq := &clientInfo{
						numReqs: int((data.msg.Upper + srv.msgSize) / srv.msgSize),
						hash:    ^uint64(0),
						nonce:   0,
					}
					srv.clientMap[data.connID] = newClientReq

					// add client request to pending requests
					request := &reqInfo{
						clientID: data.connID,
						data:     data.msg.Data,
						lower:    data.msg.Lower,
						upper:    data.msg.Upper,
					}
					srv.pendingReqs.PushFront(request)

					// check if there are free miners to start working on request
					// ** Load Balancing: idle time of miner is minimized **
					dispatchMiner(srv)
				}
			}
		}
	}
}

func main() {
	const (
		name = "log.txt"
		flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	LOGF = log.New(file, "", log.Lshortfile|log.Lmicroseconds)

	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <port>", os.Args[0])
		return
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Port must be a number:", err)
		return
	}

	srv, err := startServer(port)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("Server listening on port", port)

	defer srv.lspServer.Close()
	closed := make(chan bool)

	go handleServer(srv, closed)

	for {
		// continuously read messages from client
		id, msg, err := srv.lspServer.Read()
		var request bitcoin.Message
		json.Unmarshal(msg, &request)

		// send to main server method to handle messages from clients/miners
		serverRequest := &serverReq{
			msg:    &request,
			connID: id,
			err:    err,
		}
		srv.reqChan <- serverRequest
	}
}
