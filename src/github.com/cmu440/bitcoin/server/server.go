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
	clientMap   map[int]*clientInfo // keep track of active clients and status of their requests
	reqChan     chan *serverReq     // handle any requests to the server
	freeMiners  *list.List          // queue keeping track of miners that are free receive requests
	busyMiners  map[int]*reqInfo    // keeps track of miners currently working through requests
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

// writes the given data to the server
func writeResult(hash uint64, nonce uint64, clientID int, srv lsp.Server) {
	msg := bitcoin.NewResult(hash, nonce)
	msgBytes, _ := json.Marshal(msg)
	srv.Write(clientID, msgBytes)
}

// will check if there are any free miners and pending requests;
// if there are, it will send as many requests as possible with
// the given miners
// also handles the load balancing algorithm
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

		// if the size of the range between upper and lower is smaller than the
		// predetermined chunk size, send the entire request and remove it from the
		// queue
		if request.upper-request.lower+1 <= srv.msgSize {
			newReq = bitcoin.NewRequest(request.data, request.lower, request.upper)
			minerReq = &reqInfo{
				clientID: request.clientID,
				data:     request.data,
				lower:    request.lower,
				upper:    request.upper,
			}
		} else {
			// if the size of the range between upper nad lower is bigger
			// than predetermined chunk size, send part of the request
			// and put the rest of the request at the end of the queue, to be sent later
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
				// if it is  disconnected client, delete it from clientmap
				// (all results from miners pertaining to this client will be ignored)
				_, isClient := srv.clientMap[data.connID]
				if isClient {
					delete(srv.clientMap, data.connID)
				} else {
					// check if disconnected is a busy miner
					req, isBusy := srv.busyMiners[data.connID]
					if isBusy {
						// if busy miner returns an error, put the request they were working
						// through back on the queue to be handled by another free miner, and
						// delete the miner from list of busy miners
						srv.pendingReqs.PushFront(req)
						delete(srv.busyMiners, data.connID)
						dispatchMiner(srv)
					} else {
						// if receive error from free miner, delete it from queue of free miners
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
					// with a new miner, add it to the free miners queue
					srv.freeMiners.PushFront(data.connID)
					// use the free miners to handle pending requests
					dispatchMiner(srv)
				case bitcoin.Result:
					miner, ok := srv.busyMiners[data.connID]
					if !ok {
						// should never reach this case, since only busy miners should return results
						continue
					}
					client, ok := srv.clientMap[miner.clientID]

					// if clientID not in map, client was lost, so we just ignore it
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

					// if we receive a result, it always increases the number of free miners, and is no longer busy
					srv.freeMiners.PushBack(data.connID)
					delete(srv.busyMiners, data.connID)

					// with more free miners, check if we can send any pending requests
					dispatchMiner(srv)

				case bitcoin.Request:
					// request from a client
					request := &reqInfo{
						clientID: data.connID,
						data:     data.msg.Data,
						lower:    data.msg.Lower,
						upper:    data.msg.Upper,
					}
					newClientReq := &clientInfo{
						numReqs: int((data.msg.Upper + srv.msgSize) / srv.msgSize),
						hash:    ^uint64(0),
						nonce:   0,
					}

					// add client request to pending requests
					srv.clientMap[data.connID] = newClientReq
					srv.pendingReqs.PushFront(request)

					// try to use free miners to handle newly added requests
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

		// send messages to main server method to handle
		serverRequest := &serverReq{
			msg:    &request,
			connID: id,
			err:    err,
		}
		srv.reqChan <- serverRequest
	}
}
