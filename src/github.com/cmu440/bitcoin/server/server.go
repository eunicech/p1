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
	pendingReqs *list.List
	clientMap   map[int]clientInfo
	reqChan     chan *serverReq
	freeMiners  *list.List
	// freeMiners map[int]int
	busyMiners map[int]*reqInfo
	msgSize    uint64
}

type serverReq struct {
	msg    *bitcoin.Message
	connId int
	err    error
}

type reqInfo struct {
	data     string
	lower    uint64
	upper    uint64
	clientID int
}

type clientInfo struct {
	requestNum int
	hash       uint64
	nonce      uint64
}

func startServer(port int) (*server, error) {
	// TODO: implement this!
	lspServer, err := lsp.NewServer(port, lsp.NewParams())
	if err != nil {
		return nil, err
	}
	newServer := &server{
		lspServer:   lspServer,
		reqChan:     make(chan *serverReq),
		pendingReqs: list.New(),
		clientMap:   make(map[int]clientInfo),
		freeMiners:  list.New(),
		busyMiners:  make(map[int]*reqInfo),
	}

	return newServer, nil
}

var LOGF *log.Logger

func writeResult(hash uint64, nonce uint64, clientID int, srv lsp.Server) {
	msg := bitcoin.NewResult(hash, nonce)
	msgBytes, _ := json.Marshal(msg)
	srv.Write(clientID, msgBytes)
}

func dispatchMiner(srv *server, connID int) {
	if srv.pendingReqs.Front() != nil {
		// make request
		elem := srv.pendingReqs.Front()
		request := elem.Value.(reqInfo)
		srv.pendingReqs.Remove(elem)
		var newReq *bitcoin.Message
		var minerReq *reqInfo
		if request.upper-request.lower <= srv.msgSize {
			newReq = bitcoin.NewRequest(request.data, request.lower, request.upper)
			minerReq = &reqInfo{
				clientID: request.clientID,
				data:     request.data,
				lower:    request.lower,
				upper:    request.upper,
			}

		} else {
			newReq = bitcoin.NewRequest(request.data, request.lower, request.lower+srv.msgSize)
			minerReq = &reqInfo{
				clientID: request.clientID,
				data:     request.data,
				lower:    request.lower,
				upper:    request.lower + srv.msgSize,
			}
			request.lower = request.lower + srv.msgSize + 1
			srv.pendingReqs.PushBack(request)
		}

		srv.busyMiners[connID] = minerReq
		byteMsg, _ := json.Marshal(newReq)
		srv.lspServer.Write(connID, byteMsg)

	} else {
		srv.freeMiners.PushBack(connID)
		delete(srv.busyMiners, connID)
	}
}

func handleServer(srv *server, closed chan bool) {
	for {
		select {
		case <-closed:
			return
		case data := <-srv.reqChan:
			if data.err != nil {
				//CASE: DISCONNECTED CLIENT OR MINER
				//check if disconnected is a client
				_, isClient := srv.clientMap[data.connId]
				if isClient {
					delete(srv.clientMap, data.connId)
				} else {
					//check if disconnected is a busy miner
					req, isBusy := srv.busyMiners[data.connId]
					if isBusy {
						srv.pendingReqs.PushFront(req)
						delete(srv.busyMiners, data.connId)
					} else {
						for curr := srv.freeMiners.Front(); curr != nil; curr = curr.Next() {
							if curr.Value.(int) == data.connId {
								srv.freeMiners.Remove(curr)
								break
							}
						}
					}
				}

			} else {
				switch data.msg.Type {
				case bitcoin.Join:
					dispatchMiner(srv, data.connId)
				case bitcoin.Result:
					miner := srv.busyMiners[data.connId]
					client, ok := srv.clientMap[miner.clientID]
					if !ok {
						continue
					}
					client.requestNum--
					if data.msg.Hash < client.hash {
						client.hash = data.msg.Hash
						client.nonce = data.msg.Nonce
					}
					if client.requestNum == 0 {
						go writeResult(client.hash, client.nonce, miner.clientID, srv.lspServer)
						//remove client since request is done
						delete(srv.clientMap, miner.clientID)
					}

					dispatchMiner(srv, data.connId)

				case bitcoin.Request:
					request := &reqInfo{
						clientID: data.connId,
						data:     data.msg.Data,
						lower:    data.msg.Lower,
						upper:    data.msg.Upper,
					}
					srv.pendingReqs.PushBack(request)
					for curr := srv.freeMiners.Front(); curr != nil; curr = curr.Next() {
						if srv.pendingReqs.Len() == 0 {
							break
						}
						miner := curr.Value.(int)
						dispatchMiner(srv, miner)
					}
				}
			}

		}
	}
}

func main() {
	// You may need a logger for debug purpose
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
	// Usage: LOGF.Println() or LOGF.Printf()

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

	// TODO: implement this!
	for {

		id, msg, err := srv.lspServer.Read()
		var request *bitcoin.Message
		json.Unmarshal(msg, request)
		//TODO potentially: What if message is empty?
		serverRequest := &serverReq{
			msg:    request,
			connId: id,
			err:    err,
		}
		srv.reqChan <- serverRequest

	}
}
