// Contains the implementation of a LSP server.

package lsp

import (
	"container/list"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/cmu440/lspnet"
)

type server struct {
	clientMap  map[int]client_info
	client_num int
	// client_num_req  chan int
	conn *lspnet.UDPConn
	// add_msg         chan Message
	// new_client      chan client_info
	// read_req        chan bool
	read_res       chan Message
	pendingMsgs    *list.List
	pendingMsgChan chan Message
	// client_sn_req   chan write_req
	// client_addr_req chan addr_req
	// addPending      chan int
	// removePending   chan int
	// closeConn       chan close_req
	closed        bool
	closeActivate chan bool
	readingClose  chan bool
	reqChan       chan request
}

type request struct {
	reqType      requestType
	clientID     int
	closeResChan chan bool
	addr_res     chan *lspnet.UDPAddr
	ack_chan     chan chan Message
	sn_res       chan int
	add_msg      chan Message
	new_client   chan client_info
}
type requestType int

const (
	AddClient requestType = iota
	AddMsg
	ReadReq
	ClientSN
	ClientAddr
	AddPending
	RemovePending
	CloseCxn
)

// type close_req struct {
// 	clientID int
// 	res      chan bool
// }

// type addr_req struct {
// 	clientId int
// 	addr_res chan *lspnet.UDPAddr
// 	ack_chan chan chan Message
// }
// type write_req struct {
// 	clientID int
// 	sn_res   chan int
// }

type client_info struct {
	client_id      int
	curr_sn        int
	client_sn      int
	ack            chan Message
	addr           *lspnet.UDPAddr
	storedMessages map[int]Message
	closed         chan bool
	toWrite        int
	closeActivate  bool
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	hostport := ":" + strconv.Itoa(port)
	UDPAddr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}
	//listen on port
	udpconn, err := lspnet.ListenUDP("udp", UDPAddr)
	if err != nil {
		return nil, err
	}
	new_server := &server{
		clientMap:  make(map[int]client_info),
		client_num: 1,
		// client_num_req:  make(chan int),
		conn:           udpconn,
		read_res:       make(chan Message),
		pendingMsgs:    list.New(),
		pendingMsgChan: make(chan Message),
		closed:         false,
		closeActivate:  make(chan bool),
		readingClose:   make(chan bool),
	}
	go new_server.writeRoutine()
	go new_server.readRoutine()
	return new_server, nil
}

func (s *server) mapRequestHandler() {
	for {
		select {
		case req := <-s.reqChan:
			switch req.reqType {
			case AddClient:
				new_client := <-req.new_client
				new_client.client_id = s.client_num
				s.client_num += 1
				//send acknowledgement to client
				ack, _ := json.Marshal(NewAck(new_client.client_id, 0))
				s.conn.WriteToUDP(ack, new_client.addr)
			case AddMsg:
				client := s.clientMap[req.clientID]
				msg := <-req.add_msg
				switch msg.Type {
				case MsgAck:
					client.ack <- msg
				case MsgData:
					client.storedMessages[msg.SeqNum] = msg
				}
			case ReadReq:
				var cr client_info
				var found bool = false
				for _, v := range s.clientMap {
					_, ok := v.storedMessages[v.client_sn]
					if ok {
						cr = v
						found = true
						break
					}
				}
				if found {
					res := cr.storedMessages[cr.client_sn]
					delete(cr.storedMessages, cr.client_sn)
					cr.client_sn += 1
					s.read_res <- res
				}
			case ClientSN:
				client := s.clientMap[req.clientID]
				sn := client.curr_sn
				client.curr_sn += 1
				req.sn_res <- sn
			case ClientAddr:
				client := s.clientMap[req.clientID]
				req.addr_res <- client.addr
				req.ack_chan <- client.ack
			case AddPending:
				client := s.clientMap[req.clientID]
				client.toWrite += 1
			case RemovePending:
				client := s.clientMap[req.clientID]
				client.toWrite -= 1
				if client.toWrite == 0 && client.closeActivate {
					delete(s.clientMap, req.clientID)
				}
			case CloseCxn:
				client, ok := s.clientMap[req.clientID]
				if !ok {
					req.closeResChan <- true
				} else {
					client.closeActivate = true
					client.toWrite -= 1
					if client.toWrite == 0 && client.closeActivate {
						delete(s.clientMap, req.clientID)
					}
					req.closeResChan <- false
				}
			}
		case <-s.readingClose:
			break
		}
	}
}

// func (s *server) mapRequestHandler() {
// 	for {
// 		select {
// 		case msg := <-s.add_msg:
// 			client := s.clientMap[msg.ConnID]
// 			switch msg.Type {
// 			case MsgAck:
// 				client.ack <- msg
// 			case MsgData:
// 				client.storedMessages[msg.SeqNum] = msg
// 			}
// 		case new_client := <-s.new_client:
// 			new_client.client_id = s.client_num
// 			s.client_num += 1
// 			//send acknowledgement to client
// 			ack, _ := json.Marshal(NewAck(new_client.client_id, 0))
// 			s.conn.WriteToUDP(ack, new_client.addr)
// 		case <-s.read_req:
// 			var cr client_info
// 			var found bool = false
// 			for _, v := range s.clientMap {
// 				_, ok := v.storedMessages[v.client_sn]
// 				if ok {
// 					cr = v
// 					found = true
// 					break
// 				}
// 			}
// 			if found {
// 				res := cr.storedMessages[cr.client_sn]
// 				delete(cr.storedMessages, cr.client_sn)
// 				cr.client_sn += 1
// 				s.read_res <- res
// 			}
// 		case req := <-s.client_sn_req:
// 			client := s.clientMap[req.clientID]
// 			sn := client.curr_sn
// 			client.curr_sn += 1
// 			req.sn_res <- sn
// 		case req := <-s.client_addr_req:
// 			client := s.clientMap[req.clientId]
// 			req.addr_res <- client.addr
// 			req.ack_chan <- client.ack
// 		case clientID := <-s.addPending:
// 			client := s.clientMap[clientID]
// 			client.toWrite += 1
// 		case clientID := <-s.removePending:
// 			client := s.clientMap[clientID]
// 			client.toWrite -= 1
// 			if client.toWrite == 0 && client.closeActivate {
// 				delete(s.clientMap, clientID)
// 			}
// 		case req := <-s.closeConn:
// 			client, ok := s.clientMap[req.clientID]
// 			if !ok {
// 				req.res <- true
// 			} else {
// 				client.closeActivate = true
// 				client.toWrite -= 1
// 				if client.toWrite == 0 && client.closeActivate {
// 					delete(s.clientMap, req.clientID)
// 				}
// 				req.res <- false
// 			}
// 		case <-s.readingClose:
// 			break
// 		}
// 	}
// }

func (s *server) readRoutine() {
	go s.mapRequestHandler()
	for {
		select {
		case <-s.readingClose:
			break
		default:
			var packet [2000]byte
			bytesRead, addr, _ := s.conn.ReadFromUDP(packet[0:])
			var data Message
			json.Unmarshal(packet[:bytesRead], &data)
			//check if its a connection message
			if data.Type == MsgConnect {
				new_client := client_info{
					curr_sn:        1,
					client_sn:      1,
					ack:            make(chan Message),
					addr:           addr,
					storedMessages: make(map[int]Message),
					closed:         make(chan bool),
					toWrite:        0,
					closeActivate:  false,
				}
				req := request{
					reqType:    AddClient,
					new_client: make(chan client_info),
				}
				req.new_client <- new_client
				s.reqChan <- req
			} else {
				//check if data message and need to send ack
				if data.Type == MsgData {
					//TODO: verify checksum
					ack, _ := json.Marshal(NewAck(data.ConnID, data.SeqNum))
					s.conn.WriteToUDP(ack, addr)
				}
				req := request{
					reqType:  AddMsg,
					clientID: data.ConnID,
					add_msg:  make(chan Message),
				}
				req.add_msg <- data
				s.reqChan <- req
			}
		}
	}
}

func (s *server) Read() (int, []byte, error) {
	//create request
	req := request{
		reqType: ReadReq,
	}
	s.reqChan <- req
	//wait for result
	msg := <-s.read_res
	return msg.ConnID, msg.Payload, nil
}

func (s *server) writeRoutine() {
	var needToClose bool = false
	for {
		select {
		case data := <-s.pendingMsgChan:
			s.pendingMsgs.PushBack(data)
			req := request{
				reqType:  AddPending,
				clientID: data.ConnID,
			}
			s.reqChan <- req
		case <-s.closeActivate:
			needToClose = true
		default:
			front := s.pendingMsgs.Front()
			if front == nil {
				if needToClose {
					s.conn.Close()
					//signal to stop reading
					close(s.readingClose)
					break
				} else {
					continue
				}
			}
			s.pendingMsgs.Remove(front)

			//get upd addr of client
			msg, _ := front.Value.(Message)
			req := request{
				reqType:  ClientAddr,
				clientID: msg.ConnID,
				addr_res: make(chan *lspnet.UDPAddr),
				ack_chan: make(chan chan Message),
			}
			s.reqChan <- req
			//wait for result
			addr := <-req.addr_res
			byte_msg, _ := json.Marshal(&(msg))
			s.conn.WriteToUDP(byte_msg, addr)
			remPending := request{
				reqType:  RemovePending,
				clientID: msg.ConnID,
			}
			s.reqChan <- remPending
			//wait for acknowledgement
			ack := <-req.ack_chan
			<-ack
		}
	}
}

func (s *server) Write(connId int, payload []byte) error {
	req := request{
		clientID: connId,
		sn_res:   make(chan int),
	}
	s.reqChan <- req
	sn := <-req.sn_res
	//TODO: fix checksum
	checksum := uint16(ByteArray2Checksum(payload))
	data := NewData(connId, sn, len(payload), payload, checksum)
	s.pendingMsgChan <- *data
	return nil
}

func (s *server) CloseConn(connId int) error {
	req := request{
		reqType:      CloseCxn,
		clientID:     connId,
		closeResChan: make(chan bool),
	}
	s.reqChan <- req
	hasError := <-req.closeResChan
	if hasError {
		return errors.New("Connection closed")
	} else {
		return nil
	}
}

func (s *server) Close() error {
	if s.closed {
		return errors.New("server already closed")
	}
	close(s.closeActivate)
	<-s.readingClose
	return nil
}
