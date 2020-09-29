// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"strconv"

	"github.com/cmu440/lspnet"
)

type server struct {
	clientMap      map[int]*client_info
	client_num     int
	conn           *lspnet.UDPConn
	read_res       chan Message
	pendingMsgs    chan Message
	pendingMsgChan chan Message
	closed         bool
	closeActivate  chan bool
	readingClose   chan bool
	reqChan        chan request
	needToClose    bool
}

type request struct {
	reqType      requestType
	clientID     int
	closeResChan chan bool
	addr_res     chan *lspnet.UDPAddr
	ack_chan     chan chan Message
	sn_res       chan int
	add_msg      Message
	new_client   *client_info
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
		clientMap:      make(map[int]*client_info),
		client_num:     1,
		conn:           udpconn,
		read_res:       make(chan Message),
		pendingMsgs:    make(chan Message),
		pendingMsgChan: make(chan Message),
		closed:         false,
		closeActivate:  make(chan bool),
		readingClose:   make(chan bool),
		reqChan:        make(chan request),
	}
	go new_server.writeRoutine()
	go new_server.readRoutine()
	return new_server, nil
}

func (s *server) mapRequestHandler() {
	var failedReqs int = 0
	for {
		select {
		case req := <-s.reqChan:
			switch req.reqType {
			case AddClient:
				client := req.new_client
				client.client_id = s.client_num
				s.client_num += 1
				//send acknowledgement to client
				ack, _ := json.Marshal(NewAck(client.client_id, 0))
				s.conn.WriteToUDP(ack, client.addr)
				s.clientMap[client.client_id] = client
			case AddMsg:
				client := s.clientMap[req.clientID]
				msg := req.add_msg
				switch msg.Type {
				case MsgAck:
					client.ack <- msg
				case MsgData:
					client.storedMessages[msg.SeqNum] = msg
				}
			case ReadReq:
				var cr *client_info
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
					// fmt.Printf("CLIENT SN: %d\n", cr.client_sn)
				} else {
					failedReqs += 1
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
		case msg, ok := <-s.pendingMsgs:
			if !ok {
				close(s.readingClose)
			}
			client := s.clientMap[msg.ConnID]
			go s.writeMsg(client.addr, msg, client.ack)
			client.toWrite -= 1
			if client.toWrite == 0 && client.closeActivate {
				delete(s.clientMap, msg.ConnID)
			}

		}
		// fmt.Printf("Failed reqs: %d\n", failedReqs)
		if failedReqs > 0 {
			var cr *client_info
			var found bool = false
			for _, v := range s.clientMap {
				// fmt.Println("Curr: " + strconv.Itoa(v.client_sn))
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
				cr.client_sn = cr.client_sn + 1
				// fmt.Printf("CLIENT SN: %d\n", cr.client_sn)
				s.read_res <- res
				failedReqs -= 1
			}
		}
	}
}

func (s *server) writeMsg(addr *lspnet.UDPAddr, msg Message, ack chan Message) {
	byte_msg, _ := json.Marshal(&(msg))
	s.conn.WriteToUDP(byte_msg, addr)
	<-ack
}

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
			// fmt.Printf("Unmarshalled: %+v\n", data)
			if data.Type == MsgConnect {
				nc := &client_info{
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
					new_client: nc,
				}
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
					add_msg:  data,
				}
				s.reqChan <- req
			}
		}
	}
}

func (s *server) Read() (int, []byte, error) {
	//create request
	// fmt.Println("called read")
	req := request{
		reqType: ReadReq,
	}
	s.reqChan <- req
	//wait for result
	msg := <-s.read_res
	return msg.ConnID, msg.Payload, nil
}

func (s *server) writeRoutine() {
	// var needToClose bool = false
	for {
		select {
		case data := <-s.pendingMsgChan:
			s.pendingMsgs <- data
			// fmt.Printf("Added data to queue: %+v\n", data)
			req := request{
				reqType:  AddPending,
				clientID: data.ConnID,
			}
			s.reqChan <- req
		case <-s.closeActivate:
			close(s.pendingMsgs)
			break
		}
	}
}

func (s *server) Write(connId int, payload []byte) error {
	// fmt.Println("called write")
	req := request{
		reqType:  ClientSN,
		clientID: connId,
		sn_res:   make(chan int),
	}
	s.reqChan <- req
	// fmt.Println("Probs stuck here")
	sn := <-req.sn_res
	//TODO: fix checksum
	checksum := uint16(ByteArray2Checksum(payload))
	data := NewData(connId, sn, len(payload), payload, checksum)
	// fmt.Println("before adding write msg")
	s.pendingMsgChan <- *data
	// fmt.Println("added write msg")
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
