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
	clientMap      map[int]client_info
	client_num     int
	client_num_req chan int
	conn           *lspnet.UDPConn
	add_msg        chan Message
	new_client     chan client_info
	read_req       chan bool
	read_res       chan Message
	pendingMsgs    *list.List
	pendingMsgChan chan Message
	client_sn_req  chan write_req
}

type write_req struct {
	clientID int
	sn_res   chan int
}

type client_info struct {
	client_id      int
	curr_sn        int
	client_sn      int
	ack            chan Message
	addr           *lspnet.UDPAddr
	storedMessages map[int]Message
	// curr_sn_req    chan int
	// curr_sn_res    chan int
	// client_sn_req  chan int
	// client_sn_res  chan int
	closed chan bool
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
		clientMap:     make(map[int]client_info),
		conn:          udpconn,
		add_msg:       make(chan Message),
		read_req:      make(chan bool),
		read_res:      make(chan Message),
		pendingMsgs:   list.New(),
		client_sn_req: make(chan write_req),
	}
	go new_server.writeRoutine()
	go new_server.readRoutine()
	return new_server, nil
}

func (s *server) mapRequestHandler() {
	for {
		select {
		case msg := <-s.add_msg:
			client := s.clientMap[msg.ConnID]
			switch msg.Type {
			case MsgAck:
				client.ack <- msg
			case MsgData:
				client.storedMessages[msg.SeqNum] = msg
			}
		case new_client := <-s.new_client:
			new_client.client_id = s.client_num
			s.client_num += s.client_num
			//send acknowledgement to client
			ack, _ := json.Marshal(NewAck(new_client.client_id, 0))
			s.conn.WriteToUDP(ack, new_client.addr)
		case <-s.read_req:
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
		case req := <-s.client_sn_req:
			client := s.clientMap[req.clientID]
			sn := client.curr_sn
			client.curr_sn += 1
			req.sn_res <- sn
		}
	}
}

func (s *server) readRoutine() {
	go s.mapRequestHandler()
	for {
		var packet []byte = make([]byte, 0, 2000)
		bytesRead, addr, _ := s.conn.ReadFromUDP(packet)
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
				// curr_sn_req:    make(chan int),
				// curr_sn_res:    make(chan int),
				// client_sn_res:  make(chan int),
				closed: make(chan bool),
			}
			s.new_client <- new_client
		} else {
			//check if data message and need to send ack
			if data.Type == MsgData {
				//TODO: verify checksum
				ack, _ := json.Marshal(NewAck(data.ConnID, data.SeqNum))
				s.conn.WriteToUDP(ack, addr)
			}

			s.add_msg <- data

		}

	}

}

func (s *server) Read() (int, []byte, error) {
	//create request
	s.read_req <- true
	//wait for result
	msg := <-s.read_res
	return msg.ConnID, msg.Payload, nil
}

func (s *server) writeRoutine() {
	for {
		select {
		case data := <-s.pendingMsgChan:
			s.pendingMsgs.PushBack(data)
		}
	}
}

func (s *server) Write(connId int, payload []byte) error {
	res := make(chan int)
	req := write_req{
		clientID: connId,
		sn_res:   res,
	}
	s.client_sn_req <- req
	sn := <-req.sn_res
	//TODO: fix checksum
	checksum := uint16(ByteArray2Checksum(payload))
	data := NewData(connId, sn, len(payload), payload, checksum)
	s.pendingMsgChan <- *data
	return nil
}

func (s *server) CloseConn(connId int) error {
	return errors.New("not yet implemented")
}

func (s *server) Close() error {
	return errors.New("not yet implemented")
}
