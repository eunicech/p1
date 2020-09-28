// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"strconv"

	"github.com/cmu440/lspnet"
)

type server struct {
	clientMap map[string]client_info
	conn      *lspnet.UDPConn
}

type client_info struct {
	client_id      int
	curr_sn        int
	client_sn      int
	ack            chan Message
	addr           *lspnet.UDPAddr
	storedMessages map[int]Message
	curr_sn_req    chan int
	curr_sn_res    chan int
	client_sn_req  chan int
	client_sn_res  chan int
	closed         chan bool
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
		clientMap: make(map[string]client_info),
		conn:      udpconn,
	}
	go new_server.writeRoutine()
	go new_server.readRoutine()
	return new_server, nil
}

func (s *server) writeRoutine() {
}

func (s *server) mapRequestHandler() {

}

func (s *server) readRoutine() {
	go s.mapRequestHandler()
	for {
		var packet []byte = make([]byte, 0, 2000)
		bytesRead, _, _ := s.conn.ReadFromUDP(packet)
		var data Message
		json.Unmarshal(packet[:bytesRead], &data)

		//client_addr := addr.String()
		if data.Type == MsgAck {
			//TODO
		}

	}

}

func (s *server) Read() (int, []byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	select {} // Blocks indefinitely.
	return -1, nil, errors.New("not yet implemented")
}

func (s *server) Write(connId int, payload []byte) error {
	return errors.New("not yet implemented")
}

func (s *server) CloseConn(connId int) error {
	return errors.New("not yet implemented")
}

func (s *server) Close() error {
	return errors.New("not yet implemented")
}
