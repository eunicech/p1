// Contains the implementation of a LSP client.

package lsp

import (
	"container/list"
	"encoding/json"
	"errors"

	"github.com/cmu440/lspnet"
)

type client struct {
	//epoch          int
	//epochLimit       int
	//windowSize       int
	//maxUnackedMsgs   int
	//backOff          int
	//maxBackOff       int
	//timer            *time.Ticker
	clientID      int
	conn          *lspnet.UDPConn
	currSN        int      //keeps track of sequence numbers
	serverSN      int      //keeps track of packets recieved
	server_sn_res chan int // get what packet # we are waiting for
	get_server_sn chan int
	pendingMsgs   *list.List
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {
	udpAddr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}
	udp, err := lspnet.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return nil, err
	}

	//send connection
	msg, err := json.Marshal(NewConnect())
	if err != nil {
		return nil, err
	}
	udp.Write(msg)

	// wait for acknowledgement
	var ack []byte
	bytesRead, err := udp.Read(ack)
	if err != nil {
		return nil, err
	}
	var ack_msg Message
	json.Unmarshal(ack[:bytesRead], &ack_msg)
	if ack_msg.Type != MsgAck || ack_msg.SeqNum != 0 {
		return nil, errors.New("Not an acknowledgement to connection")
	}
	new_client := &client{
		//epochLimit:     params.EpochLimit,
		//windowSize:     params.WindowSize,
		//maxBackOff:     params.MaxBackOffInterval,
		//maxUnackedMsgs: params.MaxUnackedMessages,
		//timer:          time.NewTicker(time.Duration(1000000 * params.EpochMillis)),
		clientID:      ack_msg.ConnID,
		conn:          udp,
		currSN:        1,
		serverSN:      1,
		server_sn_res: make(chan int),
		get_server_sn: make(chan int),
		pendingMsgs:   list.New(),
	}
	go new_client.clientInfoRequests()
	return new_client, nil
}

func (c *client) clientInfoRequests() {
	for {
		select {
		case add := <-c.get_server_sn:
			if add < 0 {
				//this is a request to get the serverSN
				c.server_sn_res <- c.serverSN
			} else {
				//this is request to add to serverSN
				c.serverSN += add
			}
		}
	}
}

func (c *client) ConnID() int {
	return c.clientID
}

func (c *client) Read() ([]byte, error) {
	//get number of data packet you need to read
	c.get_server_sn <- -1
	// sn := <-c.server_sn_res
	<-c.server_sn_res

	//TODO: implement order with linked list and checking sn

	//read message from server
	var packet []byte
	bytesRead, _ := c.conn.Read(packet)
	var data Message
	json.Unmarshal(packet[:bytesRead], &data)

	//send acknowledgement
	ack := json.Marshal(NewAck(c.ConnID, data.SeqNum))
	c.conn.Write(ack)

	//increase server_sn
	c.get_server_sn <- 1

	return data.Payload, nil
}

func (c *client) Write(payload []byte) error {
	return errors.New("not yet implemented")
}

func (c *client) Close() error {
	return errors.New("not yet implemented")
}
