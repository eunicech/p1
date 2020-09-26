// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/cmu440/lspnet"
)

type client struct {
	//epoch          int
	epochLimit     int
	windowSize     int
	maxUnackedMsgs int
	backOff        int
	maxBackOff     int
	timer          *time.Ticker
	clientID       int
	conn           *lspnet.UDPConn
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

	return &client{
		clientID:       ack_msg.ConnID,
		conn:           udp,
		epochLimit:     params.EpochLimit,
		windowSize:     params.WindowSize,
		maxBackOff:     params.MaxBackOffInterval,
		maxUnackedMsgs: params.MaxUnackedMessages,
		timer:          time.NewTicker(time.Duration(1000000 * params.EpochMillis)),
	}, nil

}

func (c *client) ConnID() int {
	return c.clientID
}

func (c *client) Read() ([]byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	select {} // Blocks indefinitely.
	return nil, errors.New("not yet implemented")
}

func (c *client) Write(payload []byte) error {
	return errors.New("not yet implemented")
}

func (c *client) Close() error {
	return errors.New("not yet implemented")
}
