// Contains the implementation of a LSP client.

package lsp

import (
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
	get_server_sn chan bool
	client_sn_res chan int // get what packet # we are writing
	get_client_sn chan int
	//pendingMsgs    *list.List
	pendingMsgChan chan *Message
	acks           chan Message
	dataStorage    data
	closeActivate  chan bool
	closed         bool
}

type data struct {
	read_reqs   chan readReq
	pendingData map[int]Message
	addData     chan Message
	closed      chan bool
}

type readReq struct {
	dataSN  int
	dataRes chan Message
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
	var ack_msg Message
	var ack [2000]byte
	bytesRead, err := udp.Read(ack[0:])
	if err != nil {
		return nil, err
	}
	json.Unmarshal(ack[:bytesRead], &ack_msg)
	if ack_msg.Type != MsgAck || ack_msg.SeqNum != 0 {
		return nil, errors.New("Not an acknowledgement")
	}

	dataStore := data{
		read_reqs:   make(chan readReq),
		pendingData: make(map[int]Message),
		addData:     make(chan Message),
		closed:      make(chan bool),
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
		get_server_sn: make(chan bool),
		client_sn_res: make(chan int),
		get_client_sn: make(chan int),
		//pendingMsgs:    list.New(),
		pendingMsgChan: make(chan *Message),
		acks:           make(chan Message),
		dataStorage:    dataStore,
		closeActivate:  make(chan bool),
		closed:         false,
	}
	go new_client.clientInfoRequests()
	go new_client.writeRoutine()
	go new_client.readRoutine()
	go new_client.readRequestsRoutine()
	return new_client, nil
}

func (c *client) clientInfoRequests() {
	for {
		select {
		case <-c.get_server_sn:
			c.server_sn_res <- c.serverSN
			c.serverSN += 1
		case add := <-c.get_client_sn:
			if add < 0 {
				//this is a request to get the currSN
				c.client_sn_res <- c.currSN
			} else {
				//this is request to add to currSN
				c.currSN += add
			}
		case <-c.closeActivate:
			c.closed = true
			break
		}
	}
}

func (c *client) ConnID() int {
	return c.clientID
}

func (c *client) readRequestsRoutine() {
	var curr_req int
	var curr_chan chan Message
	for {
		select {
		case req := <-c.dataStorage.read_reqs:
			//assumes another read request from client can't come until this one is fulfilled
			curr_req = req.dataSN
			curr_chan = req.dataRes
		case packet := <-c.dataStorage.addData:
			c.dataStorage.pendingData[packet.SeqNum] = packet
		case <-c.dataStorage.closed:
			break

		}
		value, exists := c.dataStorage.pendingData[curr_req]
		if exists {
			//remove the value
			delete(c.dataStorage.pendingData, curr_req)
			//add value to channel
			curr_chan <- value
		}
	}
}

func (c *client) readRoutine() {
	for {
		select {
		case <-c.dataStorage.closed:
			break
		default:
			//read message from server
			var packet [2000]byte
			bytesRead, _ := c.conn.Read(packet[0:])
			var data Message
			json.Unmarshal(packet[:bytesRead], &data)

			//check if this an acknowledgement
			if data.Type == MsgAck {
				//ignore heartbeats
				if data.SeqNum != 0 {
					c.acks <- data
				}
			} else {
				//data message
				ack, _ := json.Marshal(NewAck(c.clientID, data.SeqNum))
				c.conn.Write(ack)
				c.dataStorage.addData <- data
			}
			//TODO: check if it is a heartbeat message

		}
	}

}

func (c *client) writeMsg(msg Message) {
	byte_msg, _ := json.Marshal(&msg)
	c.conn.Write(byte_msg)
	//wait for acknowledgement
	<-c.acks
}

func (c *client) writeRoutine() {
	//var needToClose bool = false
	for {
		select {
		case <-c.closeActivate:
			close(c.dataStorage.closed)
			break
		case dataMsg := <-c.pendingMsgChan:
			go c.writeMsg(*dataMsg)
		}

	}
}

func (c *client) Read() ([]byte, error) {
	//get number of data packet you need to read
	c.get_server_sn <- true
	sn := <-c.server_sn_res
	result := make(chan Message)
	//create read request
	request := readReq{
		dataSN:  sn,
		dataRes: result,
	}
	c.dataStorage.read_reqs <- request
	//wait for data
	data := <-request.dataRes

	return data.Payload, nil
}

func (c *client) Write(payload []byte) error {
	c.get_client_sn <- -1
	sn := <-c.client_sn_res
	checksum := uint16(ByteArray2Checksum(payload))
	dataMsg := NewData(c.clientID, sn, len(payload), payload, checksum)
	c.pendingMsgChan <- dataMsg
	c.get_client_sn <- 1
	return nil
}

func (c *client) Close() error {
	//check if client was already closed
	if c.closed {
		return errors.New("Client already closed")
	}
	close(c.closeActivate)
	<-c.dataStorage.closed
	return nil
}
