// Contains the implementation of a LSP client.

package lsp

import (
	"container/list"
	"encoding/json"
	"errors"
	"fmt"

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
	clientID       int
	conn           *lspnet.UDPConn
	currSN         int      //keeps track of sequence numbers
	serverSN       int      //keeps track of packets recieved
	server_sn_res  chan int // get what packet # we are waiting for
	get_server_sn  chan int
	client_sn_res  chan int // get what packet # we are writing
	get_client_sn  chan int
	pendingMsgs    *list.List
	pendingMsgChan chan Message
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

//map
//put into map
//take out of map -> read req

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

	fmt.Println("going to write")
	udp.Write(msg)
	fmt.Println("finished write")

	// wait for acknowledgement
	var ack_msg Message
	for {
		var temp Message
		var ack []byte = make([]byte, 0, 2000)
		fmt.Println("going to read")
		bytesRead, err := udp.Read(ack)
		fmt.Println("finished read")
		if err != nil {
			return nil, err
		}
		json.Unmarshal(ack[:bytesRead], &temp)
		fmt.Printf("if %+v\n", temp)
		if temp.Type == MsgAck && temp.SeqNum == 0 {
			ack_msg = temp
			break
		}
	}

	fmt.Println(ack_msg)
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
		clientID:       ack_msg.ConnID,
		conn:           udp,
		currSN:         1,
		serverSN:       1,
		server_sn_res:  make(chan int),
		get_server_sn:  make(chan int),
		pendingMsgs:    list.New(),
		pendingMsgChan: make(chan Message),
		acks:           make(chan Message),
		dataStorage:    dataStore,
		closeActivate:  make(chan bool),
		closed:         false,
	}
	go new_client.clientInfoRequests()
	go new_client.writeRoutine()
	go new_client.readRoutine()
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
	go c.readRequestsRoutine()
	for {
		select {
		case <-c.dataStorage.closed:
			break
		default:
			//read message from server
			var packet []byte = make([]byte, 0, 2000)
			bytesRead, _ := c.conn.Read(packet)
			var data *Message = new(Message)
			json.Unmarshal(packet[:bytesRead], data)

			//check if this an acknowledgement
			if data.Type == MsgAck {
				c.acks <- *data
			}
			//TODO: check if it is a heartbeat message

			//data message
			ack, _ := json.Marshal(NewAck(c.clientID, data.SeqNum))
			c.conn.Write(ack)
			c.dataStorage.addData <- *data

		}
	}

}

func (c *client) writeRoutine() {
	var needToClose bool = false
	for {
		select {
		case <-c.closeActivate:
			needToClose = true
		case dataMsg := <-c.pendingMsgChan:
			c.pendingMsgs.PushBack(dataMsg)
		default:
			msg := c.pendingMsgs.Front()
			if msg == nil {
				if needToClose {
					c.conn.Close()
					//signal to stop reading
					close(c.dataStorage.closed)
					break
				} else {
					continue
				}
			}
			byte_msg, _ := json.Marshal(msg.Value)
			c.conn.Write(byte_msg)
			//wait for acknowledgement
			<-c.acks
		}

	}
}

func (c *client) Read() ([]byte, error) {
	//get number of data packet you need to read
	c.get_server_sn <- -1
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
	//increase server_sn
	c.get_server_sn <- 1

	return data.Payload, nil
}

func (c *client) Write(payload []byte) error {
	c.get_client_sn <- -1
	sn := <-c.client_sn_res
	checksum := uint16(ByteArray2Checksum(payload))
	dataMsg := NewData(c.clientID, sn, len(payload), payload, checksum)
	c.pendingMsgChan <- *dataMsg
	c.get_client_sn <- 1
	return nil
}

func (c *client) Close() error {
	//check if client was already closed
	if c.closed {
		return errors.New("Client already closed")
	}
	close(c.closeActivate)
	return nil
}
