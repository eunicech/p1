// Contains the implementation of a LSP client.

package lsp

import (
	"container/list"
	"encoding/json"
	"errors"
	"time"

	"github.com/cmu440/lspnet"
)

type client struct {
	//epoch          int
	epochLimit int
	windowSize int
	//maxUnackedMsgs   int
	maxBackOff      int
	epochSize       int
	ticker          *time.Ticker
	timer           *time.Time
	clientID        int
	conn            *lspnet.UDPConn
	currSN          int      //keeps track of sequence numbers
	serverSN        int      //keeps track of packets recieved
	server_sn_res   chan int // get what packet # we are waiting for
	get_server_sn   chan bool
	client_sn_res   chan int // get what packet # we are writing
	get_client_sn   chan bool
	window          *slidingWindow
	acks            chan Message
	dataStorage     data
	closeActivate   chan bool
	closed          bool
	wroteInEpoch    bool
	writtenChan     chan bool
	signalEpoch     chan bool
	unwrittenEpochs int
	readInEpoch     bool
}

type data struct {
	readReqs    chan readReq
	pendingData map[int]Message
	addData     chan Message
	closed      chan bool
}

type readReq struct {
	dataSN  int
	dataRes chan Message
}

type slidingWindow struct {
	pendingMsgs    *list.List
	pendingMsgChan chan []byte
	start          int
}

type windowElem struct {
	sn          int
	ackChan     chan Message
	signalEpoch chan bool
	msg         *Message
	gotAck      bool
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

	dataStore := data{
		readReqs:    make(chan readReq),
		pendingData: make(map[int]Message),
		addData:     make(chan Message),
		closed:      make(chan bool),
	}
	window := &slidingWindow{
		start:          1,
		pendingMsgs:    list.New(),
		pendingMsgChan: make(chan []byte),
	}
	newClient := &client{
		epochLimit: params.EpochLimit,
		windowSize: params.WindowSize,
		maxBackOff: params.MaxBackOffInterval,
		//maxUnackedMsgs: params.MaxUnackedMessages,
		writtenChan: make(chan bool),
		epochSize:   params.EpochMillis,
		ticker:      time.NewTicker(time.Duration(1000000 * params.EpochMillis)),
		//clientID:      ack_msg.ConnID,
		conn:          udp,
		currSN:        1,
		serverSN:      1,
		server_sn_res: make(chan int),
		get_server_sn: make(chan bool),
		client_sn_res: make(chan int),
		get_client_sn: make(chan bool),
		window:        window,
		acks:          make(chan Message),
		dataStorage:   dataStore,
		closeActivate: make(chan bool),
		closed:        false,
		signalEpoch:   make(chan bool),
	}

	var epochsPassed int = 0
	var currentBackOff int = 0
	//send connection
	msg, err := json.Marshal(NewConnect())
	if err != nil {
		return nil, err
	}
	udp.Write(msg)

	ack_chan := make(chan Message)
	err_chan := make(chan error)
	var ack_msg Message
	go readConnection(ack_chan, udp, err_chan)

	var flag bool = false
	for {
		select {
		case <-newClient.ticker.C:
			epochsPassed += 1
			if epochsPassed > currentBackOff {
				udp.Write(msg)
				epochsPassed = 0
				if currentBackOff == 0 {
					currentBackOff = 1
				} else {
					currentBackOff = currentBackOff * 2
				}
			}
		case ack := <-ack_chan:
			ack_msg = ack
			flag = true
			break
		case newErr := <-err_chan:
			err = newErr
			flag = true
			break
		}
		if flag {
			break
		}

	}
	if err != nil {
		return nil, err
	}
	newClient.clientID = ack_msg.ConnID
	go newClient.clientInfoRequests()
	go newClient.writeRoutine()
	go newClient.readRoutine()
	go newClient.readRequestsRoutine()
	return newClient, nil
}

func readConnection(ack_chan chan Message, udp *lspnet.UDPConn, err_chan chan error) {
	for {
		var ack_msg Message
		var ack [2000]byte
		bytesRead, err := udp.Read(ack[0:])
		if err != nil {
			err_chan <- err
			break
		}
		json.Unmarshal(ack[:bytesRead], &ack_msg)
		if ack_msg.Type == MsgAck && ack_msg.SeqNum == 0 {
			ack_chan <- ack_msg
			break
		}
	}

}

func (c *client) clientInfoRequests() {
	for {
		select {
		case <-c.get_server_sn:
			c.server_sn_res <- c.serverSN
			c.serverSN += 1
		case <-c.get_client_sn:
			c.client_sn_res <- c.currSN
			c.currSN += 1
		case <-c.closeActivate:
			c.closed = true
			break
		case <-c.ticker.C:
			if !c.wroteInEpoch {
				//send heartbeat
				ack_msg := NewAck(c.clientID, 0)
				byte_msg, _ := json.Marshal(&ack_msg)
				c.conn.Write(byte_msg)
				c.wroteInEpoch = false
				c.signalEpoch <- true
			}
		case <-c.writtenChan:
			c.wroteInEpoch = true
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
		case req := <-c.dataStorage.readReqs:
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
				select {
				case c.writtenChan <- true:
				default:
				}
				c.dataStorage.addData <- data
			}
			//TODO: check if it is a heartbeat message

		}
	}

}

func (c *client) writeMsg(msg Message, ackChan chan Message, sigEpoch chan bool) {
	byte_msg, _ := json.Marshal(&msg)
	var currentBackOff int = 0
	var epochsPassed int = 0
	c.conn.Write(byte_msg)
	select {
	case c.writtenChan <- true:
	default:
	}
	for {
		select {
		case <-ackChan:
			break
		case <-sigEpoch:
			epochsPassed += 1
			if epochsPassed > currentBackOff {
				c.conn.Write(byte_msg)
				select {
				case c.writtenChan <- true:
				default:
				}
				epochsPassed = 0
				if currentBackOff == 0 {
					currentBackOff = 1
				} else {
					currentBackOff = currentBackOff * 2
				}
			}
		}
	}
}

func (c *client) writeRoutine() {
	//var needToClose bool = false
	for {
		select {
		case <-c.closeActivate:
			close(c.dataStorage.closed)
			break
		case payload := <-c.window.pendingMsgChan:
			c.get_client_sn <- true
			sn := <-c.client_sn_res
			checksum := uint16(ByteArray2Checksum(payload))
			dataMsg := NewData(c.clientID, sn, len(payload), payload, checksum)
			ackChan := make(chan Message)
			newSignalEpoch := make(chan bool)
			newElem := &windowElem{
				sn:          sn,
				msg:         dataMsg,
				ackChan:     ackChan,
				gotAck:      false,
				signalEpoch: newSignalEpoch,
			}
			c.window.pendingMsgs.PushBack(newElem)
			if c.window.pendingMsgs.Len() == 1 {
				go c.writeMsg(*dataMsg, ackChan, newSignalEpoch)
			}
		case <-c.signalEpoch:
			//signal epochs to all messages in window
			count := 0
			// for elem := c.window.pendingMsgs.Front(); elem != nil && count < c.windowSize; elem = elem.Next() {
			for elem := c.window.pendingMsgs.Front(); elem != nil && count < 1; elem = elem.Next() {
				currElem := elem.Value.(*windowElem)
				if !currElem.gotAck {
					currElem.signalEpoch <- true
				}
				count++
			}
		case ack := <-c.acks:
			//put ack in corresponding channel
			sn := ack.SeqNum
			for elem := c.window.pendingMsgs.Front(); elem != nil; elem = elem.Next() {
				currElem := elem.Value.(*windowElem)
				if currElem.sn == sn {
					currElem.ackChan <- ack
					currElem.gotAck = true
				}
			}
			//remove messages that are good
			var keepRemoving bool = true
			currElem := c.window.pendingMsgs.Front()
			for keepRemoving && c.window.pendingMsgs.Len() > 0 {
				wElem := currElem.Value.(*windowElem)
				if wElem.gotAck {
					c.window.pendingMsgs.Remove(currElem)
					currElem = c.window.pendingMsgs.Front()
				} else {
					keepRemoving = false
				}
			}

			// TODO: sliding window only one rn
			//update window
			if c.window.pendingMsgs.Len() > 0 {
				newElem := c.window.pendingMsgs.Front().Value.(*windowElem)
				c.window.start = newElem.sn
				//spawn new goroutines
				go c.writeMsg(*newElem.msg, newElem.ackChan, newElem.signalEpoch)
			}
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
	c.dataStorage.readReqs <- request
	//wait for data
	data := <-request.dataRes

	return data.Payload, nil
}

func (c *client) Write(payload []byte) error {
	c.window.pendingMsgChan <- payload
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
