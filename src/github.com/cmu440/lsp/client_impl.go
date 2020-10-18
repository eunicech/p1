// Contains the implementation of a LSP client.

package lsp

import (
	"container/list"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/cmu440/lspnet"
)

type client struct {
	epochLimit      int
	windowSize      int
	maxUnackedMsgs  int
	maxBackOff      int
	epochSize       int
	epochMillis     int
	ticker          *time.Ticker
	clientID        int
	conn            *lspnet.UDPConn
	currSN          int      //keeps track of sequence numbers
	serverSN        int      //keeps track of packets recieved
	serverSeqNumRes chan int // get what packet # we are waiting for
	getServerSN     chan bool
	clientSeqNumRes chan int // get what packet # we are writing
	getClientSN     chan bool
	window          *slidingWindow
	acks            chan Message
	dataStorage     data
	closeActivate   chan bool
	closed          bool
	wroteInEpoch    bool
	writtenChan     chan bool
	signalEpoch     chan bool
	unreadEpochs    int
	readInEpoch     bool
	readChan        chan bool // check if we have read in the epoch
	lostCxn         chan bool // epochlimit hit without hearing from server
	willClose       chan bool
	closeInfoReq    chan bool
	infoReq         chan bool
}

// stores messages to be Read()
type data struct {
	readReqs    chan readReq
	pendingData map[int]Message
	addData     chan Message
	closed      chan bool // signals to all goroutines to shut down
}

// requests made by Read()
type readReq struct {
	dataSN  int
	dataRes chan Message
	errChan chan error
}

// type for sliding window of client
type slidingWindow struct {
	pendingMsgs    *list.List
	pendingMsgChan chan []byte
	start          int
	numUnAcked     int
}

// type of element in list of pending messages
type windowElem struct {
	sn      int
	ackChan chan Message
	ticker  *time.Ticker
	msg     *Message
	gotAck  bool
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
		numUnAcked:     0,
	}

	newClient := &client{
		epochLimit:      params.EpochLimit,
		windowSize:      params.WindowSize,
		maxBackOff:      params.MaxBackOffInterval,
		maxUnackedMsgs:  params.MaxUnackedMessages,
		epochMillis:     params.EpochMillis,
		writtenChan:     make(chan bool),
		epochSize:       params.EpochMillis,
		ticker:          time.NewTicker(time.Duration(params.EpochMillis * 1000000)),
		conn:            udp,
		currSN:          1,
		serverSN:        1,
		serverSeqNumRes: make(chan int),
		getServerSN:     make(chan bool),
		clientSeqNumRes: make(chan int),
		getClientSN:     make(chan bool),
		window:          window,
		acks:            make(chan Message),
		dataStorage:     dataStore,
		closeActivate:   make(chan bool),
		closed:          false,
		readChan:        make(chan bool),
		closeInfoReq:    make(chan bool),
		lostCxn:         make(chan bool),
		willClose:       make(chan bool),
		infoReq:         make(chan bool),
	}

	//send connection
	msg, err := json.Marshal(NewConnect())
	if err != nil {
		return nil, err
	}

	// handle ack from server
	ackChan := make(chan Message)
	var ackMsg Message
	go readConnection(ackChan, udp)
	udp.Write(msg)

	// use exponential backoff when trying to connect to server
	var epochsPassed int = 0
	var totalEpochsPassed int = 0
	var currentBackOff int = 0
	var flag bool = false
	ticker := time.NewTicker(time.Duration(params.EpochMillis * 1000000))
	for {
		select {
		case <-ticker.C:
			epochsPassed++
			totalEpochsPassed++

			// if exceed epochLimit when attempting to connect, give up
			if totalEpochsPassed > newClient.epochLimit {
				flag = true
				err = errors.New("timed out")
			}

			if epochsPassed > currentBackOff {
				udp.Write(msg)
				epochsPassed = 0
				if currentBackOff != params.MaxBackOffInterval {
					if currentBackOff == 0 {
						currentBackOff = 1
					} else {
						currentBackOff = currentBackOff * 2
						if currentBackOff > params.MaxBackOffInterval {
							currentBackOff = params.MaxBackOffInterval
						}
					}
				}
			}
		case ack := <-ackChan:
			ackMsg = ack
			flag = true
		}
		if flag {
			// an ack or error has been received
			break
		}

	}

	if err != nil {
		return nil, err
	}

	newClient.clientID = ackMsg.ConnID
	go newClient.clientInfoRequests()
	go newClient.writeRoutine()
	go newClient.readRoutine()
	go newClient.readRequestsRoutine()

	return newClient, nil
}

// readConnection tries to read an ack from the client when trying to connect
func readConnection(ackChan chan Message, udp *lspnet.UDPConn) {
	for {
		var ackMsg Message
		var ack [2000]byte
		bytesRead, _ := udp.Read(ack[0:])
		json.Unmarshal(ack[:bytesRead], &ackMsg)
		if ackMsg.Type == MsgAck && ackMsg.SeqNum == 0 {
			ackChan <- ackMsg
			break
		}
	}

}

// clientInfoRequests handles any changes made to the client
// (e.g. updating sequence number for the client, getting the time, etc)
func (c *client) clientInfoRequests() {
	for {
		select {
		case <-c.getServerSN:
			c.serverSeqNumRes <- c.serverSN
			c.serverSN++
		case <-c.getClientSN:
			c.clientSeqNumRes <- c.currSN
			c.currSN++
		case <-c.dataStorage.closed:
			return
		case <-c.closeInfoReq:
			// return when finished closing
			return
		case <-c.ticker.C:
			if !c.wroteInEpoch {
				//send heartbeat
				ackMsg := NewAck(c.clientID, 0)
				byteMsg, _ := json.Marshal(&ackMsg)
				c.conn.Write(byteMsg)
				c.wroteInEpoch = false
			}

			if !c.readInEpoch {
				// increment the number of epochs in which we haven't heard anything
				c.unreadEpochs++
				if c.unreadEpochs == c.epochLimit {
					// if hit epoch limit, we have lost the client
					close(c.lostCxn)
				}
			} else {
				c.readInEpoch = false
				c.unreadEpochs = 0
			}

		case <-c.writtenChan:
			c.wroteInEpoch = true
		case <-c.readChan:
			c.readInEpoch = true
		}
	}
}

func (c *client) ConnID() int {
	return c.clientID
}

// handle requests from Read()
// pulls data from the data storage and sends it to Read() to be returned
func (c *client) readRequestsRoutine() {
	var currReq int
	var currChan chan Message
	var errChan chan error
	var completedReq bool = true
	for {
		select {
		case req := <-c.dataStorage.readReqs:
			// assumes another read request from client can't come until this one is fulfilled
			currReq = req.dataSN
			currChan = req.dataRes
			errChan = req.errChan
			completedReq = false
		case packet := <-c.dataStorage.addData:
			// only add packet sequence number >= current sn of server;
			// otws, we have already read it
			if packet.SeqNum >= currReq {
				c.dataStorage.pendingData[packet.SeqNum] = packet
			}
		case <-c.dataStorage.closed:
			return
		case <-c.lostCxn:
			if !completedReq {
				errChan <- errors.New("lost cxn")
				close(c.closeInfoReq)
				return
			} else {
				close(c.closeInfoReq)
				return
			}
		}

		// process current request, if request is in pending data
		value, exists := c.dataStorage.pendingData[currReq]
		if exists {
			// remove the value
			delete(c.dataStorage.pendingData, currReq)
			// add value to channel
			currChan <- value
			completedReq = true
		}
	}
}

// reads all messages from the server and saves them in the data storage
func (c *client) readRoutine() {
	var dropData bool = false
	for {
		select {
		case <-c.lostCxn:
			return
		case <-c.dataStorage.closed:
			return
		case <-c.willClose:
			dropData = true
		default:
			//read message from server
			var packet [2000]byte
			bytesRead, err := c.conn.Read(packet[0:])

			// if EOF error, have lost the cxn + can't read anymore
			if err == io.EOF {
				close(c.lostCxn)
				break
			}

			var data Message
			json.Unmarshal(packet[:bytesRead], &data)

			// indicate that we have read in this epoch
			select {
			case c.readChan <- true:
			default:
			}

			//check if this an acknowledgement
			if data.Type == MsgAck {
				//ignore heartbeats
				if data.SeqNum != 0 {
					c.acks <- data
				}
			} else if !dropData {
				// dropData = true if Close has been called so we ignore if true
				// otws process requests

				// truncate data message if needed
				if data.Size < len(data.Payload) {
					data.Payload = data.Payload[:data.Size]
				}

				// check checksum to ensure data integrity
				//chksum := c.checksum(data.ConnID, data.SeqNum, data.Size, data.Payload)
				if data.Size > len(data.Payload) {
					continue
				}
				ack, _ := json.Marshal(NewAck(c.clientID, data.SeqNum))
				c.conn.Write(ack)

				// indicate that we have written in this epoch
				select {
				case c.writtenChan <- true:
				default:
				}

				c.dataStorage.addData <- data
			}

		}
	}

}

// write messages to server and do exponential backoff for msgs being written
func (c *client) writeMsg(msg Message, ackChan chan Message, ticker *time.Ticker) {
	byteMsg, _ := json.Marshal(&msg)
	var currentBackOff int = 0
	var epochsPassed int = 0
	c.conn.Write(byteMsg)

	// indicate that we have written in this epoch
	select {
	case c.writtenChan <- true:
	default:
	}

	for {
		select {
		case <-c.lostCxn:
			return
		case <-ackChan:
			return
		case <-ticker.C:
			// handle exponential backoff rules
			if epochsPassed == currentBackOff {
				c.conn.Write(byteMsg)
				select {
				case c.writtenChan <- true:
				default:
				}
				epochsPassed = 0
				if currentBackOff != c.maxBackOff {
					if currentBackOff == 0 {
						currentBackOff = 1
					} else {
						currentBackOff *= 2
						if currentBackOff > c.maxBackOff {
							currentBackOff = c.maxBackOff
						}
					}
				}

			} else {
				epochsPassed++
			}
		}
	}
}

// store pending messages that need to be written, since Write() should not block
func (c *client) writeRoutine() {
	var needToClose bool = false
	for {
		select {
		case <-c.lostCxn:
			return
		case <-c.closeActivate:
			// Close() has been called, but still need to finish writing pending msgs
			if !needToClose {
				c.willClose <- true
				needToClose = true
			}
			if c.window.pendingMsgs.Len() == 0 {
				close(c.dataStorage.closed)
				return
			}

		case payload := <-c.window.pendingMsgChan:
			// handles pending messages that come in from writeroutine
			c.getClientSN <- true
			sn := <-c.clientSeqNumRes
			chkSum := c.checksum(c.clientID, sn, len(payload), payload)
			dataMsg := NewData(c.clientID, sn, len(payload), payload, chkSum)
			ackChan := make(chan Message)
			newElem := &windowElem{
				sn:      sn,
				msg:     dataMsg,
				ackChan: ackChan,
				gotAck:  false,
			}

			c.window.pendingMsgs.PushBack(newElem)

			// write messages to server if within sliding window
			if c.window.pendingMsgs.Len() == 1 {
				c.window.start = newElem.sn
				newTicker := time.NewTicker(time.Duration(c.epochMillis * 1000000))
				newElem.ticker = newTicker
				go c.writeMsg(*dataMsg, ackChan, newTicker)
				c.window.numUnAcked++
			} else if c.window.pendingMsgs.Len() <= c.maxUnackedMsgs && newElem.sn < c.window.start+c.windowSize {
				newTicker := time.NewTicker(time.Duration(c.epochMillis * 1000000))
				newElem.ticker = newTicker
				go c.writeMsg(*dataMsg, ackChan, newTicker)
				c.window.numUnAcked++
			}

		case ack := <-c.acks:
			// put ack in corresponding channel
			sn := ack.SeqNum
			for elem := c.window.pendingMsgs.Front(); elem != nil; elem = elem.Next() {
				currElem := elem.Value.(*windowElem)
				if currElem.sn == sn {
					currElem.ackChan <- ack
					currElem.gotAck = true
				}
			}

			// remove messages that are acknowledged
			var count int = 0
			var startFlag bool = true
			var removedFromStart int = 0
			currElem := c.window.pendingMsgs.Front()
			for count < c.windowSize && currElem != nil {
				nextElem := currElem.Next()
				wElem := currElem.Value.(*windowElem)
				if wElem.sn >= c.window.start+c.windowSize {
					break
				}
				if wElem.gotAck {
					c.window.pendingMsgs.Remove(currElem)
					c.window.numUnAcked--
					if startFlag {
						removedFromStart++
					}
				} else {
					startFlag = false
				}
				count++
				currElem = nextElem
			}

			// update start of window
			if c.window.pendingMsgs.Front() != nil {
				c.window.start = c.window.pendingMsgs.Front().Value.(*windowElem).sn
			}

			// update window
			// write any new msgs now in sliding window bc ack has been received
			count = 0
			var numStarted int = 0
			currElem = c.window.pendingMsgs.Front()
			for count < c.windowSize && currElem != nil {
				wElem := currElem.Value.(*windowElem)
				if count >= c.window.numUnAcked {
					if c.window.numUnAcked+numStarted < c.maxUnackedMsgs {
						if wElem.sn >= c.window.start+c.windowSize {
							break
						}
						newTicker := time.NewTicker(time.Duration(c.epochMillis * 1000000))
						wElem.ticker = newTicker
						go c.writeMsg(*wElem.msg, wElem.ackChan, newTicker)
						numStarted++
					} else {
						break
					}
				}
				count++
			}

			// if need to close and finished writing all pending msgs, return
			if needToClose && c.window.pendingMsgs.Len() == 0 {
				close(c.dataStorage.closed)
				return
			}

			c.window.numUnAcked += numStarted
		}
	}
}

// checksum implements a checksum to verify data integrity
func (c *client) checksum(connID int, seqNum int, size int, payload []byte) uint16 {
	var sum uint32 = 0
	MaxUint := ^uint32(0)
	var half uint32 = MaxUint / 2
	sum += Int2Checksum(connID)
	if sum > half {
		sum = sum % half
		sum++
	}
	sum += Int2Checksum(seqNum)
	if sum > half {
		sum = sum % half
		sum++
	}
	sum += Int2Checksum(size)
	if sum > half {
		sum = sum % half
		sum++
	}
	sum += ByteArray2Checksum(payload)
	if sum > half {
		sum = sum % half
		sum++
	}
	res := uint16(sum)

	return ^res
}

func (c *client) Read() ([]byte, error) {
	// get number of data packet you need to read
	if c.closed {
		return nil, errors.New("already closed")
	}
	c.getServerSN <- true
	sn := <-c.serverSeqNumRes
	err := make(chan error)
	result := make(chan Message)
	// create read request
	request := readReq{
		dataSN:  sn,
		dataRes: result,
		errChan: err,
	}
	c.dataStorage.readReqs <- request
	select {
	case data := <-request.dataRes:
		return data.Payload, nil
	case err := <-request.errChan:
		return nil, err
	}
}

func (c *client) Write(payload []byte) error {
	c.window.pendingMsgChan <- payload
	return nil
}

func (c *client) Close() error {
	// check if client was already closed
	if c.closed {
		return errors.New("Client already closed")
	}
	c.closed = true

	close(c.closeActivate)

	select {
	case <-c.dataStorage.closed:
		c.conn.Close()
		return nil
	case <-c.closeInfoReq:
		c.conn.Close()
		return nil
	}
}
