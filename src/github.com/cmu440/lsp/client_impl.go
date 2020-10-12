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
	epochLimit     int
	windowSize     int
	maxUnackedMsgs int
	maxBackOff     int
	epochSize      int
	ticker         *time.Ticker
	// timer           *time.Time
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
	readChan        chan bool
	lostCxn         chan bool
	willClose       chan bool
	//closeInfoReq    chan bool
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
	numUnAcked     int
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
		numUnAcked:     0,
	}

	newClient := &client{
		epochLimit:      params.EpochLimit,
		windowSize:      params.WindowSize,
		maxBackOff:      params.MaxBackOffInterval,
		maxUnackedMsgs:  params.MaxUnackedMessages,
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
		signalEpoch:     make(chan bool),
		readChan:        make(chan bool),
		//closeInfoReq:    make(chan bool),
		lostCxn:   make(chan bool),
		willClose: make(chan bool),
	}

	var epochsPassed int = 0
	var totalEpochsPassed int = 0
	var currentBackOff int = 0

	//send connection
	msg, err := json.Marshal(NewConnect())
	if err != nil {
		return nil, err
	}
	ackChan := make(chan Message)
	var ackMsg Message
	go readConnection(ackChan, udp)
	udp.Write(msg)
	var flag bool = false
	ticker := time.NewTicker(time.Duration(params.EpochMillis * 1000000))
	for {
		select {
		case <-ticker.C:
			epochsPassed++
			totalEpochsPassed++
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
			break
		}

	}
	if err != nil {
		return nil, err
	}
	newClient.clientID = ackMsg.ConnID
	// fmt.Printf("epoch limit: %d,backoff: %d\n", newClient.epochLimit, newClient.maxBackOff)
	go newClient.clientInfoRequests()
	go newClient.writeRoutine()
	go newClient.readRoutine()
	go newClient.readRequestsRoutine()
	return newClient, nil
}

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

func (c *client) clientInfoRequests() {
	var flag bool = false
	for {
		select {
		case <-c.getServerSN:
			c.serverSeqNumRes <- c.serverSN
			c.serverSN++
		case <-c.getClientSN:
			c.clientSeqNumRes <- c.currSN
			c.currSN++
		// case <-c.closeActivate:
		// 	c.closed = true
		// 	break
		case <-c.dataStorage.closed:
			flag = true
			break
		// case <-c.dataStorage.closed:
		// 	stopSignallingEpochs = true
		case <-c.ticker.C:

			c.signalEpoch <- true
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
				if c.unreadEpochs >= c.epochLimit {
					//c.conn.Close()
					// TODO: drop the client/ connection
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
			//c.unreadEpochs = 0
		}
		if flag {
			break
		}
	}
}

func (c *client) ConnID() int {
	return c.clientID
}

func (c *client) readRequestsRoutine() {
	var currReq int
	var currChan chan Message
	var flag bool = false
	for {
		select {
		case req := <-c.dataStorage.readReqs:
			//assumes another read request from client can't come until this one is fulfilled
			currReq = req.dataSN
			currChan = req.dataRes
		case packet := <-c.dataStorage.addData:
			//TODO: only add if it is >= current sn of server
			if packet.SeqNum >= currReq {
				c.dataStorage.pendingData[packet.SeqNum] = packet
			}
		case <-c.dataStorage.closed:
			// if len(c.dataStorage.pendingData) == 0 {
			// 	flag = true
			// }
			flag = true
			break
		}
		if flag {
			//close(c.closeInfoReq)
			break
		}
		value, exists := c.dataStorage.pendingData[currReq]
		if exists {
			//remove the value
			delete(c.dataStorage.pendingData, currReq)
			//add value to channel
			currChan <- value
		}
	}
}

func (c *client) readRoutine() {
	var flag bool = false
	var dropData bool = false
	for {
		select {
		case <-c.dataStorage.closed:
			flag = true
			break
		case <-c.willClose:
			dropData = true
		default:
			//read message from server
			var packet [2000]byte
			bytesRead, _ := c.conn.Read(packet[0:])
			var data Message
			json.Unmarshal(packet[:bytesRead], &data)
			select {
			case c.readChan <- true:
			default:
			}

			// c.readChan <- true
			//check if this an acknowledgement
			if data.Type == MsgAck {
				//ignore heartbeats
				if data.SeqNum != 0 {
					c.acks <- data
				}
			} else if !dropData {
				//data message
				if data.Size < len(data.Payload) {
					data.Payload = data.Payload[:data.Size]
				}
				chksum := c.checksum(data.ConnID, data.SeqNum, data.Size, data.Payload)
				if data.Size > len(data.Payload) || chksum != data.Checksum {
					continue
				}
				ack, _ := json.Marshal(NewAck(c.clientID, data.SeqNum))
				c.conn.Write(ack)
				select {
				case c.writtenChan <- true:
				default:
				}
				c.dataStorage.addData <- data
			}

		}
		if flag {
			break
		}
	}

}

func (c *client) writeMsg(msg Message, ackChan chan Message, sigEpoch chan bool) {
	byteMsg, _ := json.Marshal(&msg)
	var currentBackOff int = 0
	var epochsPassed int = 0
	//var totalEpochs int = 0
	var flag bool = false
	c.conn.Write(byteMsg)
	select {
	case c.writtenChan <- true:
	default:
	}
	for {
		select {
		case <-c.lostCxn:
			//fmt.Println("Lost connection")
			flag = true
			break
		case <-ackChan:
			flag = true
			break
		case <-sigEpoch:
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
		if flag {
			break
		}
	}
}

func (c *client) writeRoutine() {
	var needToClose bool = false
	var flag bool = false
	for {
		select {
		case <-c.lostCxn:
			close(c.dataStorage.closed)
			flag = true

		case <-c.closeActivate:
			// if !needToClose {
			// c.dataStorage.willClose <- true
			c.willClose <- true
			needToClose = true
			//fmt.Printf("pending: %d\n", c.window.pendingMsgs.Len())
			if c.window.pendingMsgs.Len() == 0 {
				close(c.dataStorage.closed)
				flag = true
			}

			// c.closed = true
			// }
			// break
		case payload := <-c.window.pendingMsgChan:
			c.getClientSN <- true
			sn := <-c.clientSeqNumRes
			chkSum := c.checksum(c.clientID, sn, len(payload), payload)
			dataMsg := NewData(c.clientID, sn, len(payload), payload, chkSum)
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
			if c.window.pendingMsgs.Len() <= c.maxUnackedMsgs && newElem.sn < c.window.start+c.windowSize {
				go c.writeMsg(*dataMsg, ackChan, newSignalEpoch)
				c.window.numUnAcked++
			}

		case <-c.signalEpoch:
			//signal epochs to all messages in window
			count := 0
			for elem := c.window.pendingMsgs.Front(); elem != nil && count < c.window.start+c.maxUnackedMsgs; elem = elem.Next() {
				currElem := elem.Value.(*windowElem)
				if currElem.sn >= c.window.start+c.windowSize {
					break
				}
				if !currElem.gotAck {
					go func() {
						select {
						case currElem.signalEpoch <- true:
						default:
						}
					}()
				}
				count++
			}
		case ack := <-c.acks:
			//fmt.Printf("Got ack: %d\n", ack.SeqNum)
			//put ack in corresponding channel
			sn := ack.SeqNum
			for elem := c.window.pendingMsgs.Front(); elem != nil; elem = elem.Next() {
				currElem := elem.Value.(*windowElem)
				if currElem.sn == sn {
					currElem.ackChan <- ack
					currElem.gotAck = true
				}
			}

			//remove messages that are acknowledged
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
			//update start of window
			c.window.start += removedFromStart

			//update window
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
						go c.writeMsg(*wElem.msg, wElem.ackChan, wElem.signalEpoch)
						numStarted++
					} else {
						break
					}
				}
				count++
			}
			if needToClose && c.window.pendingMsgs.Len() == 0 {
				flag = true
				close(c.dataStorage.closed)
				break
			}
			c.window.numUnAcked += numStarted
		}

		if flag {
			break
		}

	}
}

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
	//get number of data packet you need to read
	c.getServerSN <- true
	sn := <-c.serverSeqNumRes
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
	c.closed = true
	//fmt.Println("Close called")

	close(c.closeActivate)
	<-c.dataStorage.closed
	return nil
}
