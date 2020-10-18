// Contains the implementation of a LSP server.

package lsp

import (
	"container/list"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/cmu440/lspnet"
)

type server struct {
	epochLimit     int
	windowSize     int
	maxBackOff     int
	maxUnackedMsgs int
	epochMillis    int
	clientMap      map[int]*clientInfo
	clientNum      int
	conn           *lspnet.UDPConn
	readRes        chan *readResult
	writeRes       chan error
	pendingMsgs    chan Message
	pendingMsgChan chan Message
	closed         bool
	closeActivate  chan bool
	readingClose   chan bool
	reqChan        chan request
	needToClose    bool
	pendingAckList *list.List
	hasPendingAck  chan *Message
	ticker         *time.Ticker
	writtenChan    chan int
	clientErr      *list.List
}

// results of Read() request
type readResult struct {
	msg []byte
	err error
	id  int
}

// structure of requests to client map
type request struct {
	reqType      requestType
	clientID     int
	closeResChan chan bool
	data         []byte
	ackChan      chan chan Message
	snRes        chan int
	addMsg       *Message
	newClient    *clientInfo
}

type requestType int

// Type of request we are making to server
const (
	AddClient requestType = iota
	AddMsg
	ReadReq
	CloseCxn
	WriteMessage
)

// Struct to hold all the client information
type clientInfo struct {
	clientID       int
	currSN         int
	clientSN       int
	ack            chan Message
	addr           *lspnet.UDPAddr
	storedMessages map[int]Message
	serverClosed   chan bool
	cxnClosed      chan bool
	slidingWin     *serverSlidingWindow
	wroteInEpoch   bool
	readInEpoch    bool
	unreadEpochs   int
	lost           bool
	closeCxnFlag   bool
}

// Sliding window for each client
type serverSlidingWindow struct {
	pendingMsgs *list.List
	start       int
	numUnAcked  int
}

// Type for element added to sliding window list
type writeElem struct {
	sn      int
	ackChan chan Message
	ticker  *time.Ticker
	msg     *Message
	gotAck  bool
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

	// listen on port
	udpconn, err := lspnet.ListenUDP("udp", UDPAddr)
	if err != nil {
		return nil, err
	}

	newServer := &server{
		epochLimit:     params.EpochLimit,
		windowSize:     params.WindowSize,
		maxBackOff:     params.MaxBackOffInterval,
		maxUnackedMsgs: params.MaxUnackedMessages,
		epochMillis:    params.EpochMillis,
		clientMap:      make(map[int]*clientInfo),
		clientNum:      1,
		conn:           udpconn,
		readRes:        make(chan *readResult),
		writeRes:       make(chan error),
		pendingMsgs:    make(chan Message),
		pendingMsgChan: make(chan Message),
		closed:         false,
		closeActivate:  make(chan bool),
		readingClose:   make(chan bool),
		reqChan:        make(chan request),
		hasPendingAck:  make(chan *Message),
		pendingAckList: list.New(),
		ticker:         time.NewTicker(time.Duration(1000000 * params.EpochMillis)),
		writtenChan:    make(chan int),
		clientErr:      list.New(),
	}

	// start server routine
	go newServer.readRoutine()
	return newServer, nil
}

// Main Routine
func (s *server) mapRequestHandler() {
	var failedReqs int = 0
	var needToClose bool = false
	for {
		select {
		case req := <-s.reqChan:
			switch req.reqType {
			case AddClient:
				//Case: New Client to add to server
				client := req.newClient
				client.clientID = s.clientNum
				s.clientNum++

				//send acknowledgement of connection to client
				ack, _ := json.Marshal(NewAck(client.clientID, 0))
				s.conn.WriteToUDP(ack, client.addr)

				//add client into map
				s.clientMap[client.clientID] = client
			case AddMsg:
				// Case: Message sent to Client
				client, ok := s.clientMap[req.clientID]
				if !ok || client.lost {
					continue
				}
				client.readInEpoch = true
				client.unreadEpochs = 0
				msg := req.addMsg
				switch msg.Type {
				case MsgAck:
					ack := msg
					client := s.clientMap[ack.ConnID]
					//only add to client ack's channel if it isn't a heartbeat
					if ack.SeqNum != 0 {
						//add acknowledgement
						for curr := client.slidingWin.pendingMsgs.Front(); curr != nil; curr = curr.Next() {
							currElem := curr.Value.(*writeElem)
							if currElem.sn == ack.SeqNum {
								currElem.ackChan <- *ack
								currElem.gotAck = true
							}
						}
						//remove messages from window that are acknowledged
						var count int = 0
						currElem := client.slidingWin.pendingMsgs.Front()
						for count < s.windowSize && currElem != nil {
							nextElem := currElem.Next()
							wElem := currElem.Value.(*writeElem)
							//check if we are out of the window
							if wElem.sn >= client.slidingWin.start+s.windowSize {
								break
							}
							if wElem.gotAck {
								client.slidingWin.pendingMsgs.Remove(currElem)
								client.slidingWin.numUnAcked--
							}
							count++
							currElem = nextElem
						}

						//update the window for the client
						count = 0
						var numStarted int = 0
						currElem = client.slidingWin.pendingMsgs.Front()
						if currElem != nil {
							//pending message list is not empty
							//reset start of window
							client.slidingWin.start = currElem.Value.(*writeElem).sn

							for count < s.windowSize && currElem != nil {
								wElem := currElem.Value.(*writeElem)
								if count >= client.slidingWin.numUnAcked {
									//check if we are less than the max unacked messages allowed
									if client.slidingWin.numUnAcked+numStarted < s.maxUnackedMsgs {
										//check if we are out of the window
										if wElem.sn >= client.slidingWin.start+s.windowSize {
											break
										}

										//spawn goroutine to write message
										newTicker := time.NewTicker(time.Duration(s.epochMillis * 1000000))
										wElem.ticker = newTicker
										go s.writeMsg(client.addr, *wElem.msg, wElem.ackChan, newTicker, client.cxnClosed)
										numStarted++
									} else {
										//at max unacked messages
										break
									}
								}
								count++
							}
							//add to the number of unacked messages being written currently
							client.slidingWin.numUnAcked += numStarted
						} else {
							//pending messages list is empty
							if client.closeCxnFlag {
								//connection had been explicitly closed
								delete(s.clientMap, client.clientID)
							} else if needToClose {
								//server closed
								delete(s.clientMap, client.clientID)
								if len(s.clientMap) == 0 {
									close(s.readingClose)
									return
								}
							}
						}
					}

				case MsgData:
					if needToClose {
						//Server closed, ignore data messages
						continue
					}
					// only add packet sequence number >= current sn of client;
					// otws, we have already read it
					if msg.SeqNum >= client.clientSN {
						client.storedMessages[msg.SeqNum] = *msg
					}
				}
			case ReadReq:
				//Case: Read() of server was called

				//see if any clients have messages waiting to be read
				var cr *clientInfo
				var found bool = false
				for _, v := range s.clientMap {
					_, ok := v.storedMessages[v.clientSN]
					if ok {
						cr = v
						found = true
						break
					}
				}
				if found {
					res := cr.storedMessages[cr.clientSN]
					delete(cr.storedMessages, cr.clientSN)
					cr.clientSN++
					result := &readResult{
						msg: res.Payload,
						err: nil,
						id:  res.ConnID,
					}
					s.readRes <- result
				} else if s.clientErr.Front() != nil {
					//no stored messages from clients
					//send error from lost/closed connection
					elem := s.clientErr.Front()
					res := elem.Value.(*readResult)
					s.clientErr.Remove(elem)
					s.readRes <- res
				} else {
					//could not fulfill Read() request, save for later
					failedReqs++
				}

			case WriteMessage:
				//Case: Write() of server was called
				if needToClose {
					//server closed, ignore
					continue
				}
				//check if client exists
				client, ok := s.clientMap[req.clientID]
				if !ok || client.lost {
					s.writeRes <- errors.New("client does not exist")
					continue
				}
				//communicate that no error exists
				s.writeRes <- nil

				// get the current client sn and update it accordingly
				sn := client.currSN
				client.currSN++

				// create msg
				payload := req.data
				chksum := s.checksum(req.clientID, sn, len(payload), payload)
				dataMsg := NewData(req.clientID, sn, len(payload), payload, chksum)

				// add msg to pending msgs
				ackChan := make(chan Message)
				newElem := &writeElem{
					sn:      sn,
					msg:     dataMsg,
					ackChan: ackChan,
					gotAck:  false,
				}
				client.slidingWin.pendingMsgs.PushBack(newElem)

				//check if the message is in the sliding window
				if client.slidingWin.pendingMsgs.Len() == 1 {
					//only message in window, update start of window
					client.slidingWin.start = newElem.sn

					//spawn goroutine to write message
					newTicker := time.NewTicker(time.Duration(s.epochMillis * 1000000))
					newElem.ticker = newTicker
					go s.writeMsg(client.addr, *dataMsg, ackChan, newTicker, client.cxnClosed)
					client.slidingWin.numUnAcked++
				} else if client.slidingWin.pendingMsgs.Len() <= s.maxUnackedMsgs && newElem.sn < client.slidingWin.start+s.windowSize {
					//spawn goroutine to write message
					newTicker := time.NewTicker(time.Duration(s.epochMillis * 1000000))
					newElem.ticker = newTicker
					go s.writeMsg(client.addr, *dataMsg, ackChan, newTicker, client.cxnClosed)
					client.slidingWin.numUnAcked++
				}

			case CloseCxn:
				//Case: Connection explicity closed
				client, ok := s.clientMap[req.clientID]
				if !ok || client.lost {
					//client already closed
					//send error
					req.closeResChan <- true
				} else {
					//signal all goroutines associated with client to stop
					close(client.cxnClosed)
					//add error onto list
					err := &readResult{
						id:  req.clientID,
						err: errors.New("Client explicitly closed"),
					}
					s.clientErr.PushBack(err)
					if client.slidingWin.pendingMsgs.Len() == 0 {
						delete(s.clientMap, client.clientID)
					} else {
						client.storedMessages = make(map[int]Message)
						client.closeCxnFlag = true
					}
					//send that there was no error closing connection
					req.closeResChan <- false
				}
			}

		case <-s.closeActivate:
			//Case: server has been closed
			for k, v := range s.clientMap {
				//check if any clients can be deleted
				if v.slidingWin.pendingMsgs.Len() == 0 {
					delete(s.clientMap, k)
				}
			}
			// check if there are remaining clients to wait for
			if len(s.clientMap) == 0 {
				//close read routine
				close(s.readingClose)
				return

			}
			needToClose = true

		case <-s.ticker.C:
			//Epoch event
			var toDelete []int
			for k, v := range s.clientMap {
				//check if we need to send a heartbeat
				if !v.wroteInEpoch {
					ackMsg := NewAck(k, 0)
					byteMsg, _ := json.Marshal(&ackMsg)
					s.conn.WriteToUDP(byteMsg, v.addr)
				}
				v.wroteInEpoch = false
				//check if the connection is lost
				if v.lost {
					toDelete = append(toDelete, v.clientID)
				} else {
					if !v.readInEpoch {
						v.unreadEpochs++
						if v.unreadEpochs == s.epochLimit {
							v.lost = true
							toDelete = append(toDelete, v.clientID)
						}
					}
					v.readInEpoch = false

					if v.lost {
						//signal cxn as closed
						close(v.cxnClosed)
						//remove pending messages
						v.slidingWin.pendingMsgs.Init()
					}

				}

			}
			//find lost clients
			for _, v := range toDelete {
				//check if lost
				client, ok := s.clientMap[v]
				if ok && len(client.storedMessages) == 0 {
					err := &readResult{
						id:  client.clientID,
						err: errors.New("Lost a client"),
					}
					delete(s.clientMap, v)
					s.clientErr.PushBack(err)
				}

			}
		case clientNum := <-s.writtenChan:
			//Case: Client has written in the epoch
			client := s.clientMap[clientNum]
			client.wroteInEpoch = true
		}
		if failedReqs > 0 {
			//Remaining read requests
			//Check if there are any stored messages to read
			var cr *clientInfo
			var found bool = false
			for _, v := range s.clientMap {
				_, ok := v.storedMessages[v.clientSN]
				if ok {
					cr = v
					found = true
					break
				}
			}
			if found {
				res := cr.storedMessages[cr.clientSN]
				delete(cr.storedMessages, cr.clientSN)
				cr.clientSN++
				result := &readResult{
					msg: res.Payload,
					id:  res.ConnID,
					err: nil,
				}
				s.readRes <- result
				//decrement amount of failed requests
				failedReqs--
			} else if s.clientErr.Front() != nil {
				//errors on list, send back to Read()
				elem := s.clientErr.Front()
				res := elem.Value.(*readResult)
				s.clientErr.Remove(elem)
				s.readRes <- res
				//decrement amount of failed requests
				failedReqs--
			}

		}
	}
}

// write messages to client and do exponential backoff for msgs being written
func (s *server) writeMsg(addr *lspnet.UDPAddr, msg Message, ack chan Message, ticker *time.Ticker, cxnClosed chan bool) {
	//write message to client
	byteMsg, _ := json.Marshal(&(msg))
	s.conn.WriteToUDP(byteMsg, addr)

	var currentBackOff int = 0
	var epochsPassed int = 0
	for {
		select {
		case <-ack:
			return
		case <-ticker.C:
			epochsPassed++
			if epochsPassed > currentBackOff {
				s.conn.WriteToUDP(byteMsg, addr)
				s.writtenChan <- msg.ConnID

				epochsPassed = 0
				if currentBackOff < s.maxBackOff {
					if currentBackOff == 0 {
						currentBackOff = 1
					} else {
						currentBackOff *= 2
						if currentBackOff > s.maxBackOff {
							currentBackOff = s.maxBackOff
						}
					}
				}
			}
		case <-cxnClosed:
			//connection is lost
			return
		}
	}
}

// reads all messages from the server and saves them in the data storage
func (s *server) readRoutine() {
	go s.mapRequestHandler()
	for {
		select {
		case <-s.readingClose:
			return
		default:
			var packet [2000]byte
			bytesRead, addr, _ := s.conn.ReadFromUDP(packet[0:])

			var data Message
			json.Unmarshal(packet[:bytesRead], &data)

			//check if its a connection message
			if data.Type == MsgConnect {
				newWindow := &serverSlidingWindow{
					start:       1,
					pendingMsgs: list.New(),
				}
				nc := &clientInfo{
					currSN:         1,
					clientSN:       1,
					ack:            make(chan Message),
					addr:           addr,
					storedMessages: make(map[int]Message),
					slidingWin:     newWindow,
					lost:           false,
					serverClosed:   make(chan bool),
					cxnClosed:      make(chan bool),
				}
				req := request{
					reqType:   AddClient,
					newClient: nc,
				}
				s.reqChan <- req
			} else {
				// check if data message and need to send ack
				if data.Type == MsgData {
					// verify checksum
					if data.Size < len(data.Payload) {
						data.Payload = data.Payload[:data.Size]
					}
					chksum := s.checksum(data.ConnID, data.SeqNum, data.Size, data.Payload)
					if data.Size > len(data.Payload) || chksum != data.Checksum {
						continue
					}
					// send ack
					ack, _ := json.Marshal(NewAck(data.ConnID, data.SeqNum))
					s.conn.WriteToUDP(ack, addr)
				}
				req := request{
					reqType:  AddMsg,
					clientID: data.ConnID,
					addMsg:   &data,
				}
				s.reqChan <- req
			}
		}
	}
}

// checksum implements a checksum to verify data integrity
func (s *server) checksum(connID int, seqNum int, size int, payload []byte) uint16 {
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

func (s *server) Read() (int, []byte, error) {
	//create request
	req := request{
		reqType: ReadReq,
	}
	s.reqChan <- req

	//wait for result
	res := <-s.readRes
	return res.id, res.msg, res.err
}

func (s *server) Write(connID int, payload []byte) error {
	req := request{
		reqType:  WriteMessage,
		clientID: connID,
		data:     payload,
	}

	s.reqChan <- req
	err := <-s.writeRes
	return err
}

func (s *server) CloseConn(connID int) error {
	req := request{
		reqType:      CloseCxn,
		clientID:     connID,
		closeResChan: make(chan bool),
	}

	// indicate that client closed in err channel, if appropriate
	s.reqChan <- req
	hasError := <-req.closeResChan
	if hasError {
		return errors.New("Connection closed")
	}
	return nil
}

func (s *server) Close() error {
	// check if client was already closed
	if s.closed {
		return errors.New("server already closed")
	}

	s.closeActivate <- true
	<-s.readingClose
	s.conn.Close()

	return nil
}
