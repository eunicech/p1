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

type readResult struct {
	msg []byte
	err error
	id  int
}

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

//Type of request we are making to server
const (
	AddClient requestType = iota
	AddMsg
	ReadReq
	CloseCxn
	WriteMessage
)

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
}

type serverSlidingWindow struct {
	pendingMsgs *list.List
	start       int
	numUnAcked  int
}

type writeElem struct {
	sn          int
	ackChan     chan Message
	signalEpoch chan bool
	msg         *Message
	gotAck      bool
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

	newServer := &server{
		epochLimit:     params.EpochLimit,
		windowSize:     params.WindowSize,
		maxBackOff:     params.MaxBackOffInterval,
		maxUnackedMsgs: params.MaxUnackedMessages,
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

	go newServer.readRoutine()
	return newServer, nil
}

func (s *server) mapRequestHandler() {
	var failedReqs int = 0
	for {
		// fmt.Println("Map Request handler")
		select {
		case req := <-s.reqChan:
			switch req.reqType {
			case AddClient:
				client := req.newClient
				client.clientID = s.clientNum
				s.clientNum++
				//send acknowledgement to client
				ack, _ := json.Marshal(NewAck(client.clientID, 0))
				s.conn.WriteToUDP(ack, client.addr)
				s.clientMap[client.clientID] = client
			case AddMsg:
				// fmt.Printf("Adding message %+v\n", req.addMsg)
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
					//only add if not a heartbeat
					if ack.SeqNum != 0 {
						//add acknowledgement
						for curr := client.slidingWin.pendingMsgs.Front(); curr != nil; curr = curr.Next() {
							currElem := curr.Value.(*writeElem)
							if currElem.sn == ack.SeqNum {
								currElem.ackChan <- *ack
								currElem.gotAck = true
							}
						}
						//remove messages that are acknowledged
						var count int = 0
						var startFlag bool = true
						var removedFromStart int = 0
						currElem := client.slidingWin.pendingMsgs.Front()
						for count < s.windowSize && currElem != nil {
							nextElem := currElem.Next()
							wElem := currElem.Value.(*writeElem)
							if wElem.sn >= client.slidingWin.start+s.windowSize {
								break
							}
							if wElem.gotAck {
								client.slidingWin.pendingMsgs.Remove(currElem)
								client.slidingWin.numUnAcked--
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
						client.slidingWin.start += removedFromStart

						//spawn new goroutines
						count = 0
						var numStarted int = 0
						currElem = client.slidingWin.pendingMsgs.Front()
						for count < s.windowSize && currElem != nil {
							wElem := currElem.Value.(*writeElem)
							if count >= client.slidingWin.numUnAcked {
								if client.slidingWin.numUnAcked+numStarted < s.maxUnackedMsgs {
									if wElem.sn >= client.slidingWin.start+s.windowSize {
										break
									}
									go s.writeMsg(client.addr, *wElem.msg, wElem.ackChan, wElem.signalEpoch, client.cxnClosed)
									numStarted++
								} else {
									break
								}
							}
							count++
						}
						client.slidingWin.numUnAcked += numStarted
					}

				case MsgData:
					if msg.SeqNum >= client.clientSN {
						client.storedMessages[msg.SeqNum] = *msg
					}
				}
			case ReadReq:
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
					//get error off list
					elem := s.clientErr.Front()
					res := elem.Value.(*readResult)
					s.clientErr.Remove(elem)
					s.readRes <- res
				} else {
					failedReqs++
				}

			case WriteMessage:
				//check if client exists
				client, ok := s.clientMap[req.clientID]
				if !ok || client.lost {
					s.writeRes <- errors.New("client does not exist")
					continue
				}
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
				newSignalEpoch := make(chan bool)
				newElem := &writeElem{
					sn:          sn,
					msg:         dataMsg,
					ackChan:     ackChan,
					gotAck:      false,
					signalEpoch: newSignalEpoch,
				}

				client.slidingWin.pendingMsgs.PushBack(newElem)
				if client.slidingWin.pendingMsgs.Len() <= s.maxUnackedMsgs && newElem.sn < client.slidingWin.start+s.windowSize {
					go s.writeMsg(client.addr, *dataMsg, ackChan, newSignalEpoch, client.cxnClosed)
					client.slidingWin.numUnAcked++
				}
			case CloseCxn:
				client, ok := s.clientMap[req.clientID]
				if !ok || client.lost {
					req.closeResChan <- true
				} else {
					close(client.cxnClosed)
					delete(s.clientMap, req.clientID)
					req.closeResChan <- false
				}
			}

		case <-s.readingClose:
			//TODO signal the close
			break
		case <-s.ticker.C:
			//start := time.Now()
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
						if v.unreadEpochs >= s.epochLimit {
							v.lost = true
							toDelete = append(toDelete, v.clientID)
						}
					}
					v.readInEpoch = false

					if !v.lost {
						//signal elements in sliding window
						slidingWindow := v.slidingWin
						count := 0
						for elem := slidingWindow.pendingMsgs.Front(); elem != nil && count < slidingWindow.start+s.maxUnackedMsgs; elem = elem.Next() {
							currElem := elem.Value.(*writeElem)
							if currElem.sn >= slidingWindow.start+s.windowSize {
								break
							}
							if !currElem.gotAck {
								go func() {
									currElem.signalEpoch <- false
								}()
							}
							count++
						}
					} else {
						//signal cxn as closed
						close(v.cxnClosed)
						//remove pending messages
						v.slidingWin.pendingMsgs.Init()
					}

				}

			}
			//find lost clients
			for v := range toDelete {
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
			// fmt.Println(time.Since(start))
		case clientNum := <-s.writtenChan:
			client := s.clientMap[clientNum]
			client.wroteInEpoch = true
		}
		if failedReqs > 0 {
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
				failedReqs--
			} else if s.clientErr.Front() != nil {
				//get error off list
				elem := s.clientErr.Front()
				res := elem.Value.(*readResult)
				s.clientErr.Remove(elem)
				s.readRes <- res
				failedReqs--
			}
		}
	}
}

func (s *server) writeMsg(addr *lspnet.UDPAddr, msg Message, ack chan Message, newSignalEpoch chan bool, cxnClosed chan bool) {
	byteMsg, _ := json.Marshal(&(msg))
	s.conn.WriteToUDP(byteMsg, addr)

	var flag = false
	var currentBackOff int = 0
	var epochsPassed int = 0
	for {
		select {
		case <-ack:
			flag = true
		case <-newSignalEpoch:
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
			flag = true
		}
		if flag {
			break
		}
	}
}

func (s *server) readRoutine() {
	go s.mapRequestHandler()
	for {
		select {
		case <-s.readingClose:
			break
		default:
			var packet [2000]byte
			bytesRead, addr, _ := s.conn.ReadFromUDP(packet[0:])

			var data Message
			json.Unmarshal(packet[:bytesRead], &data)

			//check if its a connection message
			// fmt.Printf("Unmarshalled: %+v\n", data)
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
				//check if data message and need to send ack
				if data.Type == MsgData {
					//verify checksum
					if data.Size < len(data.Payload) {
						data.Payload = data.Payload[:data.Size]
					}
					chksum := s.checksum(data.ConnID, data.SeqNum, data.Size, data.Payload)
					if data.Size > len(data.Payload) || chksum != data.Checksum {
						continue
					}
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
	// fmt.Println("Read called")
	//create request
	// fmt.Println("called read")
	req := request{
		reqType: ReadReq,
	}
	s.reqChan <- req
	//wait for result
	res := <-s.readRes
	return res.id, res.msg, res.err
}

func (s *server) Write(connID int, payload []byte) error {
	// fmt.Println("Write called")
	req := request{
		reqType:  WriteMessage,
		clientID: connID,
		data:     payload,
	}

	s.reqChan <- req
	// s.pendingMsgChan <- *data
	err := <-s.writeRes
	return err
}

func (s *server) CloseConn(connID int) error {
	req := request{
		reqType:      CloseCxn,
		clientID:     connID,
		closeResChan: make(chan bool),
	}
	s.reqChan <- req
	hasError := <-req.closeResChan
	if hasError {
		return errors.New("Connection closed")
	} else {
		return nil
	}
}

func (s *server) Close() error {
	if s.closed {
		return errors.New("server already closed")
	}
	close(s.closeActivate)
	//<-s.readingClose
	return nil
}
