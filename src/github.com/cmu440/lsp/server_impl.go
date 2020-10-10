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
	clientMap      map[int]*clientInfo
	clientNum      int
	conn           *lspnet.UDPConn
	readRes        chan Message
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
	closed         chan bool
	toWrite        int
	closeActivate  bool
	slidingWin     *serverSlidingWindow
}

type serverSlidingWindow struct {
	pendingMsgs *list.List
	start       int
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
		clientMap:      make(map[int]*clientInfo),
		clientNum:      1,
		conn:           udpconn,
		readRes:        make(chan Message),
		pendingMsgs:    make(chan Message),
		pendingMsgChan: make(chan Message),
		closed:         false,
		closeActivate:  make(chan bool),
		readingClose:   make(chan bool),
		reqChan:        make(chan request),
		hasPendingAck:  make(chan *Message),
		pendingAckList: list.New(),
		ticker:         time.NewTicker(time.Duration(1000000 * params.EpochMillis)),
	}

	go newServer.readRoutine()
	return newServer, nil
}

func (s *server) mapRequestHandler() {
	var failedReqs int = 0
	for {
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
				client := s.clientMap[req.clientID]
				msg := req.addMsg
				switch msg.Type {
				case MsgAck:
					ack := msg
					client := s.clientMap[ack.ConnID]
					for curr := client.slidingWin.pendingMsgs.Front(); curr != nil; curr = curr.Next() {
						currElem := curr.Value.(*writeElem)
						if currElem.sn == ack.SeqNum {
							currElem.ackChan <- *ack
							currElem.gotAck = true
						}
					}
					var keepRemoving bool = true
					currElem := client.slidingWin.pendingMsgs.Front()
					for keepRemoving && client.slidingWin.pendingMsgs.Len() > 0 {
						wElem := currElem.Value.(*writeElem)
						if wElem.gotAck {
							client.slidingWin.pendingMsgs.Remove(currElem)
							currElem = client.slidingWin.pendingMsgs.Front()
						} else {
							keepRemoving = false
						}
					}
					if client.slidingWin.pendingMsgs.Len() > 0 {
						newElem := client.slidingWin.pendingMsgs.Front().Value.(*writeElem)
						client.slidingWin.start = newElem.sn
						//spawn new goroutines
						go s.writeMsg(client.addr, *newElem.msg, newElem.ackChan, newElem.signalEpoch)
					}

				case MsgData:
					client.storedMessages[msg.SeqNum] = *msg
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
					s.readRes <- res
					// fmt.Printf("CLIENT SN: %d\n", cr.client_sn)
				} else {
					failedReqs++
				}
			case WriteMessage:
				// get the current client sn and update it accordingly
				client := s.clientMap[req.clientID]
				sn := client.currSN
				client.currSN++

				// create msg
				payload := req.data
				checksum := uint16(ByteArray2Checksum(payload))
				dataMsg := NewData(req.clientID, sn, len(payload), payload, checksum)

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
				if client.slidingWin.pendingMsgs.Len() == 1 {
					go s.writeMsg(client.addr, *dataMsg, ackChan, newSignalEpoch)
				}
			case CloseCxn:
				client, ok := s.clientMap[req.clientID]
				if !ok {
					req.closeResChan <- true
				} else {
					client.closeActivate = true
					client.toWrite--
					if client.toWrite == 0 && client.closeActivate {
						delete(s.clientMap, req.clientID)
					}
					req.closeResChan <- false
				}
			}

		case <-s.readingClose:
			//TODO signal the close
			break
		case <-s.ticker.C:
			for _, v := range s.clientMap {
				slidingWindow := v.slidingWin
				count := 0
				// for elem := slidingWindow.pendingMsgs.Front(); elem != nil && count < s.windowSize; elem = elem.Next() {
				for elem := slidingWindow.pendingMsgs.Front(); elem != nil && count < 1; elem = elem.Next() {
					currElem := elem.Value.(*writeElem)
					if !currElem.gotAck {
						currElem.signalEpoch <- true
					}
					count++
				}
			}
		}

		if failedReqs > 0 {
			var cr *clientInfo
			var found bool = false
			for _, v := range s.clientMap {
				// fmt.Println("Curr: " + strconv.Itoa(v.client_sn))
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
				cr.clientSN = cr.clientSN + 1
				// fmt.Printf("CLIENT SN: %d\n", cr.client_sn)
				s.readRes <- res
				failedReqs--
			}
		}
	}
}

func (s *server) writeMsg(addr *lspnet.UDPAddr, msg Message, ack chan Message, newSignalEpoch chan bool) {
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
				epochsPassed = 0
				if currentBackOff != s.maxBackOff {
					if currentBackOff == 0 {
						currentBackOff = 1
					} else {
						currentBackOff = currentBackOff * 2
						if currentBackOff >= s.maxBackOff {
							currentBackOff = s.maxBackOff
						}
					}
				}
			}
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
					closed:         make(chan bool),
					toWrite:        0,
					closeActivate:  false,
					slidingWin:     newWindow,
				}
				req := request{
					reqType:   AddClient,
					newClient: nc,
				}
				s.reqChan <- req
			} else {
				//check if data message and need to send ack
				if data.Type == MsgData {
					//TODO: verify checksum
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

func (s *server) Read() (int, []byte, error) {
	//create request
	// fmt.Println("called read")
	req := request{
		reqType: ReadReq,
	}
	s.reqChan <- req
	//wait for result
	msg := <-s.readRes
	return msg.ConnID, msg.Payload, nil
}

func (s *server) Write(connID int, payload []byte) error {
	req := request{
		reqType:  WriteMessage,
		clientID: connID,
		data:     payload,
	}

	s.reqChan <- req

	// s.pendingMsgChan <- *data

	return nil
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
	<-s.readingClose
	return nil
}
