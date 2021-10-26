package impl

import (
	"encoding/json"
	"errors"
	"log"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

// NewPeer creates a new peer. You can change the content and location of this
// function but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {
	n := node{}
	n.conf = conf
	n.routingTableSync.routingTable = make(map[string]string)
	n.statusMapSync.statusMap = map[string]uint{}
	n.rumorsLogSync.rumorsLog = map[string][]types.Rumor{}
	n.ackReceivedChan = make(chan bool)
	n.addr = n.conf.Socket.GetAddress()
	//init routingTable with node's addr
	n.SetRoutingEntry(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress())
	//init register callback for messages
	n.conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, n.ExecChatMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.RumorsMessage{}, n.ExecRumorsMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.StatusMessage{}, n.ExecStatusMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.AckMessage{}, n.ExecAckMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.PrivateMessage{}, n.ExecPrivateMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.EmptyMessage{}, n.ExecEmptyMessage)
	//Periodically send status messages to neighbors
	n.antiEntropy()
	//Periodically update routing table of all peers
	n.heartbeat()
	n.seq = 1

	return &n
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer
	conf peer.Configuration
	addr string
	//channel to know when goroutine should stop listening
	stopchan chan struct{}
	//channel to know when goroutine should stop expecting ack message
	ackReceivedChan chan bool

	routingTableSync routingTableSync
	statusMapSync    statusMapSync
	rumorsLogSync    rumorsLogSync

	seq uint
}

type routingTableSync struct {
	sync.Mutex
	routingTable peer.RoutingTable
}

type rumorsLogSync struct {
	sync.Mutex
	rumorsLog map[string][]types.Rumor
}

type statusMapSync struct {
	sync.Mutex
	statusMap map[string]uint
}

// -------------------------------------------------------------------------------------------------------

func (n *node) ExecEmptyMessage(msg types.Message, pkt transport.Packet) error {
	return nil
}

func (n *node) ExecPrivateMessage(msg types.Message, pkt transport.Packet) error {
	privateMsg, ok := msg.(*types.PrivateMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	if _, ok := privateMsg.Recipients[n.addr]; ok {
		return n.conf.MessageRegistry.ProcessPacket(transport.Packet{Header: pkt.Header, Msg: privateMsg.Msg})
	}
	return nil
}

func (n *node) ExecRumorsMessage(msg types.Message, pkt transport.Packet) error {
	rumorsMsg, ok := msg.(*types.RumorsMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	log.Println(n.addr + " received rumor " + rumorsMsg.String() + " from " + pkt.Header.Source)

	newData := false
	// process each rumor
	for _, rumor := range rumorsMsg.Rumors {

		//If expected sequence
		if n.statusMapSync.get(rumor.Origin)+1 == rumor.Sequence {
			newData = true
			println(n.addr + " adding rumor")

			n.statusMapSync.incrementSequence(rumor.Origin)
			n.rumorsLogSync.addRumor(rumor.Origin, rumor)

			newPkt := transport.Packet{Header: pkt.Header, Msg: rumor.Msg}
			err := n.conf.MessageRegistry.ProcessPacket(newPkt)
			if err != nil {
				return err
			}

			n.SetRoutingEntry(rumor.Origin, pkt.Header.RelayedBy)

			//send back Ack message to sender (if not processing locally)
			log.Println(n.addr + " casting ack msg to " + pkt.Header.Source)
			ack := types.AckMessage{AckedPacketID: pkt.Header.PacketID, Status: n.GetStatusMap()}
			buf, err := json.Marshal(ack)
			if err != nil {
				return err
			}
			ackMsg := transport.Message{Type: types.AckMessage{}.Name(), Payload: buf}
			//we use send instead of unicast because, concluding from the tests, n doesnt always have the address of sender in the routing table
			err = n.Unicast(pkt.Header.Source, ackMsg)
			if err != nil && err.Error() == "the destination is not in the routing table" {
				err = n.conf.Socket.Send(pkt.Header.Source, transport.Packet{Header: pkt.Header, Msg: &ackMsg}, 0)
				if err != nil {
					return err
				}
			}

		}
		//else, some rumor(s) was not received by this node

	}

	if newData {
		log.Println(n.addr + " recevied new data")
		buf, err := json.Marshal(rumorsMsg)
		if err != nil {
			return err
		}
		println("new data for " + n.addr)
		//To prevent loops in closed systems, we make sure that we can send to other random neighbors than itself, source and origin
		routingTableCopy := n.GetRoutingTable()
		delete(routingTableCopy, n.addr)
		delete(routingTableCopy, pkt.Header.Source)
		delete(routingTableCopy, rumorsMsg.Rumors[0].Origin)
		if len(routingTableCopy) > 0 {
			err = n.sendMessageToRandomNeighbor(transport.Message{Type: types.RumorsMessage{}.Name(), Payload: buf}, []string{pkt.Header.Source, rumorsMsg.Rumors[0].Origin, n.addr}, false)
			if err != nil {
				return err
			}
		}

	}

	log.Println("ret")

	return nil
}

func (n *node) ExecChatMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	chatMsg, ok := msg.(*types.ChatMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	log.Println(chatMsg.Message)
	return nil
}

func (n *node) ExecAckMessage(msg types.Message, pkt transport.Packet) error {
	ackMsg, ok := msg.(*types.AckMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	println(n.addr + " received ack msg")
	n.ackReceivedChan <- true
	buf, err := json.Marshal(ackMsg.Status)
	if err != nil {
		return err
	}
	newPkt := transport.Packet{Header: pkt.Header, Msg: &transport.Message{Type: types.StatusMessage{}.Name(), Payload: buf}}
	n.conf.MessageRegistry.ProcessPacket(newPkt)
	return nil
}

func (n *node) ExecStatusMessage(msg types.Message, pkt transport.Packet) error {
	log.Println(n.addr + " received status message from " + pkt.Header.Source + " of type " + msg.Name())
	//log.Println(n.GetStatusMap())

	remoteStatus, ok := msg.(*types.StatusMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	//log.Println(remoteStatus)
	var rumors []types.Rumor
	hasMoreStatus := false
	//check if n has more status than remote
	for neighbor, seq := range n.GetStatusMap() {
		if seq > (*remoteStatus)[neighbor] {
			hasMoreStatus = true
			toAppend := n.rumorsLogSync.getRumors(neighbor)[(*remoteStatus)[neighbor]:]
			rumors = append(rumors, toAppend...)
		}
	}

	if hasMoreStatus {
		println("has more status")
		//send rumors (sorted by seq) as a rumorsMsg
		rumorsMsg := types.RumorsMessage{Rumors: rumors}
		buf, err := json.Marshal(rumorsMsg)
		if err != nil {
			return err
		}
		if err != nil && err.Error() == "the destination is not in the routing table" {
			err = n.conf.Socket.Send(pkt.Header.Source, transport.Packet{Header: pkt.Header, Msg: &transport.Message{Type: types.RumorsMessage{}.Name(), Payload: buf}}, 0)
			if err != nil {
				return err
			}
		}
	}

	hasLessStatus := false
	for neighbor, seq := range *remoteStatus {
		if seq > n.statusMapSync.get(neighbor) {
			hasLessStatus = true
			println("has less status")
			//send status to remote
			buf, err := json.Marshal(types.StatusMessage(n.GetStatusMap()))
			if err != nil {
				return err
			}
			println(n.addr + " sending his status back to " + pkt.Header.Source)
			statusMsg := transport.Message{Type: types.StatusMessage{}.Name(), Payload: buf}
			err = n.Unicast(pkt.Header.Source, statusMsg)
			if err != nil && err.Error() == "the destination is not in the routing table" {
				header := transport.NewHeader(n.addr, pkt.Header.RelayedBy, pkt.Header.Source, 0)
				err = n.conf.Socket.Send(pkt.Header.Source, transport.Packet{Header: &header, Msg: &statusMsg}, 0)
				if err != nil {
					return err
				}
			}
		}
	}

	if !hasLessStatus && !hasMoreStatus {
		println("equal status")
		//ContinueMongering
		p := rand.Float64()
		if p <= n.conf.ContinueMongering && p != 0.0 {
			println("continue mongering")
			buf, err := json.Marshal(types.StatusMessage(n.GetStatusMap()))
			if err != nil {
				return err
			}
			msg := transport.Message{Type: types.StatusMessage{}.Name(), Payload: buf}
			err = n.sendMessageToRandomNeighbor(msg, []string{pkt.Header.Source, n.addr}, false)
			if err != nil {
				return err
			}
		}

	}
	return nil
}

// ------------------------------------------------------------------------------------------------------------

// Start implements peer.Service
func (n *node) Start() error {
	errs := make(chan error)
	n.stopchan = make(chan struct{})
	go func() {
		for {
			select {
			case <-n.stopchan:
				return
			default:
				pkt, err := n.conf.Socket.Recv(time.Second * 1)
				if errors.Is(err, transport.TimeoutErr(0)) {
					continue
				} else if err != nil {
					errs <- err
				}

				if pkt.Header.Destination == "" {
					//do nothing yet
				} else if pkt.Header.Destination == n.conf.Socket.GetAddress() {
					err = n.conf.MessageRegistry.ProcessPacket(pkt)
					if err != nil {
						println("error is " + err.Error())
						errs <- err
					}
				} else {
					pkt.Header.RelayedBy = n.conf.Socket.GetAddress()
					err := n.conf.Socket.Send(pkt.Header.Destination, pkt, time.Second*1)
					if err != nil {
						errs <- err
					}
				}
			}
		}
	}()
	close(errs)
	err := <-errs
	if err != nil {
		n.Stop()
	}
	return err
}

// Stop implements peer.Service
func (n *node) Stop() error {
	close(n.stopchan)
	return nil
}

func (n *node) antiEntropy() error {
	if n.conf.AntiEntropyInterval != 0 {
		log.Println("non-zero anti-entropy")
		ticker := time.NewTicker(n.conf.AntiEntropyInterval)
		errs := make(chan error)
		go func() {
			for range ticker.C {
				buf, err := json.Marshal(types.StatusMessage(n.GetStatusMap()))
				if err != nil {
					errs <- err
				}
				log.Println("anti-entropy tick")
				msg := transport.Message{Type: types.StatusMessage{}.Name(), Payload: buf}
				err = n.sendMessageToRandomNeighbor(msg, []string{n.addr}, false)
				if err != nil {
					errs <- err
				}
			}
		}()
		close(errs)
		err := <-errs

		return err
	}
	return nil

}

func (n *node) heartbeat() error {
	buf, err := json.Marshal(types.EmptyMessage{})
	if err != nil {
		return err
	}
	log.Println("broadcasting empty msg")
	err = n.Broadcast(transport.Message{Type: types.EmptyMessage{}.Name(), Payload: buf})
	log.Println("done broadcasting")
	if err != nil {
		return err
	}
	if n.conf.HeartbeatInterval != 0 {
		log.Println("non-zero heartbeat")
		ticker := time.NewTicker(n.conf.HeartbeatInterval)
		errs := make(chan error)
		go func() {
			for range ticker.C {
				buf, err := json.Marshal(types.EmptyMessage{})
				if err != nil {
					errs <- err
				}
				log.Println("heartbeat tick")
				err = n.Broadcast(transport.Message{Type: types.EmptyMessage{}.Name(), Payload: buf})
				if err != nil {
					errs <- err
				}
			}
		}()
		close(errs)
		err := <-errs
		return err
	}
	return nil
}

// Unicast implements peer.Messaging
func (n *node) Unicast(dest string, msg transport.Message) error {
	relay := n.routingTableSync.get(dest)

	if relay == "" {
		return xerrors.Errorf("the destination is not in the routing table")
	}

	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), dest, 0)
	pkt := transport.Packet{Header: &header, Msg: &msg}
	log.Println(n.addr + " unicasting msg with type " + msg.Type + " to " + dest)
	err := n.conf.Socket.Send(relay, pkt, 0)
	return err

}

// AddPeer implements peer.Service
func (n *node) AddPeer(addr ...string) {
	for _, address := range addr {
		if address != n.conf.Socket.GetAddress() {
			n.SetRoutingEntry(address, address)
		}

	}
}

//Sends a message to a random neighbor and waits for an ack response if it's expected
func (n *node) sendMessageToRandomNeighbor(msg transport.Message, neighborsToAvoid []string, expectAck bool) error {

	for randNeighbor := range n.GetRoutingTable() {
		for _, neighborToAvoid := range neighborsToAvoid {
			if neighborToAvoid == randNeighbor {
				return n.sendMessageToRandomNeighbor(msg, neighborsToAvoid, true)
			}
		}

		println(n.addr + " sending msg to " + randNeighbor)

		err := n.Unicast(randNeighbor, msg)
		if err != nil {
			return err
		}

		//We don't need to wait for an ack if we process the msg locally
		if expectAck && n.addr != randNeighbor {
			log.Println("expecting ack")
			var ackTimeout time.Duration
			if n.conf.AckTimeout == 0 {
				ackTimeout = math.MaxInt64
			} else {
				ackTimeout = n.conf.AckTimeout
			}
			//Is this goroutine a blocking operation ?
			go func() {
				select {
				case <-n.ackReceivedChan:
					log.Println("ack channel closed")
					//ack received
					return
				case <-time.After(ackTimeout):
					//ack timeout
					log.Println("timeout")
					n.sendMessageToRandomNeighbor(msg, append(neighborsToAvoid, randNeighbor), true)
					//catch error
					return

				}
			}()
		}
		return nil
	}
	return nil
}

func (n *node) Broadcast(msg transport.Message) error {
	log.Println(n.addr + " broadcasting msg of type " + msg.Type)

	//There is no use broadcasting a message when there is only the current node in the routing table
	if len(n.GetRoutingTable()) == 1 {
		return nil
	}

	//create a rumor
	rumor := types.Rumor{Origin: n.addr, Sequence: n.seq, Msg: &msg}
	var rumors []types.Rumor
	rumors = append(rumors, rumor)
	rumorsMessage := types.RumorsMessage{Rumors: rumors[:]}

	//put the message in a pkt
	buf, err := json.Marshal(rumorsMessage)
	if err != nil {
		return err
	}
	msgToSend := transport.Message{Type: types.RumorsMessage{}.Name(), Payload: buf}

	err = n.sendMessageToRandomNeighbor(msgToSend, []string{n.addr}, true)
	if err != nil {
		return err
	}
	n.statusMapSync.incrementSequence(rumor.Origin)
	n.rumorsLogSync.addRumor(rumor.Origin, rumor)
	n.seq += 1

	//process pkt locally
	println(n.addr + " processing pkt of type " + msg.Type + " locally")
	header := transport.NewHeader(n.addr, n.addr, n.addr, 0)
	pkt := transport.Packet{Header: &header, Msg: &msg}
	return n.conf.MessageRegistry.ProcessPacket(pkt)
}

//-------------------------------------------------------------------------------------------------------------

func (n *node) GetRumorsLog() map[string][]types.Rumor {
	n.rumorsLogSync.Lock()
	defer n.rumorsLogSync.Unlock()

	rumorsLogCopy := make(map[string][]types.Rumor)

	for k, v := range n.rumorsLogSync.rumorsLog {
		rumorsLogCopy[k] = v
	}
	return rumorsLogCopy
}

func (rumorsLogSync *rumorsLogSync) getRumors(neighborAddr string) []types.Rumor {
	rumorsLogSync.Lock()
	defer rumorsLogSync.Unlock()

	return rumorsLogSync.rumorsLog[neighborAddr]
}

func (rumorsLogSync *rumorsLogSync) addRumor(neighborAddr string, rumor types.Rumor) {
	rumorsLogSync.Lock()
	defer rumorsLogSync.Unlock()

	rumorsLogSync.rumorsLog[neighborAddr] = append(rumorsLogSync.rumorsLog[neighborAddr], rumor)
}

//-------------------------------------------------------------------------------------------------------------

func (n *node) GetStatusMap() map[string]uint {
	n.statusMapSync.Lock()
	defer n.statusMapSync.Unlock()

	statusMapCopy := make(map[string]uint)

	for k, v := range n.statusMapSync.statusMap {
		statusMapCopy[k] = v
	}
	return statusMapCopy
}

func (statusMapSync *statusMapSync) get(neighborAddr string) uint {
	statusMapSync.Lock()
	defer statusMapSync.Unlock()

	return statusMapSync.statusMap[neighborAddr]
}

func (statusMapSync *statusMapSync) incrementSequence(neighborAddr string) {
	statusMapSync.Lock()
	defer statusMapSync.Unlock()

	statusMapSync.statusMap[neighborAddr]++
	println("incremented by 1 " + neighborAddr + ", now at " + strconv.FormatUint(uint64(statusMapSync.statusMap[neighborAddr]), 10))
}

//---------------------------------------------------------------------------------------------------------------

// GetRoutingTable implements peer.Service
func (n *node) GetRoutingTable() peer.RoutingTable {
	n.routingTableSync.Lock()
	defer n.routingTableSync.Unlock()

	routingTableCopy := make(peer.RoutingTable)

	for k, v := range n.routingTableSync.routingTable {
		routingTableCopy[k] = v
	}
	return routingTableCopy
}

// SetRoutingEntry implements peer.Service
func (n *node) SetRoutingEntry(origin, relayAddr string) {
	println(n.addr + " adding routing entry for " + origin + " : " + relayAddr)
	if origin == "" {

	} else if relayAddr == "" {
		n.routingTableSync.delete(origin)
	} else {
		n.routingTableSync.add(origin, relayAddr)
	}
}

func (routingTableSync *routingTableSync) add(destAddr, relayAddr string) {
	routingTableSync.Lock()
	defer routingTableSync.Unlock()

	routingTableSync.routingTable[destAddr] = relayAddr
}

func (routingTableSync *routingTableSync) get(destAddr string) string {
	routingTableSync.Lock()
	defer routingTableSync.Unlock()

	return routingTableSync.routingTable[destAddr]
}

func (routingTableSync *routingTableSync) delete(destAddr string) {
	routingTableSync.Lock()
	defer routingTableSync.Unlock()

	delete(routingTableSync.routingTable, destAddr)
}
