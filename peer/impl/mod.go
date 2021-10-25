package impl

import (
	"encoding/json"
	"errors"
	"log"
	"math/rand"
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
	n.addr = n.conf.Socket.GetAddress()
	//init routingTable with node's addr
	n.SetRoutingEntry(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress())
	//init register callback for messages
	n.conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, n.ExecChatMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.RumorsMessage{}, n.ExecRumorsMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.StatusMessage{}, n.ExecStatusMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.AckMessage{}, n.ExecAckMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.PrivateMessage{}, n.ExecPrivateMessage)
	// ----- is this the right place to call those functions ? Should be done at initialisation or when starting nodes ?
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
	ackReceivedChan chan struct{}

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

	//send back Ack message to sender
	ack := types.AckMessage{AckedPacketID: pkt.Header.PacketID, Status: n.GetStatusMap()}
	ackMsg := transport.Message{Type: "AckMessage", Payload: json.RawMessage(ack.String())}
	err := n.Unicast(pkt.Header.Source, ackMsg)
	if err != nil {
		return err
	}

	newData := false
	// process each rumor
	for _, rumor := range rumorsMsg.Rumors {
		//If new rumor in the rumorsMsg
		if n.statusMapSync.get(rumor.Origin) != rumor.Sequence {
			newData = true
		}
		//If expected sequence
		if n.statusMapSync.get(rumor.Origin)+1 == rumor.Sequence {

			n.statusMapSync.incrementSequence(rumor.Origin)
			n.rumorsLogSync.addRumor(rumor.Origin, rumor)

			newPkt := transport.Packet{Header: pkt.Header, Msg: rumor.Msg}
			err := n.conf.MessageRegistry.ProcessPacket(newPkt)
			if err != nil {
				return err
			}
		}
		//else, some rumor(s) was not received by this node
		//either way, we update the routing table (if rumor not from a neighbor) -- or should it be only when expected ?
		if _, ok := n.GetRoutingTable()[rumor.Origin]; !ok {
			n.routingTableSync.add(rumor.Origin, pkt.Header.RelayedBy)
		}

	}

	if newData {
		for randNeighbor := range n.routingTableSync.routingTable {
			//Send to random neighbor (but not source)
			if randNeighbor != pkt.Header.Source {
				err := n.Unicast(randNeighbor, transport.Message{Type: "RumorsMessage", Payload: json.RawMessage(rumorsMsg.String())})
				return err
			}
		}
	}

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
	close(n.ackReceivedChan) //might have to make at init, if ack received before broadcasting
	n.conf.MessageRegistry.ProcessPacket(pkt)
	return nil
}

func (n *node) ExecStatusMessage(msg types.Message, pkt transport.Packet) error {

	remoteStatus, ok := msg.(types.StatusMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	var rumors []types.Rumor
	hasMoreStatus := false
	//check if n has more status than remote
	for neighbor, seq := range n.GetStatusMap() {
		if seq > remoteStatus[neighbor] {
			hasMoreStatus = true
			rumors = append(rumors, n.rumorsLogSync.getRumors(neighbor)[seq+1:]...)
		}
	}
	if hasMoreStatus {
		//send rumors (sorted by seq) as a rumorsMsg
		rumorsMsg := types.RumorsMessage{Rumors: rumors}
		err := n.Unicast(pkt.Header.Source, transport.Message{Type: "RumorsMessage", Payload: json.RawMessage(rumorsMsg.String())})
		if err != nil {
			return err
		}
	}

	hasLessStatus := false
	for neighbor, seq := range remoteStatus {
		if seq > n.statusMapSync.get(neighbor) {
			hasLessStatus = true
			//send status to remote
			err := n.Unicast(pkt.Header.Source, transport.Message{Type: "StatusMessage", Payload: json.RawMessage(types.StatusMessage(n.GetStatusMap()).String())})
			if err != nil {
				return err
			}
		}
	}

	if !hasLessStatus && !hasMoreStatus {
		//ContinueMongering
		p := rand.Float64()
		if p <= n.conf.ContinueMongering && p != 0.0 {
			msg := transport.Message{Type: "StatusMessage", Payload: json.RawMessage(types.StatusMessage(n.GetStatusMap()).String())}
			err := n.sendMessageToRandomNeighbor(msg, []string{pkt.Header.Source}, false)
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

				n.AddPeer(pkt.Header.RelayedBy)
				n.SetRoutingEntry(pkt.Header.Source, pkt.Header.RelayedBy)

				if pkt.Header.Destination == "" {
					//do nothing yet
				} else if pkt.Header.Destination == n.conf.Socket.GetAddress() {
					err = n.conf.MessageRegistry.ProcessPacket(pkt)
					if err != nil {
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
		ticker := time.NewTicker(n.conf.AntiEntropyInterval)
		errs := make(chan error)
		go func() {
			for range ticker.C {
				msg := transport.Message{Type: "StatusMessage", Payload: json.RawMessage(types.StatusMessage(n.GetStatusMap()).String())}
				err := n.sendMessageToRandomNeighbor(msg, nil, false)
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
	err := n.Broadcast(transport.Message{Type: "EmptyMessage", Payload: json.RawMessage(types.EmptyMessage{}.String())})
	if err != nil {
		return err
	}
	if n.conf.HeartbeatInterval != 0 {
		ticker := time.NewTicker(n.conf.HeartbeatInterval)
		errs := make(chan error)
		go func() {
			for range ticker.C {
				err := n.Broadcast(transport.Message{Type: "EmptyMessage", Payload: json.RawMessage(types.EmptyMessage{}.String())})
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
		err := n.Unicast(randNeighbor, msg)
		if err != nil {
			return err
		}

		if expectAck {
			n.ackReceivedChan = make(chan struct{})
			//Is this goroutine a blocking operation ?
			go func() {
				select {
				case <-n.ackReceivedChan:
					//ack received
					return
				case <-time.After(n.conf.AckTimeout):
					//ack timeout
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

	//create a rumor
	rumor := types.Rumor{Origin: n.addr, Sequence: n.seq, Msg: &msg}
	var rumors []types.Rumor
	rumors[0] = rumor
	rumorsMessage := types.RumorsMessage{Rumors: rumors[:]}

	//put the message in a pkt
	msgToSend := transport.Message{Type: "RumorsMessage", Payload: json.RawMessage(rumorsMessage.String())}

	err := n.sendMessageToRandomNeighbor(msgToSend, nil, true)
	if err != nil {
		return err
	}
	n.seq += 1

	//process the pkt locally
	header := transport.NewHeader(n.addr, n.addr, n.addr, 0)
	pkt := transport.Packet{Header: &header, Msg: &msgToSend}
	//what does it mean to process rumor ?
	return n.conf.MessageRegistry.ProcessPacket(pkt)
}

//-------------------------------------------------------------------------------------------------------------

func (n *node) GetRumorsLog() map[string][]types.Rumor {
	rumorsLogCopy := make(map[string][]types.Rumor)

	for k := range n.statusMapSync.statusMap {
		rumorsLogCopy[k] = n.rumorsLogSync.getRumors(k)
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

	rumors := rumorsLogSync.getRumors(neighborAddr)
	rumorsLogSync.rumorsLog[neighborAddr] = append(rumors, rumor)
}

//-------------------------------------------------------------------------------------------------------------

func (n *node) GetStatusMap() map[string]uint {
	statusMapCopy := make(map[string]uint)

	for k := range n.statusMapSync.statusMap {
		statusMapCopy[k] = n.statusMapSync.get(k)
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

	statusMapSync.statusMap[neighborAddr] += 1
}

//---------------------------------------------------------------------------------------------------------------

// GetRoutingTable implements peer.Service
func (n *node) GetRoutingTable() peer.RoutingTable {
	routingTableCopy := make(peer.RoutingTable)

	for k := range n.routingTableSync.routingTable {
		routingTableCopy[k] = n.routingTableSync.get(k)
	}
	return routingTableCopy
}

// SetRoutingEntry implements peer.Service
func (n *node) SetRoutingEntry(origin, relayAddr string) {
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
