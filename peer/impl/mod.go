package impl

import (
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

	peerAddress := conf.Socket.GetAddress()

	node := node{
		conf:              conf,
		address:           peerAddress,
		stop:              make(chan struct{}, 3),
		routingTable:      RoutingTable{table: map[string]string{peerAddress: peerAddress}},
		rumorLists:        RumorLists{rumorLists: map[string][]types.Rumor{peerAddress: make([]types.Rumor, 0)}},
		ackWaitList:       AckWaitList{list: make(map[string]chan struct{})},
		nbRunningRoutines: 0,
		waitGroup:         sync.WaitGroup{},
	}

	node.conf.MessageRegistry.RegisterMessageCallback(types.AckMessage{}, node.processAckMessage())
	node.conf.MessageRegistry.RegisterMessageCallback(types.RumorsMessage{}, node.processRumorsMessage())
	node.conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, node.processChatMessage())
	node.conf.MessageRegistry.RegisterMessageCallback(types.StatusMessage{}, node.processStatusMessage())
	node.conf.MessageRegistry.RegisterMessageCallback(types.PrivateMessage{}, node.processPrivateMessage())
	node.conf.MessageRegistry.RegisterMessageCallback(types.EmptyMessage{}, node.processEmptyMessage())

	return &node

}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer
	// You probably want to keep the peer.Configuration on this struct:
	conf              peer.Configuration
	stop              chan struct{}
	routingTable      RoutingTable
	address           string
	rumorLists        RumorLists
	ackWaitList       AckWaitList
	nbRunningRoutines uint
	waitGroup         sync.WaitGroup
}

// launches the given func as a goroutine if the condition is respected
func (n *node) launchRoutineWithFun(f func(), cond bool) {
	if !cond {
		return
	}

	n.nbRunningRoutines += 1
	n.waitGroup.Add(1)
	go f()
}

func (n *node) Start() error {
	n.launchRoutineWithFun(n.ProcessTraffic, true)
	n.launchRoutineWithFun(n.AntiEntropy, n.conf.AntiEntropyInterval > 0)
	n.launchRoutineWithFun(n.HeartBeat, n.conf.HeartbeatInterval > 0)
	return nil
}

func (n *node) Stop() error {

	for i := 0; i < int(n.nbRunningRoutines); i += 1 {
		n.stop <- struct{}{}
	}
	close(n.stop)
	n.waitGroup.Wait()
	return nil
}

func (n *node) ProcessTraffic() {
	for {
		select {
		case <-n.stop:

			n.waitGroup.Done()
			return
		default:
			pkt, err := n.conf.Socket.Recv(time.Second * 1)
			if errors.Is(err, transport.TimeoutErr(0)) {
				continue
			}

			if err != nil {
				xerrors.Errorf("Failed to receive packet due to another error that an timeout", err)
			}

			peerAddress := n.conf.Socket.GetAddress()
			pktDestAddress := pkt.Header.Destination

			//process the packet
			if peerAddress == pktDestAddress || pktDestAddress == "" {
				processErr := n.conf.MessageRegistry.ProcessPacket(pkt)

				if processErr != nil {
					xerrors.Errorf("Failed to process packet", processErr)
				}

			}
			//relay the packet
			if pktDestAddress != peerAddress {
				//update the header and send the packet to its destination, or rebroadcast it
				pkt.Header.RelayedBy = peerAddress
				pkt.Header.TTL -= 1

				nextHopAddr := n.routingTable.getRelayAddr(pktDestAddress)

				if nextHopAddr != "" {
					sendErr := n.conf.Socket.Send(nextHopAddr, pkt, n.conf.AckTimeout)
					if sendErr != nil {
						xerrors.Errorf("Failed to forward", sendErr)
					}
				}
			}
		}
	}
}

func (n *node) Unicast(dest string, msg transport.Message) error {

	if dest == "" {
		return xerrors.Errorf("Empty destination: %v", dest)
	}

	peerAddress := n.conf.Socket.GetAddress()
	neigh := n.routingTable.getRelayAddr(dest)

	if neigh == "" {
		return xerrors.Errorf("No entry in routing table to destination %v", neigh)
	}

	header := transport.NewHeader(peerAddress, peerAddress, dest, 0)
	packet := transport.Packet{Header: &header, Msg: &msg}

	sendErr := n.conf.Socket.Send(neigh, packet, n.conf.AckTimeout)

	if sendErr != nil {
		return sendErr
	}

	return nil
}

func (n *node) Broadcast(msg transport.Message) error {

	header := transport.NewHeader(n.address, n.address, n.address, 0)
	pkt := transport.Packet{
		Header: &header,
		Msg:    &msg,
	}

	err := n.conf.MessageRegistry.ProcessPacket(pkt)
	if err != nil {
		return err
	}

	randomNeighAddr := n.routingTable.ChooseRDMNeighborAddr(MakeExceptionMap(n.address))

	inputMsg := msg.Copy()

	rumor := types.Rumor{
		Origin:   n.address,
		Sequence: n.GetOwnRumorSeq() + 1,
		Msg:      &inputMsg,
	}

	n.rumorLists.UpdateRumorSeqOf(n.address, rumor)

	rumorsArray := []types.Rumor{rumor}

	if randomNeighAddr == "" {
		log.Printf("Node has no neighbor")
		return nil
	}

	rand.Seed(time.Now().UnixNano())
	ackID, sendErr := n.SendRumorMsg(randomNeighAddr, rumorsArray)

	if sendErr != nil {
		return sendErr
	}
	//Why not launch goroutine ?
	go n.waitForAck(ackID, rumorsArray, randomNeighAddr)

	return nil

}

func (n *node) AntiEntropy() {

	ticker := time.NewTicker(n.conf.AntiEntropyInterval)

	for {
		select {
		case <-n.stop:
			ticker.Stop()
			n.waitGroup.Done()
			return
		case <-ticker.C:
			rdmNeighor := n.routingTable.ChooseRDMNeighborAddr(MakeExceptionMap(n.address))
			n.SendStatusMessage(rdmNeighor)
		}
	}
}

func (n *node) HeartBeat() {

	n.Broadcast(n.CreateEmptyMessage())

	ticker := time.NewTicker(n.conf.HeartbeatInterval)
	for {
		select {
		case <-n.stop:
			n.waitGroup.Done()
			return
		case <-ticker.C:
			n.Broadcast(n.CreateEmptyMessage())
		}
	}
}

func (n *node) CreateEmptyMessage() transport.Message {
	emptyMsg := types.EmptyMessage{}
	trsptEmptyMsg, _ := n.conf.MessageRegistry.MarshalMessage(emptyMsg)
	return trsptEmptyMsg
}

func (n *node) CreateStatusMessage() types.StatusMessage {
	currentNodeStatus := n.rumorLists.ConvertRumorsToSeq()
	return currentNodeStatus
}

func (n *node) SendStatusMessage(address string) error {

	statusMsg := n.CreateStatusMessage()

	statusMsgHeader := transport.NewHeader(n.address, n.address, address, 0)

	trsptStatusMsg, statusErr := n.conf.MessageRegistry.MarshalMessage(statusMsg)
	if statusErr != nil {
		return statusErr
	}

	pkt := transport.Packet{
		Header: &statusMsgHeader,
		Msg:    &trsptStatusMsg,
	}

	sendErr := n.conf.Socket.Send(address, pkt, 0)
	if sendErr != nil {
		return sendErr
	}

	return nil

}

func (n *node) SendRumorMsg(address string, rumors []types.Rumor) (string, error) {

	header := transport.NewHeader(n.address, n.address, address, 0)
	msg := types.RumorsMessage{Rumors: rumors}

	trsprtMsg, marshErr := n.conf.MessageRegistry.MarshalMessage(&msg)
	if marshErr != nil {
		return "", marshErr
	}

	pkt := transport.Packet{
		Header: &header,
		Msg:    &trsprtMsg,
	}

	sendErr := n.conf.Socket.Send(address, pkt, n.conf.AckTimeout)

	return header.PacketID, sendErr

}

func (n *node) waitForAck(ackID string, rumors []types.Rumor, oldAddr string) {

	ackChannel := make(chan struct{})
	n.ackWaitList.addEntry(ackID, ackChannel)

	timer := &time.Timer{}

	if n.conf.AckTimeout != 0 {
		timer = time.NewTimer(n.conf.AckTimeout)
	}

	for {
		select {
		case <-ackChannel:
			timer.Stop()
			close(ackChannel)
			n.ackWaitList.removeEntry(ackID)
			return
		case <-timer.C:
			newAckID, newAddr := n.resendRumor(oldAddr, rumors)
			go n.waitForAck(newAckID, rumors, newAddr)
			return
		}
	}
}

func (n *node) resendRumor(oldAddr string, rumors []types.Rumor) (string, string) {

	rand.Seed(time.Now().UnixNano())

	rdmAddr := n.routingTable.ChooseRDMNeighborAddr(MakeExceptionMap(oldAddr, n.address))

	newAck, _ := n.SendRumorMsg(rdmAddr, rumors)
	return newAck, rdmAddr

}

func (n *node) AddPeer(addr ...string) {

	if len(addr) == 0 {
		//nothing to add
		return
	}

	n.routingTable.lock.Lock()
	defer n.routingTable.lock.Unlock()

	for i := range addr {
		newAddress := addr[i]
		n.routingTable.table[newAddress] = newAddress
	}
}

func (n *node) GetRoutingTable() peer.RoutingTable {

	n.routingTable.lock.RLock()
	defer n.routingTable.lock.RUnlock()

	var copy peer.RoutingTable = make(peer.RoutingTable)
	for k, v := range n.routingTable.table {
		copy[k] = v
	}
	return copy
}

func (n *node) isNeighbor(addr string) bool {
	n.routingTable.lock.Lock()
	defer n.routingTable.lock.Unlock()

	if relay, ok := n.routingTable.table[addr]; ok {
		return addr == relay
	} else {
		return false
	}
}

func (n *node) SetRoutingEntry(origin, relayAddr string) {
	n.routingTable.lock.Lock()
	defer n.routingTable.lock.Unlock()

	if relayAddr == "" {
		delete(n.routingTable.table, origin)
	} else {
		n.routingTable.table[origin] = relayAddr
	}
}

//Process a packet through the messageRegistry
func (n *node) ProcessPacket(header *transport.Header, msg *transport.Message) error {

	newPkt := transport.Packet{Header: header, Msg: msg}
	err := n.conf.MessageRegistry.ProcessPacket(newPkt)

	return err
}
