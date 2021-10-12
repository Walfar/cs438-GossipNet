package impl

import (
	"errors"
	"log"
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
	n := node{conf: conf, routingTableStruct: routingTableStruct{routingTable: make(peer.RoutingTable)}, stopchan: make(chan struct{})}
	//init register callback for chat messages
	n.conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, n.ExecChatMessage)
	//init routingTable with node's addr
	n.routingTableStruct.mutex.Lock()
	n.routingTableStruct.routingTable[n.conf.Socket.GetAddress()] = n.conf.Socket.GetAddress()
	n.routingTableStruct.mutex.Unlock()
	return &n
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer
	conf peer.Configuration
	//channel to know when goroutine should stop
	stopchan           chan struct{}
	routingTableStruct routingTableStruct
}

//Thread safe routingTable struct
type routingTableStruct struct {
	routingTable peer.RoutingTable
	mutex        sync.Mutex
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

// Start implements peer.Service
func (n *node) Start() error {
	var err error
	errs := make(chan error)
	go func() {
		for {
			select {
			case <-n.stopchan:
				err = <-errs
				return
			default:
				pkt, err2 := n.conf.Socket.Recv(time.Second * 1)
				if errors.Is(err2, transport.TimeoutErr(0)) {
					continue
				}
				if pkt.Header.Destination == n.conf.Socket.GetAddress() {
					errs <- n.conf.MessageRegistry.ProcessPacket(pkt)
				} else {
					pkt.Header.RelayedBy = n.conf.Socket.GetAddress()
					n.routingTableStruct.mutex.Lock()
					newDestination := n.routingTableStruct.routingTable[pkt.Header.Destination]
					n.routingTableStruct.mutex.Unlock()
					errs <- n.conf.Socket.Send(newDestination, pkt, time.Second*1)
				}
			}
		}
	}()
	return err
}

// Stop implements peer.Service
func (n *node) Stop() error {
	close(n.stopchan)
	return nil
}

// Unicast implements peer.Messaging
func (n *node) Unicast(dest string, msg transport.Message) error {
	relay, ok := n.routingTableStruct.routingTable[dest]
	if !ok {
		return xerrors.Errorf("Unknown address")
	}
	header := transport.NewHeader(n.conf.Socket.GetAddress(), relay, dest, 0)
	header.RelayedBy = n.conf.Socket.GetAddress()
	pkt := transport.Packet{Header: &header, Msg: &msg}
	err := n.conf.Socket.Send(relay, pkt, time.Second*1)
	return err

}

// AddPeer implements peer.Service
func (n *node) AddPeer(addr ...string) {
	for _, addr := range addr {
		n.routingTableStruct.mutex.Lock()
		n.routingTableStruct.routingTable[addr] = addr
		n.routingTableStruct.mutex.Unlock()
	}
}

// GetRoutingTable implements peer.Service
func (n *node) GetRoutingTable() peer.RoutingTable {
	routingTableCopy := routingTableStruct{routingTable: make(map[string]string)}
	n.routingTableStruct.mutex.Lock()
	for k, v := range n.routingTableStruct.routingTable {
		routingTableCopy.mutex.Lock()
		routingTableCopy.routingTable[k] = v
		routingTableCopy.mutex.Unlock()
	}
	n.routingTableStruct.mutex.Unlock()
	return routingTableCopy.routingTable
}

// SetRoutingEntry implements peer.Service
func (n *node) SetRoutingEntry(origin, relayAddr string) {
	if relayAddr == "" {
		n.routingTableStruct.mutex.Lock()
		delete(n.routingTableStruct.routingTable, origin)
		n.routingTableStruct.mutex.Unlock()
	} else {
		n.routingTableStruct.mutex.Lock()
		n.routingTableStruct.routingTable[origin] = relayAddr
		n.routingTableStruct.mutex.Unlock()
	}
}
