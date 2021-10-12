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
	return &node{}
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer
	conf peer.Configuration
	//channel to know when goroutine should stop
	stop               chan bool
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
	//init register callback for chat messages
	n.conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, n.ExecChatMessage)
	//init routingTable with node's addr
	n.routingTableStruct.mutex.Lock()
	n.routingTableStruct.routingTable[n.conf.Socket.GetAddress()] = n.conf.Socket.GetAddress()
	n.routingTableStruct.mutex.Unlock()

	//we use a channel to receive errors from the anonymous goroutine
	errs := make(chan error, 1)

	go func() {
		for {
			select {
			case <-n.stop:
				return
			default:
				pkt, err := n.conf.Socket.Recv(time.Second * 1)
				if errors.Is(err, transport.TimeoutErr(0)) {
					continue
				} else if err != nil {
					errs <- err
					break
				} else {
					if pkt.Header.Destination == n.conf.Socket.GetAddress() {
						errs <- n.conf.MessageRegistry.ProcessPacket(pkt)
					} else {
						pkt.Header.RelayedBy = n.conf.Socket.GetAddress()
						errs <- n.conf.Socket.Send(pkt.Header.Destination, pkt, time.Second*1)
					}
				}
			}
		}

	}()

	if err := <-errs; err != nil {
		return err
	}
	return nil
}

// Stop implements peer.Service
func (n *node) Stop() error {
	n.stop <- true
	return nil
}

// Unicast implements peer.Messaging
func (n *node) Unicast(dest string, msg transport.Message) error {
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), dest, 0)
	pkt := transport.Packet{Header: &header, Msg: &msg}
	return n.conf.Socket.Send(dest, pkt, time.Second*1)
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
