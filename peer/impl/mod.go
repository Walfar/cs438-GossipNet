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
	n := node{}
	n.conf = conf
	n.routingTableSync.routingTable = make(map[string]string)
	n.addr = n.conf.Socket.GetAddress()
	//init routingTable with node's addr
	n.SetRoutingEntry(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress())
	//init register callback for chat messages
	n.conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, n.ExecChatMessage)

	return &n
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer
	conf peer.Configuration
	addr string
	//channel to know when goroutine should stop
	stopchan         chan struct{}
	routingTableSync routingTableSync
}

//Thread safe routingTable struct
type routingTableSync struct {
	sync.Mutex
	routingTable peer.RoutingTable
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
					//broadcast
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
