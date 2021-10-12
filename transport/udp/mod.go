package udp

import (
	"fmt"
	"math"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.dedis.ch/cs438/transport"
	"golang.org/x/xerrors"
)

const bufSize = 65000

var counter uint32 = 0

// NewUDP returns a new udp transport implementation.
func NewUDP() transport.Transport {
	return &UDP{}
}

// UDP implements a transport layer using UDP
//
// - implements transport.Transport
type UDP struct {
	sync.RWMutex
	connections map[string]net.Conn
}

// CreateSocket implements transport.Transport
func (n *UDP) CreateSocket(address string) (transport.ClosableSocket, error) {
	if net.ParseIP(address) == nil {
		return nil, xerrors.Errorf("not a valid address")
	}
	n.Lock()
	if strings.HasSuffix(address, ":0") {
		address = address[:len(address)-2]
		port := atomic.AddUint32(&counter, 1)
		address = fmt.Sprintf("%s:%d", address, port)
	}
	n.Unlock()
	return &Socket{
		UDP:    n,
		myAddr: address,
	}, nil
}

// Socket implements a network socket using UDP.
//
// - implements transport.Socket
// - implements transport.ClosableSocket
type Socket struct {
	*UDP
	myAddr string
	ins    []transport.Packet
	outs   []transport.Packet
}

// Close implements transport.Socket. It returns an error if already closed.
func (s *Socket) Close() error {
	s.Lock()
	defer s.Unlock()
	delete(s.connections, s.myAddr)
	return nil
}

// Send implements transport.Socket
func (s *Socket) Send(dest string, pkt transport.Packet, timeout time.Duration) error {
	var err error
	s.connections[dest], err = net.Dial("udp", dest)
	if err != nil {
		return err
	}

	if timeout == 0 {
		timeout = math.MaxInt64
	}
	s.connections[dest].SetWriteDeadline(time.Now().Add(timeout))

	buf, err := pkt.Marshal()
	if err != nil {
		return err
	}
	_, err = s.connections[dest].Write(buf)
	if err != nil {
		return err
	}
	s.outs = append(s.outs, pkt)
	return nil
}

// Recv implements transport.Socket. It blocks until a packet is received, or
// the timeout is reached. In the case the timeout is reached, return a
// TimeoutErr.
func (s *Socket) Recv(timeout time.Duration) (transport.Packet, error) {
	pkts := make(chan transport.Packet)
	errs := make(chan error)
	for addr, conn := range s.connections {
		go func(k string, v net.Conn) {
			var buf []byte
			var pkt transport.Packet
			_, err := s.connections[addr].Read(buf[0:bufSize])
			if err != nil {
				errs <- err
			}
			err = pkt.Unmarshal(buf)
			if err != nil {
				errs <- err
			}
			pkts <- pkt
		}(addr, conn)
	}
	select {
	case err := <-errs:
		return transport.Packet{}, err
	case pkt := <-pkts:
		s.ins = append(s.ins, pkt)
		return pkt, nil
	}
}

// GetAddress implements transport.Socket. It returns the address assigned. Can
// be useful in the case one provided a :0 address, which makes the system use a
// random free port.
func (s *Socket) GetAddress() string {
	return s.myAddr
}

// GetIns implements transport.Socket
func (s *Socket) GetIns() []transport.Packet {
	return s.ins
}

// GetOuts implements transport.Socket
func (s *Socket) GetOuts() []transport.Packet {
	return s.outs
}
