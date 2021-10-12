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

var counter uint32 = 2000

// NewUDP returns a new udp transport implementation.
func NewUDP() transport.Transport {
	return &UDP{writeConnections: make(map[string]net.Conn)}
}

// UDP implements a transport layer using UDP
//
// - implements transport.Transport
type UDP struct {
	mutex            sync.Mutex
	writeConnections map[string]net.Conn
	readConnection   net.Conn
}

// CreateSocket implements transport.Transport
func (n *UDP) CreateSocket(address string) (transport.ClosableSocket, error) {
	if net.ParseIP(strings.Split(address, ":")[0]) == nil {
		return nil, xerrors.Errorf("Not a valid address")
	}
	if strings.HasSuffix(address, ":0") {
		address = address[:len(address)-2]
		port := atomic.AddUint32(&counter, 1)
		address = fmt.Sprintf("%s:%d", address, port)
	}
	udpAddr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}
	n.mutex.Lock()
	n.readConnection, err = net.ListenUDP("udp", udpAddr)
	n.mutex.Unlock()
	if err != nil {
		return nil, err
	}
	return &Socket{
		UDP:    n,
		myAddr: address,
		ins:    packets{},
		outs:   packets{},
	}, nil
}

// Socket implements a network socket using UDP.
//
// - implements transport.Socket
// - implements transport.ClosableSocket
type Socket struct {
	*UDP
	myAddr string
	ins    packets
	outs   packets
}

type packets struct {
	sync.Mutex
	data []transport.Packet
}

func (p *packets) add(pkt transport.Packet) {
	p.Lock()
	defer p.Unlock()
	p.data = append(p.data, pkt)
}

func (p *packets) getAll() []transport.Packet {
	p.Lock()
	defer p.Unlock()

	res := make([]transport.Packet, len(p.data))

	for i, pkt := range p.data {
		res[i] = pkt.Copy()
	}

	return res
}

// Close implements transport.Socket. It returns an error if already closed.
func (s *Socket) Close() error {
	for addr := range s.writeConnections {
		err := s.writeConnections[addr].Close()
		if err != nil {
			return err
		}
		delete(s.writeConnections, addr)
	}
	return s.readConnection.Close()
}

// Send implements transport.Socket
func (s *Socket) Send(dest string, pkt transport.Packet, timeout time.Duration) error {
	if timeout == 0 {
		timeout = math.MaxInt64
	}
	select {
	case <-time.After(timeout):
		return transport.TimeoutErr(timeout)
	default:
		buf, err := pkt.Marshal()
		if err != nil {
			return err
		}
		if err != nil {
			return err
		}
		if s.writeConnections[dest] == nil {
			s.writeConnections[dest], err = net.Dial("udp", dest)
			if err != nil {
				return err
			}
		}
		_, err = s.writeConnections[dest].Write(buf)
		if err != nil {
			return err
		}
		s.outs.add(pkt)
		return nil
	}

}

// Recv implements transport.Socket. It blocks until a packet is received, or
// the timeout is reached. In the case the timeout is reached, return a
// TimeoutErr.
func (s *Socket) Recv(timeout time.Duration) (transport.Packet, error) {
	if timeout == 0 {
		timeout = math.MaxInt64
	}
	s.mutex.Lock()
	select {
	case <-time.After(timeout):
		return transport.Packet{}, transport.TimeoutErr(timeout)
	default:
		buf := make([]byte, bufSize)
		var pkt transport.Packet
		n, err := s.readConnection.Read(buf)
		if err != nil {
			s.mutex.Unlock()
			return pkt, err
		}
		err = pkt.Unmarshal(buf[0:n])
		if err != nil {
			s.mutex.Unlock()
			return pkt, err
		}
		//We discard the packets sent by the address itself
		if pkt.Header.Source != s.myAddr {
			s.ins.add(pkt)
		}
		s.mutex.Unlock()
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
	return s.ins.getAll()
}

// GetOuts implements transport.Socket
func (s *Socket) GetOuts() []transport.Packet {
	return s.outs.getAll()
}
