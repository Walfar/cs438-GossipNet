package udp

import (
	"math"
	"net"
	"sync"
	"time"

	"go.dedis.ch/cs438/internal/traffic"
	"go.dedis.ch/cs438/transport"
)

const bufSize = 65000

// NewUDP returns a new udp transport implementation.
func NewUDP() transport.Transport {
	return &UDP{
		traffic: traffic.NewTraffic(),
	}
}

// UDP implements a transport layer using UDP
//
// - implements transport.Transport
type UDP struct {
	traffic *traffic.Traffic
}

// CreateSocket implements transport.Transport
func (n *UDP) CreateSocket(address string) (transport.ClosableSocket, error) {
	socket := Socket{}
	socket.UDP = n

	var err error = nil
	socket.conn, err = net.ListenPacket("udp", address)
	if err != nil {
		return nil, err
	}

	socket.myAddr = socket.conn.LocalAddr().String()

	return &socket, err

}

// Socket implements a network socket using UDP.
//
// - implements transport.Socket
// - implements transport.ClosableSocket
type Socket struct {
	*UDP
	myAddr string

	sync.RWMutex
	conn net.PacketConn

	ins  packets
	outs packets
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
	s.Lock()
	defer s.Unlock()

	err := s.conn.Close()
	return err

}

// Send implements transport.Socket
func (s *Socket) Send(dest string, pkt transport.Packet, timeout time.Duration) error {

	errs := make(chan error, 1)

	if timeout == 0 {
		timeout = math.MaxInt64
	}

	go func() {

		dst, err := net.ResolveUDPAddr("udp", dest)

		if err != nil {
			errs <- err
		}

		buf, err := pkt.Marshal()
		if err != nil {
			errs <- err
		}

		_, err = s.conn.WriteTo(buf, dst)
		if err != nil {
			errs <- err
		}

		s.outs.add(pkt.Copy())
		s.traffic.LogSent(pkt.Header.RelayedBy, dest, pkt)

		errs <- err
	}()

	select {
	case err := <-errs:
		return err
	case <-time.After(timeout):
		return transport.TimeoutErr(timeout)
	}

}

// Recv implements transport.Socket. It blocks until a packet is received, or
// the timeout is reached. In the case the timeout is reached, return a
// TimeoutErr.
func (s *Socket) Recv(timeout time.Duration) (transport.Packet, error) {

	errs := make(chan error, 1)

	if timeout == 0 {
		timeout = math.MaxInt64
	}

	buf := make([]byte, bufSize)
	var pkt transport.Packet

	go func() {
		n, addr, err := s.conn.ReadFrom(buf)

		if n > 0 {
			err = (&pkt).Unmarshal(buf[:n])
			if err != nil {
				errs <- err
			}

			s.traffic.LogRecv(addr.String(), pkt.Header.Destination, pkt)
			s.ins.add(pkt.Copy())

		}
		errs <- err
	}()

	select {
	case err := <-errs:
		return pkt, err
	case <-time.After(timeout):
		return transport.Packet{}, transport.TimeoutErr(timeout)
	}

}

// GetAddress implements transport.Socket. It returns the address assigned. Can
// be useful in the case one provided a :0 address, which makes the system use a
// random free port.
func (s *Socket) GetAddress() string {
	return s.conn.LocalAddr().String()
}

// GetIns implements transport.Socket
func (s *Socket) GetIns() []transport.Packet {
	return s.ins.getAll()
}

// GetOuts implements transport.Socket
func (s *Socket) GetOuts() []transport.Packet {
	return s.outs.getAll()
}
