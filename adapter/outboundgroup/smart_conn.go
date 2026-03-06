package outboundgroup

import (
	"net"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/buf"
	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
)

// smartTracker holds shared passive-metrics state for TCP and UDP connections.
type smartTracker struct {
	smart      *Smart
	proxyName  string
	writeTime  time.Time
	firstWrite sync.Once
	firstRead  sync.Once
	closed     sync.Once
}

func (t *smartTracker) recordWrite() {
	t.firstWrite.Do(func() {
		t.writeTime = time.Now()
	})
}

func (t *smartTracker) recordRead() {
	if t.writeTime.IsZero() {
		return
	}
	t.firstRead.Do(func() {
		if rtt := time.Since(t.writeTime); rtt > 0 {
			// Passive traffic only triggers retests on degradation; never modifies scoring EMA.
			t.smart.recordPassiveRTT(t.proxyName, rtt)
		}
	})
}

func (t *smartTracker) onClose() {
	t.closed.Do(func() {
		t.smart.decrementActiveConns(t.proxyName)
	})
}

func (s *Smart) addActiveConn(proxyName string) {
	s.statsMu.Lock()
	s.getOrCreateStatsUnlocked(proxyName).activeConns.Add(1)
	s.statsMu.Unlock()
}

func (s *Smart) decrementActiveConns(proxyName string) {
	s.statsMu.RLock()
	if st, ok := s.stats[proxyName]; ok {
		st.activeConns.Add(-1)
	}
	s.statsMu.RUnlock()
}

// smartTrackedConn wraps a C.Conn to passively collect RTT and track connections.
type smartTrackedConn struct {
	C.Conn
	smartTracker
}

func newSmartTrackedConn(c C.Conn, s *Smart, proxyName string) *smartTrackedConn {
	s.addActiveConn(proxyName)
	return &smartTrackedConn{
		Conn:         c,
		smartTracker: smartTracker{smart: s, proxyName: proxyName},
	}
}

var _ N.ExtendedConn = (*smartTrackedConn)(nil)

func (c *smartTrackedConn) Read(b []byte) (n int, err error) {
	n, err = c.Conn.Read(b)
	if n > 0 {
		c.recordRead()
	}
	return
}

func (c *smartTrackedConn) ReadBuffer(buffer *buf.Buffer) error {
	err := c.Conn.ReadBuffer(buffer)
	if buffer.Len() > 0 {
		c.recordRead()
	}
	return err
}

func (c *smartTrackedConn) Write(b []byte) (n int, err error) {
	n, err = c.Conn.Write(b)
	if n > 0 {
		c.recordWrite()
	}
	return
}

func (c *smartTrackedConn) WriteBuffer(buffer *buf.Buffer) error {
	err := c.Conn.WriteBuffer(buffer)
	if err == nil {
		c.recordWrite()
	}
	return err
}

func (c *smartTrackedConn) Close() error {
	c.onClose()
	return c.Conn.Close()
}

func (c *smartTrackedConn) Upstream() any {
	return c.Conn
}

func (c *smartTrackedConn) ReaderReplaceable() bool {
	return false
}

func (c *smartTrackedConn) WriterReplaceable() bool {
	return false
}

// smartTrackedPacketConn wraps a C.PacketConn to passively measure first-packet RTT.
type smartTrackedPacketConn struct {
	C.PacketConn
	smartTracker
}

func newSmartTrackedPacketConn(pc C.PacketConn, s *Smart, proxyName string) *smartTrackedPacketConn {
	s.addActiveConn(proxyName)
	return &smartTrackedPacketConn{
		PacketConn:   pc,
		smartTracker: smartTracker{smart: s, proxyName: proxyName},
	}
}

func (c *smartTrackedPacketConn) WriteTo(b []byte, addr net.Addr) (n int, err error) {
	n, err = c.PacketConn.WriteTo(b, addr)
	if n > 0 {
		c.recordWrite()
	}
	return
}

func (c *smartTrackedPacketConn) ReadFrom(b []byte) (n int, addr net.Addr, err error) {
	n, addr, err = c.PacketConn.ReadFrom(b)
	if n > 0 {
		c.recordRead()
	}
	return
}

func (c *smartTrackedPacketConn) WaitReadFrom() (data []byte, put func(), addr net.Addr, err error) {
	data, put, addr, err = c.PacketConn.WaitReadFrom()
	if len(data) > 0 {
		c.recordRead()
	}
	return
}

func (c *smartTrackedPacketConn) Close() error {
	c.onClose()
	return c.PacketConn.Close()
}
