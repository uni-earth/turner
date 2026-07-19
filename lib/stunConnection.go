package turner

import (
	"io"
	"net"
	"time"

	"gortc.io/turnc"
)

type StunConnection struct {
	Conn       net.Conn
	MultiRead  io.Reader
	// 【核心修改】：改为指针类型，确保 Close 时能杀掉底层的保活协程
	CntrClient *turnc.Client
	DataClient *turnc.Client
}

// Read data from peer.
func (c *StunConnection) Read(b []byte) (n int, err error) {
	return c.MultiRead.Read(b)
}

func (c *StunConnection) Write(b []byte) (n int, err error) {
	return c.Conn.Write(b)
}

// Close stops all refreshing loops for permission and removes it from allocation.
func (c *StunConnection) Close() error {
	if c == nil {
		return nil
	}

	// 【核心修改】：显式调用底层 TURN Client 的 Close 方法
	// 发送释放指令给 TURN 服务器，并彻底清理本地的 Keep-Alive 协程防止断连卡死
	if c.CntrClient != nil {
		c.CntrClient.Close()
	}
	if c.DataClient != nil {
		c.DataClient.Close()
	}

	return c.Conn.Close()
}

// LocalAddr is relayed address from TURN server.
func (c *StunConnection) LocalAddr() net.Addr {
	return c.Conn.LocalAddr()
}

// RemoteAddr is peer address.
func (c *StunConnection) RemoteAddr() net.Addr {
	return c.Conn.RemoteAddr()
}

// SetDeadline implements net.Conn.
func (c *StunConnection) SetDeadline(t time.Time) error {
	return c.Conn.SetDeadline(t)
}

func (c *StunConnection) SetReadDeadline(t time.Time) error {
	return c.Conn.SetReadDeadline(t)
}

func (c *StunConnection) SetWriteDeadline(t time.Time) error {
	return c.Conn.SetWriteDeadline(t)
}
