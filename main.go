package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/armon/go-socks5"
	turner "github.com/staaldraad/turner/lib"
	"gortc.io/stun"
	"gortc.io/turn"
	"gortc.io/turnc"
)

var (
	server    = flag.String("server", "localhost:3478", "TURN server address")
	username  = flag.String("u", "user", "username")
	password  = flag.String("p", "secret", "password")
	socksPort = flag.Int("sp", 8000, "Port to use for SOCKS server")
	socksHost = flag.String("sh", "127.0.0.1", "Host addr to listen on SOCKS5")

	allocPool = make(chan *PreWarmedAlloc, 10)
)

type PreWarmedAlloc struct {
	ControlConn net.Conn
	Client      *turnc.Client
	Allocation  *turnc.Allocation
}

type WrappedConn struct {
	*turner.StunConnection
	ControlConn net.Conn
}

func (w *WrappedConn) Close() error {
	w.ControlConn.Close()
	return w.StunConnection.Close()
}

func startPoolMaintainer(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if len(allocPool) < cap(allocPool) {
				alloc, err := createPreWarmedAlloc(ctx)
				if err == nil {
					allocPool <- alloc
					time.Sleep(100 * time.Millisecond)
				} else {
					time.Sleep(1 * time.Second)
				}
			} else {
				time.Sleep(500 * time.Millisecond)
			}
		}
	}
}

func createPreWarmedAlloc(ctx context.Context) (*PreWarmedAlloc, error) {
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 15 * time.Second}
	cRaw, err := dialer.DialContext(ctx, "tcp", *server)
	if err != nil {
		return nil, err
	}
	c := cRaw.(*net.TCPConn)

	var success bool
	defer func() {
		if !success {
			c.Close()
		}
	}()

	client, err := turnc.New(turnc.Options{Conn: c, Username: *username, Password: *password})
	if err != nil {
		return nil, err
	}

	alloc, err := client.AllocateTCP()
	if err != nil {
		client.Close()
		return nil, err
	}

	success = true
	return &PreWarmedAlloc{ControlConn: c, Client: client, Allocation: alloc}, nil
}

func bindTargetWithAlloc(ctx context.Context, alloc *PreWarmedAlloc, target string) (*WrappedConn, error) {
	var success bool
	defer func() {
		if !success {
			alloc.Client.Close()
			alloc.ControlConn.Close()
		}
	}()

	peerAddr, err := net.ResolveTCPAddr("tcp", target)
	if err != nil {
		return nil, err
	}

	permission, err := alloc.Allocation.Create(peerAddr.IP)
	if err != nil {
		return nil, err
	}

	conn, err := permission.CreateTCP(peerAddr)
	if err != nil {
		return nil, err
	}

	connid, err := conn.Connect()
	if err != nil {
		return nil, err
	}

	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 15 * time.Second}
	cbRaw, err := dialer.DialContext(ctx, "tcp", *server)
	if err != nil {
		return nil, err
	}
	cb := cbRaw.(*net.TCPConn)

	sideChanReader, sideChanWriter := io.Pipe()
	r := io.MultiReader(sideChanReader, cb)

	clientb, err := turnc.NewData(turnc.Options{Conn: cb, Username: *username}, *sideChanWriter)
	if err != nil {
		cb.Close()
		return nil, err
	}

	connD, err := permission.CreateTCP(peerAddr)
	if err != nil {
		clientb.Close()
		cb.Close()
		return nil, err
	}

	_, err = clientb.ConnectionBind(turn.ConnectionID(binary.BigEndian.Uint32(connid.Value)), alloc.Allocation, connD)
	if err != nil {
		clientb.Close()
		cb.Close()
		return nil, err
	}

	success = true
	return &WrappedConn{
		StunConnection: &turner.StunConnection{
			CntrClient: *alloc.Client, DataClient: *clientb, Conn: cb, MultiRead: r,
		},
		ControlConn: alloc.ControlConn,
	}, nil
}

func turnDial(ctx context.Context, network, addr string) (net.Conn, error) {
	var alloc *PreWarmedAlloc
	select {
	case alloc = <-allocPool:
	case <-time.After(1500 * time.Millisecond):
		return nil, fmt.Errorf("timeout waiting for TURN allocation")
	}
	return bindTargetWithAlloc(ctx, alloc, addr)
}

func main() {
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go startPoolMaintainer(ctx)

	conf := &socks5.Config{
		Dial: turnDial,
		Logger: nil, 
	}
	
	server, err := socks5.New(conf)
	if err != nil {
		return
	}

	_ = server.ListenAndServe("tcp", fmt.Sprintf("%s:%d", *socksHost, *socksPort))
}
