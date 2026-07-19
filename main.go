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
	"gortc.io/turn"
	"gortc.io/turnc"
)

var (
	server    = flag.String("server", "localhost:3478", "TURN server address")
	username  = flag.String("u", "user", "username")
	password  = flag.String("p", "secret", "password")
	socksPort = flag.Int("sp", 8000, "Port to use for SOCKS server")
	socksHost = flag.String("sh", "127.0.0.1", "Host addr to listen on SOCKS5")

	// 【核心优化】：池容量设为 3。足以应对日常网页浏览和视频的瞬发连接，同时将后台资源占用降到极低。
	allocPool = make(chan *PreWarmedAlloc, 3)
)

type PreWarmedAlloc struct {
	ControlConn net.Conn
	Client      *turnc.Client
	Allocation  *turnc.Allocation
	CreatedAt   time.Time // 用于记录分配的存活时间
}

type WrappedConn struct {
	*turner.StunConnection
	ControlConn net.Conn
}

func (w *WrappedConn) Close() error {
	// 关闭数据通道和后台协程
	w.StunConnection.Close()
	// 关闭最底层的控制连接
	return w.ControlConn.Close()
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
					// 成功后微小延时，防止高频发起建立请求
					time.Sleep(100 * time.Millisecond)
				} else {
					// 失败后退让 1 秒，防止网络断开时狂刷日志和 CPU
					time.Sleep(1 * time.Second)
				}
			} else {
				// 池子满了，进入休眠监控
				time.Sleep(500 * time.Millisecond)
			}
		}
	}
}

func createPreWarmedAlloc(ctx context.Context) (*PreWarmedAlloc, error) {
	// TCP 层面保持底层心跳，防止 NAT 悄悄阻断
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
	return &PreWarmedAlloc{
		ControlConn: c,
		Client:      client,
		Allocation:  alloc,
		CreatedAt:   time.Now(),
	}, nil
}

func bindTargetWithAlloc(ctx context.Context, alloc *PreWarmedAlloc, target string) (*WrappedConn, error) {
	var success bool
	defer func() {
		if !success {
			// 绑定失败时，必须将池子中拿出来的废弃连接彻底销毁
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
			CntrClient: alloc.Client, // 传递指针，保障 Close() 时能精准杀死后台协程
			DataClient: clientb,
			Conn:       cb,
			MultiRead:  r,
		},
		ControlConn: alloc.ControlConn,
	}, nil
}

func turnDial(ctx context.Context, network, addr string) (net.Conn, error) {
	var alloc *PreWarmedAlloc
	for {
		select {
		case alloc = <-allocPool:
			// 【核心优化】：强制淘汰超过 4 分钟的老化分配，防断流。
			if time.Since(alloc.CreatedAt) > 4*time.Minute {
				alloc.Client.Close()
				alloc.ControlConn.Close()
				continue
			}
			return bindTargetWithAlloc(ctx, alloc, addr)
		// 【核心优化】：等待时间延长至 5 秒。
		// 浏览器打开复杂网页时瞬间并发可能达 10 个以上，延长排队时间可防止连接直接被丢弃（表现为网页图片加载失败）。
		case <-time.After(5 * time.Second):
			return nil, fmt.Errorf("timeout waiting for TURN allocation")
		}
	}
}

func main() {
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go startPoolMaintainer(ctx)

	conf := &socks5.Config{
		Dial:   turnDial,
		Logger: nil,
	}

	server, err := socks5.New(conf)
	if err != nil {
		return
	}

	_ = server.ListenAndServe("tcp", fmt.Sprintf("%s:%d", *socksHost, *socksPort))
}
