package netKcp

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"time"

	"github.com/2comjie/ngame/network/transport"
	"github.com/2comjie/ngame/network/transport/internal/netconn"
	"github.com/2comjie/ngame/packet"
	kcpgo "github.com/xtaci/kcp-go/v5"
)

type options struct {
	tlsConfig        *tls.Config
	withoutTLS       bool
	codec            *packet.Codec
	handshakeTimeout time.Duration
	writeTimeout     time.Duration
	block            kcpgo.BlockCrypt
	dataShards       int
	parityShards     int
	noDelay          int
	interval         int
	resend           int
	noCongest        int
	sendWindow       int
	readWindow       int
	mtu              int
}

type Option func(*options)

func defaultOptions() options {
	return options{
		codec: packet.NewCodec(0), handshakeTimeout: 5 * time.Second, writeTimeout: 10 * time.Second,
		dataShards: 10, parityShards: 3,
		noDelay: 1, interval: 10, resend: 2, noCongest: 1,
		sendWindow: 128, readWindow: 128, mtu: 1400,
	}
}

// WithTLS 配置TLS，与WithoutTLS互斥。客户端不得跳过证书和主机名校验。
func WithTLS(config *tls.Config) Option {
	return func(o *options) {
		o.tlsConfig = config.Clone()
		if o.tlsConfig.MinVersion < tls.VersionTLS13 {
			o.tlsConfig.MinVersion = tls.VersionTLS13
		}
	}
}

// WithoutTLS 显式关闭TLS，只允许本机回环地址上的监听和拨号。
func WithoutTLS() Option {
	return func(o *options) { o.withoutTLS = true }
}

func WithCodec(codec *packet.Codec) Option {
	return func(o *options) { o.codec = codec }
}

func WithHandshakeTimeout(timeout time.Duration) Option {
	return func(o *options) { o.handshakeTimeout = timeout }
}

func WithWriteTimeout(timeout time.Duration) Option {
	return func(o *options) { o.writeTimeout = timeout }
}

// WithBlockCrypt 配置 KCP 包级加密，不能代替 TLS 或玩家凭证认证。
func WithBlockCrypt(block kcpgo.BlockCrypt) Option {
	return func(o *options) { o.block = block }
}

func WithFEC(dataShards, parityShards int) Option {
	return func(o *options) { o.dataShards, o.parityShards = dataShards, parityShards }
}

func WithNoDelay(noDelay, interval, resend, noCongest int) Option {
	return func(o *options) {
		o.noDelay, o.interval, o.resend, o.noCongest = noDelay, interval, resend, noCongest
	}
}

func WithWindowSize(sendWindow, readWindow int) Option {
	return func(o *options) { o.sendWindow, o.readWindow = sendWindow, readWindow }
}

func WithMTU(mtu int) Option {
	return func(o *options) { o.mtu = mtu }
}

// validate 检查 Listen 和 NewDialer 共用的必需配置及 KCP 参数。
func (o options) validate() {
	if (o.tlsConfig == nil && !o.withoutTLS) || (o.tlsConfig != nil && o.withoutTLS) {
		panic("network/kcp: 必须选择WithTLS或WithoutTLS，不能同时配置")
	}
	if o.codec == nil {
		panic("network/kcp: 必须提供Codec")
	}
	if o.handshakeTimeout <= 0 || o.writeTimeout <= 0 {
		panic("network/kcp: 握手和写入超时必须大于0")
	}
	if o.tlsConfig != nil && o.tlsConfig.MaxVersion != 0 && o.tlsConfig.MaxVersion < o.tlsConfig.MinVersion {
		panic("network/kcp: TLS版本范围错误")
	}
	if o.dataShards < 0 || o.parityShards < 0 || (o.dataShards == 0) != (o.parityShards == 0) || o.dataShards+o.parityShards > 256 {
		panic("network/kcp: FEC分片配置错误")
	}
	if o.sendWindow <= 0 || o.readWindow <= 0 || o.mtu < 50 || o.mtu > 1500 {
		panic("network/kcp: 窗口或MTU配置错误")
	}
}

type listener struct {
	raw     *kcpgo.Listener
	options options
	ctx     context.Context
	cancel  context.CancelFunc
}

func Listen(address string, opts ...Option) (transport.Listener, error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	o.validate()
	if !o.withoutTLS && len(o.tlsConfig.Certificates) == 0 && o.tlsConfig.GetCertificate == nil && o.tlsConfig.GetConfigForClient == nil {
		panic("network/kcp: 服务端必须提供TLS证书")
	}
	raw, err := kcpgo.ListenWithOptions(address, o.block, o.dataShards, o.parityShards)
	if err != nil {
		return nil, err
	}
	if o.withoutTLS && !transport.IsLoopback(raw.Addr()) {
		_ = raw.Close()
		panic("network/kcp: 明文监听只能绑定本机回环地址")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &listener{raw: raw, options: o, ctx: ctx, cancel: cancel}, nil
}

// Accept 可并发调用。每次调用负责一个连接的握手，避免持有全局握手锁。
func (l *listener) Accept() (transport.Conn, error) {
	session, err := l.raw.AcceptKCP()
	if err != nil {
		return nil, err
	}
	configureSession(session, l.options)
	if l.options.withoutTLS {
		return netconn.New(session, l.options.codec, transport.TypeKCP, l.options.writeTimeout), nil
	}
	raw := tls.Server(session, l.options.tlsConfig)
	ctx, cancel := context.WithTimeout(l.ctx, l.options.handshakeTimeout)
	defer cancel()
	if err := raw.HandshakeContext(ctx); err != nil {
		_ = session.Close()
		return nil, err
	}
	return netconn.New(raw, l.options.codec, transport.TypeKCP, l.options.writeTimeout), nil
}

func (l *listener) Close() error {
	l.cancel()
	if err := l.raw.Close(); err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.ErrClosedPipe) {
		return err
	}
	return nil
}

func (l *listener) Addr() net.Addr { return l.raw.Addr() }

type dialer struct {
	address string
	options options
}

func NewDialer(address string, opts ...Option) transport.Dialer {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	o.validate()
	if o.tlsConfig != nil && o.tlsConfig.InsecureSkipVerify {
		panic("network/kcp: 禁止跳过TLS证书校验")
	}
	return &dialer{address: address, options: o}
}

func (d *dialer) DialContext(ctx context.Context) (transport.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, d.options.handshakeTimeout)
	defer cancel()
	host, port, err := net.SplitHostPort(d.address)
	if err != nil {
		return nil, err
	}
	// kcp-go 的 DialWithOptions 不接收 context，先在 ctx 内完成 DNS 解析。
	addresses, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d.options.withoutTLS && !addresses[0].IP.IsLoopback() {
		return nil, transport.ErrInsecure
	}
	session, err := kcpgo.DialWithOptions(net.JoinHostPort(addresses[0].String(), port), d.options.block, d.options.dataShards, d.options.parityShards)
	if err != nil {
		return nil, err
	}
	configureSession(session, d.options)
	if d.options.withoutTLS {
		return netconn.New(session, d.options.codec, transport.TypeKCP, d.options.writeTimeout), nil
	}
	config := d.options.tlsConfig.Clone()
	if config.ServerName == "" {
		config.ServerName = host
	}
	raw := tls.Client(session, config)
	if err := raw.HandshakeContext(ctx); err != nil {
		_ = session.Close()
		return nil, err
	}
	return netconn.New(raw, d.options.codec, transport.TypeKCP, d.options.writeTimeout), nil
}

func configureSession(session *kcpgo.UDPSession, o options) {
	session.SetStreamMode(true)
	session.SetNoDelay(o.noDelay, o.interval, o.resend, o.noCongest)
	session.SetWindowSize(o.sendWindow, o.readWindow)
	if !session.SetMtu(o.mtu) {
		panic("network/kcp: MTU不足以容纳包头")
	}
}
