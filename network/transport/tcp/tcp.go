package netTcp

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"time"

	"github.com/2comjie/ngame/network/transport"
	"github.com/2comjie/ngame/network/transport/internal/netconn"
	"github.com/2comjie/ngame/packet"
)

type options struct {
	tlsConfig        *tls.Config
	codec            *packet.Codec
	handshakeTimeout time.Duration
	writeTimeout     time.Duration
	keepAlive        time.Duration
}

type Option func(*options)

func defaultOptions() options {
	return options{
		codec: packet.NewCodec(0), handshakeTimeout: 5 * time.Second,
		writeTimeout: 10 * time.Second, keepAlive: 30 * time.Second,
	}
}

// WithTLS 是必需配置，最低使用TLS 1.3。
func WithTLS(config *tls.Config) Option {
	return func(o *options) {
		o.tlsConfig = config.Clone()
		if o.tlsConfig.MinVersion < tls.VersionTLS13 {
			o.tlsConfig.MinVersion = tls.VersionTLS13
		}
	}
}

func WithCodec(codec *packet.Codec) Option {
	return func(o *options) { o.codec = codec }
}

// WithHandshakeTimeout 限制服务端TLS握手，以及客户端拨号和TLS握手的总耗时。
func WithHandshakeTimeout(timeout time.Duration) Option {
	return func(o *options) { o.handshakeTimeout = timeout }
}

func WithWriteTimeout(timeout time.Duration) Option {
	return func(o *options) { o.writeTimeout = timeout }
}

// WithKeepAlive 设置TCP保活周期，负值表示禁用TCP保活。
func WithKeepAlive(period time.Duration) Option {
	return func(o *options) { o.keepAlive = period }
}

func (o options) validate() {
	if o.tlsConfig == nil || o.codec == nil {
		panic("network/tcp: 必须提供TLS配置和Codec")
	}
	if o.handshakeTimeout <= 0 || o.writeTimeout <= 0 {
		panic("network/tcp: 握手和写入超时必须大于0")
	}
	if o.tlsConfig.MaxVersion != 0 && o.tlsConfig.MaxVersion < o.tlsConfig.MinVersion {
		panic("network/tcp: TLS版本范围错误")
	}
}

type listener struct {
	raw     net.Listener
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
	if len(o.tlsConfig.Certificates) == 0 && o.tlsConfig.GetCertificate == nil && o.tlsConfig.GetConfigForClient == nil {
		panic("network/tcp: 服务端必须提供TLS证书")
	}
	config := net.ListenConfig{KeepAlive: o.keepAlive}
	raw, err := config.Listen(context.Background(), "tcp", address)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &listener{raw: raw, options: o, ctx: ctx, cancel: cancel}, nil
}

// Accept 可并发调用，TLS握手成功后才返回消息连接。
func (l *listener) Accept() (transport.Conn, error) {
	conn, err := l.raw.Accept()
	if err != nil {
		return nil, err
	}
	raw := tls.Server(conn, l.options.tlsConfig)
	ctx, cancel := context.WithTimeout(l.ctx, l.options.handshakeTimeout)
	defer cancel()
	if err := raw.HandshakeContext(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return netconn.New(raw, l.options.codec, transport.TypeTCP, l.options.writeTimeout), nil
}

func (l *listener) Close() error {
	l.cancel()
	if err := l.raw.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
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
	if o.tlsConfig.InsecureSkipVerify {
		panic("network/tcp: 禁止跳过TLS证书校验")
	}
	return &dialer{address: address, options: o}
}

func (d *dialer) DialContext(ctx context.Context) (transport.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, d.options.handshakeTimeout)
	defer cancel()
	dialer := tls.Dialer{
		NetDialer: &net.Dialer{KeepAlive: d.options.keepAlive},
		Config:    d.options.tlsConfig,
	}
	// 标准库负责主机名推导、证书校验及握手失败时的连接释放。
	raw, err := dialer.DialContext(ctx, "tcp", d.address)
	if err != nil {
		return nil, err
	}
	return netconn.New(raw.(*tls.Conn), d.options.codec, transport.TypeTCP, d.options.writeTimeout), nil
}
