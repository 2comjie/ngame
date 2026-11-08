package netWs

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/2comjie/ngame/network/transport"
	"github.com/2comjie/ngame/packet"
	"github.com/gorilla/websocket"
)

const Subprotocol = "ngame"

var ErrSubprotocol = errors.New("network/ws: WebSocket子协议错误")

type options struct {
	path             string
	tlsConfig        *tls.Config
	withoutTLS       bool
	codec            *packet.Codec
	handshakeTimeout time.Duration
	writeTimeout     time.Duration
	origins          map[string]struct{}
	header           http.Header
}

type Option func(*options)

func defaultOptions() options {
	return options{path: "/", codec: packet.NewCodec(0), handshakeTimeout: 5 * time.Second, writeTimeout: 10 * time.Second}
}

func WithPath(path string) Option {
	return func(o *options) { o.path = path }
}

func WithTLS(config *tls.Config) Option {
	return func(o *options) {
		o.tlsConfig = config.Clone()
		if o.tlsConfig.MinVersion < tls.VersionTLS13 {
			o.tlsConfig.MinVersion = tls.VersionTLS13
		}
		o.tlsConfig.NextProtos = []string{"http/1.1"}
	}
}

// WithoutTLS 显式关闭TLS，仅用于本机回环地址上的代理后端或本地调试。
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

// WithOrigins 配置允许的网页来源，例如 https://game.example.com，不包含路径。
// 配置后只接受列表内的网页来源；未配置时只接受同源网页。无Origin的原生客户端仍可接入。
func WithOrigins(origins ...string) Option {
	return func(o *options) {
		o.origins = make(map[string]struct{}, len(origins))
		for _, origin := range origins {
			parsed, err := url.Parse(origin)
			if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && parsed.Scheme != "http") || origin != parsed.Scheme+"://"+parsed.Host {
				panic("network/ws: Origin必须是包含协议和主机的完整来源，不包含路径")
			}
			o.origins[strings.ToLower(origin)] = struct{}{}
		}
	}
}

// WithHeader 配置Go客户端升级请求的HTTP头，浏览器的Origin由浏览器自动添加。
func WithHeader(header http.Header) Option {
	return func(o *options) { o.header = header.Clone() }
}

func (o options) validate() {
	if (o.tlsConfig == nil && !o.withoutTLS) || (o.tlsConfig != nil && o.withoutTLS) {
		panic("network/ws: 必须选择WithTLS或WithoutTLS，不能同时配置")
	}
	if o.codec == nil {
		panic("network/ws: 必须提供Codec")
	}
	if o.handshakeTimeout <= 0 || o.writeTimeout <= 0 {
		panic("network/ws: 握手和写入超时必须大于0")
	}
	if o.path == "" || o.path[0] != '/' {
		panic("network/ws: path必须以/开头")
	}
	if o.tlsConfig != nil && o.tlsConfig.MaxVersion != 0 && o.tlsConfig.MaxVersion < o.tlsConfig.MinVersion {
		panic("network/ws: TLS版本范围错误")
	}
}

func (o options) checkOrigin(request *http.Request) bool {
	values := request.Header.Values("Origin")
	if len(values) == 0 {
		return true
	}
	if len(values) != 1 {
		return false
	}
	origin, err := url.Parse(values[0])
	if err != nil || origin.Host == "" || (origin.Scheme != "https" && origin.Scheme != "http") || values[0] != origin.Scheme+"://"+origin.Host {
		return false
	}
	if o.origins != nil {
		_, allowed := o.origins[strings.ToLower(values[0])]
		return allowed
	}
	scheme := "https"
	if o.withoutTLS {
		scheme = "http"
	}
	return origin.Scheme == scheme && strings.EqualFold(origin.Host, request.Host)
}

type listener struct {
	raw      net.Listener
	options  options
	server   *http.Server
	upgrader websocket.Upgrader
	accepts  chan *conn
	done     chan struct{}
	mutex    sync.Mutex
	closed   bool
}

func Listen(address string, opts ...Option) (transport.Listener, error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	o.validate()
	if !o.withoutTLS && len(o.tlsConfig.Certificates) == 0 && o.tlsConfig.GetCertificate == nil && o.tlsConfig.GetConfigForClient == nil {
		panic("network/ws: 服务端必须提供TLS证书")
	}
	raw, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	if o.withoutTLS && !transport.IsLoopback(raw.Addr()) {
		_ = raw.Close()
		panic("network/ws: 明文监听只能绑定本机回环地址")
	}
	l := &listener{raw: raw, options: o, accepts: make(chan *conn, 128), done: make(chan struct{}),
		upgrader: websocket.Upgrader{HandshakeTimeout: o.handshakeTimeout, Subprotocols: []string{Subprotocol}, CheckOrigin: o.checkOrigin}}
	l.server = &http.Server{Handler: l, ReadHeaderTimeout: o.handshakeTimeout, WriteTimeout: o.handshakeTimeout, IdleTimeout: o.handshakeTimeout}
	go func() {
		var serving net.Listener = raw
		if !o.withoutTLS {
			serving = tls.NewListener(raw, o.tlsConfig)
		}
		if err := l.server.Serve(serving); err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			slog.Error("network/ws: 监听失败", "error", err)
			_ = l.Close()
		}
	}()
	return l, nil
}

func (l *listener) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path != l.options.path {
		http.NotFound(writer, request)
		return
	}
	if !slices.Contains(websocket.Subprotocols(request), Subprotocol) {
		http.Error(writer, ErrSubprotocol.Error(), http.StatusBadRequest)
		return
	}
	raw, err := l.upgrader.Upgrade(writer, request, nil)
	if err != nil {
		return
	}
	value := newConn(raw, l.options)
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if !l.closed {
		select {
		case l.accepts <- value:
			return
		default:
		}
	}
	_ = value.Close()
}

func (l *listener) Accept() (transport.Conn, error) {
	select {
	case value := <-l.accepts:
		l.mutex.Lock()
		closed := l.closed
		l.mutex.Unlock()
		if closed {
			_ = value.Close()
			return nil, net.ErrClosed
		}
		return value, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *listener) Close() error {
	l.mutex.Lock()
	if l.closed {
		l.mutex.Unlock()
		return nil
	}
	l.closed = true
	close(l.done)
	for {
		select {
		case value := <-l.accepts:
			_ = value.Close()
		default:
			l.mutex.Unlock()
			if err := l.server.Close(); err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
				_ = l.raw.Close()
				return err
			}
			if err := l.raw.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				return err
			}
			return nil
		}
	}
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
		panic("network/ws: 禁止跳过TLS证书校验")
	}
	parsed, err := url.Parse(address)
	scheme := "wss"
	if o.withoutTLS {
		scheme = "ws"
	}
	if err != nil || parsed.Scheme != scheme || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		panic("network/ws: 地址协议必须与TLS配置一致")
	}
	if parsed.Path == "" || parsed.Path == "/" {
		parsed.Path = o.path
	}
	return &dialer{address: parsed.String(), options: o}
}

func (d *dialer) DialContext(ctx context.Context) (transport.Conn, error) {
	dialer := websocket.Dialer{TLSClientConfig: d.options.tlsConfig, HandshakeTimeout: d.options.handshakeTimeout, Subprotocols: []string{Subprotocol}}
	if d.options.withoutTLS {
		dialer.NetDialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			var dialer net.Dialer
			raw, err := dialer.DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			if !transport.IsLoopback(raw.RemoteAddr()) {
				_ = raw.Close()
				return nil, transport.ErrInsecure
			}
			return raw, nil
		}
	}
	raw, response, err := dialer.DialContext(ctx, d.address, d.options.header)
	if err != nil {
		if response != nil {
			_ = response.Body.Close()
		}
		return nil, err
	}
	if raw.Subprotocol() != Subprotocol {
		_ = raw.Close()
		return nil, ErrSubprotocol
	}
	return newConn(raw, d.options), nil
}
