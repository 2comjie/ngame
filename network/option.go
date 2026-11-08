package network

import (
	"time"

	"github.com/2comjie/ngame/network/transport"
)

type options struct {
	listeners        []transport.Listener
	dialer           transport.Dialer
	auther           Auther
	onSessionBind    func(*Session)
	onSessionEnd     func(*Session)
	onReq            func(*ReqContext)
	zipper           Zipper
	cryptor          Cryptor
	bindTimeout      time.Duration
	heartbeat        time.Duration
	heartbeatTimeout time.Duration
	requestTimeout   time.Duration
	requestQueue     int
	maxUnbound       int
	acceptWorkers    int
	maxBody          int
	maxPending       int
	pushQueue        int
	maxPushBytes     int
}

type Option func(*options)

func defaultOptions() options {
	return options{
		bindTimeout: 5 * time.Second, heartbeat: 30 * time.Second, heartbeatTimeout: 90 * time.Second,
		requestTimeout: 30 * time.Second, requestQueue: 128, maxUnbound: 128, acceptWorkers: 8, maxBody: 8 << 20, maxPending: 1024,
		pushQueue: 128, maxPushBytes: 16 << 20,
	}
}

func WithListener(listener transport.Listener) Option {
	return func(o *options) { o.listeners = append(o.listeners, listener) }
}

func WithDialer(dialer transport.Dialer) Option {
	return func(o *options) { o.dialer = dialer }
}

func WithAuther(auther Auther) Option {
	return func(o *options) { o.auther = auther }
}

// OnSessionBind 在 BindRsp 成功写入并绑定 Uid 后调用。
func OnSessionBind(handler func(*Session)) Option {
	return func(o *options) { o.onSessionBind = handler }
}

// OnSessionEnd 只对绑定成功的 Session 调用，且在其业务回调结束后调用。
func OnSessionEnd(handler func(*Session)) Option {
	return func(o *options) { o.onSessionEnd = handler }
}

// OnReq 在同一 Session 的处理协程中串行调用，回调应响应 ReqContext 的取消。
func OnReq(handler func(*ReqContext)) Option {
	return func(o *options) { o.onReq = handler }
}

func WithZipper(zipper Zipper) Option {
	return func(o *options) { o.zipper = zipper }
}

func WithCryptor(cryptor Cryptor) Option {
	return func(o *options) { o.cryptor = cryptor }
}

func WithBindTimeout(timeout time.Duration) Option {
	return func(o *options) { o.bindTimeout = timeout }
}

func WithHeartbeat(interval, timeout time.Duration) Option {
	return func(o *options) { o.heartbeat, o.heartbeatTimeout = interval, timeout }
}

// WithRequestTimeout 设置业务处理期限，0 表示不设置期限。
func WithRequestTimeout(timeout time.Duration) Option {
	return func(o *options) { o.requestTimeout = timeout }
}

func WithRequestQueue(capacity int) Option {
	return func(o *options) { o.requestQueue = capacity }
}

func WithMaxUnbound(count int) Option {
	return func(o *options) { o.maxUnbound = count }
}

// WithAcceptWorkers 设置每个 Listener 并发执行安全握手的接入协程数。
func WithAcceptWorkers(count int) Option {
	return func(o *options) { o.acceptWorkers = count }
}

func WithMaxBody(size int) Option {
	return func(o *options) { o.maxBody = size }
}

func WithMaxPending(count int) Option {
	return func(o *options) { o.maxPending = count }
}

// WithPushQueue 设置客户端待处理推送的数量和数据字节上限，默认128条、16MiB。
// 字节统计包含固定帧头、解码后的Body及Header键值；队列满时关闭连接，不丢弃推送后继续运行。
func WithPushQueue(capacity, maxBytes int) Option {
	return func(o *options) { o.pushQueue, o.maxPushBytes = capacity, maxBytes }
}
