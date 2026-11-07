package packet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/2comjie/ngame/packet/internal/protocol"
	"github.com/2comjie/ntool/util/buffer"
	"google.golang.org/protobuf/proto"
)

const (
	Magic           uint16 = 0x822f
	HeaderSize             = 24
	DefaultMaxFrame        = 4 << 20
	MaxHeaderSize          = 16 << 10
	MaxBindBodySize        = 16 << 10
	PingBodySize           = 8
)

var (
	ErrMagic       = errors.New("packet: magic错误")
	ErrFrameLength = errors.New("packet: 帧长度错误")
	ErrHeader      = errors.New("packet: header错误")
	ErrType        = errors.New("packet: 消息类型错误")
	ErrRoute       = errors.New("packet: route错误")
	ErrSeq         = errors.New("packet: seq错误")
	ErrBody        = errors.New("packet: body错误")
)

// Codec 编解码固定帧头、Protobuf 扩展 Header 和不透明的 Body。
// 固定帧头采用大端序：Magic(2)、Type(2)、FrameLen(4)、Route(4)、Seq(8)、HeaderLen(4)。
// FrameLen 包含固定帧头，HeaderLen 只包含扩展 Header；Codec 可并发用于不同连接。
type Codec struct {
	maxFrame uint32
}

// NewCodec 创建编解码器。maxFrame 为 0 时使用默认上限，其他值不得小于 HeaderSize。
func NewCodec(maxFrame uint32) *Codec {
	if maxFrame == 0 {
		maxFrame = DefaultMaxFrame
	}
	if maxFrame < HeaderSize {
		panic("packet: maxFrame小于固定帧头长度")
	}
	return &Codec{maxFrame: maxFrame}
}

// Read 从流中读取一个包，不读取后续包。返回的消息使用完后必须 Release。
// 协议错误后调用方应关闭连接，不应继续尝试解析这个流。
func (c *Codec) Read(r io.Reader) (*Message, error) {
	var fixed [HeaderSize]byte
	if _, err := io.ReadFull(r, fixed[:]); err != nil {
		return nil, err
	}
	if binary.BigEndian.Uint16(fixed[0:2]) != Magic {
		return nil, ErrMagic
	}
	frameLen := binary.BigEndian.Uint32(fixed[4:8])
	if frameLen < HeaderSize || frameLen > c.maxFrame || uint64(frameLen) > uint64(^uint(0)>>1) {
		return nil, ErrFrameLength
	}
	headerLen := binary.BigEndian.Uint32(fixed[20:24])
	if headerLen > MaxHeaderSize || headerLen > frameLen-HeaderSize {
		return nil, ErrHeader
	}
	message := &Message{Header: Header{
		Type:  Type(binary.BigEndian.Uint16(fixed[2:4])),
		Route: binary.BigEndian.Uint32(fixed[8:12]),
		Seq:   binary.BigEndian.Uint64(fixed[12:20]),
	}}
	message.buf = buffer.NewLinkBuffer(int(frameLen))
	data, err := message.buf.Malloc(int(frameLen))
	if err != nil {
		message.Release()
		return nil, err
	}
	copy(data, fixed[:])
	if _, err := io.ReadFull(r, data[HeaderSize:]); err != nil {
		message.Release()
		return nil, err
	}
	if err := message.buf.Flush(); err != nil {
		message.Release()
		return nil, err
	}
	if headerLen > 0 {
		var values protocol.HeaderValues
		if err := proto.Unmarshal(data[HeaderSize:HeaderSize+int(headerLen)], &values); err != nil {
			message.Release()
			return nil, fmt.Errorf("%w: %w", ErrHeader, err)
		}
		message.Header.Values = values.Values
	}
	message.Body = data[HeaderSize+int(headerLen):]
	if err := c.Validate(message); err != nil {
		message.Release()
		return nil, err
	}
	return message, nil
}

// Encode 返回独立持有数据的池化 buffer，调用方发送完成后必须调用 Close。
// 返回后可以修改原消息，不影响已经编码的数据。
func (c *Codec) Encode(message *Message) (*buffer.LinkBuffer, error) {
	if err := c.Validate(message); err != nil {
		return nil, err
	}
	values := &protocol.HeaderValues{Values: message.Header.Values}
	headerLen := proto.Size(values)
	if headerLen > MaxHeaderSize {
		return nil, ErrHeader
	}
	frameLen := uint64(HeaderSize) + uint64(headerLen) + uint64(len(message.Body))
	if frameLen > uint64(c.maxFrame) || frameLen > uint64(^uint(0)>>1) {
		return nil, ErrFrameLength
	}
	frame := buffer.NewLinkBuffer(int(frameLen))
	data, err := frame.Malloc(int(frameLen))
	if err != nil {
		_ = frame.Close()
		return nil, err
	}
	binary.BigEndian.PutUint16(data[0:2], Magic)
	binary.BigEndian.PutUint16(data[2:4], uint16(message.Header.Type))
	binary.BigEndian.PutUint32(data[4:8], uint32(frameLen))
	binary.BigEndian.PutUint32(data[8:12], message.Header.Route)
	binary.BigEndian.PutUint64(data[12:20], message.Header.Seq)
	binary.BigEndian.PutUint32(data[20:24], uint32(headerLen))
	if _, err := (proto.MarshalOptions{}).MarshalAppend(data[HeaderSize:HeaderSize], values); err != nil {
		_ = frame.Close()
		return nil, fmt.Errorf("%w: %w", ErrHeader, err)
	}
	copy(data[HeaderSize+headerLen:], message.Body)
	if err := frame.Flush(); err != nil {
		_ = frame.Close()
		return nil, err
	}
	return frame, nil
}

// Validate 校验消息类型对应的路由、序号、控制包及 Header 约定。
// 帧长度和扩展 Header 编码长度分别在 Read、Encode 中校验。
func (c *Codec) Validate(message *Message) error {
	header := &message.Header
	switch header.Type {
	case Req:
		if header.Route == 0 {
			return ErrRoute
		}
	case Rsp:
		if header.Route == 0 {
			return ErrRoute
		}
		if header.Seq == 0 {
			return ErrSeq
		}
	case Push:
		if header.Route == 0 {
			return ErrRoute
		}
		if header.Seq != 0 {
			return ErrSeq
		}
	case Ping, Pong, BindReq, BindRsp:
		if header.Route != 0 {
			return ErrRoute
		}
		if header.Seq != 0 {
			return ErrSeq
		}
		if header.Type == Ping || header.Type == Pong {
			if len(header.Values) != 0 {
				return ErrHeader
			}
			if len(message.Body) != PingBodySize {
				return ErrBody
			}
		} else if len(message.Body) > MaxBindBodySize {
			return ErrBody
		}
	default:
		return ErrType
	}
	for key := range header.Values {
		if key == "" || key != strings.ToLower(key) {
			return ErrHeader
		}
	}
	return nil
}
