package network

import "github.com/2comjie/ngame/packet"

type Zipper interface {
	Zip(route uint32, body []byte) ([]byte, error)
	Unzip(route uint32, body []byte) ([]byte, error)
}

// Cryptor 接收完整消息，可以读取 Header 中的加解密参数，返回处理后的 Body。
type Cryptor interface {
	Encrypt(message *packet.Message) ([]byte, error)
	Decrypt(message *packet.Message) ([]byte, error)
}
