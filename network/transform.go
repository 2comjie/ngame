package network

import "github.com/2comjie/ngame/packet"

func encodeBody(o options, message *packet.Message) error {
	if len(message.Body) > o.maxBody {
		return ErrBodyTooLarge
	}
	if o.zipper != nil {
		body, err := o.zipper.Zip(message.Header.Route, message.Body)
		if err != nil {
			return err
		}
		message.Body = body
	}
	if o.cryptor != nil {
		body, err := o.cryptor.Encrypt(message)
		if err != nil {
			return err
		}
		message.Body = body
	}
	return nil
}

func decodeBody(o options, message *packet.Message) error {
	if o.cryptor != nil {
		body, err := o.cryptor.Decrypt(message)
		if err != nil {
			return err
		}
		message.Body = body
	}
	if o.zipper != nil {
		body, err := o.zipper.Unzip(message.Header.Route, message.Body)
		if err != nil {
			return err
		}
		message.Body = body
	}
	if len(message.Body) > o.maxBody {
		return ErrBodyTooLarge
	}
	return nil
}
