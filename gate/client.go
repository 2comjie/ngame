package gate

import (
	"context"
	"errors"
	"fmt"

	gatepb "github.com/2comjie/ngame/gate/protocol"
	"github.com/2comjie/ngame/packet"
	"github.com/2comjie/ngame/rpc"
	"github.com/2comjie/ntool/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrSessionNotHere  = errors.New("gate: 玩家没有可用的网关会话")
	ErrDeliveryUnknown = errors.New("gate: 消息投递结果未知")
)

const maxRouteRetries = 3

type Client struct {
	rpcClient gatepb.GateClient
	locator   *Locator
}

func NewClient(rpcClient gatepb.GateClient, locator *Locator) *Client {
	if rpcClient == nil || locator == nil {
		panic("gate: 必须提供 Gate RPC Client 和带缓存的 Locator")
	}
	return &Client{
		rpcClient: rpcClient,
		locator:   locator,
	}
}

func (c *Client) Push(ctx context.Context, uid uint64, message *packet.Message) error {
	if uid == 0 {
		return status.Error(codes.InvalidArgument, "gate: 必须提供 Uid")
	}
	gateId, found, err := c.locator.Locate(ctx, uid)
	if err != nil {
		return err
	}
	for attempt := 0; ; attempt++ {
		if !found {
			return ErrSessionNotHere
		}
		response, err := c.rpcClient.Push(rpc.WithInstance(ctx, gateId), &gatepb.PushRequest{
			Uid:    uid,
			Route:  message.Header.Route,
			Values: message.Header.Values,
			Body:   message.Body,
		})
		if err == nil {
			switch response.Code {
			case gatepb.PushCode_PUSH_SENT:
				return nil
			case gatepb.PushCode_PUSH_SESSION_NOT_HERE:
				previous := gateId
				gateId, found, err = c.locator.Refresh(ctx, uid, previous)
				if err != nil {
					return err
				}
				if attempt == maxRouteRetries || (found && gateId == previous) {
					return ErrSessionNotHere
				}
				continue
			case gatepb.PushCode_PUSH_DELIVERY_UNKNOWN:
				err = ErrDeliveryUnknown
			default:
				return fmt.Errorf("gate: 不支持的 PushCode %d", response.Code)
			}
		}
		// 通信错误或投递结果未知，只刷新后续调用的路由，不重发当前消息。
		if _, _, refreshErr := c.locator.Refresh(ctx, uid, gateId); refreshErr != nil {
			logx.Warnf("gate: 推送失败后刷新归属失败 uid=%d error=%v", uid, refreshErr)
		}
		return err
	}
}
