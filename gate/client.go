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

type SessionRoute struct {
	GateId    string `json:"gate_id"`
	SessionId uint64 `json:"session_id"`
}

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
	route, found, err := c.locator.Locate(ctx, uid)
	if err != nil {
		return err
	}
	for attempt := 0; ; attempt++ {
		if !found {
			return ErrSessionNotHere
		}
		response, err := c.rpcClient.Push(rpc.WithInstance(ctx, route.GateId), &gatepb.PushRequest{
			Uid:       uid,
			SessionId: route.SessionId,
			Route:     message.Header.Route,
			Values:    message.Header.Values,
			Body:      message.Body,
		})
		if err != nil {
			if _, _, refreshErr := c.locator.Refresh(ctx, uid, route); refreshErr != nil {
				logx.Warnf("gate: 推送失败后刷新归属失败 uid=%d error=%v", uid, refreshErr)
			}
			return err
		}
		switch response.Code {
		case gatepb.PushCode_PUSH_SENT:
			return nil
		case gatepb.PushCode_PUSH_DELIVERY_UNKNOWN:
			if _, _, refreshErr := c.locator.Refresh(ctx, uid, route); refreshErr != nil {
				logx.Warnf("gate: 投递结果未知时刷新归属失败 uid=%d error=%v", uid, refreshErr)
			}
			return ErrDeliveryUnknown
		case gatepb.PushCode_PUSH_SESSION_NOT_HERE:
			previous := route
			route, found, err = c.locator.Refresh(ctx, uid, previous)
			if err != nil {
				return err
			}
			if attempt == maxRouteRetries || (found && route == previous) {
				return ErrSessionNotHere
			}
		default:
			return fmt.Errorf("gate: 不支持的 PushCode %d", response.Code)
		}
	}
}
