package node

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	nodepb "github.com/2comjie/ngame/node/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestRequestComponentOptional(t *testing.T) {
	for _, mode := range []string{"api", "game"} {
		t.Run(mode, func(t *testing.T) {
			n := nodeApp(t, newMetaRegistry())
			var calls atomic.Int32
			if mode == "game" {
				n.AddComponent(NewRequestComponent(n, func(ctx context.Context, request *nodepb.CallRequest) (*nodepb.CallResponse, error) {
					calls.Add(1)
					if request.Route == 2 {
						return nil, status.Error(codes.FailedPrecondition, "业务拒绝")
					}
					return &nodepb.CallResponse{Replied: true, Body: request.Body}, nil
				}))
			}
			if err := n.Start(); err != nil {
				t.Fatal(err)
			}
			connection, err := grpc.NewClient(n.Instance().RpcTarget(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			client := nodepb.NewNodeClient(connection)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			request := &nodepb.CallRequest{Uid: 1, GateId: "gate", Route: 1, Body: []byte("hello")}
			response, err := client.Call(ctx, request)
			if mode == "api" {
				if status.Code(err) != codes.Unimplemented {
					t.Fatalf("未安装组件的节点提供了网关请求服务: %v", err)
				}
				return
			}
			if err != nil || !response.Replied || string(response.Body) != "hello" {
				t.Fatalf("请求组件未处理网关消息: response=%v error=%v", response, err)
			}
			request.Uid = 0
			if _, err := client.Call(ctx, request); status.Code(err) != codes.InvalidArgument || calls.Load() != 1 {
				t.Fatalf("无效请求进入业务处理: calls=%d error=%v", calls.Load(), err)
			}
			request.Uid = 1
			request.Route = 2
			if _, err := client.Call(ctx, request); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("业务错误未直接返回: %v", err)
			}
		})
	}
}

func TestRequestComponentCancelsOnNodeStop(t *testing.T) {
	n := nodeApp(t, newMetaRegistry())
	entered := make(chan struct{})
	n.AddComponent(NewRequestComponent(n, func(ctx context.Context, request *nodepb.CallRequest) (*nodepb.CallResponse, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	connection, err := grpc.NewClient(n.Instance().RpcTarget(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := nodepb.NewNodeClient(connection).Call(ctx, &nodepb.CallRequest{Uid: 1, GateId: "gate", Route: 1})
		result <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("请求未进入组件")
	}
	n.RequestStop()
	select {
	case err := <-result:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("节点停止后请求未取消: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("节点停止后请求仍未退出")
	}
}
