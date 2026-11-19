package gate

import (
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/2comjie/ngame/app"
	gatepb "github.com/2comjie/ngame/gate/protocol"
	"github.com/2comjie/ngame/network"
	netTcp "github.com/2comjie/ngame/network/transport/tcp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestGateRegisterFailureRollsBackListenersAndComponents(t *testing.T) {
	registerErr := errors.New("注册失败")
	var events []string
	fixture := prepareUidGate(t, uidRedis(t), Config{
		Registry: &uidRegistry{err: registerErr},
		Components: []app.Component{&app.CommonComponent{
			MStart: func() error {
				events = append(events, "start")
				return nil
			},
			MRequestStop: func() { events = append(events, "stop") },
			MShutdown: func(context.Context) error {
				events = append(events, "shutdown")
				return nil
			},
		}},
	})
	if err := fixture.app.Start(); !errors.Is(err, registerErr) {
		t.Fatalf("Start = %v, want %v", err, registerErr)
	}
	if want := []string{"start", "stop", "shutdown"}; !slices.Equal(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	uidWait(t, fixture.app.syncDone)
	for _, address := range []string{fixture.address, fixture.app.Instance().RpcTarget()} {
		conn, err := net.DialTimeout("tcp", address, time.Second)
		if err == nil {
			_ = conn.Close()
			t.Errorf("注册失败后仍在监听 %s", address)
		}
	}
}

func TestGateShutdownWaitsForSessionAndRPC(t *testing.T) {
	for _, blocked := range []string{"session", "rpc"} {
		t.Run(blocked, func(t *testing.T) {
			center := &uidRegistry{}
			sessionEnding := make(chan struct{})
			sessionExited := make(chan struct{})
			rpcEntered := make(chan struct{})
			rpcExited := make(chan struct{})
			allowSessionExit := make(chan struct{})
			allowRPCExit := make(chan struct{})
			releaseSession := sync.OnceFunc(func() { close(allowSessionExit) })
			releaseRPC := sync.OnceFunc(func() { close(allowRPCExit) })
			defer releaseSession()
			defer releaseRPC()
			componentClosed := make(chan struct{})
			fixture := prepareUidGate(t, uidRedis(t), Config{
				Registry: center,
				NetworkOptions: []network.Option{network.OnSessionEnd(func(*network.Session) {
					defer close(sessionExited)
					close(sessionEnding)
					<-allowSessionExit
				})},
				RPCOptions: []grpc.ServerOption{grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
					defer close(rpcExited)
					close(rpcEntered)
					<-allowRPCExit
					return handler(ctx, req)
				})},
				Components: []app.Component{&app.CommonComponent{
					MRequestStop: func() {
						select {
						case <-sessionExited:
						default:
							t.Error("Session 回调退出前停止了业务组件")
						}
						select {
						case <-rpcExited:
						default:
							t.Error("在途 RPC 退出前停止了业务组件")
						}
					},
					MShutdown: func(context.Context) error {
						close(componentClosed)
						return nil
					},
				}},
			})
			if err := fixture.app.Start(); err != nil {
				t.Fatal(err)
			}
			fixture.connect(t)
			conn, err := grpc.NewClient(fixture.app.Instance().RpcTarget(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			callDone := make(chan struct{})
			go func() {
				defer close(callDone)
				_, _ = gatepb.NewGateClient(conn).SessionCount(ctx, &emptypb.Empty{})
			}()
			uidWait(t, rpcEntered)

			shutdownCtx, stopWaiting := context.WithTimeout(ctx, 250*time.Millisecond)
			defer stopWaiting()
			shutdownDone := make(chan error, 1)
			go func() { shutdownDone <- fixture.app.Shutdown(shutdownCtx) }()
			uidWait(t, center.registration.Done())
			uidWait(t, sessionEnding)
			if blocked == "session" {
				releaseRPC()
				uidWait(t, rpcExited)
			} else {
				releaseSession()
				uidWait(t, sessionExited)
			}
			if err := uidWait(t, shutdownDone); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Shutdown = %v, want timeout while %s is blocked", err, blocked)
			}
			select {
			case <-componentClosed:
				t.Fatalf("%s 还在执行，业务组件已关闭", blocked)
			default:
			}

			releaseSession()
			releaseRPC()
			if err := fixture.app.Shutdown(ctx); err != nil {
				t.Fatal(err)
			}
			uidWait(t, componentClosed)
			uidWait(t, callDone)
		})
	}
}

func TestGateRegistrationLossStopsAcceptingSessions(t *testing.T) {
	center := &uidRegistry{}
	fixture := prepareUidGate(t, uidRedis(t), Config{Registry: center})
	if err := fixture.app.Start(); err != nil {
		t.Fatal(err)
	}
	session := fixture.connect(t).session
	if err := center.registration.Unregister(t.Context()); err != nil {
		t.Fatal(err)
	}
	uidWait(t, fixture.app.Context().Done())
	if ended := uidWait(t, fixture.ended); ended != session {
		t.Fatal("注册失效后未关闭原有 Session")
	}
	client := network.NewClient(network.WithDialer(netTcp.NewDialer(fixture.address, netTcp.WithoutTLS())))
	defer client.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := client.Connect(ctx, []byte("ticket")); err == nil {
		t.Fatal("注册失效后仍可建立新 Session")
	}
}
