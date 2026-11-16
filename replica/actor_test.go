package replica_test

import (
	"context"
	"testing"
	"time"

	"github.com/2comjie/ngame/actor"
	"github.com/2comjie/ngame/diff"
	"github.com/2comjie/ngame/replica"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type replicatedActor struct {
	state   *state
	manager *replica.Manager
}

func (a *replicatedActor) OnStart(ctx actor.StartContext) error {
	a.state = newState()
	a.state.set("hp", 100)
	a.state.set("gold", 10)
	a.state.set("audit", 1)
	a.manager = replica.New(replica.Owner{
		Actor: replica.EntityKey{
			Type: ctx.Self.Key().Type,
			Id:   ctx.Self.Key().Id,
		},
		ActivationId: ctx.Self.ActivationId(),
	})
	register(a.manager, tank, a.state)
	return nil
}

func (*replicatedActor) OnMessage(actor.MessageContext, actor.Message) ([]byte, error) {
	return nil, nil
}

func (*replicatedActor) OnUpdate(actor.UpdateContext) time.Duration { return -1 }
func (*replicatedActor) OnStop(actor.StopContext)                   {}

func TestActorOwnedPlayerAndRoomReplication(t *testing.T) {
	redisServer := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = rc.Close() })
	system := actor.New(actor.NewGuard("replica-test", rc))
	instances := make(map[string]*replicatedActor)
	for _, kind := range []string{"player", "room"} {
		system.Register(kind, func(_ context.Context, key actor.Key) (actor.Actor, error) {
			value := new(replicatedActor)
			instances[key.Type] = value
			return value, nil
		})
	}
	if err := system.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := system.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	for _, kind := range []string{"player", "room"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			handle, err := system.Spawn(ctx, actor.Key{
				Type: kind,
				Id:   "1",
			})
			if err != nil {
				t.Fatal(err)
			}
			value := instances[kind]
			run := func(task func(context.Context) error) {
				t.Helper()
				if err := handle.WaitResultOnMainLoop(ctx, task); err != nil {
					t.Fatal(err)
				}
			}
			actualOwner := replica.Owner{
				Actor: replica.EntityKey{
					Type: handle.Key().Type,
					Id:   handle.Key().Id,
				},
				ActivationId: handle.ActivationId(),
			}
			self := newClient("self")
			self.receiver = replica.NewReceiver(actualOwner, "self", self.handler)
			near := newClient("near")
			near.receiver = replica.NewReceiver(actualOwner, "near", near.handler)
			clients := map[string]*client{"self": self, "near": near}
			var result replica.Result
			now := time.Unix(100, 0)
			run(func(context.Context) error {
				value.manager.Observe("self", tank, diff.Public|diff.Private)
				var err error
				result, err = value.manager.Flush(now)
				return err
			})
			apply := func() {
				t.Helper()
				for _, delivery := range result.Deliveries {
					feedback := receive(t, clients[delivery.Observer], delivery.Frame)
					run(func(context.Context) error {
						if feedback.Reset != nil {
							value.manager.RequestReset(delivery.Observer, feedback.Reset)
						}
						if feedback.Ack != nil && !value.manager.Ack(delivery.Observer, feedback.Ack) {
							t.Error("Actor rejected client acknowledgment")
						}
						return nil
					})
				}
			}
			apply()
			run(func(context.Context) error {
				value.state.set("hp", 80)
				value.state.set("gold", 20)
				if kind == "room" {
					value.manager.Observe("near", tank, diff.Public)
				}
				var err error
				result, err = value.manager.Flush(now.Add(time.Millisecond))
				return err
			})
			apply()
			if self.data[tank].Fields["gold"].GetNumberValue() != 20 {
				t.Fatal("owner did not receive private delta")
			}
			if kind == "room" {
				if near.data[tank].Fields["hp"].GetNumberValue() != 80 || near.data[tank].Fields["gold"] != nil {
					t.Fatal("AOI entrant did not receive public baseline")
				}
				run(func(context.Context) error {
					value.manager.Unobserve("near", tank)
					var err error
					result, err = value.manager.Flush(now.Add(2 * time.Millisecond))
					return err
				})
				apply()
				if near.data[tank] != nil {
					t.Fatal("AOI leaver retained entity")
				}
			}
		})
	}
}
