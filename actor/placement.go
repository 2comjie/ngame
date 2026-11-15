package actor

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"time"

	"github.com/2comjie/ngame/endpoint"
	"github.com/2comjie/ntool/logx"
)

// WithLoadMaxAge 设置节点负载有效期，默认 10 秒
func WithLoadMaxAge(age time.Duration) ClientOption {
	return func(o *clientOptions) { o.loadMaxAge = age }
}

func (c *Client) selectNode(ctx context.Context, actorType string, rejected map[string]bool) (string, error) {
	instances, err := c.discover.List(ctx, c.services[actorType])
	if err != nil {
		return "", err
	}
	type candidate struct {
		id     string
		weight float64
		score  float64
	}
	candidates := make([]candidate, 0, len(instances))
	now := time.Now()
	for _, instance := range instances {
		if rejected[instance.Id] || instance.Status != endpoint.Working || instance.Weight <= 0 {
			continue
		}
		value := instance.Meta[NodeLoadMetaKey]
		if value == "" {
			continue
		}
		var load NodeLoad
		if err := json.Unmarshal([]byte(value), &load); err != nil {
			logx.Warnf("actor: 节点负载损坏 id=%s error=%v", instance.Id, err)
			continue
		}
		if load.ActorLimit <= 0 || load.ActorCount < 0 || load.CPUUsage < 0 || load.MemoryUsage < 0 {
			logx.Warnf("actor: 节点负载无效 id=%s", instance.Id)
			continue
		}
		if now.Sub(load.SampledAt) > c.loadMaxAge {
			continue
		}
		score := max(float64(load.ActorCount)/float64(load.ActorLimit), load.CPUUsage, load.MemoryUsage)
		if score >= 1 {
			continue
		}
		candidates = append(candidates, candidate{id: instance.Id, weight: float64(instance.Weight), score: score})
	}
	if len(candidates) == 0 {
		return "", ErrNoAvailableNode
	}
	if len(candidates) == 1 {
		return candidates[0].id, nil
	}
	// 按静态权重抽取两个不同节点，比较实际压力
	pick := func(excluded int) int {
		selected := -1
		var total float64
		for index, candidate := range candidates {
			if index == excluded {
				continue
			}
			total += candidate.weight
			if rand.Float64()*total < candidate.weight {
				selected = index
			}
		}
		return selected
	}
	first := pick(-1)
	second := pick(first)
	if candidates[second].score < candidates[first].score {
		first = second
	}
	return candidates[first].id, nil
}
