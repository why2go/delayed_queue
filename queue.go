package delayed_queue

import (
	"context"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	enqueueScript = `
local key = KEYS[1]
local msg = ARGV[1]
local delayMs = tonumber(ARGV[2])
local curTimestamp = redis.call('TIME')
local tsInMs = curTimestamp[1] * 1000 + math.floor(curTimestamp[2] / 1000)
local dueTs = tsInMs + delayMs
return redis.call('ZADD', key, dueTs, msg)
	`

	dequeueScript = `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local curTimestamp = redis.call('TIME')
local tsInMs = curTimestamp[1] * 1000 + math.floor(curTimestamp[2] / 1000)
local msgs = {}
if limit > 0 then
	msgs = redis.call('ZRANGE', key, '-inf', tsInMs, 'BYSCORE', 'LIMIT', 0, limit)
else
	msgs = redis.call('ZRANGE', key, '-inf', tsInMs, 'BYSCORE')
end
if #msgs > 0 then
	redis.call('ZREM', key, unpack(msgs))
end
return msgs
	`
)

var (
	enqueueScriptSha1 string
	dequeueScriptSha1 string
)

// 加载入队脚本到redis缓存
func loadEnqueueScript(ctx context.Context, client *redis.Client) error {
	sc := client.ScriptLoad(ctx, enqueueScript)
	if sc.Err() != nil {
		return sc.Err()
	}
	enqueueScriptSha1, _ = sc.Result()
	return nil
}

// 加载出队脚本到redis缓存
func loadDequeueScript(ctx context.Context, client *redis.Client) error {
	sc := client.ScriptLoad(ctx, dequeueScript)
	if sc.Err() != nil {
		return sc.Err()
	}
	dequeueScriptSha1, _ = sc.Result()
	return nil
}

// 将一条消息msg放入到延时队列中，该消息的延时时间为delayedMs
func Enqueue(ctx context.Context, client *redis.Client, queueID string, msg string, delayedMs int64) error {
	c := client.EvalSha(ctx, enqueueScriptSha1, []string{queueID}, genUniqueData(msg), delayedMs)
	if c.Err() != nil && redis.HasErrorPrefix(c.Err(), "NOSCRIPT") {
		err := loadEnqueueScript(ctx, client)
		if err != nil {
			return err
		}
		c = client.EvalSha(ctx, enqueueScriptSha1, []string{queueID}, genUniqueData(msg), delayedMs)
	}
	return c.Err()
}

// 拉取已经到期的消息
func Dequeue(ctx context.Context, client *redis.Client, queueID string, limit int64) ([]string, error) {
	var msgs []string = make([]string, 0, limit)
	cmdResult := client.EvalSha(ctx, dequeueScriptSha1, []string{queueID}, limit)
	if cmdResult.Err() != nil {
		if redis.HasErrorPrefix(cmdResult.Err(), "NOSCRIPT") {
			if err := loadDequeueScript(ctx, client); err != nil {
				return nil, err
			}
			cmdResult = client.EvalSha(ctx, dequeueScriptSha1, []string{queueID}, limit)
			if cmdResult.Err() != nil {
				return nil, cmdResult.Err()
			}
		} else {
			return nil, cmdResult.Err()
		}
	}
	rets, err := cmdResult.Slice()
	if err != nil {
		return nil, err
	}
	for i := range rets {
		msgs = append(msgs, extractOriginData(rets[i].(string)))
	}
	return msgs, nil
}

func genUniqueData(data string) string {
	u := uuid.New()
	return u.String() + data
}

func extractOriginData(uniqueData string) string {
	if len(uniqueData) < 36 {
		return uniqueData
	}
	return uniqueData[36:]
}
