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

	// ZRANGE BYSCORE 需要6.2才能支持
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
	enqueueRedisScript = redis.NewScript(enqueueScript)
	dequeueRedisScript = redis.NewScript(dequeueScript)
)

// 将一条消息msg放入到延时队列中，该消息的延时时间为delayedMs
func Enqueue(ctx context.Context, client *redis.Client, queueID string, msg string, delayedMs int64) error {
	c := enqueueRedisScript.Run(ctx, client, []string{queueID}, genUniqueData(msg), delayedMs)
	return c.Err()
}

// 拉取已经到期的消息
func Dequeue(ctx context.Context, client *redis.Client, queueID string, limit int64) ([]string, error) {
	cmdResult := dequeueRedisScript.Run(ctx, client, []string{queueID}, limit)
	if cmdResult.Err() != nil {
		return nil, cmdResult.Err()
	}
	rets, err := cmdResult.Slice()
	if err != nil {
		return nil, err
	}
	var msgs []string = make([]string, 0, limit)
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
