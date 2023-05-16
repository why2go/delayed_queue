package delayed_queue

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// 将一条消息msg放入到延时队列中，该消息的延时时间为delayedMs
func Enqueue(ctx context.Context, client *redis.Client, queueID string, msg []byte, delayedMs int64) error {
	const script = `
local key = KEYS[1]
local msg = ARGV[1]
local delayMs = tonumber(ARGV[2])
local curTimestamp = redis.call('TIME')
local tsInMs = curTimestamp[1] * 1000 + math.floor(curTimestamp[2] / 1000)
local dueTs = tsInMs + delayMs
return redis.call('ZADD', key, dueTs, msg)
	`
	c := client.Eval(ctx, script, []string{queueID}, msg, delayedMs)
	return c.Err()
}

// 拉取已经到期的消息
func Dequeue(ctx context.Context, client *redis.Client, queueID string, limit int64) ([][]byte, error) {
	var msgs [][]byte
	const script = `
local key = KEYS[1]
local limit = ARGV[1]
local curTimestamp = redis.call('TIME')
local tsInMs = curTimestamp[1] * 1000 + math.floor(curTimestamp[2] / 1000)
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
	return msgs, nil
}
