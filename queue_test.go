package delayed_queue

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestQ(t *testing.T) {
	const queueID = "q_nums"
	const connUrl = "redis://localhost:16379/0"
	opts, err := redis.ParseURL(strings.TrimSpace(connUrl))
	assert.Empty(t, err)
	client := redis.NewClient(opts)
	ctx, cf := context.WithTimeout(context.Background(), time.Second)
	sc := client.Ping(ctx)
	cf()
	assert.Empty(t, sc.Err())
	defer client.Close()

	t.Run("EnqueueSmoke", func(t *testing.T) {
		msg := fmt.Sprintf(`{"num":%d}`, 10)
		delay := rand.Int()%3000 + 100
		err := Enqueue(context.Background(), client, queueID, msg, int64(delay))
		assert.Empty(t, err)
	})

	t.Run("EnqueueBulk", func(t *testing.T) {
		for i := 0; i < 10000; i++ {
			msg := fmt.Sprintf(`{"num":%d}`, i)
			delay := rand.Int()%3000 + 100
			err := Enqueue(context.Background(), client, queueID, msg, int64(delay))
			assert.Empty(t, err)
		}
	})

	t.Run("DequeueSmoke", func(t *testing.T) {
		msgs, err := Dequeue(context.Background(), client, queueID, 50)
		assert.Empty(t, err)
		assert.LessOrEqual(t, len(msgs), 10)
		for i := range msgs {
			fmt.Printf("msgs[%d]: %v\n", i, msgs[i])
		}
	})

	t.Run("DequeueBulk", func(t *testing.T) {
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				msgs, err := Dequeue(context.Background(), client, queueID, 10)
				assert.Empty(t, err)
				assert.LessOrEqual(t, len(msgs), 10, "goroutine 1, dequeue bulk")
				time.Sleep(800 * time.Millisecond)
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				msgs, err := Dequeue(context.Background(), client, queueID, 10)
				assert.Empty(t, err)
				assert.LessOrEqual(t, len(msgs), 10, "goroutine 2, dequeue bulk")
				time.Sleep(800 * time.Millisecond)
			}
		}()
		wg.Wait()
	})

	t.Run("Bin", func(t *testing.T) {
		var err error
		_, err = client.Set(context.Background(), "key1", []byte("hello\nworld"), 0).Result()
		assert.Empty(t, err)

		s, err2 := client.Get(context.Background(), "key1").Result()
		assert.Empty(t, err2)
		fmt.Println(s)
	})
}

type Msg struct {
	To   int
	Text string
}
