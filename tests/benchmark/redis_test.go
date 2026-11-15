package benchmark

import (
	"context"
	"fmt"
	"os/exec"
	"testing"

	redis "github.com/redis/go-redis/v9"
)

const (
	IMAGE_NAME = "redis:8.12-rc3-trixie"
	CONTAINER_NAME = "redis"
)

// Pulls the Redis image and starts a container
func startRedis() error {
	out, err := exec.Command("docker", "pull", IMAGE_NAME).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker run: %w\n%s", err, out)
	}

	out, err = exec.Command("docker", "run", "-d", "--name", CONTAINER_NAME, 
												"-p", "6379:6379", "--rm", IMAGE_NAME).
								CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker run: %w\n%s", err, out)
	}

	return nil
}

func stopRedis() error {
	out, err := exec.Command("docker", "stop", CONTAINER_NAME).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker run: %w\n%s", err, out)
	}

	return nil
}

func getClient() (*redis.Client, error) {
	opt, err := redis.ParseURL("redis://default@localhost:6379/0")
	if err != nil {
		return nil, err
	}

	return redis.NewClient(opt), nil
}

func BenchmarkRedisGet(b *testing.B) {
	keyCounts := []int{100}

	for _, keyCount := range keyCounts {
		keys := createKeyList(keyCount)

		b.Run(fmt.Sprintf("keys:%d", keyCount), func(b *testing.B) {
			err := startRedis()
			if err != nil {
				b.Fatal(err)
			}

			b.Cleanup(func() {
				stopRedis()
			})

			client, err := getClient()
			if err != nil {
				b.Fatal(err)
			}

			ctx := context.Background()

			for _, key := range keys {
				val := 123
				res := client.Set(ctx, key, val, 0)

				if res.Err() != nil {
					b.Fatal(res.Err())
				}
			}

			// Warm keys
			for _, key := range keys {
				res := client.Get(ctx, key)
				
				if res.Err() != nil {
					b.Fatal(res.Err())
				}
			}

			b.ResetTimer()
			b.RunParallel(func(p *testing.PB) {
				keyIndex := 0

				for p.Next() {
					res := client.Get(ctx, keys[keyIndex])

					if res.Err() != nil {
						b.Error(res.Err())
					}

					keyIndex = (keyIndex + 1) % keyCount
				}
			})
		})
	}
}