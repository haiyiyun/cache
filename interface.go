package cache

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Cache 统一缓存接口
type Cache interface {
	Set(k string, x interface{}, d time.Duration) error
	SetDefault(k string, x interface{}) error
	Add(k string, x interface{}, d time.Duration) error
	Replace(k string, x interface{}, d time.Duration) error
	Get(k string, target interface{}) (bool, error)
	GetWithExpiration(k string, target interface{}) (bool, time.Time)
	Increment(k string, n int64) error
	IncrementFloat(k string, n float64) error
	Decrement(k string, n int64) error
	DecrementFloat(k string, n float64) error
	Delete(k string)
	DeleteExpired()
	OnEvicted(f func(string, interface{}))
	ItemCount() int
	Flush()
	Close()
}

// 根据环境变量选择缓存实现
func New(defaultExpiration, cleanupInterval time.Duration) Cache {
	cacheType := os.Getenv("HYY_CACHE_TYPE")

	switch cacheType {
	case "memory":
		shardCount := 32 // 默认值
		if sc := os.Getenv("HYY_SHARD_COUNT"); sc != "" {
			if n, err := strconv.Atoi(sc); err == nil {
				shardCount = n
			}
		}

		strictTypeCheck := false
		if stc := os.Getenv("HYY_STRICT_TYPE_CHECK"); stc != "" {
			strictTypeCheck = (stc == "true")
		}

		return NewMemoryCache(defaultExpiration, cleanupInterval, 0, shardCount, strictTypeCheck)
	case "redis":
		redisURL := os.Getenv("HYY_CACHE_URL")
		if redisURL == "" {
			redisURL = "redis://localhost:6379/0"
		}
		cache, err := NewRedisCacheFromURL(redisURL, defaultExpiration)
		if err != nil {
			panic(fmt.Sprintf("Failed to create Redis cache: %v", err))
		}
		return cache
	case "hyy":
		shardCount := 32 // 默认值
		if sc := os.Getenv("HYY_SHARD_COUNT"); sc != "" {
			if n, err := strconv.Atoi(sc); err == nil {
				shardCount = n
			}
		}

		strictTypeCheck := false
		if stc := os.Getenv("HYY_STRICT_TYPE_CHECK"); stc != "" {
			strictTypeCheck = (stc == "true")
		}
		// 创建本地缓存
		localCache := NewMemoryCache(defaultExpiration, cleanupInterval, 0, shardCount, strictTypeCheck)

		// 创建Redis缓存
		redisURL := os.Getenv("HYY_CACHE_URL")
		if redisURL == "" {
			redisURL = "redis://localhost:6379/0"
		}
		remoteCache, err := NewRedisCacheFromURL(redisURL, defaultExpiration)
		if err != nil {
			panic(fmt.Sprintf("Failed to create Redis cache: %v", err))
		}

		// 创建两级缓存
		return NewHYYCache(localCache, remoteCache)
	default:
		// 默认使用内存缓存
		shardCount := 32 // 默认值
		if sc := os.Getenv("HYY_SHARD_COUNT"); sc != "" {
			if n, err := strconv.Atoi(sc); err == nil {
				shardCount = n
			}
		}

		strictTypeCheck := false
		if stc := os.Getenv("HYY_STRICT_TYPE_CHECK"); stc != "" {
			strictTypeCheck = (stc == "true")
		}

		return NewMemoryCache(defaultExpiration, cleanupInterval, 0, shardCount, strictTypeCheck)
	}
}
