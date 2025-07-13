package cache

import (
	"fmt"
	"io"
	"os"
	"time"
)

// Cache 统一缓存接口
type Cache interface {
	Set(k string, x interface{}, d time.Duration) error
	SetDefault(k string, x interface{}) error
	Add(k string, x interface{}, d time.Duration) error
	Replace(k string, x interface{}, d time.Duration) error
	Get(k string) (interface{}, bool)
	GetWithExpiration(k string) (interface{}, time.Time, bool)
	Increment(k string, n int64) error
	IncrementFloat(k string, n float64) error
	Decrement(k string, n int64) error
	DecrementFloat(k string, n float64) error
	Delete(k string)
	DeleteExpired()
	OnEvicted(f func(string, interface{}))
	Save(w io.Writer) error
	SaveFile(fname string) error
	Load(r io.Reader) error
	LoadFile(fname string) error
	Items() map[string]Item
	Interfaces() map[string]interface{}
	ItemCount() int
	Flush()
	Close()
}

// 根据环境变量选择缓存实现
func New(defaultExpiration, cleanupInterval time.Duration) Cache {
	cacheType := os.Getenv("HYY_CACHE_TYPE")

	switch cacheType {
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
	default:
		return NewMemoryCache(defaultExpiration, cleanupInterval)
	}
}
