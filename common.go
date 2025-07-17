package cache

import "time"

// Item 表示缓存项，包含值和过期时间（UnixNano时间戳）
type Item struct {
	Object     interface{} `msgpack:"o"` // 存储的值
	Expiration int64       `msgpack:"e"` // 过期时间戳（纳秒单位，0=永不过期）
}

// Expired 检查缓存项是否过期
// 返回值：true=已过期，false=未过期
func (item Item) Expired() bool {
	if item.Expiration == 0 {
		return false
	}
	return time.Now().UnixNano() > item.Expiration
}

const (
	// For use with functions that take an expiration time.
	NoExpiration time.Duration = -1
	// For use with functions that take an expiration time. Equivalent to
	// passing in the same expiration duration as was given to New() or
	// NewFrom() when the cache was created (e.g. 5 minutes.)
	DefaultExpiration time.Duration = 0
)
