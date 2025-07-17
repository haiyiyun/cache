package cache

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 测试Redis缓存基础功能
func TestRedisCacheSetGet(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	// miniredis 不支持 ConfigSet，直接跳过该设置
	// 真实 Redis 环境下应设置 notify-keyspace-events 为 "Ex"
	// s.Server().ConfigSet("notify-keyspace-events", "Ex")

	cache := createTestCache(t, s.Addr(), false)
	defer cache.Close()

	// 基本设置和获取
	t.Run("Basic Set/Get", func(t *testing.T) {
		err := cache.Set("key1", "value1", 5*time.Minute)
		require.NoError(t, err)

		var res string
		found, err := cache.Get("key1", &res)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, "value1", res)
	})

	// 过期测试
	t.Run("Expiration", func(t *testing.T) {
		err := cache.Set("expiring", "value", 100*time.Millisecond)
		require.NoError(t, err)

		// 立即检查应存在
		var val string
		found, err := cache.Get("expiring", &val)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "value", val)

		// 推进时间并触发过期
		s.FastForward(150 * time.Millisecond) // 推进虚拟时钟
		s.FastForward(0)                      // 触发立即过期处理

		// 再次检查应不存在
		found, err = cache.Get("expiring", &val)
		require.NoError(t, err)
		require.False(t, found)
	})

	// 类型测试
	t.Run("Different Types", func(t *testing.T) {
		// 测试整数
		err := cache.Set("int", 42, 5*time.Minute)
		require.NoError(t, err)

		var intVal int
		found, err := cache.Get("int", &intVal)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, 42, intVal)

		// 测试结构体
		type testStruct struct {
			Name string
			Age  int
		}
		obj := testStruct{"Alice", 30}
		err = cache.Set("struct", obj, 5*time.Minute)
		require.NoError(t, err)

		var structVal testStruct
		found, err = cache.Get("struct", &structVal)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, "Alice", structVal.Name)
		assert.Equal(t, 30, structVal.Age)
	})
}

// 测试Redis缓存高级功能
func TestRedisCacheAdvanced(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	cache := createTestCache(t, s.Addr(), true) // 启用压缩
	defer cache.Close()

	// 压缩功能测试
	t.Run("Compression", func(t *testing.T) {
		// 生成大于压缩阈值的数据
		largeData := strings.Repeat("A", 2000)
		err := cache.Set("large", largeData, 5*time.Minute)
		require.NoError(t, err)

		// 直接检查Redis值是否被压缩
		val, err := cache.client.Get(cache.ctx, cache.generateKey("large")).Bytes()
		require.NoError(t, err)

		// 检查是否是压缩数据（魔数检查）
		if len(val) > 0 && val[0] == gzipMagicByte {
			t.Log("Data is compressed as expected")
		} else {
			t.Error("Data was not compressed")
		}

		// 正常获取验证
		var res string
		found, err := cache.Get("large", &res)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, largeData, res)
	})

	// 增量操作测试
	t.Run("Increment/Decrement", func(t *testing.T) {
		// 整数增量
		err := cache.Set("counter", int64(10), 5*time.Minute) // 明确使用 int64 类型
		require.NoError(t, err)

		err = cache.Increment("counter", 5)
		require.NoError(t, err)

		var intVal int64 // 改为 int64 类型
		found, err := cache.Get("counter", &intVal)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, int64(15), intVal) // 使用 int64 比较

		// 浮点数增量
		err = cache.Set("float", 10.5, 5*time.Minute)
		require.NoError(t, err)

		err = cache.IncrementFloat("float", 2.5)
		require.NoError(t, err)

		var floatVal float64
		found, err = cache.Get("float", &floatVal)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, 13.0, floatVal)
	})

	// 驱逐回调测试
	t.Run("Eviction Callback", func(t *testing.T) {
		t.Skip("miniredis does not fully support keyspace notifications")

		evicted := make(chan string, 1)
		cache.OnEvicted(func(k string, v interface{}) {
			evicted <- k
		})

		// 确保设置足够长的过期时间
		err := cache.Set("evict", "data", 100*time.Millisecond)
		require.NoError(t, err)

		// 精确控制时间并多次触发
		start := time.Now()
		for time.Since(start) < 1*time.Second {
			s.FastForward(50 * time.Millisecond)
			s.FastForward(0)                  // 触发过期处理
			time.Sleep(20 * time.Millisecond) // 增加等待时间

			select {
			case key := <-evicted:
				assert.Equal(t, "evict", key)
				return
			default:
			}
		}
		t.Fatal("Eviction callback not called within 1 second")
	})
}

// 测试Redis缓存并发操作
func TestRedisCacheConcurrency(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	cache := createTestCache(t, s.Addr(), false)
	defer cache.Close()

	// 并发写入测试
	t.Run("Concurrent Writes", func(t *testing.T) {
		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				key := fmt.Sprintf("key%d", i)
				err := cache.Set(key, i, 5*time.Minute)
				assert.NoError(t, err)
			}(i)
		}
		wg.Wait()

		// 验证所有键都存在
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("key%d", i)
			var val int
			found, err := cache.Get(key, &val)
			assert.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, i, val)
		}
	})

	// 分布式锁测试
	t.Run("Distributed Lock", func(t *testing.T) {
		lockKey := "resource_lock"
		var counter int
		var wg sync.WaitGroup
		mu := sync.Mutex{}

		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// 获取锁
				lockAcquired, lockVal, _ := cache.acquireLock(lockKey)
				if !lockAcquired {
					return
				}

				// 临界区操作
				mu.Lock()
				counter++
				mu.Unlock()

				// 释放锁
				cache.releaseLock(lockKey, lockVal)
			}()
		}

		wg.Wait()
		assert.Equal(t, 20, counter)
	})
}

// 测试Redis缓存错误处理
func TestRedisCacheErrorHandling(t *testing.T) {
	// 连接失败测试
	t.Run("Connection Failure", func(t *testing.T) {
		// 使用无效地址并设置超时
		_, err := NewRedisCache(RedisOptions{
			Addresses:      []string{"localhost:9999"},
			ConnectTimeout: 50 * time.Millisecond, // 更短的超时
		}, DefaultExpiration)
		require.Error(t, err)

		// 检查错误类型
		if !strings.Contains(err.Error(), "connection refused") &&
			!strings.Contains(err.Error(), "timeout") {
			t.Errorf("Unexpected error: %v", err)
		}
	})

	s := miniredis.RunT(t)
	defer s.Close()

	// 键不存在测试
	t.Run("Key Not Found", func(t *testing.T) {
		cache := createTestCache(t, s.Addr(), false)
		defer cache.Close()

		var res string
		found, err := cache.Get("nonexistent", &res)
		require.NoError(t, err)
		assert.False(t, found)
	})

	// 类型不匹配测试
	t.Run("Type Mismatch", func(t *testing.T) {
		cache := createTestCache(t, s.Addr(), false)
		defer cache.Close()

		err := cache.Set("int", 42, 5*time.Minute)
		require.NoError(t, err)

		var strVal string
		_, err = cache.Get("int", &strVal)
		require.Error(t, err)
		// 检查错误信息中是否包含 "decoding" 或 "invalid"
		assert.True(t, strings.Contains(err.Error(), "decoding") ||
			strings.Contains(err.Error(), "invalid"))
	})
}

// 测试Redis缓存清理操作
func TestRedisCacheCleanup(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	cache := createTestCache(t, s.Addr(), false)
	defer cache.Close()

	// 填充数据
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		err := cache.Set(key, i, 5*time.Minute)
		require.NoError(t, err)
	}

	// 删除测试
	t.Run("Delete", func(t *testing.T) {
		// 直接调用Delete方法（无返回值）
		cache.Delete("key5")

		var val int
		found, err := cache.Get("key5", &val)
		require.NoError(t, err)
		assert.False(t, found)
	})

	// 清空测试
	t.Run("Flush", func(t *testing.T) {
		cache.Flush()

		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key%d", i)
			var val int
			found, _ := cache.Get(key, &val)
			assert.False(t, found)
		}
	})
}

// 创建测试缓存实例
func createTestCache(t *testing.T, addr string, compression bool) *RedisCache {
	options := RedisOptions{
		Addresses:            []string{addr},
		Compression:          compression,
		CompressionThreshold: 1024,
		// 设置流式压缩阈值为50MB+1（避免阈值过低错误）
		StreamCompressThreshold: 50*1024*1024 + 1, // 50MB + 1 byte
		Namespace:               "testns" + strconv.Itoa(os.Getpid()),
	}

	cache, err := NewRedisCache(options, DefaultExpiration)
	require.NoError(t, err)
	return cache
}

// 测试URL解析功能
func TestParseRedisURL(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		expected RedisOptions
		err      string
	}{
		{
			name: "Single Node",
			url:  "redis://user:pass@localhost:6379/1",
			expected: RedisOptions{
				Addresses:      []string{"localhost:6379"},
				Username:       "user",
				Password:       "pass",
				DB:             1,
				PoolSize:       defaultPoolSize,
				IsSharedDB:     false,
				Compression:    false,
				Namespace:      "",
				MaxRetries:     3,
				ConnectTimeout: 5 * time.Second,
			},
		},
		{
			name: "Cluster Mode",
			url:  "redis-cluster://user:pass@node1:6379,node2:6379?db=2",
			expected: RedisOptions{
				Addresses:   []string{"node1:6379", "node2:6379"},
				Username:    "user",
				Password:    "pass",
				DB:          0, // 集群模式下DB应被忽略
				PoolSize:    defaultPoolSize,
				IsSharedDB:  false,
				Compression: false,
				Namespace:   "",
				MaxRetries:  3,
			},
		},
		{
			name: "Invalid URL",
			url:  "invalid://localhost",
			err:  "unsupported scheme",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opt, err := ParseRedisURL(tt.url)
			if tt.err != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected.Addresses, opt.Addresses)
				assert.Equal(t, tt.expected.Username, opt.Username)
				assert.Equal(t, tt.expected.Password, opt.Password)
				assert.Equal(t, tt.expected.DB, opt.DB)
			}
		})
	}
}

// 测试缓存关闭功能
func TestRedisCacheClose(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	cache := createTestCache(t, s.Addr(), false)

	// 设置回调验证关闭
	closed := make(chan struct{})
	cache.OnConnectionLost(func(err error) {
		close(closed)
	})

	// 关闭缓存
	cache.Close()

	// 验证后续操作失败
	err := cache.Set("test", "value", DefaultExpiration)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "client is closed")

	// 验证连接丢失回调
	select {
	case <-closed:
		// 正常
	case <-time.After(100 * time.Millisecond):
		t.Error("OnConnectionLost not called")
	}
}
