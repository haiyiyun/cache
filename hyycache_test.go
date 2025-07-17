package cache

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 测试HYYCache基础功能
func TestHYYCacheSetGet(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	cache := createTestHYYCache(t, s.Addr())
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
		fullKey := cache.remote.generateKey("temp_key")
		err := cache.Set("temp_key", "temp_value", 100*time.Millisecond)
		require.NoError(t, err)

		// 验证初始TTL
		ttl := s.TTL(fullKey)
		assert.True(t, ttl > 0, "Key should have TTL")

		// 推进时间并触发过期
		s.FastForward(150 * time.Millisecond)
		s.FastForward(0) // 触发过期处理

		// 验证键已删除
		_, err = s.Get(fullKey)
		assert.Error(t, err, "Key should be expired")
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

// 测试HYYCache两级缓存特性
func TestHYYCacheTwoLevel(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	cache := createTestHYYCache(t, s.Addr())
	defer cache.Close()

	t.Run("Local Cache Hit", func(t *testing.T) {
		// 设置数据
		err := cache.Set("local_test", "value", 5*time.Minute)
		require.NoError(t, err)

		// 第一次获取（从远程缓存）
		var val1 string
		found, err := cache.Get("local_test", &val1)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "value", val1)

		// 第二次获取（应从本地缓存）
		var val2 string
		found, err = cache.Get("local_test", &val2)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "value", val2)
	})

	t.Run("Remote Fallback", func(t *testing.T) {
		// 直接在远程设置数据（绕过本地缓存）
		err := cache.remote.Set("remote_only", "remote_value", 5*time.Minute)
		require.NoError(t, err)

		// 获取数据（应从远程获取并缓存到本地）
		var val string
		found, err := cache.Get("remote_only", &val)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "remote_value", val)
	})
}

// 测试HYYCache增量操作
func TestHYYCacheIncrement(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	cache := createTestHYYCache(t, s.Addr())
	defer cache.Close()

	t.Run("Increment/Decrement", func(t *testing.T) {
		// 整数增量
		err := cache.Set("counter", int64(10), 5*time.Minute)
		require.NoError(t, err)

		err = cache.Increment("counter", 5)
		require.NoError(t, err)

		var intVal int64
		found, err := cache.Get("counter", &intVal)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, int64(15), intVal)

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
}

// 测试HYYCache并发操作
func TestHYYCacheConcurrency(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	cache := createTestHYYCache(t, s.Addr())
	defer cache.Close()

	t.Run("Concurrent Writes", func(t *testing.T) {
		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
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
		for i := 0; i < 50; i++ {
			key := fmt.Sprintf("key%d", i)
			var val int
			found, err := cache.Get(key, &val)
			assert.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, i, val)
		}
	})

	t.Run("Concurrent Reads", func(t *testing.T) {
		// 预设数据
		err := cache.Set("shared", "shared_value", 5*time.Minute)
		require.NoError(t, err)

		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var val string
				found, err := cache.Get("shared", &val)
				assert.NoError(t, err)
				assert.True(t, found)
				assert.Equal(t, "shared_value", val)
			}()
		}
		wg.Wait()
	})
}

// 测试HYYCache错误处理
func TestHYYCacheErrorHandling(t *testing.T) {
	t.Run("Key Not Found", func(t *testing.T) {
		s := miniredis.RunT(t)
		defer s.Close()

		cache := createTestHYYCache(t, s.Addr())
		defer cache.Close()

		var res string
		found, err := cache.Get("nonexistent", &res)
		require.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("Invalid Target Type", func(t *testing.T) {
		s := miniredis.RunT(t)
		defer s.Close()

		cache := createTestHYYCache(t, s.Addr())
		defer cache.Close()

		err := cache.Set("test", "string_value", 5*time.Minute)
		require.NoError(t, err)

		// 尝试用错误类型获取
		var intVal int
		found, err := cache.Get("test", &intVal)
		// 应该找到但类型转换失败
		assert.True(t, found)
		assert.Error(t, err)
	})
}

// 测试HYYCache清理操作
func TestHYYCacheCleanup(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	cache := createTestHYYCache(t, s.Addr())
	defer cache.Close()

	// 填充数据
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		err := cache.Set(key, i, 5*time.Minute)
		require.NoError(t, err)
	}

	t.Run("Delete", func(t *testing.T) {
		cache.Delete("key5")

		var val int
		found, err := cache.Get("key5", &val)
		require.NoError(t, err)
		assert.False(t, found)
	})

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

// 测试HYYCache Add/Replace操作
func TestHYYCacheAddReplace(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	cache := createTestHYYCache(t, s.Addr())
	defer cache.Close()

	t.Run("Add", func(t *testing.T) {
		err := cache.Add("new_key", "new_value", 5*time.Minute)
		require.NoError(t, err)

		// 再次添加应失败
		err = cache.Add("new_key", "another_value", 5*time.Minute)
		require.Error(t, err)

		// 验证值未改变
		var val string
		found, err := cache.Get("new_key", &val)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, "new_value", val)
	})

	t.Run("Replace", func(t *testing.T) {
		// 替换不存在的键应失败
		err := cache.Replace("nonexistent", "value", 5*time.Minute)
		require.Error(t, err)

		// 设置键后替换应成功
		err = cache.Set("replace_key", "original", 5*time.Minute)
		require.NoError(t, err)

		err = cache.Replace("replace_key", "replaced", 5*time.Minute)
		require.NoError(t, err)

		var val string
		found, err := cache.Get("replace_key", &val)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, "replaced", val)
	})
}

// 测试HYYCache GetWithExpiration
func TestHYYCacheGetWithExpiration(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	cache := createTestHYYCache(t, s.Addr())
	defer cache.Close()

	t.Run("With Expiration", func(t *testing.T) {
		err := cache.Set("expiring", "value", 5*time.Minute)
		require.NoError(t, err)

		var val string
		var found bool                                                 // 添加变量声明
		found, expiration := cache.GetWithExpiration("expiring", &val) // 这里使用 := 声明 found 和 expiration
		require.True(t, found)
		assert.Equal(t, "value", val)
		assert.False(t, expiration.IsZero())
		assert.True(t, expiration.After(time.Now()))
	})

	t.Run("No Expiration", func(t *testing.T) {
		err := cache.Set("no_expire", "value", NoExpiration)
		require.NoError(t, err)

		// 确保从远程获取最新数据
		cache.local.Flush()

		var val string
		found, expiration := cache.GetWithExpiration("no_expire", &val)
		require.True(t, found)
		assert.Equal(t, "value", val)

		// 检查是否为永不过期（零时间或合理的大值）
		if !expiration.IsZero() {
			// miniredis 返回实际过期时间，验证是否合理（至少1分钟）
			assert.True(t, expiration.After(time.Now().Add(time.Minute)),
				"expiration should be at least 1 minute in future, got: %v", expiration)
		} else {
			// 如果是零时间，也接受
			assert.True(t, expiration.IsZero())
		}
	})
}

// 测试HYYCache SetDefault
func TestHYYCacheSetDefault(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	cache := createTestHYYCache(t, s.Addr())
	defer cache.Close()

	err := cache.SetDefault("default_key", "default_value")
	require.NoError(t, err)

	var val string
	found, err := cache.Get("default_key", &val)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "default_value", val)
}

// 测试HYYCache ItemCount
func TestHYYCacheItemCount(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	cache := createTestHYYCache(t, s.Addr())
	defer cache.Close()

	// 初始计数应为0
	assert.Equal(t, 0, cache.ItemCount())

	// 添加项目
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("count_key%d", i)
		err := cache.Set(key, i, 5*time.Minute)
		require.NoError(t, err)
	}

	// 计数应为5（本地缓存计数）
	assert.Equal(t, 5, cache.ItemCount())
}

// 测试HYYCache关闭功能
func TestHYYCacheClose(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	cache := createTestHYYCache(t, s.Addr())

	// 设置一些数据
	err := cache.Set("test", "value", 5*time.Minute)
	require.NoError(t, err)

	// 关闭缓存
	cache.Close()

	// 验证后续操作失败
	err = cache.Set("test2", "value2", DefaultExpiration)
	require.Error(t, err)
}

// 创建测试用的HYYCache实例
func createTestHYYCache(t *testing.T, addr string) *HYYCache {
	// 创建本地缓存
	local := NewMemoryCache(DefaultExpiration, 100*time.Millisecond, 0, 32, false)

	// 创建Redis缓存
	options := RedisOptions{
		Addresses:               []string{addr},
		Compression:             false,
		CompressionThreshold:    1024,
		StreamCompressThreshold: 50*1024*1024 + 1,
		Namespace:               "testns" + strconv.Itoa(os.Getpid()),
	}

	remote, err := NewRedisCache(options, DefaultExpiration)
	require.NoError(t, err)

	// 创建两级缓存
	return NewHYYCache(local, remote)
}

// 测试HYYCache删除过期项
func TestHYYCacheDeleteExpired(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	cache := createTestHYYCache(t, s.Addr())
	defer cache.Close()

	// 设置短期过期的项目
	fullKey := cache.remote.generateKey("short_lived")
	err := cache.Set("short_lived", "value", 50*time.Millisecond)
	require.NoError(t, err)

	// 推进时间并触发过期
	s.FastForward(100 * time.Millisecond)
	s.FastForward(0) // 触发过期处理

	// 调用删除过期项
	cache.DeleteExpired()

	// 验证项目已被删除
	_, err = s.Get(fullKey)
	assert.Equal(t, miniredis.ErrKeyNotFound, err, "Key should be expired and deleted")
}

// 测试HYYCache驱逐回调
func TestHYYCacheOnEvicted(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	cache := createTestHYYCache(t, s.Addr())
	defer cache.Close()

	evictedKeys := make([]string, 0)
	var mu sync.Mutex

	// 设置驱逐回调
	cache.OnEvicted(func(key string, value interface{}) {
		mu.Lock()
		evictedKeys = append(evictedKeys, key)
		mu.Unlock()
	})

	// 设置并删除项目
	err := cache.Set("evict_test", "value", 5*time.Minute)
	require.NoError(t, err)

	cache.Delete("evict_test")

	// 等待回调执行
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	assert.Contains(t, evictedKeys, "evict_test")
	mu.Unlock()
}
