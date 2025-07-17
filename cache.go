package cache

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2" // 高性能哈希库，用于分片键计算
)

// MemoryCache 是缓存系统的客户端接口，封装底层实现细节
// 提供线程安全的缓存操作接口
type MemoryCache struct{ *memorycache }

// memorycache 内存缓存核心实现结构
// 采用分片设计减少锁竞争，提高并发性能
type memorycache struct {
	defaultExpiration time.Duration             // 默认过期时间（0=永不过期）
	shards            []*cacheShard             // 分片数组（每个分片独立锁）
	shardCount        int                       // 分片总数
	onEvicted         func(string, interface{}) // 驱逐回调函数（项目被删除时触发）
	janitor           *janitor                  // 后台清理器（定期执行维护任务）
	evictionWorker    chan func()               // 异步驱逐任务队列
	evictionDropped   int64                     // 丢弃的驱逐任务计数（监控指标）
	isClosed          int32                     // 关闭状态标志（原子操作保护）
	wg                sync.WaitGroup            // 工作协程等待组（确保安全关闭）
	strictTypeCheck   bool                      // 严格类型检查模式（启用时强制类型匹配）
}

// cacheShard 单个缓存分片结构
// 包含独立的锁和存储，减少全局锁竞争
type cacheShard struct {
	items       map[string]Item // 键值对存储（核心数据结构）
	mu          sync.RWMutex    // 读写锁（保护items并发安全）
	count       int64           // 当前项目数（原子计数器）
	maxItems    int             // 分片最大容量限制（0=无限制）
	accessCount uint64          // 访问计数（用于热点分片检测）
}

// NewMemoryCache 创建内存缓存实例
// 参数说明：
//
//	defaultExpiration: 默认过期时长
//	cleanupInterval: 后台清理间隔（0=禁用自动清理）
//	maxItems: 最大缓存项总数（0=无限制）
//	shardCount: 分片数量（建议为CPU核心倍数）
//	strictTypeCheck: 是否启用严格类型检查
//
// 返回值：初始化好的缓存实例指针
func NewMemoryCache(defaultExpiration, cleanupInterval time.Duration,
	maxItems int, shardCount int, strictTypeCheck bool) *MemoryCache {
	if shardCount <= 0 {
		shardCount = 32
	}
	shards := make([]*cacheShard, shardCount)
	maxItemsPerShard := 0
	if maxItems > 0 {
		maxItemsPerShard = maxItems / shardCount
		if maxItemsPerShard < 1 {
			maxItemsPerShard = 1
		}
	}

	for i := 0; i < shardCount; i++ {
		shards[i] = &cacheShard{
			items:       make(map[string]Item),
			count:       0,
			maxItems:    maxItemsPerShard,
			accessCount: 0,
		}
	}

	c := &memorycache{
		defaultExpiration: defaultExpiration,
		shards:            shards,
		shardCount:        shardCount,
		evictionWorker:    make(chan func(), 1000),
		evictionDropped:   0,
		isClosed:          0,
		strictTypeCheck:   strictTypeCheck,
	}

	workerCount := runtime.NumCPU() * 2
	for i := 0; i < workerCount; i++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			defer func() {
				if r := recover(); r != nil {
					// 最小化恢复逻辑
					return
				}
			}()
			for task := range c.evictionWorker {
				task()
			}
		}()
	}

	C := &MemoryCache{
		memorycache: c,
	}

	if cleanupInterval > 0 {
		runJanitor(c, cleanupInterval)
		runtime.SetFinalizer(C, stopJanitor)
	}

	return C
}

// Set 存储键值对到缓存
// 参数说明：
//
//	k: 键
//	x: 值
//	d: 自定义过期时间（0=使用默认过期时间）
//
// 返回值：错误信息（如分片已满）
func (c *memorycache) Set(k string, x interface{}, d time.Duration) error {
	shard := c.getShard(k)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if shard.maxItems > 0 && atomic.LoadInt64(&shard.count) >= int64(shard.maxItems) {
		return errors.New("max items reached in shard")
	}

	var e int64
	if d == DefaultExpiration {
		d = c.defaultExpiration
	}
	if d > 0 {
		e = time.Now().Add(d).UnixNano()
	}
	shard.items[k] = Item{Object: x, Expiration: e}
	atomic.AddInt64(&shard.count, 1)
	return nil
}

// SetDefault 使用默认过期时间存储键值对
func (c *memorycache) SetDefault(k string, x interface{}) error {
	return c.Set(k, x, DefaultExpiration)
}

// Add 仅当键不存在时添加新项目
// 返回值：错误信息（如键已存在或分片已满）
func (c *memorycache) Add(k string, x interface{}, d time.Duration) error {
	shard := c.getShard(k)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	_, found := shard.items[k]
	if found {
		return fmt.Errorf("Item %s already exists", k)
	}

	if shard.maxItems > 0 && atomic.LoadInt64(&shard.count) >= int64(shard.maxItems) {
		return errors.New("max items reached in shard")
	}
	var e int64
	if d == DefaultExpiration {
		d = c.defaultExpiration
	}
	if d > 0 {
		e = time.Now().Add(d).UnixNano()
	}
	shard.items[k] = Item{Object: x, Expiration: e}
	atomic.AddInt64(&shard.count, 1)

	return nil
}

// Replace 仅当键存在时替换项目值
// 返回值：错误信息（如键不存在）
func (c *memorycache) Replace(k string, x interface{}, d time.Duration) error {
	shard := c.getShard(k)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	_, found := shard.items[k]
	if !found {
		return fmt.Errorf("Item %s doesn't exist", k)
	}

	var e int64
	if d == DefaultExpiration {
		d = c.defaultExpiration
	}
	if d > 0 {
		e = time.Now().Add(d).UnixNano()
	}
	shard.items[k] = Item{Object: x, Expiration: e}

	return nil
}

// Get 获取缓存值
// 参数target必须是指针类型，用于接收解码后的值
// 返回值：是否存在, 错误信息
func (c *memorycache) Get(k string, target interface{}) (bool, error) {
	found, _, err := c.getTyped(k, target)
	return found, err
}

// GetWithExpiration 获取缓存值及其过期时间
// 返回值：是否存在, 过期时间
func (c *memorycache) GetWithExpiration(k string, target interface{}) (bool, time.Time) {
	found, expiration, _ := c.getTyped(k, target)
	if expiration > 0 {
		return found, time.Unix(0, expiration)
	}
	return found, time.Time{}
}

// getTyped 类型安全的缓存获取内部实现
// 处理严格类型检查和接口类型转换
func (c *memorycache) getTyped(k string, target interface{}) (bool, int64, error) {
	// 添加指针检查
	if reflect.ValueOf(target).Kind() != reflect.Ptr {
		return false, 0, errors.New("target must be pointer")
	}
	shard := c.getShard(k)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	item, found := shard.items[k]
	if !found {
		return false, 0, nil
	}
	if item.Expiration > 0 && time.Now().UnixNano() > item.Expiration {
		return false, 0, nil
	}

	targetValue := reflect.ValueOf(target)
	if targetValue.Kind() != reflect.Ptr || targetValue.IsNil() {
		return false, 0, errors.New("target must be a non-nil pointer")
	}
	targetType := targetValue.Type().Elem()

	objType := reflect.TypeOf(item.Object)
	if c.strictTypeCheck && objType != targetType {
		return false, 0, fmt.Errorf("strict type mismatch")
	}
	if targetType.Kind() == reflect.Interface {
		// 允许接口类型赋值
		targetValue.Elem().Set(reflect.ValueOf(item.Object))
	} else if objType.AssignableTo(targetType) {
		// 类型兼容检查
		targetValue.Elem().Set(reflect.ValueOf(item.Object))
	} else {
		return false, 0, fmt.Errorf("type mismatch")
	}
	return true, item.Expiration, nil
}

// Increment 对数值型缓存项增加整数值
// 支持int/int8/int16/int32/int64/uint系列/float32/float64
func (c *memorycache) Increment(k string, n int64) error {
	shard := c.getShard(k)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	v, found := shard.items[k]
	if !found || v.Expired() {
		return fmt.Errorf("Item %s not found", k)
	}

	defer func() {
		if r := recover(); r != nil {
		}
	}()

	switch v.Object.(type) {
	case int:
		v.Object = v.Object.(int) + int(n)
	case int8:
		v.Object = v.Object.(int8) + int8(n)
	case int16:
		v.Object = v.Object.(int16) + int16(n)
	case int32:
		v.Object = v.Object.(int32) + int32(n)
	case int64:
		v.Object = v.Object.(int64) + n
	case uint:
		v.Object = v.Object.(uint) + uint(n)
	case uintptr:
		v.Object = v.Object.(uintptr) + uintptr(n)
	case uint8:
		v.Object = v.Object.(uint8) + uint8(n)
	case uint16:
		v.Object = v.Object.(uint16) + uint16(n)
	case uint32:
		v.Object = v.Object.(uint32) + uint32(n)
	case uint64:
		v.Object = v.Object.(uint64) + uint64(n)
	case float32:
		v.Object = v.Object.(float32) + float32(n)
	case float64:
		v.Object = v.Object.(float64) + float64(n)
	default:
		return fmt.Errorf("The value for %s is not an integer", k)
	}
	shard.items[k] = v
	return nil
}

// IncrementFloat 对浮点型缓存项增加浮点值
func (c *memorycache) IncrementFloat(k string, n float64) error {
	shard := c.getShard(k)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	v, found := shard.items[k]
	if !found || v.Expired() {
		return fmt.Errorf("Item %s not found", k)
	}

	defer func() {
		if r := recover(); r != nil {
		}
	}()

	switch v.Object.(type) {
	case float32:
		v.Object = v.Object.(float32) + float32(n)
	case float64:
		v.Object = v.Object.(float64) + n
	default:
		return fmt.Errorf("The value for %s does not have type float32 or float64", k)
	}
	shard.items[k] = v
	return nil
}

// Decrement 对数值型缓存项减少整数值
func (c *memorycache) Decrement(k string, n int64) error {
	shard := c.getShard(k)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	v, found := shard.items[k]
	if !found || v.Expired() {
		return fmt.Errorf("Item not found")
	}

	defer func() {
		if r := recover(); r != nil {
		}
	}()

	switch v.Object.(type) {
	case int:
		v.Object = v.Object.(int) - int(n)
	case int8:
		v.Object = v.Object.(int8) - int8(n)
	case int16:
		v.Object = v.Object.(int16) - int16(n)
	case int32:
		v.Object = v.Object.(int32) - int32(n)
	case int64:
		v.Object = v.Object.(int64) - n
	case uint:
		v.Object = v.Object.(uint) - uint(n)
	case uintptr:
		v.Object = v.Object.(uintptr) - uintptr(n)
	case uint8:
		v.Object = v.Object.(uint8) - uint8(n)
	case uint16:
		v.Object = v.Object.(uint16) - uint16(n)
	case uint32:
		v.Object = v.Object.(uint32) - uint32(n)
	case uint64:
		v.Object = v.Object.(uint64) - uint64(n)
	case float32:
		v.Object = v.Object.(float32) - float32(n)
	case float64:
		v.Object = v.Object.(float64) - float64(n)
	default:
		return fmt.Errorf("The value for %s is not an integer", k)
	}
	shard.items[k] = v
	return nil
}

// DecrementFloat 对浮点型缓存项减少浮点值
func (c *memorycache) DecrementFloat(k string, n float64) error {
	shard := c.getShard(k)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	v, found := shard.items[k]
	if !found || v.Expired() {
		return fmt.Errorf("Item %s not found", k)
	}

	defer func() {
		if r := recover(); r != nil {
		}
	}()

	switch v.Object.(type) {
	case float32:
		v.Object = v.Object.(float32) - float32(n)
	case float64:
		v.Object = v.Object.(float64) - n
	default:
		return fmt.Errorf("The value for %s does not have type float32 or float64", k)
	}
	shard.items[k] = v
	return nil
}

// Delete 删除指定键的缓存项
// 若配置了驱逐回调，会异步触发回调函数
func (c *memorycache) Delete(k string) {
	shard := c.getShard(k)
	shard.mu.Lock()
	var value interface{}
	evicted := false
	if v, ok := c.deleteFromShard(shard, k); ok {
		value = v
		evicted = true
	}
	shard.mu.Unlock()

	if evicted && c.onEvicted != nil {
		c.enqueueEvictionTask(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			done := make(chan struct{})
			go func() {
				defer close(done)
				timeout := time.NewTimer(100 * time.Millisecond)
				defer timeout.Stop()

				doneCh := make(chan struct{})
				go func() {
					defer close(doneCh)
					c.onEvicted(k, value)
				}()

				select {
				case <-doneCh:
				case <-timeout.C:
				}
			}()
			select {
			case <-done:
			case <-ctx.Done():
			}
		})
	}
}

// keyAndValue 删除操作返回的键值对结构
// 用于批量驱逐回调传递参数
type keyAndValue struct {
	key   string
	value interface{}
}

// DeleteExpired 删除所有过期缓存项
// 采用分片级并行处理，避免全局锁阻塞
func (c *memorycache) DeleteExpired() {
	const maxBatchSize = 1000
	var evictedItems []keyAndValue

	rand.Shuffle(len(c.shards), func(i, j int) {
		c.shards[i], c.shards[j] = c.shards[j], c.shards[i]
	})

	for _, shard := range c.shards {
		shard.mu.Lock()
		for k, v := range shard.items {
			if len(evictedItems) >= maxBatchSize {
				break
			}
			if v.Expired() {
				if value, ok := c.deleteFromShard(shard, k); ok {
					evictedItems = append(evictedItems, keyAndValue{k, value})
				}
			}
		}
		shard.mu.Unlock()

		if c.onEvicted != nil && len(evictedItems) > 0 {
			c.enqueueEvictionTask(func() {
				for _, item := range evictedItems {
					c.onEvicted(item.key, item.value)
				}
			})
			evictedItems = make([]keyAndValue, 0, maxBatchSize)
		}
	}
}

// OnEvicted 设置驱逐回调函数
// 当缓存项被删除（非主动删除）时触发
func (c *memorycache) OnEvicted(f func(string, interface{})) {
	c.onEvicted = f
}

// ItemCount 获取当前缓存项总数
// 通过原子计数器累加各分片计数
func (c *memorycache) ItemCount() int {
	total := int64(0)
	for _, shard := range c.shards {
		total += atomic.LoadInt64(&shard.count)
	}
	return int(total)
}

// Flush 清空所有缓存
// 重置每个分片的存储映射
func (c *memorycache) Flush() {
	for _, shard := range c.shards {
		shard.mu.Lock()
		shard.items = make(map[string]Item)
		shard.mu.Unlock()
	}
}

// Close 安全关闭缓存实例
// 执行顺序：1.标记关闭 2.停止任务队列 3.等待任务完成 4.停止清理器
func (m *memorycache) Close() {
	atomic.StoreInt32(&m.isClosed, 1) // 先标记关闭
	close(m.evictionWorker)           // 停止任务队列
	m.wg.Wait()                       // 等待任务完成

	if m.janitor != nil {
		close(m.janitor.stop) // 最后停janitor
		<-m.janitor.done
	}
}

// janitor 后台清理器结构
// 定期执行缓存维护任务
type janitor struct {
	Interval time.Duration // 任务执行间隔
	stop     chan bool     // 停止信号通道
	done     chan bool     // 完成确认通道
}

// Run 后台清理任务主循环
// 执行三种定时任务：过期清理/计数器校准/监控报告
func (j *janitor) Run(c *memorycache) {
	ticker := time.NewTicker(j.Interval)
	defer ticker.Stop()

	calibrateTicker := time.NewTicker(5 * time.Minute)
	defer calibrateTicker.Stop()

	reportTicker := time.NewTicker(1 * time.Minute)
	defer reportTicker.Stop()

	for {
		select {
		case <-ticker.C:
			c.DeleteExpired()
		case <-calibrateTicker.C:
			c.calibrateCounters()
		case <-reportTicker.C:
			dropped := atomic.SwapInt64(&c.evictionDropped, 0)
			if dropped > 0 {
			}
		case <-j.stop:
			ticker.Stop()
			calibrateTicker.Stop()
			reportTicker.Stop()
			j.done <- true
			return
		}
	}
}

// stopJanitor 停止清理器的终结器函数
func stopJanitor(c *MemoryCache) {
	c.janitor.stop <- true
}

// runJanitor 启动后台清理器协程
func runJanitor(c *memorycache, ci time.Duration) {
	j := &janitor{
		Interval: ci,
		stop:     make(chan bool, 1),
		done:     make(chan bool, 1),
	}
	c.janitor = j
	go j.Run(c)
}

// getShard 根据键计算哈希选择分片
// 使用xxhash高性能算法，同时更新分片访问计数
func (c *memorycache) getShard(key string) *cacheShard {
	h := xxhash.Sum64String(key)
	shard := c.shards[h%uint64(c.shardCount)]
	atomic.AddUint64(&shard.accessCount, 1)
	return shard
}

// deleteFromShard 从指定分片删除键（内部方法）
// 返回值：被删除的值, 是否成功删除
func (c *memorycache) deleteFromShard(shard *cacheShard, k string) (interface{}, bool) {
	if item, found := shard.items[k]; found {
		delete(shard.items, k)
		atomic.AddInt64(&shard.count, -1)
		return item.Object, true
	}
	return nil, false
}

// calibrateCounters 校准分片计数器
// 通过随机采样+非阻塞锁机制，避免影响正常请求
func (c *memorycache) calibrateCounters() {
	// 回退到固定采样率
	sampleCount := min(c.shardCount/5, 10) // 固定采样率
	if sampleCount < 1 {
		sampleCount = 1
	}

	rand.Shuffle(c.shardCount, func(i, j int) {
		c.shards[i], c.shards[j] = c.shards[j], c.shards[i]
	})

	for i := 0; i < sampleCount; i++ {
		shard := c.shards[i]
		for attempt := 0; attempt < 3; attempt++ {
			if shard.mu.TryLock() {
				actual := int64(len(shard.items))
				if actual != atomic.LoadInt64(&shard.count) {
					atomic.StoreInt64(&shard.count, actual)
				}
				atomic.StoreUint64(&shard.accessCount, 0) // 重置计数
				shard.mu.Unlock()
				break
			}
			time.Sleep(time.Duration(attempt*10) * time.Millisecond) // 指数退避
		}
	}
}

// enqueueEvictionTask 提交驱逐任务到队列
// 采用带超时的重试机制，队列满时丢弃任务并计数
func (c *memorycache) enqueueEvictionTask(task func()) {
	if atomic.LoadInt32(&c.isClosed) == 1 {
		return
	}
	for i := 0; i < 3; i++ {
		select {
		case c.evictionWorker <- task:
			return
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	atomic.AddInt64(&c.evictionDropped, 1)
}

// ShardStats 获取各分片项目数统计
// 用于监控分片负载均衡情况
func (c *memorycache) ShardStats() []int64 {
	stats := make([]int64, c.shardCount)
	for i, shard := range c.shards {
		stats[i] = atomic.LoadInt64(&shard.count)
	}
	return stats
}

// HotShards 检测热点分片（访问频率过高）
// 参数threshold为访问计数阈值
// 返回值：热点分片索引列表
func (c *memorycache) HotShards() []int {
	hotSpots := []int{}
	for i, shard := range c.shards {
		if atomic.LoadUint64(&shard.accessCount) > 10000 { // 阈值可配置
			hotSpots = append(hotSpots, i)
		}
	}
	return hotSpots
}
