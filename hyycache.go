package cache

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/haiyiyun/log"
	"github.com/redis/go-redis/v9"
)

// HYYCache 两级缓存实现
type HYYCache struct {
	local        *MemoryCache
	remote       *RedisCache
	instanceID   string
	ctx          context.Context
	cancel       context.CancelFunc
	subscription *redis.PubSub
	updateChan   chan string
}

// NewHYYCache 创建两级缓存
func NewHYYCache(local *MemoryCache, remote *RedisCache) *HYYCache {
	ctx, cancel := context.WithCancel(context.Background())

	// 生成唯一实例ID
	instanceID := generateInstanceID()

	tlc := &HYYCache{
		local:      local,
		remote:     remote,
		instanceID: instanceID,
		ctx:        ctx,
		cancel:     cancel,
		updateChan: make(chan string, 10000), // 从1000调整到10000
	}

	// 启动订阅
	tlc.subscribeToUpdates()

	// 启动处理goroutine
	go tlc.processUpdates()

	return tlc
}

// generateInstanceID 生成唯一实例ID
func generateInstanceID() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	return fmt.Sprintf("%s:%d:%d", host, os.Getpid(), time.Now().UnixNano())
}

// subscribeToUpdates 订阅Redis更新通知
func (t *HYYCache) subscribeToUpdates() {
	if t.remote == nil || t.remote.client == nil {
		return
	}

	channel := "hyy_cache_updates:" + t.remote.namespace
	t.subscription = t.remote.client.Subscribe(t.ctx, channel)

	go func() {
		ch := t.subscription.Channel()
		for {
			select {
			case msg := <-ch:
				// 消息格式: instanceID:key
				parts := strings.SplitN(msg.Payload, ":", 2)
				if len(parts) != 2 {
					continue
				}

				sourceID := parts[0]
				key := parts[1]

				// 忽略自己发出的消息
				if sourceID == t.instanceID {
					continue
				}

				// 放入更新通道
				select {
				case t.updateChan <- key:
				default:
					log.Warnf("Update channel full, dropping key: %s", key)
				}
			case <-t.ctx.Done():
				return
			}
		}
	}()
}

// processUpdates 处理更新消息
func (t *HYYCache) processUpdates() {
	const batchSize = 100
	const batchTimeout = 100 * time.Millisecond
	const numWorkers = 10 // 工作协程数量

	workCh := t.startInvalidationWorkers(numWorkers)
	defer close(workCh)

	batch := make([]string, 0, batchSize)
	timer := time.NewTimer(batchTimeout)
	defer timer.Stop()

	resetTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(batchTimeout)
	}

	for {
		select {
		case key := <-t.updateChan:
			batch = append(batch, key)
			if len(batch) >= batchSize {
				// 批量发送到工作池
				for _, k := range batch {
					select {
					case workCh <- k:
					default:
						log.Warnf("Work channel full, dropping key: %s", k)
					}
				}
				batch = batch[:0]
				resetTimer()
			}
		case <-timer.C:
			if len(batch) > 0 {
				for _, k := range batch {
					select {
					case workCh <- k:
					default:
						log.Warnf("Work channel full, dropping key: %s", k)
					}
				}
				batch = batch[:0]
			}
			resetTimer()
		case <-t.ctx.Done():
			if len(batch) > 0 {
				for _, k := range batch {
					workCh <- k
				}
			}
			return
		}
	}
}

// publishUpdate 发布更新通知
func (t *HYYCache) publishUpdate(key string) {
	if t.remote == nil || t.remote.client == nil {
		return
	}

	channel := "hyy_cache_updates:" + t.remote.namespace
	message := fmt.Sprintf("%s:%s", t.instanceID, key)

	// 异步发布，不阻塞主流程
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		if err := t.remote.client.Publish(ctx, channel, message).Err(); err != nil {
			log.Errorf("Failed to publish update for key %s: %v", key, err)
		}
	}()
}

// calculateLocalDuration 计算本地缓存过期时间
func calculateLocalDuration(remoteDuration time.Duration) time.Duration {
	minDuration := 1 * time.Second
	maxDuration := 1 * time.Minute

	if remoteDuration <= 0 {
		return maxDuration
	}

	local := remoteDuration / 10
	if local < minDuration {
		return minDuration
	}
	if local > maxDuration {
		return maxDuration
	}

	// 添加随机抖动（±10%）
	jitter := time.Duration(rand.Int63n(int64(local/5))) - local/10
	return local + jitter
}

// ====================== 实现Cache接口 ====================== //

// Set 设置缓存
func (t *HYYCache) Set(k string, x interface{}, d time.Duration) error {
	// 先设置远程缓存
	if err := t.remote.Set(k, x, d); err != nil {
		return err
	}

	// 设置本地缓存（缩短过期时间）
	localDuration := calculateLocalDuration(d)
	if err := t.local.Set(k, x, localDuration); err != nil {
		log.Warnf("Local cache set failed for key %s: %v", k, err)
	}

	// 发布更新通知
	t.publishUpdate(k)
	return nil
}

// SetDefault 设置默认过期时间的缓存
func (t *HYYCache) SetDefault(k string, x interface{}) error {
	return t.Set(k, x, DefaultExpiration)
}

// Get 获取缓存（兼容流式压缩数据）
func (t *HYYCache) Get(k string, target interface{}) (bool, error) {
	// 先尝试本地缓存
	found, err := t.local.Get(k, target)
	if found && err == nil {
		return true, nil
	}

	// 创建临时缓冲区用于接收远程数据
	temp := reflect.New(reflect.TypeOf(target).Elem()).Interface()

	// 查询远程
	found, err = t.remote.Get(k, temp)
	if !found || err != nil {
		return found, err
	}

	// 深拷贝到目标
	deepCopyValue(temp, target)

	// 设置到本地缓存
	_, expiration := t.remote.GetWithExpiration(k, temp)
	localDuration := calculateLocalDuration(time.Until(expiration))

	if err := t.local.Set(k, temp, localDuration); err != nil {
		log.Warnf("Local cache set failed for key %s: %v", k, err)
	}

	return true, nil
}

// GetWithExpiration 获取缓存及其过期时间
func (t *HYYCache) GetWithExpiration(k string, target interface{}) (bool, time.Time) {
	// 先尝试本地缓存
	found, localExp := t.local.GetWithExpiration(k, target)
	if found {
		return found, localExp
	}

	// 本地未找到，查询远程
	found, remoteExp := t.remote.GetWithExpiration(k, target)
	if !found {
		return false, time.Time{}
	}

	// 创建临时变量用于深拷贝
	temp := reflect.New(reflect.TypeOf(target).Elem()).Interface()
	deepCopyValue(target, temp) // 使用新的深拷贝方法

	// 设置到本地缓存
	localDuration := calculateLocalDuration(time.Until(remoteExp))
	if err := t.local.Set(k, temp, localDuration); err != nil {
		log.Warnf("Local cache set failed for key %s: %v", k, err)
	}

	return true, remoteExp
}

// Add 添加缓存
func (t *HYYCache) Add(k string, x interface{}, d time.Duration) error {
	if err := t.remote.Add(k, x, d); err != nil {
		return err
	}

	localDuration := calculateLocalDuration(d)
	if err := t.local.Set(k, x, localDuration); err != nil {
		log.Warnf("Local cache set failed for key %s: %v", k, err)
	}

	t.publishUpdate(k)
	return nil
}

// Replace 替换缓存
func (t *HYYCache) Replace(k string, x interface{}, d time.Duration) error {
	if err := t.remote.Replace(k, x, d); err != nil {
		return err
	}

	localDuration := calculateLocalDuration(d)
	if err := t.local.Set(k, x, localDuration); err != nil {
		log.Warnf("Local cache set failed for key %s: %v", k, err)
	}

	t.publishUpdate(k)
	return nil
}

// Delete 删除缓存
func (t *HYYCache) Delete(k string) {
	// 先删除远程
	t.remote.Delete(k)

	// 再删除本地
	t.local.Delete(k)

	// 发布更新通知
	t.publishUpdate(k)
}

// Increment 增加整数值（适配新的modifyNumber方法）
func (t *HYYCache) Increment(k string, n int64) error {
	if err := t.remote.Increment(k, n); err != nil {
		return err
	}
	t.publishUpdate(k)
	return nil
}

// IncrementFloat 增加浮点数值（适配新的modifyNumber方法）
func (t *HYYCache) IncrementFloat(k string, n float64) error {
	if err := t.remote.IncrementFloat(k, n); err != nil {
		return err
	}
	t.publishUpdate(k)
	return nil
}

// Decrement 减少整数值（适配新的modifyNumber方法）
func (t *HYYCache) Decrement(k string, n int64) error {
	return t.Increment(k, -n)
}

// DecrementFloat 减少浮点数值（适配新的modifyNumber方法）
func (t *HYYCache) DecrementFloat(k string, n float64) error {
	return t.IncrementFloat(k, -n)
}

// DeleteExpired 删除过期缓存
func (t *HYYCache) DeleteExpired() {
	t.local.DeleteExpired()
}

// OnEvicted 设置驱逐回调函数
func (t *HYYCache) OnEvicted(f func(string, interface{})) {
	t.local.OnEvicted(f)
	t.remote.OnEvicted(f)
}

// ItemCount 获取项目数量
func (t *HYYCache) ItemCount() int {
	return t.local.ItemCount()
}

// Flush 清空缓存
func (t *HYYCache) Flush() {
	t.local.Flush()
	t.remote.Flush()
}

// Close 关闭缓存（适配新的连接管理）
func (t *HYYCache) Close() {
	t.cancel()

	if t.subscription != nil {
		t.subscription.Close()
	}

	// 先关闭本地缓存
	t.local.Close()

	// 再关闭远程缓存（适配新的关闭顺序）
	t.remote.Close()
}

// 使用工作池处理本地缓存失效
func (t *HYYCache) startInvalidationWorkers(numWorkers int) chan string {
	workCh := make(chan string, 10000)
	for i := 0; i < numWorkers; i++ {
		go func() {
			for key := range workCh {
				t.local.Delete(key)
			}
		}()
	}
	return workCh
}

// 优化的深拷贝方法（兼容压缩数据）
func deepCopyValue(src, dst interface{}) error {
	srcVal := reflect.ValueOf(src)
	dstVal := reflect.ValueOf(dst)

	if srcVal.Kind() == reflect.Ptr {
		srcVal = srcVal.Elem()
	}

	if dstVal.Kind() == reflect.Ptr {
		dstVal = dstVal.Elem()
	}

	if reflect.TypeOf(src) != reflect.TypeOf(dst) {
		return fmt.Errorf("type mismatch: src=%T, dst=%T", src, dst)
	}

	dstVal.Set(srcVal)
	return nil
}
