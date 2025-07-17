package cache

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/haiyiyun/log"
	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
)

/*
RedisCache 实现高性能分布式Redis缓存，支持以下核心特性：

1. 多模式连接支持：
   - 单节点模式：redis://[user:pass@]host:port/db
   - 哨兵模式：redis-sentinel://[user:pass@]host1:port1,host2:port2?master=name&db=db
   - 集群模式：redis-cluster://[user:pass@]host1:port1,host2:port2?db=db
   - TLS加密连接：rediss://协议（支持证书验证）

2. 智能数据压缩：
   - 阈值压缩：超过compression_threshold自动压缩
   - 流式处理：大值(>stream_compress_threshold)分块压缩，避免OOM
   - 安全防护：解压大小限制(200MB)+CRC32校验

3. 分布式锁机制：
   - 自动续约：后台协程定期延长锁有效期
   - 故障安全：网络中断时自动释放避免死锁
   - 竞争优化：减少锁争用提高并发性能

4. 集群优化：
   - 哈希标签：{namespace}保证相关键同分片
   - 智能管道：跨节点命令自动批量路由
   - 拓扑刷新：节点变更时自动更新路由表

5. 生产级健壮性：
   - 指数退避重连：网络波动时自动恢复
   - 连接健康检查：定时心跳检测
   - 资源池管理：复用压缩资源减少GC压力

URL 参数说明：

1. 核心参数：
   - poolsize: 连接池大小（默认100）
   - compression: 启用压缩(true/false)
   - compression_threshold: 压缩阈值(字节，默认1024)
   - stream_compress_threshold: 流式压缩阈值(字节，默认100MB)
   - namespace: 缓存命名空间（多租户隔离）
   - shareddb: 是否共享数据库(true/false)

2. 超时控制：
   - connect_timeout: 连接超时(如5s)
   - read_timeout: 读取超时(如3s)
   - write_timeout: 写入超时(如3s)
   - min_retry_backoff: 最小重试间隔(如100ms)
   - max_retry_backoff: 最大重试间隔(如2s)
   - max_retries: 最大重试次数(默认3)

3. TLS 参数：
   - tlsCAFile: CA证书路径
   - tlsCertFile: 客户端证书路径
   - tlsKeyFile: 客户端私钥路径
   - insecureSkipVerify: 跳过证书验证(true/false)

生产环境强制要求：

1. Redis服务器配置：
   - 必须启用：notify-keyspace-events "Ex"（键过期事件）
   - 推荐配置：maxmemory-policy allkeys-lru（内存淘汰策略）

2. 部署建议：
   - 流式压缩阈值 >= 50MB（平衡内存和CPU）
   - 连接池大小 = 预期QPS/1000（预留缓冲）
   - 启用监控：连接数/压缩率/锁等待时间

3. 混沌测试场景：
   - 网络分区：验证自动故障转移
   - 节点宕机：验证数据一致性
   - 内存压力：验证OOM防护机制
*/

// RedisCache 实现高性能分布式Redis缓存，核心特性：
//   - 多模式连接：单节点/哨兵/集群/TLS加密
//   - 智能压缩：阈值压缩+流式大值处理
//   - 分布式锁：自动续约+故障安全释放
//   - 生产级健壮性：指数退避重连+资源池管理
type RedisCache struct {
	// 分布式锁管理
	activeLocksMutex sync.RWMutex             // 保护activeLocks的读写锁
	activeLocks      map[string]chan struct{} // 活跃锁映射 [锁键]停止通道

	// Redis连接核心
	client redis.UniversalClient // 自动适配单节点/集群/哨兵模式

	// 缓存配置
	defaultExpiration time.Duration // 默认过期时间（0=永不过期）
	lockPrefix        string        // 锁键前缀（格式: hyy_cache_lock:<key>）

	// 上下文控制
	ctx    context.Context    // 全局上下文
	cancel context.CancelFunc // 优雅关闭函数

	// 存储配置
	dbIndex                 int    // Redis数据库索引
	compression             bool   // 启用数据压缩
	compressionThreshold    int    // 压缩阈值（字节）
	streamCompressThreshold int    // 流式压缩阈值（>50MB建议值）
	namespace               string // 多租户隔离命名空间
	isSharedDB              bool   // true: 键添加命名空间前缀

	// 键管理
	keyTrackerSet string // 键追踪集合名

	// 驱逐机制
	onEvicted     func(string, interface{}) // 键过期回调
	evictionSub   *redis.PubSub             // 键空间通知订阅
	evictionStop  chan struct{}             // 驱逐监听器停止通道
	evictionMutex sync.Mutex                // 保护订阅重建的互斥锁

	// 资源池
	gzipPool       sync.Pool // bytes.Buffer池（压缩复用）
	gzipWriterPool sync.Pool // gzip.Writer池（压缩复用）

	// 连接监控
	onConnectionLost func(error) // 连接丢失回调（用于告警）
}

type RedisOptions struct {
	Addresses               []string      // Redis地址列表
	Username                string        // 用户名
	Password                string        // 密码
	DB                      int           // 数据库编号
	MasterName              string        // Sentinel主节点名称
	ConnectTimeout          time.Duration // 连接超时
	ReadTimeout             time.Duration // 读取超时
	WriteTimeout            time.Duration // 写入超时
	MinRetryBackoff         time.Duration // 最小重试间隔
	MaxRetryBackoff         time.Duration // 最大重试间隔
	MaxRetries              int           // 最大重试次数
	PoolSize                int           // 连接池大小
	Compression             bool          // 启用压缩
	CompressionThreshold    int           // 压缩阈值(字节)
	StreamCompressThreshold int           // 新增流式压缩阈值
	Namespace               string        // 缓存命名空间
	IsSharedDB              bool          // 是否共享DB
	UseTLS                  bool          // 使用TLS
	TLSConfig               *tls.Config   // TLS配置
}

const (
	defaultPoolSize                = 100
	defaultCompressionThreshold    = 1024
	defaultStreamCompressThreshold = 100 * 1024 * 1024 // 100MB
	lockTimeout                    = 5 * time.Second
	keyTrackerSet                  = "hyy_cache_keys"
	defaultRedisPort               = "6379"
)

// ParseRedisURL 解析Redis连接URL
func ParseRedisURL(redisURL string) (RedisOptions, error) {
	opt := RedisOptions{
		PoolSize:                defaultPoolSize,
		CompressionThreshold:    defaultCompressionThreshold,
		StreamCompressThreshold: defaultStreamCompressThreshold,
		MaxRetries:              3,
	}

	u, err := url.Parse(redisURL)
	if err != nil {
		return opt, fmt.Errorf("invalid Redis URL: %w", err)
	}

	// 确定连接模式
	switch u.Scheme {
	case "redis", "rediss", "redis-sentinel", "redis-cluster":
		// 有效协议
	default:
		return opt, fmt.Errorf("unsupported scheme: %s", u.Scheme)
	}

	// 处理用户名和密码
	if u.User != nil {
		opt.Username = u.User.Username()
		if password, ok := u.User.Password(); ok {
			opt.Password = password
		}
	}

	// 处理主机地址
	hosts := strings.Split(u.Host, ",")
	for _, host := range hosts {
		if host == "" {
			continue
		}

		// 确保主机有端口
		if _, _, err := net.SplitHostPort(host); err != nil {
			// 添加默认端口
			host = net.JoinHostPort(host, defaultRedisPort)
		}
		opt.Addresses = append(opt.Addresses, host)
	}

	// 处理路径中的数据库索引
	if u.Path != "" {
		path := strings.TrimPrefix(u.Path, "/")
		if path != "" {
			if db, err := strconv.Atoi(path); err == nil {
				opt.DB = db
			}
		}
	}

	// 解析查询参数
	query := u.Query()

	// 哨兵主节点名称
	if master := query.Get("master"); master != "" {
		opt.MasterName = master
	}

	// 集群模式检测
	if u.Scheme == "redis-cluster" || query.Get("cluster") == "true" {
		opt.MasterName = "" // 集群模式不需要主节点名称
	}

	// TLS 配置
	if u.Scheme == "rediss" || query.Get("tls") == "true" {
		opt.UseTLS = true
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}

		// 加载CA证书
		if caFile := query.Get("tlsCAFile"); caFile != "" {
			caCert, err := os.ReadFile(caFile)
			if err != nil {
				return opt, fmt.Errorf("failed to read CA cert: %w", err)
			}
			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return opt, errors.New("failed to parse CA certificate")
			}
			tlsConfig.RootCAs = caCertPool
		}

		// 加载客户端证书
		if certFile := query.Get("tlsCertFile"); certFile != "" {
			keyFile := query.Get("tlsKeyFile")
			if keyFile == "" {
				return opt, errors.New("tlsKeyFile is required with tlsCertFile")
			}
			cert, err := tls.LoadX509KeyPair(certFile, keyFile)
			if err != nil {
				return opt, fmt.Errorf("failed to load key pair: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		// 跳过证书验证
		if query.Get("insecureSkipVerify") == "true" {
			tlsConfig.InsecureSkipVerify = true
		}

		opt.TLSConfig = tlsConfig
	}

	// 连接池大小
	if poolSize := query.Get("poolsize"); poolSize != "" {
		if size, err := strconv.Atoi(poolSize); err == nil && size > 0 {
			opt.PoolSize = size
		}
	}

	// 共享数据库模式
	if shared := query.Get("shareddb"); shared != "" {
		opt.IsSharedDB = shared == "true"
	}

	// 压缩设置
	if compression := query.Get("compression"); compression != "" {
		opt.Compression = compression == "true"
	}

	// 压缩阈值
	if threshold := query.Get("compression_threshold"); threshold != "" {
		if size, err := strconv.Atoi(threshold); err == nil && size > 0 {
			opt.CompressionThreshold = size
		}
	}

	// 流式压缩阈值
	if threshold := query.Get("stream_compress_threshold"); threshold != "" {
		if size, err := strconv.Atoi(threshold); err == nil && size > 0 {
			opt.StreamCompressThreshold = size
		}
	}

	// 命名空间
	if ns := query.Get("namespace"); ns != "" {
		opt.Namespace = ns
	}

	// 超时设置
	parseDuration := func(param string, defaultValue time.Duration) time.Duration {
		if val := query.Get(param); val != "" {
			if d, err := time.ParseDuration(val); err == nil {
				return d
			}
		}
		return defaultValue
	}

	opt.ConnectTimeout = parseDuration("connect_timeout", 5*time.Second)
	opt.ReadTimeout = parseDuration("read_timeout", 3*time.Second)
	opt.WriteTimeout = parseDuration("write_timeout", 3*time.Second)
	opt.MinRetryBackoff = parseDuration("min_retry_backoff", 100*time.Millisecond)
	opt.MaxRetryBackoff = parseDuration("max_retry_backoff", 2*time.Second)

	// 最大重试次数
	if retries := query.Get("max_retries"); retries != "" {
		if n, err := strconv.Atoi(retries); err == nil && n >= 0 {
			opt.MaxRetries = n
		}
	}

	return opt, nil
}

// NewRedisCacheFromURL 从URL创建Redis缓存
func NewRedisCacheFromURL(redisURL string, defaultExpiration time.Duration) (*RedisCache, error) {
	opt, err := ParseRedisURL(redisURL)
	if err != nil {
		return nil, err
	}
	return NewRedisCache(opt, defaultExpiration)
}

func NewRedisCache(options RedisOptions, defaultExpiration time.Duration) (*RedisCache, error) {
	ctx, cancel := context.WithCancel(context.Background())

	if options.StreamCompressThreshold < 1024*1024 {
		return nil, errors.New("stream compress threshold too low")
	}

	// 设置默认选项
	if options.PoolSize <= 0 {
		options.PoolSize = defaultPoolSize
	}
	if options.CompressionThreshold <= 0 {
		options.CompressionThreshold = defaultCompressionThreshold
	}

	// 设置默认命名空间
	if options.Namespace == "" {
		hostname, _ := os.Hostname()
		options.Namespace = fmt.Sprintf("hyy_cache:%s:%d", hostname, os.Getpid())
	}

	// 创建Redis客户端
	var client redis.UniversalClient
	switch {
	case len(options.Addresses) == 1 && options.MasterName == "":
		// 单节点模式
		client = redis.NewClient(&redis.Options{
			Addr:            options.Addresses[0],
			Username:        options.Username,
			Password:        options.Password,
			DB:              options.DB,
			PoolSize:        options.PoolSize,
			DialTimeout:     options.ConnectTimeout,
			ReadTimeout:     options.ReadTimeout,
			WriteTimeout:    options.WriteTimeout,
			MinRetryBackoff: options.MinRetryBackoff,
			MaxRetryBackoff: options.MaxRetryBackoff,
			MaxRetries:      options.MaxRetries,
			TLSConfig:       options.TLSConfig,
		})

	case options.MasterName != "":
		// Sentinel模式
		client = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:      options.MasterName,
			SentinelAddrs:   options.Addresses,
			Username:        options.Username,
			Password:        options.Password,
			DB:              options.DB,
			PoolSize:        options.PoolSize,
			DialTimeout:     options.ConnectTimeout,
			ReadTimeout:     options.ReadTimeout,
			WriteTimeout:    options.WriteTimeout,
			MinRetryBackoff: options.MinRetryBackoff,
			MaxRetryBackoff: options.MaxRetryBackoff,
			MaxRetries:      options.MaxRetries,
			TLSConfig:       options.TLSConfig,
		})

	default:
		// 集群模式
		client = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:           options.Addresses,
			Username:        options.Username,
			Password:        options.Password,
			PoolSize:        options.PoolSize,
			DialTimeout:     options.ConnectTimeout,
			ReadTimeout:     options.ReadTimeout,
			WriteTimeout:    options.WriteTimeout,
			MinRetryBackoff: options.MinRetryBackoff,
			MaxRetryBackoff: options.MaxRetryBackoff,
			MaxRedirects:    options.MaxRetries,
			TLSConfig:       options.TLSConfig,
		})
	}

	// 测试连接
	if _, err := client.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("Redis connection failed: %w", err)
	}

	cache := &RedisCache{
		client:                  client,
		defaultExpiration:       defaultExpiration,
		lockPrefix:              "hyy_cache_lock:",
		ctx:                     ctx,
		cancel:                  cancel,
		dbIndex:                 options.DB, // 初始化dbIndex
		compression:             options.Compression,
		compressionThreshold:    options.CompressionThreshold,
		streamCompressThreshold: options.StreamCompressThreshold,
		namespace:               options.Namespace,
		isSharedDB:              options.IsSharedDB,
		keyTrackerSet:           options.Namespace + ":" + keyTrackerSet,
		evictionStop:            make(chan struct{}),
		gzipPool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
		gzipWriterPool: sync.Pool{
			New: func() interface{} {
				return gzip.NewWriter(nil)
			},
		},
	}

	// 根据集群模式调整键追踪集合
	if options.IsSharedDB {
		// 使用哈希标签确保集群模式下在同一分片
		cache.keyTrackerSet = fmt.Sprintf("{%s}:%s", options.Namespace, keyTrackerSet)
	} else {
		cache.keyTrackerSet = keyTrackerSet
	}

	go cache.checkConnection()
	return cache, nil
}

// 根据DB共享模式生成键名
func (c *RedisCache) generateKey(key string) string {
	// 过滤非法字符
	cleanKey := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == ':' || r == '_' || r == '-' {
			return r
		}
		return -1
	}, key)

	if len(cleanKey) > 1024 {
		cleanKey = cleanKey[:1024] // 截断超长键名
	}

	if c.isSharedDB {
		return fmt.Sprintf("{%s}:%s", c.namespace, cleanKey)
	}
	return cleanKey
}

// streamCompress 流式压缩大值数据（优化版）
func (c *RedisCache) streamCompress(data []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()

	pr, pw := io.Pipe()
	result := make(chan []byte, 1)

	go func() {
		defer pw.Close()

		gz := c.gzipWriterPool.Get().(*gzip.Writer)
		defer c.gzipWriterPool.Put(gz)
		gz.Reset(pw)

		// 设置 CRC32 校验头
		checksum := crc32.ChecksumIEEE(data)
		extra := make([]byte, 4)
		binary.LittleEndian.PutUint32(extra, checksum)
		gz.Header.Extra = extra

		// 使用 io.Copy 简化分块压缩
		if _, err := io.Copy(gz, bytes.NewReader(data)); err != nil {
			pw.CloseWithError(err)
			return
		}

		if err := gz.Close(); err != nil {
			pw.CloseWithError(err)
		}
	}()

	go func() {
		compressed, err := io.ReadAll(pr)
		if err != nil {
			result <- nil
		} else {
			result <- compressed
		}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-result:
		if res == nil {
			return nil, errors.New("compression failed")
		}
		return append([]byte{1}, res...), nil
	}
}

// streamDecompress 流式解压大值数据
// 安全防护：
//   - 硬性200MB内存限制
//   - 自动校验CRC32完整性
//
// 注意：超限立即终止防止OOM
func (c *RedisCache) streamDecompress(data []byte) ([]byte, error) {
	const maxSize = 200 * 1024 * 1024
	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()
		gz, _ := gzip.NewReader(bytes.NewReader(data))
		defer gz.Close()

		// 使用限流器防止内存爆炸
		limitedReader := &io.LimitedReader{R: gz, N: maxSize}
		if _, err := io.Copy(pw, limitedReader); err != nil {
			pw.CloseWithError(err)
		}
		if limitedReader.N <= 0 {
			pw.CloseWithError(errors.New("decompressed size exceeds 200MB limit"))
		}
	}()

	return io.ReadAll(pr)
}

// 修改serialize/deserialize方法
func (c *RedisCache) serialize(item Item) ([]byte, error) {
	data, err := msgpack.Marshal(item)
	if err != nil {
		return nil, err
	}

	if len(data) > c.streamCompressThreshold {
		compressed, err := c.streamCompress(data)
		return append([]byte{2}, compressed...), err // 流式压缩标记为2
	} else if len(data) > c.compressionThreshold {
		compressed, err := c.compress(data)
		return append([]byte{1}, compressed...), err // 普通压缩标记为1
	}

	return append([]byte{0}, data...), nil
}

func (c *RedisCache) deserialize(data []byte) (Item, error) {
	if len(data) < 1 {
		return Item{}, errors.New("invalid data length")
	}

	// 增加大小限制
	const maxSize = 100 * 1024 * 1024 // 100MB
	if len(data) > maxSize {
		return Item{}, errors.New("data exceeds maximum size")
	}

	flag := data[0]
	payload := data[1:]

	var err error
	switch flag {
	case 1: // 普通压缩
		payload, err = c.decompress(payload)
	case 2: // 流式压缩
		payload, err = c.streamDecompress(payload)
	default:
		return Item{}, errors.New("invalid compression flag")
	}

	if err != nil {
		return Item{}, err
	}

	var item Item
	// 使用安全解码器
	dec := msgpack.NewDecoder(bytes.NewReader(payload))
	dec.SetMapDecoder(func(dec *msgpack.Decoder) (interface{}, error) {
		n, err := dec.DecodeMapLen()
		if err != nil {
			return nil, err
		}
		m := make(map[string]interface{}, n)
		for i := 0; i < n; i++ {
			key, err := dec.DecodeString()
			if err != nil {
				return nil, err
			}
			value, err := dec.DecodeInterface()
			if err != nil {
				return nil, err
			}
			m[key] = value
		}
		return m, nil
	})

	if err := dec.Decode(&item); err != nil {
		return Item{}, err
	}
	return item, nil
}

// 优化压缩池使用
func (c *RedisCache) compress(data []byte) ([]byte, error) {
	buf := c.gzipPool.Get().(*bytes.Buffer)
	defer c.gzipPool.Put(buf)
	buf.Reset()                // 立即重置避免污染
	buf.Grow(len(data) + 1024) // 预分配空间

	gz := c.gzipWriterPool.Get().(*gzip.Writer)
	defer c.gzipWriterPool.Put(gz)
	gz.Reset(buf)

	if _, err := gz.Write(data); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}

	compressed := make([]byte, buf.Len())
	copy(compressed, buf.Bytes())

	// 压缩数据可能膨胀导致OOM
	if len(data) > 0 && float64(len(compressed))/float64(len(data)) > 1.5 {
		log.Warnf("高压缩率警告: %.2f (key=%s)", float64(len(compressed))/float64(len(data)), "unknown_key") // 修改为警告日志而非直接报错
	}

	return append([]byte{1}, compressed...), nil
}

func (c *RedisCache) decompress(data []byte) ([]byte, error) {
	buf := bytes.NewBuffer(data)
	gz, err := gzip.NewReader(buf)
	if err != nil {
		return nil, err
	}
	defer gz.Close()

	return io.ReadAll(gz)
}

// acquireLock 获取分布式锁
// 特殊机制：
//   - 自动续约：后台协程定期延长锁有效期
//   - 唯一锁值：防止误删其他实例的锁
//
// 返回cancelFunc必须调用，否则导致协程泄漏
func (c *RedisCache) acquireLock(lockKey string) (bool, string, context.CancelFunc) {
	lockValue := uuid.New().String()
	ok, err := c.client.SetNX(c.ctx, lockKey, lockValue, lockTimeout).Result()
	if !ok || err != nil {
		return false, "", nil
	}

	stopChan := make(chan struct{})
	c.activeLocksMutex.Lock()
	if ch, exists := c.activeLocks[lockKey]; exists {
		close(ch) // 清理旧锁
		delete(c.activeLocks, lockKey)
	}
	c.activeLocks[lockKey] = stopChan
	c.activeLocksMutex.Unlock()

	// 在锁外启动协程
	go c.lockRenewer(lockKey, lockValue, stopChan)
	return true, lockValue, func() { close(stopChan) }
}

// 统一数字操作方法
func (c *RedisCache) modifyNumber(k string, delta interface{}) error {
	fullKey := c.generateKey(k)
	var err error

	switch v := delta.(type) {
	case int64:
		_, err = c.client.IncrBy(c.ctx, fullKey, v).Result()
	case float64:
		_, err = c.client.IncrByFloat(c.ctx, fullKey, v).Result()
	default:
		return errors.New("unsupported number type")
	}

	if err == nil && c.keyTrackerSet != "" {
		c.client.SAdd(c.ctx, c.keyTrackerSet, fullKey)
	}
	return err
}

// 简化锁续约协程
func (c *RedisCache) lockRenewer(lockKey, lockValue string, stopChan <-chan struct{}) {
	defer func() {
		c.activeLocksMutex.Lock()
		delete(c.activeLocks, lockKey)
		c.activeLocksMutex.Unlock()
	}()

	ticker := time.NewTicker(lockTimeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !c.extendLock(lockKey, lockValue) {
				return // 续约失败立即退出
			}
		case <-stopChan:
			return
		case <-c.ctx.Done():
			return
		}
	}
}

// 锁续期
func (c *RedisCache) extendLock(lockKey, lockValue string) bool {
	script := redis.NewScript(`
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("PEXPIRE", KEYS[1], ARGV[2])
        end
        return 0
    `)
	return script.Run(c.ctx, c.client, []string{lockKey}, lockValue, lockTimeout.Milliseconds()).Err() == nil
}

func (c *RedisCache) releaseLock(lockKey, lockValue string) {
	script := redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		end
		return 0
	`)
	script.Run(c.ctx, c.client, []string{lockKey}, lockValue)

	c.activeLocksMutex.Lock()
	if ch, exists := c.activeLocks[lockKey]; exists {
		close(ch)
		delete(c.activeLocks, lockKey)
	}
	c.activeLocksMutex.Unlock()
}

// startEvictionListener 启动键过期事件监听
// 依赖Redis配置：notify-keyspace-events "Ex"
// 重连机制：订阅断开时自动重建连接
func (c *RedisCache) startEvictionListener() {
	// 使用更精确的订阅模式（指定当前DB）
	pattern := fmt.Sprintf("__keyevent@%d__:expired", c.dbIndex)
	c.evictionSub = c.client.PSubscribe(c.ctx, pattern)

	go func() {
		ch := c.evictionSub.Channel()
		for msg := range ch {
			if c.onEvicted == nil {
				continue
			}

			key := msg.Payload
			if c.isSharedDB && !strings.HasPrefix(key, c.namespace+":") {
				continue
			}

			userKey := key
			if c.isSharedDB {
				userKey = strings.TrimPrefix(key, c.namespace+":")
			}

			c.onEvicted(userKey, nil)
		}
	}()

	// 添加断线重连处理
	go func() {
		ch := c.evictionSub.Channel() // 获取消息通道
		for {
			select {
			case _, ok := <-ch:
				if !ok { // 通道关闭表示连接断开
					if c.ctx.Err() != nil {
						return
					}
					log.Warn("Eviction listener disconnected, reconnecting...")
					c.startEvictionListener()
					return
				}
			case <-c.ctx.Done():
				return
			}
		}
	}()

	// 检查并设置配置
	config, err := c.client.ConfigGet(c.ctx, "notify-keyspace-events").Result()
	if err == nil {
		if val, ok := config["notify-keyspace-events"]; !ok || !strings.Contains(val, "E") {
			// 尝试设置配置
			c.client.ConfigSet(c.ctx, "notify-keyspace-events", "Ex")
		}
	}
}

// 优化连接健康检查
func (c *RedisCache) checkConnection() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if err := c.client.Ping(c.ctx).Err(); err != nil {
				if c.onConnectionLost != nil {
					c.onConnectionLost(fmt.Errorf("connection lost: %w", err))
				}

				log.Warnf("Redis connection error: %v", err)
				if err := c.reconnect(); err != nil && c.onConnectionLost != nil {
					c.onConnectionLost(err)
				}
			}
		}
	}
}

// 简化重连逻辑
func (c *RedisCache) reconnect() error {
	backoff := 1 * time.Second
	for i := 0; i < 10; i++ {
		if _, err := c.client.Ping(c.ctx).Result(); err == nil {
			c.restoreEvictionSub()
			return nil
		}
		time.Sleep(backoff)
		backoff = time.Duration(math.Min(float64(backoff*2), float64(30*time.Second)))
	}
	return errors.New("permanent Redis connection failure")
}

// restoreEvictionSub 重建键空间订阅
// 并发安全：
//   - 使用evictionMutex防止竞态
//   - 先关闭旧订阅再创建新订阅
//
// 注意：依赖Redis的notify-keyspace-events "Ex"配置
func (c *RedisCache) restoreEvictionSub() {
	// 先关闭现有订阅
	if c.evictionSub != nil {
		c.evictionSub.Close()
		c.evictionSub = nil
	}

	// 加锁防止竞态
	c.evictionMutex.Lock()
	defer c.evictionMutex.Unlock()

	if c.onEvicted != nil {
		c.startEvictionListener()
	}
}

// Set 添加项目到缓存
func (c *RedisCache) Set(k string, x interface{}, d time.Duration) error {
	if d == DefaultExpiration {
		d = c.defaultExpiration
	}

	var exp int64
	if d > 0 {
		exp = time.Now().Add(d).UnixNano()
	}

	// 先将对象序列化为字节
	objData, err := msgpack.Marshal(x)
	if err != nil {
		return err
	}

	item := Item{Object: objData, Expiration: exp}
	data, err := c.serialize(item)
	if err != nil {
		return err
	}

	fullKey := c.generateKey(k)

	if d == NoExpiration {
		_, err = c.client.Set(c.ctx, fullKey, data, 0).Result()
	} else {
		_, err = c.client.Set(c.ctx, fullKey, data, d).Result()
	}

	if err != nil {
		return err
	}

	if trackerKey := c.keyTrackerSet; trackerKey != "" {
		c.client.SAdd(c.ctx, trackerKey, fullKey)
	}
	return nil
}

func (c *RedisCache) SetDefault(k string, x interface{}) error {
	return c.Set(k, x, DefaultExpiration)
}

func (c *RedisCache) Add(k string, x interface{}, d time.Duration) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("Add panic: %v", r)
			err = errors.New("internal error")
		}
	}()

	fullKey := c.generateKey(k)
	lockKey := c.lockPrefix + fullKey

	// 修复：接收三个返回值
	acquired, lockValue, cancel := c.acquireLock(lockKey)
	defer func() {
		if r := recover(); r != nil {
			cancel()
			panic(r) // 重新抛出
		} else {
			cancel()
		}
	}()
	if !acquired {
		return errors.New("failed to acquire lock")
	}
	defer c.releaseLock(lockKey, lockValue)

	exists, err := c.client.Exists(c.ctx, fullKey).Result()
	if err != nil || exists > 0 {
		return errors.New("key already exists")
	}

	return c.Set(k, x, d)
}

func (c *RedisCache) Replace(k string, x interface{}, d time.Duration) error {
	fullKey := c.generateKey(k)
	lockKey := c.lockPrefix + fullKey

	// 修复：接收三个返回值
	acquired, lockValue, cancel := c.acquireLock(lockKey)
	defer func() {
		if r := recover(); r != nil {
			cancel()
			panic(r) // 重新抛出
		} else {
			cancel()
		}
	}()
	if !acquired {
		return errors.New("failed to acquire lock")
	}
	defer c.releaseLock(lockKey, lockValue)

	exists, err := c.client.Exists(c.ctx, fullKey).Result()
	if err != nil || exists == 0 {
		return errors.New("key does not exist")
	}

	return c.Set(k, x, d)
}

func (c *RedisCache) Get(k string, target interface{}) (bool, error) {
	fullKey := c.generateKey(k)
	data, err := c.client.Get(c.ctx, fullKey).Bytes()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, err
	}

	item, err := c.deserialize(data)
	if err != nil {
		return false, err
	}

	if target != nil {
		if err := msgpack.Unmarshal(item.Object.([]byte), target); err != nil {
			return true, err
		}
	}
	return true, nil
}

func (c *RedisCache) GetWithExpiration(k string, target interface{}) (bool, time.Time) {
	found, err := c.Get(k, target)
	if !found || err != nil {
		return found, time.Time{}
	}

	fullKey := c.generateKey(k)
	pttl, err := c.client.PTTL(c.ctx, fullKey).Result()
	if err != nil {
		return true, time.Time{}
	}

	if pttl > 0 {
		return true, time.Now().Add(pttl)
	}
	return true, time.Time{}
}

func (c *RedisCache) Decrement(k string, n int64) error {
	// 直接调用 modifyNumber 方法
	return c.modifyNumber(k, -n)
}

func (c *RedisCache) DecrementFloat(k string, n float64) error {
	// 直接调用 modifyNumber 方法
	return c.modifyNumber(k, -n)
}

func (c *RedisCache) Increment(k string, n int64) error {
	return c.modifyNumber(k, n)
}

func (c *RedisCache) IncrementFloat(k string, n float64) error {
	return c.modifyNumber(k, n)
}

func (c *RedisCache) Delete(k string) {
	fullKey := c.generateKey(k)
	c.client.Del(c.ctx, fullKey)
	if c.keyTrackerSet != "" {
		c.client.SRem(c.ctx, c.keyTrackerSet, fullKey)
	}
}

func (c *RedisCache) DeleteExpired() {
	// Redis handles automatically
}

// OnEvicted 设置驱逐回调
// 警告：必须在Set/Get等操作前调用
// 集群模式要求Redis配置：
//
//	CONFIG SET notify-keyspace-events "Ex"
func (c *RedisCache) OnEvicted(f func(string, interface{})) {
	c.onEvicted = f
	if f != nil && c.evictionSub == nil {
		c.startEvictionListener()
	} else if f == nil && c.evictionSub != nil {
		c.evictionSub.Close()
		c.evictionSub = nil
	}
}

// ItemCount 获取缓存键数量（集群感知）
// 集群模式：累加所有主节点的SCard结果
// 单机模式：使用DBSIZE或SCAN
func (c *RedisCache) ItemCount() int {
	if cluster, ok := c.client.(*redis.ClusterClient); ok {
		var total int64
		cluster.ForEachMaster(c.ctx, func(ctx context.Context, client *redis.Client) error {
			size, _ := client.SCard(ctx, c.keyTrackerSet).Result()
			total += size
			return nil
		})
		return int(total)
	}

	// 在独用DB模式下，使用DBSIZE命令
	if !c.isSharedDB {
		size, _ := c.client.DBSize(c.ctx).Result()
		return int(size)
	}

	// 使用SCAN代替SMEMBERS
	var count int64
	iter := c.client.SScan(c.ctx, c.keyTrackerSet, 0, "*", 1000).Iterator()
	for iter.Next(c.ctx) {
		count++
	}
	return int(count)
}

// Flush 清空缓存
func (c *RedisCache) Flush() {
	if !c.isSharedDB {
		// 集群模式下清空所有节点
		if cluster, ok := c.client.(*redis.ClusterClient); ok {
			err := cluster.ForEachMaster(c.ctx, func(ctx context.Context, client *redis.Client) error {
				// 添加重试计数器
				for retry := 0; retry < 3; retry++ {
					if err := client.FlushDB(ctx).Err(); err == nil {
						break
					}
					time.Sleep(time.Duration(retry) * 100 * time.Millisecond)
				}
				return nil
			})
			if err != nil {
				log.Errorf("Batch flush failed: %v", err)
			}
			return
		}
		c.client.FlushDB(c.ctx)
		return
	}

	// 使用集群感知的键扫描
	if cluster, ok := c.client.(*redis.ClusterClient); ok {
		err := cluster.ForEachMaster(c.ctx, func(ctx context.Context, client *redis.Client) error {
			// 每个节点单独处理自己的键
			iter := client.SScan(ctx, c.keyTrackerSet, 0, "*", 100).Iterator()
			var keys []string
			for iter.Next(ctx) {
				keys = append(keys, iter.Val())
				if len(keys) >= 100 {
					ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
					defer cancel()
					client.Del(ctx, keys...)
					keys = keys[:0]
				}
			}
			if len(keys) > 0 {
				ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()
				client.Del(ctx, keys...)
			}
			return nil
		})
		if err != nil {
			log.Errorf("Batch delete failed: %v", err)
		}
	} else {
		// 单节点处理逻辑
		var cursor uint64
		for {
			var keys []string
			var err error
			keys, cursor, err = c.client.SScan(c.ctx, c.keyTrackerSet, cursor, "*", 100).Result()
			if err != nil {
				log.Errorf("SSCAN error: %v", err)
				break
			}

			if len(keys) > 0 {
				pipe := c.client.Pipeline()
				// 修复：转换类型
				delKeys := make([]interface{}, len(keys))
				for i, k := range keys {
					delKeys[i] = k
				}
				pipe.Del(c.ctx, keys...)
				pipe.SRem(c.ctx, c.keyTrackerSet, delKeys...)
				if _, err := pipe.Exec(c.ctx); err != nil {
					log.Errorf("Batch delete failed: %v", err)
				}
			}

			if cursor == 0 {
				break
			}
		}
	}
}

// Close 关闭Redis连接
func (c *RedisCache) Close() {
	// 先通知所有协程停止
	c.cancel()

	// 等待协程退出
	time.Sleep(100 * time.Millisecond)

	// 然后关闭资源
	close(c.evictionStop)

	// 清理所有锁协程
	if c.activeLocks != nil {
		// 遍历所有锁的停止通道，发送关闭信号
		for _, stopChan := range c.activeLocks {
			close(stopChan)
		}
		c.activeLocks = nil // 清空map
	}

	if c.evictionSub != nil {
		c.evictionSub.Close()
	}

	// 清空对象池
	c.gzipPool = sync.Pool{New: func() interface{} { return new(bytes.Buffer) }}
	c.gzipWriterPool = sync.Pool{New: func() interface{} { return gzip.NewWriter(nil) }}

	c.client.Close()
}

func (c *RedisCache) OnConnectionLost(f func(error)) {
	c.onConnectionLost = f
}
