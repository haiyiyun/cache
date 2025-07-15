package cache

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/haiyiyun/log"
	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
)

/*
RedisCache 实现分布式安全的Redis缓存

URL 格式支持：
1. 单节点模式：
   redis://[username:password@]host[:port][/dbnumber]
   示例:
     redis://localhost:6379/0
     redis://user:pass@redis.example.com:6380/1

2. 哨兵模式：
   redis-sentinel://[username:password@]host1:port1,host2:port2[?master=name&db=dbnumber]
   示例:
     redis-sentinel://sentinel1:26379,sentinel2:26379?master=mymaster&db=0

3. 集群模式：
   redis-cluster://[username:password@]host1:port1,host2:port2[?db=dbnumber]
   示例:
     redis-cluster://cluster-node1:6379,cluster-node2:6379,cluster-node3:6379

4. TLS 加密连接：
   使用 rediss:// 协议前缀：
   rediss://[username:password@]host[:port][/dbnumber][?insecureSkipVerify=true]
   示例:
     rediss://secure.redis.com:6379/0
     rediss://user:pass@redis.example.com:6380/1?tlsCAFile=ca.pem

5. 高级参数：
   所有模式都支持以下查询参数：
   - poolsize: 连接池大小 (默认 100)
   - shareddb: 是否共享数据库 (true/false, 默认 false)
   - compression: 是否启用压缩 (true/false, 默认 false)
   - compression_threshold: 压缩阈值字节数 (默认 1024)
   - namespace: 缓存命名空间
   - connect_timeout: 连接超时 (如 5s)
   - read_timeout: 读取超时 (如 3s)
   - write_timeout: 写入超时 (如 3s)
   - min_retry_backoff: 最小重试间隔 (如 100ms)
   - max_retry_backoff: 最大重试间隔 (如 2s)
   - max_retries: 最大重试次数 (默认 3)

TLS 参数：
   - tlsCAFile: CA 证书文件路径
   - tlsCertFile: 客户端证书文件路径
   - tlsKeyFile: 客户端私钥文件路径
   - insecureSkipVerify: 是否跳过证书验证 (true/false)

Redis 服务器强制配置要求：
1. 必须启用键空间通知功能，配置示例：
   notify-keyspace-events "Ex"

2. 当使用集群模式时，确保所有节点配置一致
3. 若使用持久化，建议配置为：
   appendonly yes
   appendfsync everysec

4. 内存管理建议设置最大内存限制：
   maxmemory 2gb
   maxmemory-policy allkeys-lru
*/

type RedisCache struct {
	client               redis.UniversalClient
	defaultExpiration    time.Duration
	lockPrefix           string
	ctx                  context.Context
	cancel               context.CancelFunc
	compression          bool
	compressionThreshold int
	namespace            string
	isSharedDB           bool
	keyTrackerSet        string
	onEvicted            func(string, interface{})
	evictionSub          *redis.PubSub
	evictionStop         chan struct{}
	gzipPool             sync.Pool
}

type RedisOptions struct {
	Addresses            []string      // Redis地址列表
	Username             string        // 用户名
	Password             string        // 密码
	DB                   int           // 数据库编号
	MasterName           string        // Sentinel主节点名称
	ConnectTimeout       time.Duration // 连接超时
	ReadTimeout          time.Duration // 读取超时
	WriteTimeout         time.Duration // 写入超时
	MinRetryBackoff      time.Duration // 最小重试间隔
	MaxRetryBackoff      time.Duration // 最大重试间隔
	MaxRetries           int           // 最大重试次数
	PoolSize             int           // 连接池大小
	Compression          bool          // 启用压缩
	CompressionThreshold int           // 压缩阈值(字节)
	Namespace            string        // 缓存命名空间
	IsSharedDB           bool          // 是否共享DB
	UseTLS               bool          // 使用TLS
	TLSConfig            *tls.Config   // TLS配置
}

const (
	defaultPoolSize             = 100
	defaultCompressionThreshold = 1024
	lockTimeout                 = 5 * time.Second
	keyTrackerSet               = "hyy_cache_keys"
	defaultRedisPort            = "6379"
)

// ParseRedisURL 解析Redis连接URL
func ParseRedisURL(redisURL string) (RedisOptions, error) {
	opt := RedisOptions{
		PoolSize:             defaultPoolSize,
		CompressionThreshold: defaultCompressionThreshold,
		MaxRetries:           3,
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
		client:               client,
		defaultExpiration:    defaultExpiration,
		lockPrefix:           "hyy_cache_lock:",
		ctx:                  ctx,
		cancel:               cancel,
		compression:          options.Compression,
		compressionThreshold: options.CompressionThreshold,
		namespace:            options.Namespace,
		isSharedDB:           options.IsSharedDB,
		keyTrackerSet:        options.Namespace + ":" + keyTrackerSet,
		evictionStop:         make(chan struct{}),
		gzipPool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}

	go cache.checkConnection()
	return cache, nil
}

// 根据DB共享模式生成键名
func (c *RedisCache) generateKey(key string) string {
	if c.isSharedDB {
		return fmt.Sprintf("%s:%s", c.namespace, key)
	}
	return key
}

func (c *RedisCache) serialize(item Item) ([]byte, error) {
	data, err := msgpack.Marshal(item)
	if err != nil {
		return nil, err
	}

	if c.compression && len(data) > c.compressionThreshold {
		return c.compress(data)
	}
	return append([]byte{0}, data...), nil
}

func (c *RedisCache) deserialize(data []byte) (Item, error) {
	if len(data) < 1 {
		return Item{}, errors.New("invalid data length")
	}

	// 第一个字节是压缩标记
	flag := data[0]
	payload := data[1:]

	if flag == 1 {
		decompressed, err := c.decompress(payload)
		if err != nil {
			return Item{}, fmt.Errorf("decompression failed: %w", err)
		}
		payload = decompressed
	}

	var item Item
	if err := msgpack.Unmarshal(payload, &item); err != nil {
		return Item{}, err
	}
	return item, nil
}

func (c *RedisCache) compress(data []byte) ([]byte, error) {
	buf := c.gzipPool.Get().(*bytes.Buffer)
	defer c.gzipPool.Put(buf)
	buf.Reset()

	gz := gzip.NewWriter(buf)
	if _, err := gz.Write(data); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}

	compressed := make([]byte, buf.Len())
	copy(compressed, buf.Bytes())
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

// 分布式锁
func (c *RedisCache) acquireLock(lockKey string) (bool, string) {
	lockValue := fmt.Sprintf("%d", time.Now().UnixNano())
	ok, err := c.client.SetNX(c.ctx, lockKey, lockValue, lockTimeout).Result()
	if err != nil || !ok {
		return false, ""
	}
	return true, lockValue
}

func (c *RedisCache) releaseLock(lockKey, lockValue string) {
	script := redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		end
		return 0
	`)
	script.Run(c.ctx, c.client, []string{lockKey}, lockValue)
}

// 驱逐回调优化
func (c *RedisCache) startEvictionListener() {
	c.evictionSub = c.client.PSubscribe(c.ctx, "__keyevent@*__:expired")

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
}

// 连接健康管理
func (c *RedisCache) checkConnection() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if _, err := c.client.Ping(c.ctx).Result(); err != nil {
				log.Errorf("Redis connection error: %v", err)
				c.reconnect()
			}
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *RedisCache) reconnect() {
	// 实现重连逻辑
	for i := 0; i < 5; i++ {
		if _, err := c.client.Ping(c.ctx).Result(); err == nil {
			return
		}
		time.Sleep(time.Duration(i) * time.Second)
	}
	log.Error("Failed to reconnect to Redis")
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

	if c.keyTrackerSet != "" {
		c.client.SAdd(c.ctx, c.keyTrackerSet, fullKey)
	}
	return nil
}

func (c *RedisCache) SetDefault(k string, x interface{}) error {
	return c.Set(k, x, DefaultExpiration)
}

func (c *RedisCache) Add(k string, x interface{}, d time.Duration) error {
	fullKey := c.generateKey(k)
	lockKey := c.lockPrefix + fullKey

	acquired, lockValue := c.acquireLock(lockKey)
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

	acquired, lockValue := c.acquireLock(lockKey)
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

func (c *RedisCache) Increment(k string, n int64) error {
	return c.modifyNumber(k, n, false)
}

func (c *RedisCache) IncrementFloat(k string, n float64) error {
	return c.modifyNumber(k, n, true)
}

func (c *RedisCache) modifyNumber(k string, delta interface{}, isFloat bool) error {
	fullKey := c.generateKey(k)
	lockKey := c.lockPrefix + fullKey

	acquired, lockValue := c.acquireLock(lockKey)
	if !acquired {
		return errors.New("failed to acquire lock")
	}
	defer c.releaseLock(lockKey, lockValue)

	var err error
	if isFloat {
		_, err = c.client.IncrByFloat(c.ctx, fullKey, delta.(float64)).Result()
	} else {
		_, err = c.client.IncrBy(c.ctx, fullKey, delta.(int64)).Result()
	}

	if err == nil && c.keyTrackerSet != "" {
		c.client.SAdd(c.ctx, c.keyTrackerSet, fullKey)
	}
	return err
}

func (c *RedisCache) Decrement(k string, n int64) error {
	return c.Increment(k, -n)
}

func (c *RedisCache) DecrementFloat(k string, n float64) error {
	return c.IncrementFloat(k, -n)
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

// OnEvicted 设置驱逐回调函数
func (c *RedisCache) OnEvicted(f func(string, interface{})) {
	c.onEvicted = f
	if f != nil && c.evictionSub == nil {
		c.startEvictionListener()
	} else if f == nil && c.evictionSub != nil {
		c.evictionSub.Close()
		c.evictionSub = nil
	}
}

// ItemCount 获取项目数量
func (c *RedisCache) ItemCount() int {
	// 在独用DB模式下，使用DBSIZE命令
	if !c.isSharedDB {
		size, _ := c.client.DBSize(c.ctx).Result()
		return int(size)
	}

	// 共享DB模式使用键追踪集合
	count, _ := c.client.SCard(c.ctx, c.keyTrackerSet).Result()
	return int(count)
}

// Flush 清空缓存
func (c *RedisCache) Flush() {
	// 在独用DB模式下，使用FLUSHDB命令
	if !c.isSharedDB {
		c.client.FlushDB(c.ctx)
		return
	}

	// 共享DB模式 - 只清空当前命名空间的键
	keys, _ := c.client.SMembers(c.ctx, c.keyTrackerSet).Result()
	if len(keys) > 0 {
		c.client.Del(c.ctx, keys...)
	}
	c.client.Del(c.ctx, c.keyTrackerSet)
}

// Close 关闭Redis连接
func (c *RedisCache) Close() {
	c.cancel()
	if c.evictionSub != nil {
		c.evictionSub.Close()
	}
	c.client.Close()
}
