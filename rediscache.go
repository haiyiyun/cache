package cache

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
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

// RedisCache 实现分布式安全的Redis缓存
type RedisCache struct {
	client               redis.UniversalClient
	defaultExpiration    time.Duration
	lockPrefix           string
	ctx                  context.Context
	cancel               context.CancelFunc
	scriptCache          map[string]*redis.Script
	scriptMutex          sync.RWMutex
	onEvicted            func(string, interface{})
	evictionSub          *redis.PubSub
	evictionMutex        sync.RWMutex
	evictionStop         chan struct{}
	evictionStarted      bool
	evictionQueue        []string
	evictionQueueMutex   sync.Mutex
	lockHolders          map[string]lockHolder
	lockHolderMutex      sync.Mutex
	compression          bool
	compressionThreshold int
	namespace            string
	isSharedDB           bool
	keyTrackerSet        string
}

type lockHolder struct {
	value      string
	renewCount int
	lastRenew  time.Time
}

// RedisOptions 封装Redis连接选项
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
	lockTimeout                 = 5 * time.Second
	lockRenewInterval           = 1 * time.Second
	maxRenewCount               = 10
	defaultPoolSize             = 100
	defaultCompressionThreshold = 1024
	maxBackoff                  = 2 * time.Second
	evictionBatchSize           = 100
	evictionProcessInterval     = 100 * time.Millisecond
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

		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}

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
	if options.ConnectTimeout <= 0 {
		options.ConnectTimeout = 5 * time.Second
	}
	if options.ReadTimeout <= 0 {
		options.ReadTimeout = 3 * time.Second
	}
	if options.WriteTimeout <= 0 {
		options.WriteTimeout = 3 * time.Second
	}
	if options.CompressionThreshold <= 0 {
		options.CompressionThreshold = defaultCompressionThreshold
	}

	// 设置默认命名空间
	if options.Namespace == "" {
		hostname, _ := os.Hostname()
		options.Namespace = fmt.Sprintf("hyy_cache:%s:%d:%d", hostname, os.Getpid(), rand.Int63())
	}

	// 解析DNS地址
	resolvedAddrs := resolveDNSAddresses(options.Addresses)
	if len(resolvedAddrs) == 0 {
		return nil, errors.New("no valid Redis addresses provided")
	}

	// 创建Redis客户端
	var client redis.UniversalClient

	switch {
	case len(resolvedAddrs) == 1 && options.MasterName == "":
		// 单节点模式
		client = redis.NewClient(&redis.Options{
			Addr:            resolvedAddrs[0],
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
			SentinelAddrs:   resolvedAddrs,
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
			Addrs:           resolvedAddrs,
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
		scriptCache:          make(map[string]*redis.Script),
		evictionStop:         make(chan struct{}),
		lockHolders:          make(map[string]lockHolder),
		compression:          options.Compression,
		compressionThreshold: options.CompressionThreshold,
		namespace:            options.Namespace,
		isSharedDB:           options.IsSharedDB,
		keyTrackerSet:        options.Namespace + ":" + keyTrackerSet,
	}

	// 预加载脚本
	if err := cache.loadScripts(); err != nil {
		return nil, fmt.Errorf("failed to load scripts: %w", err)
	}

	// 启动连接健康检查
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

// 序列化函数
func serialize(item Item) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(item); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// 反序列化函数
func deserialize(data []byte) (Item, error) {
	var item Item
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&item); err != nil {
		return Item{}, err
	}
	return item, nil
}

// 带压缩的序列化
func (c *RedisCache) serializeWithCompression(item Item) ([]byte, error) {
	data, err := serialize(item)
	if err != nil {
		return nil, err
	}

	// 如果启用压缩且数据大小超过阈值，则进行压缩
	if c.compression && len(data) > c.compressionThreshold {
		compressed, err := compressData(data)
		if err == nil {
			// 在压缩数据前添加压缩标记
			return append([]byte{1}, compressed...), nil
		}
		// 压缩失败时返回原始数据
		log.Printf("compression failed: %v, using uncompressed data", err)
	}

	// 未压缩数据添加未压缩标记
	return append([]byte{0}, data...), nil
}

// 带解压的反序列化
func (c *RedisCache) deserializeWithCompression(data []byte) (Item, error) {
	if len(data) == 0 {
		return Item{}, errors.New("empty data")
	}

	// 第一个字节是压缩标记
	flag := data[0]
	payload := data[1:]

	// 如果是压缩数据则解压
	if flag == 1 {
		decompressed, err := decompressData(payload)
		if err != nil {
			return Item{}, fmt.Errorf("decompression failed: %w", err)
		}
		return deserialize(decompressed)
	}

	// 未压缩数据直接反序列化
	return deserialize(payload)
}

// 压缩数据
func compressData(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(data); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// 解压数据
func decompressData(data []byte) ([]byte, error) {
	buf := bytes.NewBuffer(data)
	gz, err := gzip.NewReader(buf)
	if err != nil {
		return nil, err
	}
	defer gz.Close()

	return io.ReadAll(gz)
}

// 分布式锁优化
func (c *RedisCache) acquireLock(lockKey string, timeout time.Duration) (bool, string) {
	c.lockHolderMutex.Lock()
	defer c.lockHolderMutex.Unlock()

	// 检查是否已持有锁（可重入）
	if holder, exists := c.lockHolders[lockKey]; exists {
		if time.Since(holder.lastRenew) < lockRenewInterval*2 {
			holder.renewCount++
			holder.lastRenew = time.Now()
			c.lockHolders[lockKey] = holder
			return true, holder.value
		}
	}

	lockValue := generateLockID()
	start := time.Now()
	attempt := 0

	for {
		ok, err := c.client.SetNX(c.ctx, lockKey, lockValue, lockTimeout).Result()
		if err == nil && ok {
			c.lockHolders[lockKey] = lockHolder{
				value:      lockValue,
				renewCount: 0,
				lastRenew:  time.Now(),
			}
			// 启动锁续期协程
			go c.renewLock(lockKey, lockValue)
			return true, lockValue
		}

		// 指数退避
		attempt++
		backoff := time.Duration(math.Pow(2, float64(attempt))) * time.Millisecond
		if backoff > maxBackoff {
			backoff = maxBackoff
		}

		time.Sleep(backoff)

		if time.Since(start) > timeout {
			return false, ""
		}
	}
}

func (c *RedisCache) renewLock(lockKey, lockValue string) {
	ticker := time.NewTicker(lockRenewInterval)
	defer ticker.Stop()

	renewCount := 0
	for {
		select {
		case <-ticker.C:
			c.lockHolderMutex.Lock()
			holder, exists := c.lockHolders[lockKey]
			c.lockHolderMutex.Unlock()

			if !exists || holder.value != lockValue {
				return
			}

			renewed, err := c.client.Expire(c.ctx, lockKey, lockTimeout).Result()
			if err != nil || !renewed {
				return
			}

			renewCount++
			if renewCount >= maxRenewCount {
				return
			}

		case <-c.ctx.Done():
			return
		}
	}
}

func (c *RedisCache) releaseLock(lockKey, lockValue string) {
	c.lockHolderMutex.Lock()
	defer c.lockHolderMutex.Unlock()

	holder, exists := c.lockHolders[lockKey]
	if !exists || holder.value != lockValue {
		return
	}

	delete(c.lockHolders, lockKey)

	script := redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`)
	script.Run(c.ctx, c.client, []string{lockKey}, lockValue)
}

// 驱逐回调优化
func (c *RedisCache) startEvictionListener() {
	c.evictionSub = c.client.PSubscribe(c.ctx, "__keyevent@*__:expired")

	// 启动处理协程
	go c.processEvictionQueue()

	go func() {
		ch := c.evictionSub.ChannelSize(1000)
		for {
			select {
			case msg := <-ch:
				// 共享DB时检查命名空间前缀
				if c.isSharedDB && !strings.HasPrefix(msg.Payload, c.namespace+":") {
					continue
				}
				c.evictionQueueMutex.Lock()
				c.evictionQueue = append(c.evictionQueue, msg.Payload)
				c.evictionQueueMutex.Unlock()
			case <-c.evictionStop:
				return
			case <-c.ctx.Done():
				return
			}
		}
	}()
}

func (c *RedisCache) processEvictionQueue() {
	ticker := time.NewTicker(evictionProcessInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.evictionQueueMutex.Lock()
			if len(c.evictionQueue) == 0 {
				c.evictionQueueMutex.Unlock()
				continue
			}

			// 批量处理
			batchSize := evictionBatchSize
			if len(c.evictionQueue) < batchSize {
				batchSize = len(c.evictionQueue)
			}

			batch := make([]string, batchSize)
			copy(batch, c.evictionQueue[:batchSize])
			c.evictionQueue = c.evictionQueue[batchSize:]
			c.evictionQueueMutex.Unlock()

			// 处理批次
			for _, key := range batch {
				c.evictionMutex.RLock()
				if c.onEvicted != nil {
					// 共享DB时移除命名空间前缀
					userKey := key
					if c.isSharedDB {
						userKey = strings.TrimPrefix(key, c.namespace+":")
					}
					c.onEvicted(userKey, nil)
				}
				c.evictionMutex.RUnlock()
			}

		case <-c.evictionStop:
			return
		case <-c.ctx.Done():
			return
		}
	}
}

// 连接健康管理
func (c *RedisCache) checkConnection() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if _, err := c.client.Ping(c.ctx).Result(); err != nil {
				log.Printf("Redis connection lost: %v", err)
				c.reconnect()
			}

			// 检查连接池状态
			poolStats := c.client.PoolStats()
			if poolStats.Hits == 0 && poolStats.Misses > 100 {
				log.Println("Redis connection pool may be starved")
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
			log.Println("Redis connection reestablished")
			return
		}
		time.Sleep(time.Duration(i) * time.Second)
	}
	log.Fatal("Failed to reconnect to Redis after multiple attempts")
}

// 辅助函数
func generateLockID() string {
	hostname, _ := os.Hostname()
	return fmt.Sprintf("%s-%d-%d", hostname, os.Getpid(), rand.Int63())
}

func resolveDNSAddresses(addresses []string) []string {
	var resolved []string
	for _, addr := range addresses {
		if strings.Contains(addr, "://") {
			// 已经是IP地址或解析过的地址
			resolved = append(resolved, addr)
			continue
		}

		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			// 无效地址，跳过
			resolved = append(resolved, addr)
			continue
		}

		// 解析DNS
		ips, err := net.LookupIP(host)
		if err != nil || len(ips) == 0 {
			resolved = append(resolved, addr)
			continue
		}

		// 使用所有解析的IP
		for _, ip := range ips {
			resolved = append(resolved, net.JoinHostPort(ip.String(), port))
		}
	}
	return resolved
}

// 预加载Lua脚本
func (c *RedisCache) loadScripts() error {
	// 原子设置脚本（新增键追踪）
	setScript := redis.NewScript(`
		local key = KEYS[1]
		local value = ARGV[1]
		local expiration = tonumber(ARGV[2])
		local mode = ARGV[3]
		local tracker = ARGV[4]  -- 键追踪集合
		
		-- 检查模式
		if mode == "add" then
			if redis.call("EXISTS", key) == 1 then
				return 0
			end
		elseif mode == "replace" then
			if redis.call("EXISTS", key) == 0 then
				return 0
			end
		end
		
		-- 设置值
		redis.call("SET", key, value)
		
		-- 设置过期时间
		if expiration > 0 then
			redis.call("PEXPIRE", key, expiration)
		end
		
		if tracker ~= "" then
			redis.call("SADD", tracker, key)
		end
		
		return 1
	`)
	c.scriptCache["set_script"] = setScript

	incrScript := redis.NewScript(`
		local key = KEYS[1]
		local delta = tonumber(ARGV[1])
		local isFloat = ARGV[2] == "1"
		local tracker = ARGV[3]  -- 键追踪集合
		
		if isFloat then
			local result = redis.call("INCRBYFLOAT", key, delta)
			if tracker ~= "" then
				redis.call("SADD", tracker, key)
			end
			return result
		else
			-- 检查并执行整数操作
			local value = redis.call("GET", key)
			if not value then 
				value = "0"
				redis.call("SET", key, value)
				if tracker ~= "" then
					redis.call("SADD", tracker, key)
				end
			end
			local num = tonumber(value)
			if not num then return redis.error_reply("value not a number") end
			if math.floor(num) ~= num then 
				return redis.error_reply("value not integer") 
			end
			if math.floor(delta) ~= delta then
				return redis.error_reply("delta not integer")
			end
			local result = redis.call("INCRBY", key, math.floor(delta))
			return result
		end
	`)
	c.scriptCache["incr_script"] = incrScript

	deleteScript := redis.NewScript(`
		local key = KEYS[1]
		local tracker = ARGV[1]  -- 键追踪集合
		local value = redis.call("GET", key)
		if value then
			redis.call("DEL", key)
			if tracker ~= "" then
				redis.call("SREM", tracker, key)
			end
		end
		return value
	`)
	c.scriptCache["delete_script"] = deleteScript

	return nil
}

// getScript 获取或加载脚本
func (c *RedisCache) getScript(name string) *redis.Script {
	c.scriptMutex.RLock()
	defer c.scriptMutex.RUnlock()

	if script, ok := c.scriptCache[name]; ok {
		return script
	}

	return nil
}

// Set 添加项目到缓存
func (c *RedisCache) Set(k string, x interface{}, d time.Duration) error {
	return c.setWithMode(k, x, d, "set")
}

// SetDefault 使用默认过期时间添加项目
func (c *RedisCache) SetDefault(k string, x interface{}) error {
	return c.Set(k, x, DefaultExpiration)
}

// setWithMode 支持不同模式的设置操作
func (c *RedisCache) setWithMode(k string, x interface{}, d time.Duration, mode string) error {
	if d == DefaultExpiration {
		d = c.defaultExpiration
	}

	item := Item{
		Object:     x,
		Expiration: 0,
	}

	data, err := c.serializeWithCompression(item)
	if err != nil {
		return err
	}

	var expirationMs int64
	if d > 0 {
		expirationMs = int64(d / time.Millisecond)
	} else if d == NoExpiration {
		expirationMs = -1
	} else {
		expirationMs = 0
	}

	fullKey := c.generateKey(k)

	script := c.getScript("set_script")
	if script == nil {
		return errors.New("set script not found")
	}

	_, err = script.Run(c.ctx, c.client, []string{fullKey},
		data, expirationMs, mode, c.keyTrackerSet).Result()
	return err
}

// Add 仅当键不存在时添加项目
func (c *RedisCache) Add(k string, x interface{}, d time.Duration) error {
	return c.setWithMode(k, x, d, "add")
}

// Replace 仅当键存在时替换项目
func (c *RedisCache) Replace(k string, x interface{}, d time.Duration) error {
	return c.setWithMode(k, x, d, "replace")
}

// Get 获取项目
func (c *RedisCache) Get(k string) (interface{}, bool) {
	fullKey := c.generateKey(k)
	item, found, err := c.getItem(fullKey)
	if err != nil || !found {
		return nil, false
	}
	return item.Object, true
}

// GetWithExpiration 获取项目及其过期时间
func (c *RedisCache) GetWithExpiration(k string) (interface{}, time.Time, bool) {
	fullKey := c.generateKey(k)
	item, found, err := c.getItem(fullKey)
	if err != nil || !found {
		return nil, time.Time{}, false
	}

	ttl, err := c.client.TTL(c.ctx, fullKey).Result()
	if err != nil {
		return item.Object, time.Time{}, true
	}

	expiration := time.Time{}
	if ttl > 0 {
		expiration = time.Now().Add(ttl)
	}

	return item.Object, expiration, true
}

// getItem 内部方法获取项目
func (c *RedisCache) getItem(fullKey string) (Item, bool, error) {
	data, err := c.client.Get(c.ctx, fullKey).Bytes()
	if err != nil {
		if err == redis.Nil {
			return Item{}, false, nil
		}
		return Item{}, false, err
	}

	item, err := c.deserializeWithCompression(data)
	if err != nil {
		return Item{}, false, err
	}

	if item.Expired() {
		c.Delete(fullKey)
		return Item{}, false, nil
	}

	return item, true, nil
}

// Increment 增加数值类型
func (c *RedisCache) Increment(k string, n int64) error {
	fullKey := c.generateKey(k)
	lockKey := c.lockPrefix + fullKey

	acquired, lockValue := c.acquireLock(lockKey, 500*time.Millisecond)
	if !acquired {
		return errors.New("failed to acquire lock")
	}
	defer c.releaseLock(lockKey, lockValue)

	script := c.getScript("incr_script")
	if script == nil {
		return errors.New("increment script not found")
	}

	_, err := script.Run(c.ctx, c.client, []string{fullKey},
		n, "0", c.keyTrackerSet).Result()
	return err
}

// IncrementFloat 增加浮点类型
func (c *RedisCache) IncrementFloat(k string, n float64) error {
	fullKey := c.generateKey(k)
	lockKey := c.lockPrefix + fullKey

	acquired, lockValue := c.acquireLock(lockKey, 500*time.Millisecond)
	if !acquired {
		return errors.New("failed to acquire lock")
	}
	defer c.releaseLock(lockKey, lockValue)

	script := c.getScript("incr_script")
	if script == nil {
		return errors.New("increment script not found")
	}

	_, err := script.Run(c.ctx, c.client, []string{fullKey},
		n, "1", c.keyTrackerSet).Result()
	return err
}

// Decrement 减少数值类型
func (c *RedisCache) Decrement(k string, n int64) error {
	return c.Increment(k, -n)
}

// DecrementFloat 减少浮点类型
func (c *RedisCache) DecrementFloat(k string, n float64) error {
	return c.IncrementFloat(k, -n)
}

// Delete 删除项目
func (c *RedisCache) Delete(k string) {
	fullKey := c.generateKey(k)

	script := c.getScript("delete_script")
	if script == nil {
		c.client.Del(c.ctx, fullKey)
		if c.keyTrackerSet != "" {
			c.client.SRem(c.ctx, c.keyTrackerSet, fullKey)
		}
		return
	}

	result, err := script.Run(c.ctx, c.client, []string{fullKey}, c.keyTrackerSet).Result()
	if err != nil {
		c.client.Del(c.ctx, fullKey)
		if c.keyTrackerSet != "" {
			c.client.SRem(c.ctx, c.keyTrackerSet, fullKey)
		}
		return
	}

	// 如果有驱逐回调且获取到了值
	if c.onEvicted != nil && result != nil {
		if data, ok := result.(string); ok {
			if item, err := c.deserializeWithCompression([]byte(data)); err == nil {
				userKey := k
				if c.isSharedDB {
					userKey = strings.TrimPrefix(k, c.namespace+":")
				}
				c.onEvicted(userKey, item.Object)
			} else {
				c.onEvicted(k, nil)
			}
		}
	}
}

// DeleteExpired 删除过期项目
func (c *RedisCache) DeleteExpired() {
	// Redis自动处理
}

// OnEvicted 设置驱逐回调函数
func (c *RedisCache) OnEvicted(f func(string, interface{})) {
	c.evictionMutex.Lock()
	defer c.evictionMutex.Unlock()

	c.onEvicted = f

	// 如果设置了回调且尚未启动监听，则启动键空间通知监听
	if f != nil && !c.evictionStarted {
		c.startEvictionListener()
		c.evictionStarted = true
	} else if f == nil && c.evictionStarted {
		// 停止监听
		c.stopEvictionListener()
		c.evictionStarted = false
	}
}

// stopEvictionListener 停止键空间通知监听
func (c *RedisCache) stopEvictionListener() {
	if c.evictionSub != nil {
		c.evictionSub.Close()
		c.evictionSub = nil
	}
	close(c.evictionStop)
	c.evictionStop = make(chan struct{})
}

// Items 获取所有项目
func (c *RedisCache) Items() map[string]Item {
	items := make(map[string]Item)

	// 在独用DB模式下，直接扫描整个DB
	if !c.isSharedDB {
		var cursor uint64
		for {
			var keys []string
			var err error

			// 根据客户端类型使用不同的扫描方法
			switch client := c.client.(type) {
			case *redis.ClusterClient:
				// 集群模式 - 遍历所有主节点
				err = client.ForEachMaster(c.ctx, func(ctx context.Context, node *redis.Client) error {
					scanKeys, nextCursor, e := node.Scan(ctx, cursor, "*", 1000).Result()
					if e != nil {
						return e
					}
					keys = append(keys, scanKeys...)
					cursor = nextCursor
					return nil
				})
			default:
				// 单节点或Sentinel模式
				keys, cursor, err = c.client.Scan(c.ctx, cursor, "*", 1000).Result()
			}

			if err != nil {
				log.Printf("Error scanning keys: %v", err)
				break
			}

			// 批量获取值
			if len(keys) > 0 {
				values, err := c.client.MGet(c.ctx, keys...).Result()
				if err != nil {
					log.Printf("Error getting values: %v", err)
				} else {
					for i, val := range values {
						if val == nil {
							continue
						}

						strVal, ok := val.(string)
						if !ok {
							continue
						}

						item, err := c.deserializeWithCompression([]byte(strVal))
						if err != nil {
							log.Printf("Deserialization error for key %s: %v", keys[i], err)
							continue
						}

						if item.Expired() {
							c.Delete(keys[i])
							continue
						}

						// 在独用模式下，键名就是原始键名
						items[keys[i]] = item
					}
				}
			}

			if cursor == 0 {
				break
			}
		}
	} else {
		// 共享DB模式 - 使用键追踪集合
		keys, err := c.client.SMembers(c.ctx, c.keyTrackerSet).Result()
		if err != nil {
			log.Printf("Error getting keys from tracker: %v", err)
			return items
		}

		// 批量获取值
		batchSize := 100
		for i := 0; i < len(keys); i += batchSize {
			end := i + batchSize
			if end > len(keys) {
				end = len(keys)
			}

			batch := keys[i:end]
			values, err := c.client.MGet(c.ctx, batch...).Result()
			if err != nil {
				log.Printf("Error getting batch values: %v", err)
				continue
			}

			for j, val := range values {
				if val == nil {
					continue
				}

				strVal, ok := val.(string)
				if !ok {
					continue
				}

				item, err := c.deserializeWithCompression([]byte(strVal))
				if err != nil {
					log.Printf("Deserialization error for key %s: %v", batch[j], err)
					continue
				}

				if item.Expired() {
					c.Delete(batch[j])
					continue
				}

				// 在共享模式下，需要移除命名空间前缀
				userKey := strings.TrimPrefix(batch[j], c.namespace+":")
				items[userKey] = item
			}
		}
	}

	return items
}

func (c *RedisCache) Interfaces() map[string]interface{} {
	items := make(map[string]interface{})
	for k, item := range c.Items() {
		items[k] = item.Object
	}
	return items
}

// ItemCount 获取项目数量
func (c *RedisCache) ItemCount() int {
	// 在独用DB模式下，使用DBSIZE命令
	if !c.isSharedDB {
		size, err := c.client.DBSize(c.ctx).Result()
		if err != nil {
			return 0
		}
		return int(size)
	}

	// 共享DB模式使用键追踪集合
	count, err := c.client.SCard(c.ctx, c.keyTrackerSet).Result()
	if err != nil {
		return 0
	}
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
	keys, err := c.client.SMembers(c.ctx, c.keyTrackerSet).Result()
	if err != nil {
		return
	}

	// 批量删除
	batchSize := 100
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}

		batch := keys[i:end]
		c.client.Del(c.ctx, batch...)
	}

	// 清空键追踪集合
	c.client.Del(c.ctx, c.keyTrackerSet)
}

// Close 关闭Redis连接
func (c *RedisCache) Close() {
	c.cancel()

	// 停止驱逐监听
	if c.evictionStop != nil {
		close(c.evictionStop)
	}

	if c.evictionSub != nil {
		c.evictionSub.Close()
	}

	c.client.Close()
}

// Save 保存缓存到文件 - 不支持
func (c *RedisCache) Save(w io.Writer) error {
	return errors.New("not supported in RedisCache")
}

// SaveFile 保存缓存到文件 - 不支持
func (c *RedisCache) SaveFile(fname string) error {
	return errors.New("not supported in RedisCache")
}

// Load 从文件加载缓存 - 不支持
func (c *RedisCache) Load(r io.Reader) error {
	return errors.New("not supported in RedisCache")
}

// LoadFile 从文件加载缓存 - 不支持
func (c *RedisCache) LoadFile(fname string) error {
	return errors.New("not supported in RedisCache")
}
