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
	"math"
	"math/rand"
	"net"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/haiyiyun/log"
	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
)

// 类型注册系统
var typeRegistry = struct {
	sync.RWMutex
	types    map[string]reflect.Type
	names    map[reflect.Type]string
	encoders map[reflect.Type]func(interface{}) ([]byte, error) // 修改为函数类型
	decoders map[reflect.Type]func([]byte) (interface{}, error) // 修改为函数类型
}{
	types:    make(map[string]reflect.Type),
	names:    make(map[reflect.Type]string),
	encoders: make(map[reflect.Type]func(interface{}) ([]byte, error)),
	decoders: make(map[reflect.Type]func([]byte) (interface{}, error)),
}

// 注册自定义类型
func RegisterType(name string, typ interface{}) {
	t := reflect.TypeOf(typ)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	typeRegistry.Lock()
	defer typeRegistry.Unlock()

	typeRegistry.types[name] = t
	typeRegistry.names[t] = name
}

// 注册自定义编码器/解码器
func RegisterCodec(typ interface{},
	encoder func(interface{}) ([]byte, error),
	decoder func([]byte) (interface{}, error)) {

	t := reflect.TypeOf(typ)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	typeRegistry.Lock()
	defer typeRegistry.Unlock()

	typeRegistry.encoders[t] = encoder
	typeRegistry.decoders[t] = decoder
}

// 序列化包装器
type serializedObject struct {
	Type  string      `msgpack:"t"` // 类型标识符
	Value interface{} `msgpack:"v"` // 原始值
}

// 初始化基本类型
func init() {
	RegisterType("int", 0)
	RegisterType("int8", int8(0))
	RegisterType("int16", int16(0))
	RegisterType("int32", int32(0))
	RegisterType("int64", int64(0))
	RegisterType("uint", uint(0))
	RegisterType("uint8", uint8(0))
	RegisterType("uint16", uint16(0))
	RegisterType("uint32", uint32(0))
	RegisterType("uint64", uint64(0))
	RegisterType("float32", float32(0))
	RegisterType("float64", float64(0))
	RegisterType("bool", false)
	RegisterType("string", "")
	RegisterType("time.Time", time.Time{})
	RegisterType("[]byte", []byte{})
	RegisterType("[]int", []int{})
	RegisterType("[]string", []string{})
	RegisterType("map[string]string", map[string]string{})
	RegisterType("map[string]interface{}", map[string]interface{}{})
}

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
	typeCache            sync.Map // 类型反射缓存
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

// 序列化对象（使用自定义包装器）
func (c *RedisCache) serializeObject(obj interface{}) ([]byte, error) {
	// 获取类型名称
	typeName := ""
	if obj != nil {
		t := reflect.TypeOf(obj)
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}

		typeRegistry.RLock()
		name, exists := typeRegistry.names[t]
		typeRegistry.RUnlock()

		if !exists {
			name = t.String()
		}
		typeName = name
	}

	// 创建序列化对象
	wrapper := serializedObject{
		Type:  typeName,
		Value: obj,
	}

	// 序列化
	return msgpack.Marshal(wrapper)
}

// 反序列化对象（使用自定义包装器）
func (c *RedisCache) deserializeObject(data []byte) (interface{}, error) {
	var wrapper serializedObject
	if err := msgpack.Unmarshal(data, &wrapper); err != nil {
		return nil, err
	}

	// 如果没有类型信息，直接返回值
	if wrapper.Type == "" {
		return wrapper.Value, nil
	}

	// 获取类型
	typeRegistry.RLock()
	typ, exists := typeRegistry.types[wrapper.Type]
	typeRegistry.RUnlock()

	if !exists {
		// 尝试从字符串解析类型
		if typ = c.resolveTypeFromString(wrapper.Type); typ == nil {
			return wrapper.Value, nil
		}
	}

	// 创建目标类型的实例
	target := reflect.New(typ).Interface()

	// 使用自定义解码器（如果存在）
	typeRegistry.RLock()
	decoder, hasDecoder := typeRegistry.decoders[typ]
	typeRegistry.RUnlock()

	if hasDecoder {
		// 将wrapper.Value序列化为字节，然后使用自定义解码器
		valueBytes, err := msgpack.Marshal(wrapper.Value)
		if err != nil {
			return nil, err
		}

		return decoder(valueBytes)
	}

	// 标准解码：将值映射到目标类型
	valueBytes, err := msgpack.Marshal(wrapper.Value)
	if err != nil {
		return nil, err
	}

	if err := msgpack.Unmarshal(valueBytes, target); err != nil {
		return nil, err
	}

	// 解引用指针
	if reflect.TypeOf(target).Kind() == reflect.Ptr {
		return reflect.ValueOf(target).Elem().Interface(), nil
	}

	return target, nil
}

// 从类型字符串解析反射类型
func (c *RedisCache) resolveTypeFromString(typeStr string) reflect.Type {
	// 检查类型缓存
	if typ, ok := c.typeCache.Load(typeStr); ok {
		return typ.(reflect.Type)
	}

	// 尝试解析类型
	parts := strings.Split(typeStr, ".")
	if len(parts) < 2 {
		return nil
	}

	// 获取包路径和类型名
	pkgPath := strings.Join(parts[:len(parts)-1], ".")
	typeName := parts[len(parts)-1]

	// 使用反射查找类型
	var foundType reflect.Type
	reflectTypes := []reflect.Type{
		reflect.TypeOf((*error)(nil)).Elem(),
		reflect.TypeOf((*interface{})(nil)).Elem(),
	}

	for _, rt := range reflectTypes {
		if rt.PkgPath() == pkgPath && rt.Name() == typeName {
			foundType = rt
			break
		}
	}

	// 缓存结果
	if foundType != nil {
		c.typeCache.Store(typeStr, foundType)
	}

	return foundType
}

// 带压缩的序列化
func (c *RedisCache) serializeWithCompression(item Item) ([]byte, error) {
	// 序列化对象部分
	objData, err := c.serializeObject(item.Object)
	if err != nil {
		return nil, err
	}

	// 创建完整Item的序列化数据
	fullItem := struct {
		Object     []byte `msgpack:"o"`
		Expiration int64  `msgpack:"e"`
	}{
		Object:     objData,
		Expiration: item.Expiration,
	}

	data, err := msgpack.Marshal(fullItem)
	if err != nil {
		return nil, err
	}

	if c.compression && len(data) > c.compressionThreshold {
		compressed, err := compressData(data)
		if err == nil {
			return append([]byte{1}, compressed...), nil
		}
		log.Errorf("compression failed: %v, using uncompressed data", err)
	}

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

	var decompressed []byte
	var err error

	// 如果是压缩数据则解压
	if flag == 1 {
		decompressed, err = decompressData(payload)
		if err != nil {
			return Item{}, fmt.Errorf("decompression failed: %w", err)
		}
	} else {
		decompressed = payload
	}

	// 解析完整Item结构
	var fullItem struct {
		Object     []byte `msgpack:"o"`
		Expiration int64  `msgpack:"e"`
	}

	if err := msgpack.Unmarshal(decompressed, &fullItem); err != nil {
		return Item{}, err
	}

	// 反序列化对象
	obj, err := c.deserializeObject(fullItem.Object)
	if err != nil {
		return Item{}, err
	}

	return Item{
		Object:     obj,
		Expiration: fullItem.Expiration,
	}, nil
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
				log.Errorf("Redis connection lost: %v", err)
				c.reconnect()
			}

			// 检查连接池状态
			poolStats := c.client.PoolStats()
			if poolStats.Hits == 0 && poolStats.Misses > 100 {
				log.Errorln("Redis connection pool may be starved")
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
			log.Errorln("Redis connection reestablished")
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
	if d == DefaultExpiration {
		d = c.defaultExpiration
	}

	var expiration int64
	if d > 0 {
		expiration = time.Now().Add(d).UnixNano()
	} else if d == NoExpiration {
		expiration = 0
	} else {
		expiration = 0
	}

	item := Item{
		Object:     x,
		Expiration: expiration,
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
		data, expirationMs, "set", c.keyTrackerSet).Result()
	return err
}

// SetDefault 使用默认过期时间添加项目
func (c *RedisCache) SetDefault(k string, x interface{}) error {
	return c.Set(k, x, DefaultExpiration)
}

// Add 仅当键不存在时添加项目
func (c *RedisCache) Add(k string, x interface{}, d time.Duration) error {
	if d == DefaultExpiration {
		d = c.defaultExpiration
	}

	var expiration int64
	if d > 0 {
		expiration = time.Now().Add(d).UnixNano()
	} else if d == NoExpiration {
		expiration = 0
	} else {
		expiration = 0
	}

	item := Item{
		Object:     x,
		Expiration: expiration,
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
		data, expirationMs, "add", c.keyTrackerSet).Result()
	return err
}

// Replace 仅当键存在时替换项目
func (c *RedisCache) Replace(k string, x interface{}, d time.Duration) error {
	if d == DefaultExpiration {
		d = c.defaultExpiration
	}

	var expiration int64
	if d > 0 {
		expiration = time.Now().Add(d).UnixNano()
	} else if d == NoExpiration {
		expiration = 0
	} else {
		expiration = 0
	}

	item := Item{
		Object:     x,
		Expiration: expiration,
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
		data, expirationMs, "replace", c.keyTrackerSet).Result()
	return err
}

func (c *RedisCache) Get(k string, target interface{}) (bool, error) {
	found, _, err := c.getTyped(k, target)
	return found, err
}

// GetWithExpiration 获取项目及其过期时间
func (c *RedisCache) GetWithExpiration(k string, target interface{}) (bool, time.Time) {
	found, expiration, _ := c.getTyped(k, target)
	if expiration > 0 {
		return found, time.Unix(0, expiration)
	}

	return found, time.Time{}
}

// getTyped 类型安全获取
func (c *RedisCache) getTyped(k string, target interface{}) (bool, int64, error) {
	fullKey := c.generateKey(k)
	data, err := c.client.Get(c.ctx, fullKey).Bytes()
	if err != nil {
		if err == redis.Nil {
			return false, 0, nil
		}
		return false, 0, err
	}

	// 处理压缩数据
	if len(data) > 0 {
		flag := data[0]
		payload := data[1:]

		if flag == 1 {
			decompressed, err := decompressData(payload)
			if err != nil {
				return false, 0, fmt.Errorf("decompression failed: %w", err)
			}
			data = decompressed
		} else {
			data = payload
		}
	}

	// 解析完整Item结构
	var fullItem struct {
		Object     []byte `msgpack:"o"`
		Expiration int64  `msgpack:"e"`
	}

	if err := msgpack.Unmarshal(data, &fullItem); err != nil {
		return false, 0, err
	}

	// 获取目标类型
	targetValue := reflect.ValueOf(target)
	if targetValue.Kind() != reflect.Ptr || targetValue.IsNil() {
		return false, 0, errors.New("target must be a non-nil pointer")
	}
	targetType := targetValue.Type().Elem()

	// 反序列化对象
	obj, err := c.deserializeObject(fullItem.Object)
	if err != nil {
		return false, 0, err
	}

	// 处理结构体转换
	if targetType.Kind() == reflect.Struct {
		if objMap, ok := obj.(map[string]interface{}); ok {
			if err := c.mapToStruct(objMap, target); err != nil {
				return false, 0, fmt.Errorf("struct conversion failed: %w", err)
			}
			return true, fullItem.Expiration, nil
		}
	}

	// 检查目标类型是否为切片
	if targetType.Kind() == reflect.Slice {
		found, err := c.handleSliceConversion(obj, targetValue, targetType)
		return found, fullItem.Expiration, err
	}

	// 验证类型
	objType := reflect.TypeOf(obj)
	if objType != targetType {
		return false, 0, fmt.Errorf("type mismatch: expected %s, got %s",
			targetType.String(), objType.String())
	}

	targetValue.Elem().Set(reflect.ValueOf(obj))
	return true, fullItem.Expiration, nil
}

// 处理切片类型转换
func (c *RedisCache) handleSliceConversion(obj interface{}, targetValue reflect.Value, targetType reflect.Type) (bool, error) {
	// 获取目标切片的元素类型
	elemType := targetType.Elem()

	// 检查对象是否为切片
	objValue := reflect.ValueOf(obj)
	if objValue.Kind() != reflect.Slice {
		return false, fmt.Errorf("expected slice, got %s", objValue.Kind())
	}

	// 创建目标切片
	newSlice := reflect.MakeSlice(targetType, objValue.Len(), objValue.Len())

	// 转换每个元素
	for i := 0; i < objValue.Len(); i++ {
		elem := objValue.Index(i).Interface()

		// 如果元素已经是目标类型，直接赋值
		if reflect.TypeOf(elem) == elemType {
			newSlice.Index(i).Set(reflect.ValueOf(elem))
			continue
		}

		// 尝试转换 map 到结构体
		if elemMap, ok := elem.(map[string]interface{}); ok && elemType.Kind() == reflect.Struct {
			if err := c.mapToStruct(elemMap, newSlice.Index(i).Addr().Interface()); err != nil {
				return false, fmt.Errorf("element %d conversion failed: %w", i, err)
			}
			continue
		}

		// 尝试直接转换
		elemValue := reflect.ValueOf(elem)
		if elemValue.Type().ConvertibleTo(elemType) {
			converted := elemValue.Convert(elemType)
			newSlice.Index(i).Set(converted)
			continue
		}

		return false, fmt.Errorf("element %d type mismatch: expected %s, got %s",
			i, elemType.String(), reflect.TypeOf(elem).String())
	}

	targetValue.Elem().Set(newSlice)
	return true, nil
}

// 将 map 转换为结构体
func (c *RedisCache) mapToStruct(m map[string]interface{}, target interface{}) error {
	targetValue := reflect.ValueOf(target)
	if targetValue.Kind() != reflect.Ptr || targetValue.IsNil() {
		return errors.New("target must be a non-nil pointer to a struct")
	}

	targetElem := targetValue.Elem()
	if targetElem.Kind() != reflect.Struct {
		return errors.New("target must be a pointer to a struct")
	}

	// 使用反射设置字段
	targetType := targetElem.Type()
	for i := 0; i < targetType.NumField(); i++ {
		field := targetType.Field(i)
		fieldName := field.Name

		// 获取 msgpack 标签（如果有）
		if tag := field.Tag.Get("msgpack"); tag != "" {
			if parts := strings.Split(tag, ","); len(parts) > 0 {
				if parts[0] != "" {
					fieldName = parts[0]
				}
			}
		} else if tag := field.Tag.Get("map"); tag != "" {
			if parts := strings.Split(tag, ","); len(parts) > 0 {
				if parts[0] != "" {
					fieldName = parts[0]
				}
			}
		} else if tag := field.Tag.Get("json"); tag != "" {
			if parts := strings.Split(tag, ","); len(parts) > 0 {
				if parts[0] != "" {
					fieldName = parts[0]
				}
			}
		} else if tag := field.Tag.Get("bson"); tag != "" {
			if parts := strings.Split(tag, ","); len(parts) > 0 {
				if parts[0] != "" {
					fieldName = parts[0]
				}
			}
		}

		// 从 map 中获取值
		value, exists := m[fieldName]
		if !exists {
			// 尝试小写字段名（Go 默认序列化使用小写）
			value, exists = m[strings.ToLower(fieldName)]
			if !exists {
				continue
			}
		}

		fieldValue := targetElem.Field(i)

		// 设置字段值
		if err := c.setFieldValue(fieldValue, value); err != nil {
			return fmt.Errorf("field %s: %w", field.Name, err)
		}
	}

	return nil
}

// 设置字段值（处理类型转换）
func (c *RedisCache) setFieldValue(field reflect.Value, value interface{}) error {
	// 检查字段是否可设置
	if !field.CanSet() {
		return nil
	}

	// 处理 value 为 nil 的情况
	if value == nil {
		// 如果字段是指针类型，设置为 nil
		if field.Kind() == reflect.Ptr || field.Kind() == reflect.Interface {
			field.Set(reflect.Zero(field.Type()))
		}
		return nil
	}

	fieldType := field.Type()
	valueType := reflect.TypeOf(value)

	// 如果类型匹配，直接设置
	if valueType.AssignableTo(fieldType) {
		field.Set(reflect.ValueOf(value))
		return nil
	}

	// 尝试转换基本类型
	switch fieldType.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if num, ok := value.(int64); ok {
			field.SetInt(num)
			return nil
		}
		if num, ok := value.(int); ok {
			field.SetInt(int64(num))
			return nil
		}
		if num, ok := value.(float64); ok {
			field.SetInt(int64(num))
			return nil
		}
		if str, ok := value.(string); ok {
			if num, err := strconv.ParseInt(str, 10, 64); err == nil {
				field.SetInt(num)
				return nil
			}
		}

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if num, ok := value.(uint64); ok {
			field.SetUint(num)
			return nil
		}
		if num, ok := value.(int); ok && num >= 0 {
			field.SetUint(uint64(num))
			return nil
		}
		if num, ok := value.(int64); ok && num >= 0 {
			field.SetUint(uint64(num))
			return nil
		}
		if num, ok := value.(float64); ok && num >= 0 {
			field.SetUint(uint64(num))
			return nil
		}
		if str, ok := value.(string); ok {
			if num, err := strconv.ParseUint(str, 10, 64); err == nil {
				field.SetUint(num)
				return nil
			}
		}

	case reflect.Float32, reflect.Float64:
		if num, ok := value.(float64); ok {
			field.SetFloat(num)
			return nil
		}
		if num, ok := value.(int); ok {
			field.SetFloat(float64(num))
			return nil
		}
		if num, ok := value.(int64); ok {
			field.SetFloat(float64(num))
			return nil
		}
		if str, ok := value.(string); ok {
			if num, err := strconv.ParseFloat(str, 64); err == nil {
				field.SetFloat(num)
				return nil
			}
		}

	case reflect.String:
		if str, ok := value.(string); ok {
			field.SetString(str)
			return nil
		}
		// 尝试将数字转换为字符串
		if num, ok := value.(int64); ok {
			field.SetString(strconv.FormatInt(num, 10))
			return nil
		}
		if num, ok := value.(float64); ok {
			field.SetString(strconv.FormatFloat(num, 'f', -1, 64))
			return nil
		}
		if num, ok := value.(int); ok {
			field.SetString(strconv.Itoa(num))
			return nil
		}
		// 其他类型直接转换为字符串
		field.SetString(fmt.Sprint(value))
		return nil

	case reflect.Bool:
		if b, ok := value.(bool); ok {
			field.SetBool(b)
			return nil
		}
		if str, ok := value.(string); ok {
			if b, err := strconv.ParseBool(str); err == nil {
				field.SetBool(b)
				return nil
			}
		}
		if num, ok := value.(int64); ok {
			field.SetBool(num != 0)
			return nil
		}

	case reflect.Struct:
		// 处理嵌套结构体
		if valueMap, ok := value.(map[string]interface{}); ok {
			// 确保字段是结构体类型
			if fieldType.Kind() != reflect.Struct {
				return fmt.Errorf("field is not a struct")
			}

			// 创建一个新的结构体实例
			newField := reflect.New(fieldType)
			if err := c.mapToStruct(valueMap, newField.Interface()); err != nil {
				return err
			}
			field.Set(newField.Elem())
			return nil
		}

	case reflect.Slice:
		// 处理切片
		sliceValue := reflect.ValueOf(value)
		if sliceValue.Kind() != reflect.Slice {
			return fmt.Errorf("expected slice, got %s", sliceValue.Kind())
		}

		// 创建目标类型的切片
		newSlice := reflect.MakeSlice(fieldType, sliceValue.Len(), sliceValue.Len())
		for i := 0; i < sliceValue.Len(); i++ {
			elem := sliceValue.Index(i).Interface()
			if err := c.setFieldValue(newSlice.Index(i), elem); err != nil {
				return fmt.Errorf("slice element %d: %w", i, err)
			}
		}
		field.Set(newSlice)
		return nil

	case reflect.Ptr:
		// 处理指针类型
		if field.IsNil() {
			field.Set(reflect.New(fieldType.Elem()))
		}
		return c.setFieldValue(field.Elem(), value)
	}

	// 尝试直接转换
	valueValue := reflect.ValueOf(value)
	if valueValue.IsValid() && valueValue.Type().ConvertibleTo(fieldType) {
		converted := valueValue.Convert(fieldType)
		field.Set(converted)
		return nil
	}

	return fmt.Errorf("cannot convert %s to %s", valueType, fieldType)
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
