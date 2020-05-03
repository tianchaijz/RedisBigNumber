package main

import (
	"flag"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/go-redis/redis"
)

var (
	_clear    = flag.Bool("c", false, "clear redis entries")
	_clients  = flag.Int("n", 1, "number of clients")
	_poolSize = flag.Int("p", 0, "pool size")
	_server   = flag.String("s", "127.0.0.1:7379", "redis server")
	_iters    = flag.Int("i", 100, "iterations")

	_radixKey  = "bn:radix"
	_fracKey   = "bn:frac"
	_randomKey = "bn:random"
	_hashKey   = "bn:hash"
	_eps       = "0.000000000000000000000000000000001"
	_delta     = "0.00000000000000000000000000000001"

	_signals   = []os.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT}
	_quitCh    = make(chan bool)
	_count     int64
	_wg        sync.WaitGroup
	_redisOpts *redis.Options
)

var (
	// _apdCtx is the default context for decimal operations.
	// decimal128
	_apdCtx = &apd.Context{
		Precision:   34,
		Rounding:    apd.RoundDown,
		MaxExponent: 6144,
		MinExponent: -6143,
		// Don't error on invalid operation, return NaN instead.
		Traps: apd.DefaultTraps &^ apd.InvalidOperation,
	}

	_apd1       = mustParseDecimal("1")
	_apdEpsilon = mustParseDecimal(_eps)
	_apdDelta   = mustParseDecimal(_delta)

	_apdLock       sync.Mutex
	_apdRadix      = mustParseDecimal("0")
	_apdFrac       = mustParseDecimal("0")
	_apdRandom     = mustParseDecimal("0")
	_apdHashRadix  = mustParseDecimal("0")
	_apdHashFrac   = mustParseDecimal("0")
	_apdHashRandom = mustParseDecimal("0")
)

type Operation int

const (
	OpADD Operation = iota
	OpSUB
	OpMUL
	OpDIV
	OpTO_FIXED
	OpINCR
	OpDECR
	OpINCRBY
	OpDECRBY
	OpHINCR
	OpHDECR
	OpHINCRBY
	OpHDECRBY
	OpRANDOM
	OpHRANDOM
)

func mustParseDecimal(s string) *apd.Decimal {
	d, _, err := _apdCtx.NewFromString(s)
	if err != nil {
		panic(err)
	}
	return d
}

func randFloat() string {
	// [-100, 100)
	r := -100 + rand.Float64()*200
	return strconv.FormatFloat(r, 'f', -1, 64)
}

func doCmd(client *redis.Client, args ...interface{}) interface{} {
	val, err := client.Do(args...).Result()
	if err != nil {
		panic(err)
	}
	return val
}

func cmdAdd(client *redis.Client) {
	doCmd(client, "bn.add", _delta, _delta)
}

func cmdSub(client *redis.Client) {
	doCmd(client, "bn.sub", _delta, _delta)
}

func cmdMul(client *redis.Client) {
	doCmd(client, "bn.mul", _delta, _delta)
}

func cmdDiv(client *redis.Client) {
	doCmd(client, "bn.div", _delta, _delta)
}

func cmdToFixed(client *redis.Client) {
	doCmd(client, "bn.to_fixed", "0.123456789", 2)
}

func cmdIncr(client *redis.Client) {
	doCmd(client, "bn.incr", _radixKey)

	_apdLock.Lock()
	defer _apdLock.Unlock()
	_apdCtx.Add(_apdRadix, _apdRadix, _apd1)
}

func cmdDecr(client *redis.Client) {
	doCmd(client, "bn.decr", _radixKey)

	_apdLock.Lock()
	defer _apdLock.Unlock()
	_apdCtx.Sub(_apdRadix, _apdRadix, _apd1)
}

func cmdIncrby(client *redis.Client) {
	doCmd(client, "bn.incrby", _fracKey, _delta)

	_apdLock.Lock()
	defer _apdLock.Unlock()
	_apdCtx.Add(_apdFrac, _apdFrac, _apdDelta)
}

func cmdDecrby(client *redis.Client) {
	doCmd(client, "bn.decrby", _fracKey, _delta)

	_apdLock.Lock()
	defer _apdLock.Unlock()
	_apdCtx.Sub(_apdFrac, _apdFrac, _apdDelta)
}

func cmdRandom(client *redis.Client) {
	v := randFloat()
	incr := rand.Intn(2) == 1
	if incr {
		doCmd(client, "bn.incrby", _randomKey, v)
	} else {
		doCmd(client, "bn.decrby", _randomKey, v)
	}

	d := mustParseDecimal(v)

	_apdLock.Lock()
	defer _apdLock.Unlock()
	if incr {
		_apdCtx.Add(_apdRandom, _apdRandom, d)
	} else {
		_apdCtx.Sub(_apdRandom, _apdRandom, d)
	}
}

func cmdHincr(client *redis.Client) {
	doCmd(client, "bn.hincr", _hashKey, _radixKey)

	_apdLock.Lock()
	defer _apdLock.Unlock()
	_apdCtx.Add(_apdHashRadix, _apdHashRadix, _apd1)
}

func cmdHdecr(client *redis.Client) {
	doCmd(client, "bn.hdecr", _hashKey, _radixKey)

	_apdLock.Lock()
	defer _apdLock.Unlock()
	_apdCtx.Sub(_apdHashRadix, _apdHashRadix, _apd1)
}

func cmdHincrby(client *redis.Client) {
	doCmd(client, "bn.hincrby", _hashKey, _fracKey, _delta)

	_apdLock.Lock()
	defer _apdLock.Unlock()
	_apdCtx.Add(_apdHashFrac, _apdHashFrac, _apdDelta)
}

func cmdHdecrby(client *redis.Client) {
	doCmd(client, "bn.hdecrby", _hashKey, _fracKey, _delta)

	_apdLock.Lock()
	defer _apdLock.Unlock()
	_apdCtx.Sub(_apdHashFrac, _apdHashFrac, _apdDelta)
}

func cmdHrandom(client *redis.Client) {
	v := randFloat()
	incr := rand.Intn(2) == 1
	if incr {
		doCmd(client, "bn.hincrby", _hashKey, _randomKey, v)
	} else {
		doCmd(client, "bn.hdecrby", _hashKey, _randomKey, v)
	}

	d := mustParseDecimal(v)

	_apdLock.Lock()
	defer _apdLock.Unlock()
	if incr {
		_apdCtx.Add(_apdHashRandom, _apdHashRandom, d)
	} else {
		_apdCtx.Sub(_apdHashRandom, _apdHashRandom, d)
	}
}

func loop(cmd func(client *redis.Client)) {
	_wg.Add(1)
	defer _wg.Done()

	client := redis.NewClient(_redisOpts)
	defer client.Close()

	for {
		select {
		case <-_quitCh:
			return
		default:
			for i := 0; i < *_iters; i++ {
				cmd(client)
				atomic.AddInt64(&_count, 1)
			}
		}
	}
}

func wait() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, _signals...)

	<-ch
	close(_quitCh)
	_wg.Wait()
}

func clear() {
	client := redis.NewClient(_redisOpts)
	defer client.Close()

	keys, _ := client.Keys("bn:*").Result()

	for _, key := range keys {
		log.Printf("del key=%s", key)
		client.Del(key)
	}
}

func bench() {
	ops := []struct {
		op   Operation
		name string
		cmd  func(client *redis.Client)
	}{
		{OpADD, "OpADD", cmdAdd},
		{OpSUB, "OpSUB", cmdSub},
		{OpMUL, "OpMUL", cmdMul},
		{OpDIV, "OpDIV", cmdDiv},
		{OpTO_FIXED, "OpTO_FIXED", cmdToFixed},
		{OpINCR, "OpINCR", cmdIncr},
		{OpDECR, "OpDECR", cmdDecr},
		{OpINCRBY, "OpINCRBY", cmdIncrby},
		{OpDECRBY, "OpDECRBY", cmdDecrby},
		{OpRANDOM, "OpRANDOM", cmdRandom},
		{OpHINCR, "OpHINCR", cmdHincr},
		{OpHDECR, "OpHDECR", cmdHdecr},
		{OpHINCRBY, "OpHINCRBY", cmdHincrby},
		{OpHDECRBY, "OpHDECRBY", cmdHdecrby},
		{OpHRANDOM, "OpHRANDOM", cmdHrandom},
	}

	for i := 0; i < *_clients; i++ {
		op := ops[rand.Intn(len(ops))]
		log.Printf("index=%d op=%s", i, op.name)
		go loop(op.cmd)
	}
}

func report(elapsed time.Duration) {
	client := redis.NewClient(_redisOpts)
	defer client.Close()

	log.Printf("key=%s redis=%s apd=%s", _radixKey, client.Get(_radixKey).Val(), _apdRadix.String())
	log.Printf("key=%s redis=%s apd=%s", _fracKey, client.Get(_fracKey).Val(), _apdFrac.String())
	log.Printf("key=%s redis=%s apd=%s", _randomKey, client.Get(_randomKey).Val(), _apdRandom.String())
	log.Printf("key=%s[%s] redis=%s apd=%s", _hashKey, _radixKey, client.HGet(_hashKey, _radixKey).Val(), _apdHashRadix.String())
	log.Printf("key=%s[%s] redis=%s apd=%s", _hashKey, _fracKey, client.HGet(_hashKey, _fracKey).Val(), _apdHashFrac.String())
	log.Printf("key=%s[%s] redis=%s apd=%s", _hashKey, _randomKey, client.HGet(_hashKey, _randomKey).Val(), _apdHashRandom.String())

	count := atomic.LoadInt64(&_count)
	qps := float64(count) / elapsed.Seconds()

	log.Printf("count=%d qps=%f", count, qps)
}

func main() {
	flag.Parse()

	startTime := time.Now()
	rand.Seed(startTime.UnixNano())

	_redisOpts = &redis.Options{
		Addr:     *_server,
		PoolSize: *_poolSize,
	}

	if *_clear {
		clear()
	}

	bench()
	wait()
	report(time.Since(startTime))
}
