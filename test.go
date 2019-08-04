package main

import (
	"flag"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-redis/redis"
)

var (
	gClear    = flag.Bool("c", false, "clear redis entries")
	gClients  = flag.Int("n", 1, "number of clients")
	gPoolSize = flag.Int("p", 0, "pool size")
	gAddress  = flag.String("address", "127.0.0.1:6379", "redis address")

	gIters    = 100
	gRadixKey = "bn:radix"
	gFracKey  = "bn:frac"
	gHash     = "bn:hash"
	gEpsilon  = "0.000000000000000000000000000000001"
	gDelta    = "0.00000000000000000000000000000001"

	gSignals  = []os.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT}
	gQuitChan = make(chan bool)
	gCount    int64
	gWg       sync.WaitGroup
	gOpts     *redis.Options
)

type Operation int

const (
	OpPING Operation = iota
	OpINCR
	OpDECR
	OpINCRBY
	OpDECRBY
	OpHINCR
	OpHDECR
	OpHINCRBY
	OpHDECRBY
)

func doCmd(client *redis.Client, args ...interface{}) interface{} {
	val, err := client.Do(args...).Result()
	if err != nil {
		panic(err)
	}
	return val
}

func ping(client *redis.Client) {
	client.Ping()
}

func incr(client *redis.Client) {
	doCmd(client, "bn.incr", gRadixKey)
}

func decr(client *redis.Client) {
	doCmd(client, "bn.decr", gRadixKey)
}

func incrby(client *redis.Client) {
	doCmd(client, "bn.incrby", gFracKey, gDelta)
}

func decrby(client *redis.Client) {
	doCmd(client, "bn.decrby", gFracKey, gDelta)
}

func hincr(client *redis.Client) {
	doCmd(client, "bn.hincr", gHash, gRadixKey)
}

func hdecr(client *redis.Client) {
	doCmd(client, "bn.hdecr", gHash, gRadixKey)
}

func hincrby(client *redis.Client) {
	doCmd(client, "bn.hincrby", gHash, gFracKey, gDelta)
}

func hdecrby(client *redis.Client) {
	doCmd(client, "bn.hdecrby", gHash, gFracKey, gDelta)
}

func loop(op Operation) {
	var (
		cmd func(client *redis.Client)
	)

	switch op {
	case OpPING:
		cmd = ping
	case OpINCR:
		cmd = incr
	case OpDECR:
		cmd = decr
	case OpINCRBY:
		cmd = incrby
	case OpDECRBY:
		cmd = decrby
	case OpHINCR:
		cmd = hincr
	case OpHDECR:
		cmd = hdecr
	case OpHINCRBY:
		cmd = hincrby
	case OpHDECRBY:
		cmd = hdecrby
	}

	gWg.Add(1)
	defer gWg.Done()

	client := redis.NewClient(gOpts)
	defer client.Close()

	for {
		select {
		case <-gQuitChan:
			return
		default:
			for i := 0; i < gIters; i++ {
				cmd(client)
				atomic.AddInt64(&gCount, 1)
			}
		}
	}
}

func wait() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, gSignals...)

	<-ch
	close(gQuitChan)
	gWg.Wait()
}

func clear() {
	client := redis.NewClient(gOpts)
	defer client.Close()

	client.Del(gRadixKey)
	client.Set(gFracKey, gEpsilon, 0)
	client.Del(gHash)
	client.HSet(gHash, gFracKey, gEpsilon)

	log.Print("cleared")
}

func bench() {
	ops := []struct {
		op   Operation
		name string
	}{
		{OpINCR, "OpINCR"},
		{OpDECR, "OpDECR"},
		{OpINCRBY, "OpINCRBY"},
		{OpDECRBY, "OpDECRBY"},
		{OpHINCR, "OpHINCR"},
		{OpHDECR, "OpHDECR"},
		{OpHINCRBY, "OpHINCRBY"},
		{OpHDECRBY, "OpHDECRBY"},
	}

	for i := 0; i < *gClients; i++ {
		op := ops[rand.Intn(len(ops))]
		log.Printf("[%2d]%s", i, op.name)
		go loop(op.op)
	}
}

func report(elapsed time.Duration) {
	client := redis.NewClient(gOpts)
	defer client.Close()

	log.Printf("%s: %s", gRadixKey, client.Get(gRadixKey).Val())
	log.Printf("%s: %s", gFracKey, client.Get(gFracKey).Val())
	log.Printf("%s[%s]: %s", gHash, gRadixKey, client.HGet(gHash, gRadixKey).Val())
	log.Printf("%s[%s]: %s", gHash, gFracKey, client.HGet(gHash, gFracKey).Val())

	count := atomic.LoadInt64(&gCount)
	qps := float64(count) / elapsed.Seconds()

	log.Printf("count: %d, qps: %f", count, qps)
}

func main() {
	flag.Parse()

	startTime := time.Now()
	rand.Seed(startTime.Unix())

	gOpts = &redis.Options{
		Addr:     *gAddress,
		PoolSize: *gPoolSize,
	}

	if *gClear {
		clear()
	}

	bench()
	wait()
	report(time.Now().Sub(startTime))
}
