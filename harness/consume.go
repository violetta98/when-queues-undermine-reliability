package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"sync"
	"sync/atomic"
	"time"
)

const (
	cOutDone    = "done"
	cOutExpired = "skipped_expired" // consumer-side deadline check fired
)

type consumerCfg struct {
	broker     string
	addr       string
	queue      string
	group      string
	workers    int
	prefetch   int
	baseMs     float64
	plan       string
	t0Ns       int64
	runSec     float64
	out        string
	events     string
	checkDL    bool
	downstream float64 // shared capacity limit, msg/s; 0 = unlimited
}

func runConsume() {
	var c consumerCfg
	flag.StringVar(&c.broker, "broker", "rabbit", "rabbit|kafka")
	flag.StringVar(&c.addr, "addr", "amqp://guest:guest@127.0.0.1:5672/", "broker address")
	flag.StringVar(&c.queue, "queue", "main", "queue (rabbit) or topic (kafka)")
	flag.StringVar(&c.group, "group", "main-cg", "kafka consumer group")
	flag.IntVar(&c.workers, "workers", 100, "concurrent in-flight handlers")
	flag.IntVar(&c.prefetch, "prefetch", 200, "rabbit basic.qos prefetch")
	flag.Float64Var(&c.baseMs, "service-ms", 50, "nominal per-message service time")
	flag.StringVar(&c.plan, "plan", "60:540:5", "slowStart:slowEnd:factor, seconds from t0")
	var t0 int64
	flag.Int64Var(&t0, "t0", 0, "absolute run origin, unix nanoseconds")
	flag.Float64Var(&c.runSec, "duration", 900, "stop consuming this many seconds after t0")
	flag.StringVar(&c.out, "out", "consumer.csv", "per-message output")
	flag.StringVar(&c.events, "events", "consumer_events.csv", "consumer-level events")
	flag.BoolVar(&c.checkDL, "deadline-check", false, "drop messages past their deadline instead of processing them")
	flag.Float64Var(&c.downstream, "downstream-limit", 0, "shared downstream capacity in msg/s (0 = unlimited)")
	flag.Parse()
	if t0 == 0 {
		t0 = time.Now().UnixNano()
	}
	c.t0Ns = t0
	checkT0(t0, 0)

	switch c.broker {
	case "rabbit":
		consumeRabbit(c)
	case "kafka":
		consumeKafka(c)
	default:
		fatal(fmt.Errorf("unknown broker %q", c.broker))
	}
}

// bucket models a shared downstream capacity limit. Without it, adding handler
// goroutines always increases throughput, which would make consumer autoscaling
// look unconditionally effective; real downstreams rate-limit.
type bucket struct {
	tokens chan struct{}
	stop   chan struct{}
}

func newBucket(rate float64) *bucket {
	if rate <= 0 {
		return nil
	}
	b := &bucket{tokens: make(chan struct{}, int(rate)+1), stop: make(chan struct{})}
	go func() {
		const tick = 10 * time.Millisecond
		per := rate * tick.Seconds()
		carry := 0.0
		t := time.NewTicker(tick)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				carry += per
				n := int(carry)
				carry -= float64(n)
				for i := 0; i < n; i++ {
					select {
					case b.tokens <- struct{}{}:
					default:
					}
				}
			case <-b.stop:
				return
			}
		}
	}()
	return b
}

// take blocks for a token, but only inside the slow window: the limit models a
// downstream that starts rate-limiting during the incident, not one that is
// permanently slower than the workload. Outside that window it is a no-op, so
// arms that set no limit are unaffected.
func (b *bucket) take(ctx context.Context, inSlow bool) {
	if b == nil || !inSlow {
		return
	}
	select {
	case <-b.tokens:
	case <-ctx.Done():
	}
}

func consumeRabbit(c consumerCfg) {
	plan, err := parsePlan(c.plan, c.baseMs)
	fatal(err)
	out, err := newCSV(c.out, "seq,t_create_ns,deadline_ns,t_receive_ns,t_done_ns,outcome,partition,offset")
	fatal(err)
	defer out.close()
	ev, err := newCSVLive(c.events, "t_ns,event,detail")
	fatal(err)
	defer ev.close()

	conn, err := dialAMQP(c.addr)
	fatal(err)
	defer conn.Close()
	ch, err := conn.Channel()
	fatal(err)
	defer ch.Close()
	fatal(ch.Qos(c.prefetch, 0, false))
	deliveries, err := ch.Consume(c.queue, "", false, false, false, false, nil)
	fatal(err)

	t0 := time.Unix(0, c.t0Ns)
	ctx, cancel := context.WithDeadline(context.Background(), t0.Add(time.Duration(c.runSec*float64(time.Second))))
	defer cancel()

	bk := newBucket(c.downstream)
	var processed, expired int64
	var wg sync.WaitGroup
	ev.row(i64(time.Now().UnixNano()), "consumer.start",
		fmt.Sprintf("workers=%d prefetch=%d service_ms=%.1f plan=%s deadline_check=%v downstream=%.0f",
			c.workers, c.prefetch, c.baseMs, c.plan, c.checkDL, c.downstream))

	worker := func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case d, ok := <-deliveries:
				if !ok {
					return
				}
				recvNs := time.Now().UnixNano()
				seq, createNs, deadlineNs, ok2 := decode(d.Body)
				if !ok2 {
					d.Ack(false)
					continue
				}
				if c.checkDL && recvNs > deadlineNs {
					d.Ack(false)
					atomic.AddInt64(&expired, 1)
					out.row(u64(seq), i64(createNs), i64(deadlineNs), i64(recvNs), i64(recvNs), cOutExpired, "", "")
					continue
				}
				elapsed := float64(recvNs-c.t0Ns) / 1e9
				bk.take(ctx, plan.inSlow(elapsed))
				time.Sleep(plan.serviceAt(elapsed))
				d.Ack(false)
				doneNs := time.Now().UnixNano()
				atomic.AddInt64(&processed, 1)
				out.row(u64(seq), i64(createNs), i64(deadlineNs), i64(recvNs), i64(doneNs), cOutDone, "", "")
			}
		}
	}
	for i := 0; i < c.workers; i++ {
		wg.Add(1)
		go worker()
	}
	wg.Wait()
	ev.row(i64(time.Now().UnixNano()), "consumer.stop",
		fmt.Sprintf("processed=%d expired=%d", atomic.LoadInt64(&processed), atomic.LoadInt64(&expired)))
}

func consumeKafka(c consumerCfg) {
	plan, err := parsePlan(c.plan, c.baseMs)
	fatal(err)
	out, err := newCSV(c.out, "seq,t_create_ns,deadline_ns,t_receive_ns,t_done_ns,outcome,partition,offset")
	fatal(err)
	defer out.close()
	ev, err := newCSVLive(c.events, "t_ns,event,detail")
	fatal(err)
	defer ev.close()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.addr),
		kgo.ConsumeTopics(c.queue),
		kgo.ConsumerGroup(c.group),
		kgo.DisableAutoCommit(),
		// When retention deletes a range the group has not reached yet, the
		// fetch fails with OFFSET_OUT_OF_RANGE and the client restarts from the
		// earliest surviving offset. That jump is exactly the silent loss we
		// want to observe, so we record it rather than hide it.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	fatal(err)
	defer cl.Close()

	t0 := time.Unix(0, c.t0Ns)
	ctx, cancel := context.WithDeadline(context.Background(), t0.Add(time.Duration(c.runSec*float64(time.Second))))
	defer cancel()

	bk := newBucket(c.downstream)
	lastOffset := map[int32]int64{}
	var processed, expired int64
	ev.row(i64(time.Now().UnixNano()), "consumer.start",
		fmt.Sprintf("workers=%d service_ms=%.1f plan=%s deadline_check=%v downstream=%.0f group=%s",
			c.workers, c.baseMs, c.plan, c.checkDL, c.downstream, c.group))

	for ctx.Err() == nil {
		fetches := cl.PollRecords(ctx, 500)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachError(func(t string, p int32, err error) {
			if ctx.Err() == nil {
				ev.row(i64(time.Now().UnixNano()), "fetch.error", fmt.Sprintf("%s/%d %v", t, p, err))
			}
		})
		var recs []*kgo.Record
		fetches.EachRecord(func(r *kgo.Record) { recs = append(recs, r) })
		if len(recs) == 0 {
			continue
		}
		for _, r := range recs {
			if last, ok := lastOffset[r.Partition]; ok && r.Offset > last+1 {
				ev.row(i64(time.Now().UnixNano()), "offset.jump",
					fmt.Sprintf("partition=%d from=%d to=%d skipped=%d", r.Partition, last, r.Offset, r.Offset-last-1))
			}
			lastOffset[r.Partition] = r.Offset
		}

		sem := make(chan struct{}, c.workers)
		var wg sync.WaitGroup
		for _, r := range recs {
			wg.Add(1)
			sem <- struct{}{}
			go func(r *kgo.Record) {
				defer wg.Done()
				defer func() { <-sem }()
				recvNs := time.Now().UnixNano()
				seq, createNs, deadlineNs, ok := decode(r.Value)
				if !ok {
					return
				}
				if c.checkDL && recvNs > deadlineNs {
					atomic.AddInt64(&expired, 1)
					out.row(u64(seq), i64(createNs), i64(deadlineNs), i64(recvNs), i64(recvNs), cOutExpired,
						fmt.Sprint(r.Partition), i64(r.Offset))
					return
				}
				elapsed := float64(recvNs-c.t0Ns) / 1e9
				bk.take(ctx, plan.inSlow(elapsed))
				time.Sleep(plan.serviceAt(elapsed))
				doneNs := time.Now().UnixNano()
				atomic.AddInt64(&processed, 1)
				out.row(u64(seq), i64(createNs), i64(deadlineNs), i64(recvNs), i64(doneNs), cOutDone,
					fmt.Sprint(r.Partition), i64(r.Offset))
			}(r)
		}
		wg.Wait()
		if err := cl.CommitRecords(context.Background(), recs...); err != nil && ctx.Err() == nil {
			ev.row(i64(time.Now().UnixNano()), "commit.error", err.Error())
		}
	}
	ev.row(i64(time.Now().UnixNano()), "consumer.stop",
		fmt.Sprintf("processed=%d expired=%d", atomic.LoadInt64(&processed), atomic.LoadInt64(&expired)))
}
