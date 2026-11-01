package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Publish outcomes recorded in producer.csv. Every produced sequence number
// ends up in exactly one of these, which is what makes the loss accounting in
// analysis/analyze.py add up.
const (
	outConfirmed  = "confirmed"            // broker acked
	outNacked     = "nacked"               // broker refused (reject-publish / quota)
	outError      = "error"                // transport or protocol error
	outSkipped    = "skipped_backpressure" // producer shed it: socket blocked
	outRejectedBP = "rejected_local_bp"    // producer-side lag backpressure (Kafka)
)

type producerCfg struct {
	broker     string
	addr       string
	queue      string
	rate       float64
	size       int
	deadlineMs int
	t0Ns       int64
	startSec   float64
	durSec     float64
	out        string
	events     string
	lagLimit   int64
	group      string
	partitions int
}

func runProduce() {
	var c producerCfg
	flag.StringVar(&c.broker, "broker", "rabbit", "rabbit|kafka")
	flag.StringVar(&c.addr, "addr", "amqp://guest:guest@127.0.0.1:5672/", "broker address")
	flag.StringVar(&c.queue, "queue", "main", "queue (rabbit) or topic (kafka)")
	flag.Float64Var(&c.rate, "rate", 1000, "target publish rate, msg/s")
	flag.IntVar(&c.size, "size", 1024, "message size in bytes")
	flag.IntVar(&c.deadlineMs, "deadline-ms", 60000, "useful lifetime D of each message")
	var t0 int64
	flag.Int64Var(&t0, "t0", 0, "absolute run origin, unix nanoseconds")
	flag.Float64Var(&c.startSec, "start", 0, "begin publishing this many seconds after t0")
	flag.Float64Var(&c.durSec, "duration", 600, "publish for this many seconds")
	flag.StringVar(&c.out, "out", "producer.csv", "per-message output")
	flag.StringVar(&c.events, "events", "producer_events.csv", "connection-level events")
	flag.Int64Var(&c.lagLimit, "lag-backpressure", 0, "kafka: reject locally when group lag exceeds this (0=off)")
	flag.StringVar(&c.group, "group", "main-cg", "kafka: consumer group to watch for backpressure")
	flag.IntVar(&c.partitions, "partitions", 6, "kafka: partitions to spread over")
	flag.Parse()
	if t0 == 0 {
		t0 = time.Now().UnixNano()
	}
	c.t0Ns = t0
	checkT0(t0, c.startSec)

	switch c.broker {
	case "rabbit":
		produceRabbit(c)
	case "kafka":
		produceKafka(c)
	default:
		fatal(fmt.Errorf("unknown broker %q", c.broker))
	}
}

// pacer yields the wall-clock instant at which message i is due. The schedule
// is absolute rather than incremental, so a producer that was blocked by the
// broker resumes on the original timeline instead of emitting a catch-up burst.
type pacer struct {
	t0   time.Time
	rate float64
}

func (p pacer) due(i int64) time.Time {
	return p.t0.Add(time.Duration(float64(i) / p.rate * float64(time.Second)))
}

// msg is one scheduled message handed from the pacing goroutine to the
// publishing goroutine.
type msg struct {
	seq      uint64
	createNs int64
	body     []byte
}

// produceRabbit publishes at a fixed rate with publisher confirms.
//
// The pacing loop and the actual publishing run in separate goroutines joined by
// a buffer that holds one second of output. When the broker blocks the
// connection, the publishing goroutine stalls on the socket, the buffer fills,
// and the pacing loop records the messages it cannot hand over as shed. That is
// what a producer with a small local buffer does, and it keeps the schedule
// honest: no timeouts on a half-written frame, and no burst to catch up.
//
// The publisher reconnects when the broker closes the connection, because a
// producer that gave up on the first error would understate how much a blocked
// broker actually costs.
func produceRabbit(c producerCfg) {
	out, err := newCSV(c.out, "seq,t_create_ns,t_publish_ns,t_confirm_ns,size,outcome,partition,offset")
	fatal(err)
	defer out.close()
	ev, err := newCSVLive(c.events, "t_ns,event,detail")
	fatal(err)
	defer ev.close()

	bufSize := int(c.rate)
	if bufSize < 1 {
		bufSize = 1
	}
	sendCh := make(chan msg, bufSize)
	done := make(chan struct{})

	go func() {
		defer close(done)
		type pending struct {
			seq      uint64
			createNs int64
			pubNs    int64
		}
		var reconnects int
		for {
			conn, err := dialAMQP(c.addr)
			if err != nil {
				ev.row(i64(time.Now().UnixNano()), "connect.error", err.Error())
				time.Sleep(250 * time.Millisecond)
				continue
			}
			ch, err := conn.Channel()
			if err == nil {
				err = ch.Confirm(false)
			}
			if err != nil {
				ev.row(i64(time.Now().UnixNano()), "channel.error", err.Error())
				conn.Close()
				time.Sleep(250 * time.Millisecond)
				continue
			}

			confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 200000))
			blocked := conn.NotifyBlocked(make(chan amqp.Blocking, 64))
			closed := conn.NotifyClose(make(chan *amqp.Error, 4))

			var mu sync.Mutex
			pend := map[uint64]pending{}
			var drain sync.WaitGroup
			drain.Add(1)
			go func() {
				defer drain.Done()
				for cf := range confirms {
					now := time.Now().UnixNano()
					mu.Lock()
					p, ok := pend[cf.DeliveryTag]
					if ok {
						delete(pend, cf.DeliveryTag)
					}
					mu.Unlock()
					if !ok {
						continue
					}
					o := outConfirmed
					if !cf.Ack {
						o = outNacked
					}
					out.row(u64(p.seq), i64(p.createNs), i64(p.pubNs), i64(now),
						fmt.Sprint(c.size), o, "", "")
				}
			}()
			go func() {
				for b := range blocked {
					st := "unblocked"
					if b.Active {
						st = "blocked"
					}
					ev.row(i64(time.Now().UnixNano()), "connection."+st, b.Reason)
				}
			}()
			go func() {
				for e := range closed {
					if e != nil {
						ev.row(i64(time.Now().UnixNano()), "connection.closed",
							fmt.Sprintf("%d %s", e.Code, e.Reason))
					}
				}
			}()

			var tag uint64
			publishing := true
			for publishing {
				m, ok := <-sendCh
				if !ok {
					publishing = false
					break
				}
				tag++
				mu.Lock()
				pend[tag] = pending{seq: m.seq, createNs: m.createNs, pubNs: time.Now().UnixNano()}
				mu.Unlock()
				err := ch.PublishWithContext(context.Background(), "", c.queue, false, false,
					amqp.Publishing{
						DeliveryMode: amqp.Persistent,
						Body:         m.body,
						Timestamp:    time.Unix(0, m.createNs),
					})
				if err != nil {
					mu.Lock()
					delete(pend, tag)
					mu.Unlock()
					out.row(u64(m.seq), i64(m.createNs), i64(m.createNs), "",
						fmt.Sprint(c.size), outError, "", "")
					ev.row(i64(time.Now().UnixNano()), "publish.error", err.Error())
					break
				}
			}

			// Let confirms for what we already sent land, then account for
			// anything still outstanding as unconfirmed.
			deadline := time.Now().Add(15 * time.Second)
			for time.Now().Before(deadline) {
				mu.Lock()
				n := len(pend)
				mu.Unlock()
				if n == 0 {
					break
				}
				time.Sleep(200 * time.Millisecond)
			}
			ch.Close()
			drain.Wait()
			mu.Lock()
			for _, p := range pend {
				out.row(u64(p.seq), i64(p.createNs), i64(p.pubNs), "",
					fmt.Sprint(c.size), outError, "", "")
			}
			mu.Unlock()
			conn.Close()

			if !publishing {
				return
			}
			reconnects++
			ev.row(i64(time.Now().UnixNano()), "producer.reconnect",
				fmt.Sprintf("attempt=%d", reconnects))
			time.Sleep(250 * time.Millisecond)
		}
	}()

	t0 := time.Unix(0, c.t0Ns)
	p := pacer{t0: t0.Add(time.Duration(c.startSec * float64(time.Second))), rate: c.rate}
	total := int64(math.Round(c.durSec * c.rate))
	pad := make([]byte, c.size)
	for i := range pad[headerLen:] {
		pad[headerLen+i] = byte('a' + i%26)
	}

	ev.row(i64(time.Now().UnixNano()), "producer.start",
		fmt.Sprintf("rate=%.0f size=%d total=%d buffer=%d", c.rate, c.size, total, bufSize))
	for i := int64(0); i < total; i++ {
		if d := time.Until(p.due(i)); d > 0 {
			time.Sleep(d)
		}
		createNs := time.Now().UnixNano()
		body := make([]byte, c.size)
		copy(body, pad)
		encode(body, uint64(i), createNs, createNs+int64(c.deadlineMs)*int64(time.Millisecond), 0)
		select {
		case sendCh <- msg{seq: uint64(i), createNs: createNs, body: body}:
		default:
			// Local buffer full: the broker is not accepting fast enough.
			out.row(u64(uint64(i)), i64(createNs), "", "", fmt.Sprint(c.size), outSkipped, "", "")
		}
	}
	ev.row(i64(time.Now().UnixNano()), "producer.stop", "publish loop finished")
	close(sendCh)
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		ev.row(i64(time.Now().UnixNano()), "producer.drain_timeout", "")
	}
}

func produceKafka(c producerCfg) {
	out, err := newCSV(c.out, "seq,t_create_ns,t_publish_ns,t_confirm_ns,size,outcome,partition,offset")
	fatal(err)
	defer out.close()
	ev, err := newCSVLive(c.events, "t_ns,event,detail")
	fatal(err)
	defer ev.close()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.addr),
		kgo.DefaultProduceTopic(c.queue),
		// We assign partitions ourselves so the load is spread evenly and
		// reproducibly; without this franz-go would override Record.Partition.
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5*time.Millisecond),
		// Same local buffer as the RabbitMQ producer, one second of output, so
		// that "shed" means the same thing on both brokers.
		kgo.MaxBufferedRecords(int(c.rate)),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
	)
	fatal(err)
	defer cl.Close()

	// Producer-side lag backpressure: the Kafka analogue of reject-publish.
	// Kafka has no broker-side equivalent, so the producer polls group lag and
	// refuses locally. This is the configuration class 3 arm for Kafka.
	var overLimit atomic.Bool
	if c.lagLimit > 0 {
		adm := kadm.NewClient(cl)
		go func() {
			for {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				lags, err := adm.Lag(ctx, c.group)
				cancel()
				if err == nil {
					if dl, ok := lags[c.group]; ok {
						tot := dl.Lag.Total()
						was := overLimit.Load()
						now := tot > c.lagLimit
						if was != now {
							ev.row(i64(time.Now().UnixNano()), "backpressure.toggle",
								fmt.Sprintf("lag=%d limit=%d active=%v", tot, c.lagLimit, now))
						}
						overLimit.Store(now)
					}
				}
				time.Sleep(500 * time.Millisecond)
			}
		}()
	}

	t0 := time.Unix(0, c.t0Ns)
	p := pacer{t0: t0.Add(time.Duration(c.startSec * float64(time.Second))), rate: c.rate}
	total := int64(math.Round(c.durSec * c.rate))
	pad := make([]byte, c.size)
	for i := range pad[headerLen:] {
		pad[headerLen+i] = byte('a' + i%26)
	}

	var wg sync.WaitGroup
	ev.row(i64(time.Now().UnixNano()), "producer.start", fmt.Sprintf("rate=%.0f size=%d total=%d", c.rate, c.size, total))
	for i := int64(0); i < total; i++ {
		if d := time.Until(p.due(i)); d > 0 {
			time.Sleep(d)
		}
		createNs := time.Now().UnixNano()
		if overLimit.Load() {
			out.row(u64(uint64(i)), i64(createNs), "", "", fmt.Sprint(c.size), outRejectedBP, "", "")
			continue
		}
		body := make([]byte, c.size)
		copy(body, pad)
		encode(body, uint64(i), createNs, createNs+int64(c.deadlineMs)*int64(time.Millisecond), 0)

		seq := uint64(i)
		wg.Add(1)
		// TryProduce fails immediately when the local buffer is full, which is
		// the Kafka counterpart of the RabbitMQ producer's full send buffer.
		cl.TryProduce(context.Background(), &kgo.Record{
			Value:     body,
			Partition: int32(i % int64(c.partitions)),
			Timestamp: time.Unix(0, createNs),
		}, func(r *kgo.Record, err error) {
			defer wg.Done()
			now := time.Now().UnixNano()
			if err != nil {
				o := outError
				if errors.Is(err, kgo.ErrMaxBuffered) {
					o = outSkipped
				}
				out.row(u64(seq), i64(createNs), i64(createNs), "", fmt.Sprint(c.size), o, "", "")
				return
			}
			out.row(u64(seq), i64(createNs), i64(createNs), i64(now), fmt.Sprint(c.size), outConfirmed,
				fmt.Sprint(r.Partition), i64(r.Offset))
		})
	}
	ev.row(i64(time.Now().UnixNano()), "producer.stop", "publish loop finished")
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		ev.row(i64(time.Now().UnixNano()), "producer.flush_timeout", "")
	}
}
