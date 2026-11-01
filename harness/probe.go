package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// runProbeKafka samples per-partition log-start offsets, end offsets and
// committed offsets at a fixed interval. log_start_offset is the quantity that
// proves retention-driven deletion: it advances only when the broker removes
// segments the group has not read yet.
func runProbeKafka() {
	var addr, topics, group, out string
	var interval, duration float64
	var t0 int64
	flag.StringVar(&addr, "addr", "127.0.0.1:9092", "bootstrap server")
	flag.StringVar(&topics, "topics", "main,neighbor", "comma-separated topics")
	flag.StringVar(&group, "groups", "main-cg,neighbor-cg", "comma-separated consumer groups, aligned with -topics")
	flag.StringVar(&out, "out", "kafka_probe.csv", "output csv")
	flag.Float64Var(&interval, "interval", 1.0, "sampling interval in seconds")
	flag.Float64Var(&duration, "duration", 900, "stop this many seconds after t0")
	flag.Int64Var(&t0, "t0", 0, "absolute run origin, unix nanoseconds")
	flag.Parse()
	if t0 == 0 {
		t0 = time.Now().UnixNano()
	}

	tl := strings.Split(topics, ",")
	gl := strings.Split(group, ",")
	w, err := newCSVLive(out, "t_ns,topic,partition,log_start_offset,end_offset,committed_offset,lag")
	fatal(err)
	defer w.close()

	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	fatal(err)
	defer cl.Close()
	adm := kadm.NewClient(cl)

	stop := time.Unix(0, t0).Add(time.Duration(duration * float64(time.Second)))
	tick := time.NewTicker(time.Duration(interval * float64(time.Second)))
	defer tick.Stop()
	for range tick.C {
		if time.Now().After(stop) {
			return
		}
		now := time.Now().UnixNano()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		starts, errS := adm.ListStartOffsets(ctx, tl...)
		ends, errE := adm.ListEndOffsets(ctx, tl...)
		committed := map[string]map[int32]int64{}
		for i, t := range tl {
			if i >= len(gl) {
				break
			}
			if resp, err := adm.FetchOffsets(ctx, gl[i]); err == nil {
				m := map[int32]int64{}
				resp.Each(func(o kadm.OffsetResponse) {
					if o.Topic == t {
						m[o.Partition] = o.At
					}
				})
				committed[t] = m
			}
		}
		cancel()
		if errS != nil || errE != nil {
			continue
		}
		ends.Each(func(e kadm.ListedOffset) {
			var start int64 = -1
			if so, ok := starts.Lookup(e.Topic, e.Partition); ok {
				start = so.Offset
			}
			com := int64(-1)
			if m, ok := committed[e.Topic]; ok {
				if v, ok := m[e.Partition]; ok {
					com = v
				}
			}
			lag := int64(-1)
			if com >= 0 {
				lag = e.Offset - com
				if lag < 0 {
					lag = 0
				}
			}
			w.row(i64(now), e.Topic, fmt.Sprint(e.Partition), i64(start), i64(e.Offset), i64(com), i64(lag))
		})
	}
}
