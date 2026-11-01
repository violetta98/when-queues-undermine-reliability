// qlab is the data-plane harness for the queue-backlog experiments.
//
// Subcommands:
//
//	produce      publish at a fixed rate, record per-message publish + confirm outcome
//	consume      consume with a configurable per-message service time and slowdown plan
//	probe-kafka  sample Kafka end offsets, log-start-offsets and group lag at 1 Hz
//
// All processes are handed the same absolute t0 (unix nanoseconds) by the
// orchestrator, so phase boundaries line up without any inter-process
// coordination.
package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const headerLen = 33

// encode lays out a message as a fixed binary header followed by padding.
// Keeping the header fixed-width means the consumer never has to allocate to
// find out when the message was created.
func encode(buf []byte, seq uint64, createNs, deadlineNs int64, flags byte) {
	binary.BigEndian.PutUint64(buf[0:], seq)
	binary.BigEndian.PutUint64(buf[8:], uint64(createNs))
	binary.BigEndian.PutUint64(buf[16:], uint64(deadlineNs))
	binary.BigEndian.PutUint64(buf[24:], 0) // reserved
	buf[32] = flags
}

func decode(buf []byte) (seq uint64, createNs, deadlineNs int64, ok bool) {
	if len(buf) < headerLen {
		return 0, 0, 0, false
	}
	seq = binary.BigEndian.Uint64(buf[0:])
	createNs = int64(binary.BigEndian.Uint64(buf[8:]))
	deadlineNs = int64(binary.BigEndian.Uint64(buf[16:]))
	return seq, createNs, deadlineNs, true
}

// csvw is a buffered CSV writer. We write one row per message, so the write
// path has to stay off the critical path: a 4 MiB buffer and a mutex is enough
// at the rates used here (<= 2000 rows/s).
type csvw struct {
	mu        sync.Mutex
	f         *os.File
	w         *bufio.Writer
	flushEach bool
}

func newCSV(path, header string) (*csvw, error) {
	return openCSV(path, header, false)
}

// newCSVLive flushes after every row. Used for low-volume files that the
// orchestrator reads while the run is still in progress, and for event logs
// that must survive an aborted run.
func newCSVLive(path, header string) (*csvw, error) {
	return openCSV(path, header, true)
}

func openCSV(path, header string, flushEach bool) (*csvw, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w := bufio.NewWriterSize(f, 4<<20)
	if _, err := w.WriteString(header + "\n"); err != nil {
		return nil, err
	}
	if flushEach {
		w.Flush()
	}
	return &csvw{f: f, w: w, flushEach: flushEach}, nil
}

func (c *csvw) row(parts ...string) {
	c.mu.Lock()
	for i, p := range parts {
		if i > 0 {
			c.w.WriteByte(',')
		}
		c.w.WriteString(p)
	}
	c.w.WriteByte('\n')
	if c.flushEach {
		c.w.Flush()
	}
	c.mu.Unlock()
}

func (c *csvw) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.w.Flush()
	c.f.Sync()
	c.f.Close()
}

func i64(v int64) string  { return strconv.FormatInt(v, 10) }
func u64(v uint64) string { return strconv.FormatUint(v, 10) }

// slowPlan describes how the per-message service time changes over the run.
// Phases are expressed as offsets in seconds from t0.
type slowPlan struct {
	slowStart  float64 // seconds after t0 when the downstream slows down
	slowEnd    float64 // seconds after t0 when it recovers
	factor     float64 // service-time multiplier during the slow phase
	baseMillis float64
}

func (p slowPlan) inSlow(elapsed float64) bool {
	return elapsed >= p.slowStart && elapsed < p.slowEnd
}

func (p slowPlan) serviceAt(elapsed float64) time.Duration {
	f := 1.0
	if elapsed >= p.slowStart && elapsed < p.slowEnd {
		f = p.factor
	}
	return time.Duration(p.baseMillis * f * float64(time.Millisecond))
}

func parsePlan(s string, baseMillis float64) (slowPlan, error) {
	// format: slowStart:slowEnd:factor  e.g. "60:540:5"
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return slowPlan{}, fmt.Errorf("plan must be slowStart:slowEnd:factor, got %q", s)
	}
	var p slowPlan
	var err error
	if p.slowStart, err = strconv.ParseFloat(parts[0], 64); err != nil {
		return p, err
	}
	if p.slowEnd, err = strconv.ParseFloat(parts[1], 64); err != nil {
		return p, err
	}
	if p.factor, err = strconv.ParseFloat(parts[2], 64); err != nil {
		return p, err
	}
	p.baseMillis = baseMillis
	return p, nil
}

// checkT0 refuses to start if the run origin is already in the past. Without
// this guard a wrong t0 silently turns every message into a shed message and
// the run looks complete but contains no data.
func checkT0(t0Ns int64, startSec float64) {
	start := time.Unix(0, t0Ns).Add(time.Duration(startSec * float64(time.Second)))
	if d := time.Until(start); d < -5*time.Second {
		fatal(fmt.Errorf("t0 is %s in the past (t0=%d, now=%d); refusing to start",
			(-d).Round(time.Millisecond), t0Ns, time.Now().UnixNano()))
	}
}

// dialAMQP opens a connection with a tolerant heartbeat. The library default of
// ten seconds drops the connection whenever a node under a resource alarm is
// slow to answer, which would show up as a client-side failure rather than as
// the broker-side blocking we are trying to measure. Sixty seconds is a normal
// production setting.
func dialAMQP(addr string) (*amqp.Connection, error) {
	return amqp.DialConfig(addr, amqp.Config{
		Heartbeat: 60 * time.Second,
		Locale:    "en_US",
	})
}

func fatal(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "qlab: "+err.Error())
		os.Exit(1)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: qlab {produce|consume|probe-kafka} [flags]")
		os.Exit(2)
	}
	sub := os.Args[1]
	os.Args = append(os.Args[:1], os.Args[2:]...)
	switch sub {
	case "produce":
		runProduce()
	case "consume":
		runConsume()
	case "probe-kafka":
		runProbeKafka()
	case "now":
		// Used by the orchestrator to measure the offset between its own clock
		// and the clock inside the Docker VM. BusyBox date has no %N, so the
		// reading has to come from here.
		fmt.Println(time.Now().UnixNano())
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", sub)
		os.Exit(2)
	}
}
