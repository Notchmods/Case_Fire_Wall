package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"
)

var ErrTimeout = errors.New("timeout: threshold time exceeded")

var ErrUnknownHandler = errors.New("unknown handler name")

var errUnknownSymbol = errors.New("unknown symbol")

// HandleFunc does the work for one request
// It must check ctx and stop its work when ctx ends
type HandleFunc func(ctx context.Context, payload interface{}) (interface{}, error)

// HandlerConfig sets the limits for one handle func.
type HandlerConfig struct {
	Workers   int           // number of workers that run this handle func
	QueueSize int           // max number of requests that can wait in the queue
	Threshold time.Duration // max time from submit to finish
}

// task is one request in a handler queue
type task struct {
	ctx     context.Context
	payload interface{}
	result  chan taskResult
}

type taskResult struct {
	value interface{}
	err   error
}

// handlerQueue holds the queue and the settings for one handle func
type handlerQueue struct {
	fn        HandleFunc
	queue     chan task
	threshold time.Duration
}

// Dispatcher manages many handle funcs
// Each handle func has its own queue and its own threshold time
type Dispatcher struct {
	mu       sync.RWMutex
	handlers map[string]*handlerQueue
	wg       sync.WaitGroup
	quit     chan struct{}
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		handlers: make(map[string]*handlerQueue),
		quit:     make(chan struct{}),
	}
}

// Register adds one handle func to the dispatcher, called before Submit
func (d *Dispatcher) Register(name string, fn HandleFunc, cfg HandlerConfig) {
	hq := &handlerQueue{
		fn:        fn,
		queue:     make(chan task, cfg.QueueSize),
		threshold: cfg.Threshold,
	}

	d.mu.Lock()
	d.handlers[name] = hq
	d.mu.Unlock()

	for i := 0; i < cfg.Workers; i++ {
		d.wg.Add(1)
		go d.worker(hq)
	}
}

// Worker takes tasks from one handler queue and runs them one at a time
func (d *Dispatcher) worker(hq *handlerQueue) {
	defer d.wg.Done()
	for {
		select {
		case <-d.quit:
			return
		case t := <-hq.queue:
			// If timeout, skip work
			if t.ctx.Err() != nil {
				continue
			}
			value, err := hq.fn(t.ctx, t.payload)
			t.result <- taskResult{value: value, err: err}
		}
	}
}

// Submit sends one request to the named handle func
// Submit blocks until the request finishes or until the threshold time for that handle func runs out
// If the queue is full, or the work does not finish in time Submit returns ErrTimeout
func (d *Dispatcher) Submit(ctx context.Context, name string, payload interface{}) (interface{}, error) {
	d.mu.RLock()
	hq, ok := d.handlers[name]
	d.mu.RUnlock()
	if !ok {
		return nil, ErrUnknownHandler
	}

	// Sets request deadline to the wait time in the queue plus the run time of the handle func
	reqCtx, cancel := context.WithTimeout(ctx, hq.threshold)
	defer cancel()

	t := task{
		ctx:     reqCtx,
		payload: payload,
		result:  make(chan taskResult, 1),
	}

	select {
	case hq.queue <- t:
		// The request is now in queue
	case <-reqCtx.Done():
		return nil, translateErr(reqCtx.Err())
	}

	select {
	case r := <-t.result:
		return r.value, r.err
	case <-reqCtx.Done():
		return nil, translateErr(reqCtx.Err())
	}
}

// Turns only DeadlineExceeded into ErrTimeout
func translateErr(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrTimeout
	}
	return err
}

var prices = map[string]float64{
	"AAPL": 187.42, "GOOG": 141.80, "MSFT": 412.30, "AMZN": 178.10,
	"NVDA": 120.15, "META": 502.60, "TSLA": 244.70, "JPM": 198.35,
}

var series = map[string][]float64{}

func init() {
	for s, base := range prices {
		arr := make([]float64, 500)
		for i := 0; i < 500; i++ {
			arr[i] = base * (1 + math.Sin(float64(i))/50)
		}
		series[s] = arr
	}
}

// Handle funcs (endpoint work)

// Looks up one stock price
func priceWork(ctx context.Context, payload interface{}) (interface{}, error) {
	s, _ := payload.(string)
	p, ok := prices[s]
	if !ok {
		return nil, errUnknownSymbol
	}
	return map[string]interface{}{"symbol": s, "price": p}, nil
}

// Calculates the mean, min, max, and stddev for one stock
func statsWork(ctx context.Context, payload interface{}) (interface{}, error) {
	s, _ := payload.(string)
	arr, ok := series[s]
	if !ok {
		return nil, errUnknownSymbol
	}
	n := float64(len(arr))
	sum, mn, mx := 0.0, arr[0], arr[0]
	for _, v := range arr {
		sum += v
		if v < mn {
			mn = v
		}
		if v > mx {
			mx = v
		}
	}

	mean := sum / n
	varr := 0.0
	for _, v := range arr {
		varr += (v - mean) * (v - mean)
	}
	return map[string]interface{}{
		"symbol": s, "mean": mean, "min": mn, "max": mx,
		"stddev": math.Sqrt(varr / n),
	}, nil
}

// Run 50000 rounds of SHA-256 over the seed, dynamically checking the context for cancellation.
func riskWork(ctx context.Context, payload interface{}) (interface{}, error) {
	seed, _ := payload.(string)
	if seed == "" {
		seed = "none"
	}
	buf := make([]byte, hex.EncodedLen(sha256.Size)) // reused every round, minimizing allocations
	h := []byte(seed)
	for i := 0; i < 50000; i++ {
		if i%1000 == 0 {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			runtime.Gosched()
		}
		sum := sha256.Sum256(h)
		hex.Encode(buf, sum[:])
		h = buf
	}
	return map[string]interface{}{"seed": seed, "risk_hash": string(h)}, nil
}

// Write one JSON response
func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// Turn handle func error into matching HTTP response
func writeErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrTimeout), errors.Is(err, context.DeadlineExceeded):
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "server busy, try again later"})
	case errors.Is(err, errUnknownSymbol):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown symbol"})
	case errors.Is(err, context.Canceled):
		// client request canceled
	default:
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
	}
}

// Handles endpoint request, queue allocation, and endpoint response
func symbolHandler(d *Dispatcher, queueName string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s := r.URL.Query().Get("symbol")
		result, err := d.Submit(r.Context(), queueName, s)
		if err != nil {
			writeErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
	}
}

func main() {
	// Cap the process to 2 CPU's (feel free to comment this out if needed)
	runtime.GOMAXPROCS(2)

	d := NewDispatcher()
	
	const ERROR_MARGIN = 0.1 // 10% margin for threshold time, to account for scheduling delays

	// /price endpoint queue
	d.Register("price", priceWork, HandlerConfig{
		Workers:   8,
		QueueSize: 200,
		Threshold: (200 - (ERROR_MARGIN * 200)) * time.Millisecond,
	})

	// /stats endpoint queue
	d.Register("stats", statsWork, HandlerConfig{
		Workers:   4,
		QueueSize: 100,
		Threshold: (500 - (ERROR_MARGIN * 500)) * time.Millisecond,
	})

	// /risk endpoint queue
	d.Register("risk", riskWork, HandlerConfig{
		Workers:   2,
		QueueSize: 20,
		Threshold: (1500 - (ERROR_MARGIN * 1500)) * time.Millisecond,
	})

	// /health, no queue as it is called once
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	http.HandleFunc("/price", symbolHandler(d, "price"))
	http.HandleFunc("/stats", symbolHandler(d, "stats"))

	http.HandleFunc("/risk", func(w http.ResponseWriter, r *http.Request) {
		seed := r.URL.Query().Get("seed")
		result, err := d.Submit(r.Context(), "risk", seed)
		if err != nil {
			writeErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	http.ListenAndServe(":"+port, nil)
}
