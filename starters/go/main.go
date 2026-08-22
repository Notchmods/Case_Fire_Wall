package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
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

// Opening and creating database (if does not exist)
func openDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(`
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        PRAGMA busy_timeout=5000;
    `); err != nil {
		return nil, err
	}
	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS prices (
            symbol TEXT PRIMARY KEY,
            price  REAL NOT NULL
        )
    `)
	return db, err
}

// Database struct with mutex for concurrent access
var (
	db       *sql.DB
	pricesMu sync.RWMutex
)

// Write /POST price into database
func postPriceHandler(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Symbol string  `json:"symbol"`
		Price  float64 `json:"price"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Symbol == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad request"})
		return
	}
	_, err := db.Exec(
		`INSERT INTO prices(symbol, price) VALUES(?, ?)
		 ON CONFLICT(symbol) DO UPDATE SET price = excluded.price`,
		body.Symbol, body.Price)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "write failed"})
		return
	}
	pricesMu.Lock()
	prices[body.Symbol] = body.Price
	pricesMu.Unlock()
	writeJSON(w, http.StatusOK, map[string]interface{}{"symbol": body.Symbol, "price": body.Price})
}

var ErrTimeout = errors.New("timeout: threshold time exceeded")
var ErrUnknownHandler = errors.New("unknown handler name")
var errUnknownSymbol = errors.New("unknown symbol")

// HandleFunc does the work for one request, must check ctx and stop its work when ctx ends
type HandleFunc func(ctx context.Context, payload interface{}) (interface{}, error)

// HandlerConfig sets the limits for one handle func
type HandlerConfig struct {
	Workers   int           // number of workers that run this handle func
	QueueSize int           // max number of requests that can wait in the queue
	Threshold time.Duration // max time from submit to finish
}

// One request in a handler queue
type task struct {
	ctx     context.Context
	payload interface{}
	result  chan taskResult
}

type taskResult struct {
	value interface{}
	err   error
}

// Holds the queue and the settings for one handle func
type handlerQueue struct {
	fn        HandleFunc
	queue     chan task
	threshold time.Duration
}

// Central dispatcher that holds all the handler queues and runs the workers
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

// Adds one handle func to the dispatcher, call before Submit
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

// Takes tasks from one handler queue and runs them one at a time
func (d *Dispatcher) worker(hq *handlerQueue) {
	defer d.wg.Done()
	for {
		select {
		case <-d.quit:
			return
		case t := <-hq.queue:
			// If the requester's time is already up, skip the work
			if t.ctx.Err() != nil {
				continue
			}
			value, err := hq.fn(t.ctx, t.payload)
			t.result <- taskResult{value: value, err: err}
		}
	}
}

// Sends one request to the named handle func
// Blocks until the request finishes or until the threshold time for that handle func runs out
// If the queue is full or the work does not finish in time, returns ErrTimeout
func (d *Dispatcher) Submit(ctx context.Context, name string, payload interface{}) (interface{}, error) {
	d.mu.RLock()
	hq, ok := d.handlers[name]
	d.mu.RUnlock()
	if !ok {
		return nil, ErrUnknownHandler
	}

	// This context sets one deadline for the whole request:
	// the wait time in the queue plus the run time of the handle func.
	reqCtx, cancel := context.WithTimeout(ctx, hq.threshold)
	defer cancel()

	t := task{
		ctx:     reqCtx,
		payload: payload,
		result:  make(chan taskResult, 1),
	}

	select {
	case hq.queue <- t:
		// The request is now in the queue.
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

// Turns DeadlineExceeded into ErrTimeout
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

// Path to the JSON file that stores the prices
var priceDBPath = "priceDB.json"

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

// Loads prices from the JSON file into the prices map
func loadPrices() error {
	data, err := os.ReadFile(priceDBPath)
	if errors.Is(err, os.ErrNotExist) || len(data) == 0 {
		return nil
	}
	if err != nil {
		return err
	}

	stored := make(map[string]float64)
	if err := json.Unmarshal(data, &stored); err != nil {
		return err
	}

	pricesMu.Lock()
	for symbol, price := range stored {
		if _, ok := prices[symbol]; ok {
			prices[symbol] = price
		}
	}
	pricesMu.Unlock()
	return nil
}

func savePricesLocked() error {
	data, err := json.MarshalIndent(prices, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(priceDBPath, append(data, '\n'), 0644)
}

// Represents a price update request
type priceUpdate struct {
	Symbol string  `json:"symbol"`
	Price  float64 `json:"price"`
}

// Handles POST /price requests to update a stock price
func updatePrice(w http.ResponseWriter, r *http.Request) {
	var update priceUpdate
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil || update.Symbol == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid price update"})
		return
	}

	pricesMu.Lock()
	defer pricesMu.Unlock()
	if _, ok := prices[update.Symbol]; !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown symbol"})
		return
	}
	prices[update.Symbol] = update.Price
	if err := savePricesLocked(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "could not save price"})
		return
	}

	writeJSON(w, http.StatusOK, update)
}

// endpoint query work

// GET /price, returns price associated with symbol
func priceWork(ctx context.Context, payload interface{}) (interface{}, error) {
	s, _ := payload.(string)
	pricesMu.RLock()
	defer pricesMu.RUnlock()
	p, ok := prices[s]
	if !ok {
		return nil, errUnknownSymbol
	}
	return map[string]interface{}{"symbol": s, "price": p}, nil
}

// GET /stats, calculates mean, min, max, and stddev for one stock
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

// Runs 50000 rounds of SHA-256 over the seed 
// Checks the context every 1000 rounds to see if it should stop early
func riskWork(ctx context.Context, payload interface{}) (interface{}, error) {
	seed, _ := payload.(string)
	if seed == "" {
		seed = "none"
	}
	buf := make([]byte, hex.EncodedLen(sha256.Size)) // reused every round, no per-round alloc
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

// Writes one JSON response
func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// Turns a handle func error into the right HTTP response
func writeErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrTimeout), errors.Is(err, context.DeadlineExceeded):
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "server busy, try again later"})
	case errors.Is(err, errUnknownSymbol):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown symbol"})
	case errors.Is(err, context.Canceled):
		// client canceled
	default:
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
	}
}

// endpoint query handler logic
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

// Handles both GET and POST requests to /price
// GET requests are dispatched to the priceWork handler
// POST requests update the price in the database
func priceHandler(d *Dispatcher) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			updatePrice(w, r)
			return
		}
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET, POST")
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		symbolHandler(d, "price")(w, r)
	}
}

func main() {
	// Cap the process to 2 CPU's (feel free to comment this out if needed)
	runtime.GOMAXPROCS(2)
	if configuredPath := os.Getenv("PRICE_DB_PATH"); configuredPath != "" {
		priceDBPath = configuredPath
	}

	if err := loadPrices(); err != nil {
		panic(err)
	}

	d := NewDispatcher()

	const ERROR_MARGIN = 0.1 // 10% margin for threshold times to account for scheduling and other delays

	// Register the handlers with their respective configurations	

	d.Register("price", priceWork, HandlerConfig{
		Workers:   8,
		QueueSize: 200,
		Threshold: (200 - (ERROR_MARGIN * 200)) * time.Millisecond,
	})

	d.Register("stats", statsWork, HandlerConfig{
		Workers:   4,
		QueueSize: 100,
		Threshold: (500 - (ERROR_MARGIN * 500)) * time.Millisecond,
	})

	d.Register("risk", riskWork, HandlerConfig{
		Workers:   2,
		QueueSize: 20,
		Threshold: (1500 - (ERROR_MARGIN * 1500)) * time.Millisecond,
	})

	// /health queue unneeded as called once initially
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	http.HandleFunc("/price", priceHandler(d))
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
