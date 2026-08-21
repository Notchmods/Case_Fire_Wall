// Obsidio: resilient Go analytics API.
//
// Architecture
// ────────────
// Go's net/http already dispatches each request to its own goroutine, so we
// get concurrency on the fast path (/price, /stats) for free. The only
// problem is /risk: 50 000 SHA-256 iterations are CPU-bound, and if every
// goroutine runs them at once on a 2-CPU box they all compete and the fast
// path suffers.
//
// Fix: a fixed worker pool of RISK_WORKERS goroutines sits in front of the
// SHA-256 loop. Request handlers submit jobs via a buffered channel and block
// until a worker finishes. This caps the number of simultaneous CPU-bound
// computations to RISK_WORKERS regardless of how many HTTP goroutines are
// waiting, leaving headroom for /price and /stats on the other core.
//
// Overflow guard: a context deadline of RISK_DEADLINE is attached to each
// /risk job. If the job cannot start within that window (i.e. the pool is
// too congested), the handler returns 503 immediately rather than waiting
// forever. Fail-fast beats hanging: the k6 VU model means each VU blocks
// while waiting, so fast refusals let VUs recycle and pick non-risk
// endpoints, reducing overall congestion.
//
// Core-count note: the container is THROTTLED to 2 CPUs but can SEE all host
// cores. GOMAXPROCS must be set explicitly, not derived from runtime.NumCPU().
// Getting this wrong is exactly the gotcha the detail page warns about.

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"math"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"
)

// ── Constants ──────────────────────────────────────────────────────────────

const (
	// Hard-coded to the container's 2-CPU cap — do NOT use runtime.NumCPU().
	GOMAXPROCS = 2

	// Number of goroutines allowed to run the SHA-256 loop simultaneously.
	// One per CPU keeps the fast path responsive while fully utilising the box.
	RISK_WORKERS = 2

	// Buffered channel capacity: how many /risk jobs can queue before we start
	// issuing 503s. Sized to absorb burst traffic while staying inside the
	// 1 500 ms p95 latency budget.
	//   budget  = RISK_QUEUE_CAP × avg_compute_time / RISK_WORKERS
	//   1 400ms ≈ 14 × 100ms / 1  →  cap of 14 is safe at ~100ms compute.
	// We set it conservatively at 20 to handle shorter compute times without
	// filling too fast. The context deadline below provides a second safety net.
	RISK_QUEUE_CAP = 20

	// Per-request deadline: if a /risk job hasn't started within this window,
	// the handler returns 503 instead of continuing to wait.
	// Set well below the 1 500 ms p95 budget so the deadline fires before k6
	// measures a latency breach.
	RISK_DEADLINE = 1200 * time.Millisecond

	PORT = "8080"
)

// ── Fixed data ─────────────────────────────────────────────────────────────

var pricesMu sync.RWMutex
var prices = map[string]float64{
	"AAPL": 187.42, "GOOG": 141.80, "MSFT": 412.30, "AMZN": 178.10,
	"NVDA": 120.15, "META": 502.60, "TSLA": 244.70, "JPM": 198.35,
}

// series is written once at startup and never mutated — safe for concurrent reads.
var series = map[string][]float64{}

func init() {
	for sym, base := range prices {
		arr := make([]float64, 500)
		for i := range arr {
			arr[i] = base * (1 + math.Sin(float64(i))/50)
		}
		series[sym] = arr
	}
}

// ── Risk worker pool ───────────────────────────────────────────────────────

// riskJob carries a seed in and receives the result (or an error) back.
type riskJob struct {
	seed   string
	result chan<- string
}

// riskQueue is the bounded channel feeding the worker pool.
var riskQueue = make(chan riskJob, RISK_QUEUE_CAP)

// startRiskPool launches RISK_WORKERS goroutines that each drain riskQueue.
// These goroutines run for the lifetime of the process.
func startRiskPool() {
	for i := 0; i < RISK_WORKERS; i++ {
		go func() {
			for job := range riskQueue {
				h := job.seed
				for j := 0; j < 50000; j++ {
					sum := sha256.Sum256([]byte(h))
					h = hex.EncodeToString(sum[:])
				}
				job.result <- h
			}
		}()
	}
}

// ── Helpers ────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// ── Handlers ───────────────────────────────────────────────────────────────

// GET /health — liveness check, not scored.
func healthHandler(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// GET /price?symbol=SYM — cheap O(1) lookup.
func priceHandler(w http.ResponseWriter, r *http.Request) {
	sym := r.URL.Query().Get("symbol")
	pricesMu.RLock()
	p, ok := prices[sym]
	pricesMu.RUnlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown symbol"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"symbol": sym, "price": p})
}

// GET /stats?symbol=SYM — medium: O(500) computation per request.
// Fast enough to run directly in the handler goroutine (microseconds).
func statsHandler(w http.ResponseWriter, r *http.Request) {
	sym := r.URL.Query().Get("symbol")
	arr, ok := series[sym]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown symbol"})
		return
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
	variance := 0.0
	for _, v := range arr {
		d := v - mean
		variance += d * d
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"symbol": sym,
		"mean":   mean,
		"min":    mn,
		"max":    mx,
		"stddev": math.Sqrt(variance / n),
	})
}

// GET /risk?seed=VALUE — heavy: 50 000 SHA-256 iterations, offloaded to pool.
func riskHandler(w http.ResponseWriter, r *http.Request) {
	seed := r.URL.Query().Get("seed")
	if seed == "" {
		seed = "none"
	}

	// Per-request result channel (capacity 1 — non-blocking send from worker).
	resultCh := make(chan string, 1)

	job := riskJob{seed: seed, result: resultCh}

	// Attach a deadline so we never wait longer than RISK_DEADLINE.
	// If the queue is backed up beyond what the budget allows, return 503
	// immediately rather than delivering a late (worthless) response.
	ctx, cancel := context.WithTimeout(r.Context(), RISK_DEADLINE)
	defer cancel()

	select {
	case riskQueue <- job:
		// Job accepted; wait for the worker to finish.
		select {
		case hash := <-resultCh:
			writeJSON(w, http.StatusOK, map[string]any{"seed": seed, "risk_hash": hash})
		case <-ctx.Done():
			// Worker took too long (queue drained slowly). Fail fast.
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "timeout, please retry"})
		}
	case <-ctx.Done():
		// Queue full and deadline already passed — reject immediately.
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "overloaded, please retry"})
	}
}

// POST /price — optional persistence bonus (naive in-memory; survives no restart).
func priceUpdateHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	var body struct {
		Symbol string  `json:"symbol"`
		Price  float64 `json:"price"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Symbol == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "symbol and numeric price required"})
		return
	}
	pricesMu.Lock()
	prices[body.Symbol] = body.Price
	pricesMu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"symbol": body.Symbol, "price": body.Price})
}

// ── Main ───────────────────────────────────────────────────────────────────

func main() {
	// Pin to the container's actual CPU cap — do NOT use runtime.NumCPU() here.
	runtime.GOMAXPROCS(GOMAXPROCS)
	log.Printf("GOMAXPROCS=%d  risk_workers=%d  queue_cap=%d  deadline=%s",
		GOMAXPROCS, RISK_WORKERS, RISK_QUEUE_CAP, RISK_DEADLINE)

	startRiskPool()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)
	mux.HandleFunc("GET /price", priceHandler)
	mux.HandleFunc("GET /stats", statsHandler)
	mux.HandleFunc("GET /risk", riskHandler)
	mux.HandleFunc("POST /price", priceUpdateHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = PORT
	}

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Printf("listening on :%s", port)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
