// Obsidio starter: Go + net/http. NAIVE ON PURPOSE.
//
// Implements the contract correctly, so it builds, runs, and passes the health
// check. Go's net/http already handles each request in its own goroutine, so
// this starter may hold up better out of the box than the others, but it still
// has no deliberate resilience design, and under enough load the /risk work
// will saturate the box and drag the fast path down. Making it genuinely
// resilient is YOUR job.
//
// There is deliberately NO resilience machinery here: no worker pool tuning, no
// caching, no queueing, no load shedding. That is the part you build.
//
// Core-count note: your container is capped at 2 CPUs but Go reads the HOST
// core count for GOMAXPROCS, not your cap. Set it explicitly to match:
//   runtime.GOMAXPROCS(2)   // or the GOMAXPROCS env var
// Leaving it at the host default can hurt you badly on a throttled box.

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"net/http"
	"os"
)

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

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func main() {
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]string{"status": "ok"})
	})

	// CHEAP (weight 1)
	http.HandleFunc("/price", func(w http.ResponseWriter, r *http.Request) {
		s := r.URL.Query().Get("symbol")
		p, ok := prices[s]
		if !ok {
			writeJSON(w, 404, map[string]string{"error": "unknown symbol"})
			return
		}
		writeJSON(w, 200, map[string]interface{}{"symbol": s, "price": p})
	})

	// MEDIUM (weight 3)
	http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		s := r.URL.Query().Get("symbol")
		arr, ok := series[s]
		if !ok {
			writeJSON(w, 404, map[string]string{"error": "unknown symbol"})
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
		varr := 0.0
		for _, v := range arr {
			varr += (v - mean) * (v - mean)
		}
		writeJSON(w, 200, map[string]interface{}{
			"symbol": s, "mean": mean, "min": mn, "max": mx,
			"stddev": math.Sqrt(varr / n),
		})
	})

	// HEAVY (weight 10): 50000 iterations of SHA-256 over the seed. Uncacheable.
	http.HandleFunc("/risk", func(w http.ResponseWriter, r *http.Request) {
		seed := r.URL.Query().Get("seed")
		if seed == "" {
			seed = "none"
		}
		h := seed
		for i := 0; i < 50000; i++ {
			sum := sha256.Sum256([]byte(h))
			h = hex.EncodeToString(sum[:])
		}
		writeJSON(w, 200, map[string]interface{}{"seed": seed, "risk_hash": h})
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	http.ListenAndServe(":"+port, nil)
}
