// Obsidio starter: Java + Spring Boot. NAIVE ON PURPOSE.
//
// Implements the contract correctly, so it builds, runs, and passes the health
// check. Spring Boot's embedded Tomcat has a thread pool, so it uses several
// threads by default, but the pool is sized from the HOST core count, not your
// 2-core cap, and nothing here is tuned for a throttled box. It WILL struggle
// under the load test. Making it genuinely resilient is YOUR job.
//
// There is deliberately NO resilience machinery here: no explicit thread-pool
// sizing, no caching, no queueing, no load shedding. That is the part you build.
//
// Core-count note: the JVM and Tomcat read the HOST core count. On a 2-core cap
// that can lead to oversized pools that thrash. Set thread counts and, if
// needed, -XX:ActiveProcessorCount=2 explicitly.

package com.obsidio;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

@SpringBootApplication
@RestController
public class App {

    static final Map<String, Double> PRICES = new HashMap<>();
    static final Map<String, double[]> SERIES = new HashMap<>();

    static {
        PRICES.put("AAPL", 187.42); PRICES.put("GOOG", 141.80);
        PRICES.put("MSFT", 412.30); PRICES.put("AMZN", 178.10);
        PRICES.put("NVDA", 120.15); PRICES.put("META", 502.60);
        PRICES.put("TSLA", 244.70); PRICES.put("JPM", 198.35);
        for (Map.Entry<String, Double> e : PRICES.entrySet()) {
            double[] arr = new double[500];
            for (int i = 0; i < 500; i++) arr[i] = e.getValue() * (1 + Math.sin(i) / 50);
            SERIES.put(e.getKey(), arr);
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }

    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("status", "ok");
    }

    // CHEAP (weight 1)
    @GetMapping("/price")
    public Map<String, Object> price(@RequestParam String symbol) {
        Double p = PRICES.get(symbol);
        if (p == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "unknown symbol");
        return Map.of("symbol", symbol, "price", p);
    }

    // MEDIUM (weight 3)
    @GetMapping("/stats")
    public Map<String, Object> stats(@RequestParam String symbol) {
        double[] s = SERIES.get(symbol);
        if (s == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "unknown symbol");
        int n = s.length;
        double sum = 0, mn = s[0], mx = s[0];
        for (double v : s) { sum += v; if (v < mn) mn = v; if (v > mx) mx = v; }
        double mean = sum / n, var = 0;
        for (double v : s) var += (v - mean) * (v - mean);
        return Map.of("symbol", symbol, "mean", mean, "min", mn, "max", mx,
                "stddev", Math.sqrt(var / n));
    }

    // HEAVY (weight 10): 50000 iterations of SHA-256 over the seed. Uncacheable.
    @GetMapping("/risk")
    public Map<String, Object> risk(@RequestParam(defaultValue = "none") String seed) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            String h = seed;
            for (int i = 0; i < 50000; i++) {
                byte[] d = md.digest(h.getBytes(StandardCharsets.UTF_8));
                h = toHex(d);
            }
            return Map.of("seed", seed, "risk_hash", h);
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "hash error");
        }
    }

    // OPTIONAL, only for the persistence bonus. In-memory only; does NOT survive
    // a restart. Add real persistence (and pay its cost) to claim the bonus.
    @PostMapping("/price")
    public Map<String, Object> updatePrice(@RequestBody Map<String, Object> body) {
        Object symbol = body.get("symbol");
        Object price = body.get("price");
        if (!(symbol instanceof String) || !(price instanceof Number)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "symbol and numeric price required");
        }
        PRICES.put((String) symbol, ((Number) price).doubleValue());
        return Map.of("symbol", symbol, "price", price);
    }

    private static String toHex(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) sb.append(Character.forDigit((x >> 4) & 0xF, 16))
                           .append(Character.forDigit(x & 0xF, 16));
        return sb.toString();
    }
}
