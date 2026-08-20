# Obsidio: The Siege (Detailed Guide)

> ⚔️ This page is the *how*. The track overview told you what Obsidio is; this page gives you the rules of the siege, the box you deploy into, the exact API you build, how it is graded, and how points are scored. Read it fully before you write code. The constraints here are the whole game.

---

## The one-sentence version

Build a small analytics API, containerise it to a spec we give you, and make it stay fast and correct while we flood it with real, measured traffic on a box that is identical for every team.

---

## What this actually is (and is not)

This is a *performance engineering* challenge, not a security one. The load we send is cooperative measurement, not an attack: it is the same kind of load test that real engineering teams run against their own systems before shipping. You are not defending against a hacker. You are proving your backend holds up under honest, heavy use. The tools involved (a load generator called k6, profilers, your own metrics) are standard engineering instruments, and using them well is exactly the point.

---

## Why the constraints exist

Two things could quietly make this track unfair, and we have designed both out.

**Hardware budget.** If teams deployed to whatever machine they could rent, we would be grading wallets, not engineering. So every backend runs in a standardised container with hard, identical resource limits. Nobody wins by throwing more compute at the problem. You win by engineering well inside the same box as everyone else.

**Hidden tests.** A resilience track where you do not know the load until judging is just a guessing game. So we publish the load profile in advance: the traffic mix, the ramp, and the exact thresholds we grade against. We even give you the grading script. There is no secret test. The challenge is building something that holds against a siege you can see coming.

---

## The environment

Your backend runs in Docker, capped and identical for all teams.

- **You submit a `Dockerfile`.** It builds your app in whatever language and framework you choose. The inside of the container is entirely yours.
- **Your backend must listen on port `8080`.** We build your image and run it ourselves at grading time.
- **It must build reproducibly.** Pin your versions. Do not rely on floating `latest` tags or anything that could change between when you test and when we grade. If it builds differently on our machine, that is on the submission.
- **The box is capped at 2 CPUs and 2 GB RAM, total, per team.** If you add more services (see the database bonus), that is the *combined* budget, not per service. You cannot buy capacity by adding containers.

> ⚠️ **The core-count gotcha (a real part of the challenge).** Your container is CPU *throttled* but can still *see* all of the host machine's cores. Several runtimes size their worker or thread pools from the visible core count (Go's `GOMAXPROCS`, the JVM, Node's cluster module, Gunicorn and Uvicorn workers, and others). If yours does, it may spin up workers for cores it cannot actually use, and your performance will crater in ways that are hard to debug. Set your worker or thread count explicitly to match the 2 core cap. Getting this right is exactly the kind of thing this track rewards.

New to Docker? Every starter in the code bundle already builds and runs correctly, so you clear the containerisation hurdle for free. See also the [Docker guide] from the Backend Guide for a walkthrough. Containerising your app is table stakes here, not the hard part; the hard part is what you build inside.

---

## The task: a price analytics API

You are building a small service of the kind a trading or analytics firm might run internally. It answers three kinds of request, cheap to expensive, plus a health check. This shape is deliberate: the mix of light and heavy work under load is the whole problem. A naive build lets the expensive requests clog the pipe so that even the cheap ones crawl. A good build keeps the fast path fast no matter what the slow path is doing.

The work each endpoint does is *specified exactly* below, so every team does identical work and results are comparable. You do not get to make the heavy endpoint cheaper; you only get to serve the same work more efficiently.

---

## The endpoint contract (build exactly this)

Every endpoint returns JSON. Implement all four exactly as specified. The grading script depends on these shapes.

### `GET /health`

Liveness check. Not scored, but we use it to confirm your container is up before the siege starts.

- **Response 200:** `{"status":"ok"}`

### `GET /price?symbol=SYM`  (cheap, weight 1)

Return the latest stored price for a symbol. This is a simple in-memory lookup and should be near instant.

- **Response 200:** `{"symbol":"AAPL","price":187.42}`
- **Response 404** if the symbol is unknown: `{"error":"unknown symbol"}`
- Valid symbols are provided in the starter code (a fixed set of about a dozen).

### `GET /stats?symbol=SYM`  (medium, weight 3)

Return summary statistics over that symbol's fixed recent price series (500 points, provided in the starter). Compute mean, min, max, and standard deviation over the series on every request. Do not precompute and cache the answer; the grader treats this as work that must be done per request.

- **Response 200:** `{"symbol":"AAPL","mean":187.4,"min":183.6,"max":191.2,"stddev":2.1}`
- **Response 404** if the symbol is unknown: `{"error":"unknown symbol"}`

### `GET /risk?seed=VALUE`  (heavy, weight 10)

Run an expensive deterministic computation seeded by `seed`. This stands in for a real per request risk calculation. The computation is specified precisely so every team does identical work and so we can verify the answer:

> Starting from the string value of `seed`, apply SHA-256 and hex-encode the result. Feed that hex string back in and repeat, for **50,000 iterations** total. Return the final hex digest.

Because `seed` differs on every request, this work cannot be cached or precomputed. Because it is deterministic, we can verify your output is correct for the seed we sent (returning a fake or constant value fails verification and scores zero for that request).

- **Response 200:** `{"seed":"0.4821","risk_hash":"<final hex digest>"}`

> 📌 The `/risk` computation is the same fixed amount of work for everyone. How fast your stack chews through it, and whether it blocks your other endpoints while doing so, is the engineering.

---

## How grading works

On the day, per team, we:

1. `docker build` your submission and `docker run` it capped at 2 CPUs and 2 GB.
2. Confirm `GET /health` returns 200.
3. Run our k6 script (the same one in your code bundle) against your container.
4. Read your results and record your score.
5. Tear down, and on to the next team.

The k6 script sends a **ramp** of virtual users (a growing crowd of simulated clients) hitting a fixed **mix** of the endpoints:

- 60% `GET /price`
- 30% `GET /stats`
- 10% `GET /risk`

The ramp climbs from calm to a sustained peak, holds, then winds down. The exact schedule is in the script and is the same for everyone.

### The thresholds (the qualifying bar)

k6 checks each endpoint tier against its own latency budget, plus an overall error ceiling. A heavy request is legitimately slower than a cheap one, so they are judged on separate bars, not one shared bar. The starting values are below. **These are placeholders pending final calibration on the grading hardware; the published version before the event will carry the locked numbers.**

| Metric | Bar (placeholder) |
| --- | --- |
| `/price` p95 latency | under 200 ms |
| `/stats` p95 latency | under 500 ms |
| `/risk` p95 latency | under 1500 ms |
| Error rate (all endpoints) | under 1% |

The p95 (95th percentile) is the value 95% of requests come in under. It is the standard way to measure the slow tail of your responses, the part users actually feel.

> ⚠️ The single most revealing number is the `/price` p95. A price lookup does almost no work, so if it is slow, it is not slow because of computation; it is slow because it is stuck in line behind heavy requests. Keeping that number low under load is the core skill this track measures. Simply using both CPU cores is a start but is usually not enough on its own.

---

## Scoring

There are two layers: a qualifying gate, then a continuous score.

### 1. Qualify

To make the leaderboard, your build must clear the thresholds above. A build that fails them is not resilient by the track's definition, however clever it looks.

### 2. Core score: useful work served

Among qualifiers, your score is the **weighted count of requests you served correctly and within the latency bars** over the run:

```
score = (1 x completed /price) + (3 x completed /stats) + (10 x completed /risk)
```

A request only counts if it returned the correct response within its latency budget. Requests that time out, error, or arrive late do not count. This is why graceful behaviour matters: a build that rejects overflow quickly and keeps its good requests fast will out-score one that accepts everything and lets it all rot in a slow queue. There is no ceiling here, so the best teams compete on how much they squeeze from the fixed box.

### 3. Optional bonus: persistence under restart

This one is a genuine tradeoff, not free points, and it is entirely optional. The base challenge is stateless and needs no database.

If you choose to add persistence, you can earn a bonus for surviving a restart:

- Implement an optional `POST /price` that records a price update (body: `{"symbol":"AAPL","price":190.0}`), and have `GET /price` return the most recently recorded value.
- During grading, we will send some updates, restart your container mid run, and check that the updated values survived.
- **The bonus is awarded only if your data survives the restart AND you still clear all the core thresholds.**

That proviso is the whole point. Persistence costs you: a database eats into your shared 2 CPU and 2 GB budget, and a naive integration can drag your latency below the qualifying bar and cost you more than the bonus is worth. Doing it well, cheaply, without wrecking your resilience, is the real achievement being rewarded. If you add a database, submit a `docker-compose.yml` (see the code bundle for the shape); remember the resource budget is shared across all services.

---

## Where to start

- **Containerise first, not last.** Take the starter for your stack, get it building and running under the 2 core, 2 GB caps on day one. Do not leave containerisation to the final hour; the caps change how your app behaves. The starters are deliberately naive: they work, pass the health check, and then fail the load test. Making them hold is the whole job.
- **Profile before you optimise.** Find *your* bottleneck with evidence before changing anything. It is usually not where you would guess. Run the k6 script against your own container and read the numbers.
- **Watch the fast path.** The `/price` p95 under load is your headline number. If it climbs, ask why a request that does no work is slow. The answer points straight at your architecture.
- **Think about the edges.** Timeouts, overflow, what happens when more requests arrive than you can serve. Failing fast beats hanging.
- **Tools are engineering, not cheating.** Load generators, profilers, and libraries are all fair game. The line is that the thinking has to be yours.

---

## What to submit

- **A working backend** as a `Dockerfile` (plus `docker-compose.yml` only if you attempt the persistence bonus) that builds and runs to the spec above.
- **A short resilience write-up:** where your bottlenecks were, what you did about them, and your own measured k6 results under stress. Numbers, not adjectives.
- **A video pitch** on your architecture and the trade-offs you chose. Tell us what you deliberately gave up, and why.

> ⚖️ **A note on judging.** The on-theme prize rewards *resilience by design*. We can tell the difference between a system that held because you engineered it to and one that held by luck. Your write-up and pitch are where you show the choice was deliberate.
