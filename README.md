# Case_Fire_Wall
This project was built for the 2026 hackathon [Catalyst](https://cissa-unimelb.notion.site/Catalyst-2026-Track-Guide-3b199473577c8006ba1cc1be704b1eec) which was based off Track 2. This project reached the finalist stage.

# Description
The goal was to build a resilient API server that's fast even when loaded with traffic and must satisfy the constraints of 2GB of RAM and 2 CPU cores. The core challenge is that all endpoints share the same two cores. A naive server lets the expensive /risk requests fill up the calls until even the instant /price requested. 

The API server exposes 3 endpoints + a health check:
/PRICE- Fetches the price of stocks at a near instant rate
/STATS- Computation of the statistics of N(N=500) amount of calls
/RISK- Performs N(N=50,000) chained SHA-256 hashing operations per request (Most expensive endpoint)

# How to use each endpoints:
curl http://127.0.0.1:8080/health #Check for health     \
curl http://127.0.0.1:8080/price?symbol=AAPL  #Run /PRICE for APPL stock   \
curl http://127.0.0.1:8080/stats?symbol=AAPL  #Run /STATS for APPL stock   \
curl http://127.0.0.1:8080/risk?seed=0.48   #Run /RISK where 0.48 is the seed value   \
curl.exe -X POST http://127.0.0.1:8080/price -H "Content-Type: application/json" -d '{\"symbol\":\"AAPL\",\"price\":999.99}'  #Changing value of price for Persistence Setup test   \

# Persistence (BONUS)
POST /price records a price update that survives a container restart.  Updates are written to a JSON file on a mounted volume, with a mutex guarding concurrent writes and almost all latency threshold is passed.

# Our approach:
Every endpoint is served by its own isolated worker pool and bounded queue, sized to how expensive that endpoint is:   
```
/price — many workers, deep queue (cheap, absorbs bursts)   
/stats — fewer workers, medium queue  
/risk — exactly 2 workers (matching the 2-core cap), short queue
```
Each the pools are separate therefore a flood of slow /risk requests can't starve the fast /price endpoint. Each request also carries a threshold trimmed by a 10% safety margin, so the server returns timeout call. Inside the /risk worker, the hash loop checks its deadline every 1,000 rounds and yields the core, so no single computation can hold a core hostage.  \

The design choice, in one line: let the server say no quickly and cheaply, rather than accept unlimited work and slowly grind to a halt.

# Presentation slide:
[Finalist pitch](https://canva.link/ynmznaip1os1bjk)

## Instruction to run this API server:

Build and run with the same 2 CPU / 2 GB limits the grader uses:

```
cd starters/Go          # or Python, Node or Java (Strongest server is built with Go)  
docker build -t obsidio .  #Build a Docker image (From Obsidio)
docker run --rm --cpus=2 --memory=2g -p 8080:8080 obsidio   #Run the Docker file with a constraint of 2 CPU cores and 2GBof memory in port 8080
```

Confirm it is alive:

```
curl http://127.0.0.1:8080/health
```

## Run the load test against it

From the k6 folder, point k6 at your running container:

```
k6 run -e TARGET=http://127.0.0.1:8080 grading.js
```

Read three things in the summary:
- `work_score`  : your weighted useful-work total (the leaderboard number).
- `http_req_failed` : your error rate (must stay under the ceiling).
- `http_req_duration{tier:price}` p95 : your fast-path latency, the headline.

The first run on a naive starter will fail the latency thresholds. That is
expected. Now go make it hold.


# Requirements
-Docker (with the ability to set --cpus and --memory limits)
-k6 for load testing
-Go 1.2x (only if building outside Docker)

# FAQ:
Why did we chose Go?   \
Go's go-routine is what optimizes this project, resulting in genuine concurrency without fail.
