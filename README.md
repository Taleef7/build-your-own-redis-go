[![progress-banner](https://backend.codecrafters.io/progress/redis/bd41f805-6048-40ae-b95d-0efead917cb8)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)
## Build Your Own Redis (Go)

A compact, single-binary Redis-like server implemented in Go. It speaks the Redis RESP protocol and supports many core commands, including strings, lists, sorted sets, streams, pub/sub, simple replication, and geospatial decode (GEOPOS).

This repo started as part of the CodeCrafters “Build Your Own Redis” challenge and has been extended and polished to showcase the implementation as a standalone project.

## Features

- RESP protocol server (single TCP binary)
- Strings: PING, ECHO, SET (with PX), GET, INCR
- Lists: LPUSH, RPUSH, LPOP (count), LLEN, LRANGE, BLPOP
- Sorted sets: ZADD, ZREM, ZSCORE, ZRANGE, ZRANK, ZCARD
- Streams: XADD, XRANGE, XREAD (BLOCK)
- Pub/Sub: SUBSCRIBE, UNSUBSCRIBE, PUBLISH
- Replication (minimal): PSYNC, REPLCONF, INFO replication, WAIT
- Geospatial: GEOADD (validations), GEOPOS (score decoding)
- In-memory storage with expirations

## Project layout

- `app/main.go` – the entire server implementation
- `your_program.sh` – build-and-run helper (mirrors challenge runner)
- `go.mod` – module declaration (Go 1.24)

## Run locally

Requirements: Go 1.24+

Quick run (recommended, matches the challenge runner):

```bash
./your_program.sh
```

Connect with redis-cli:

```bash
redis-cli -p 6379
> PING
PONG
> SET key value PX 5000
OK
> GET key
"value"
> ZADD places 3663832752681684 Paris
1
> GEOPOS places Paris
[["2.294471561908722","48.85846255040141"]]
```

## Notable implementation details

- Single process, concurrent clients via goroutines
- Minimal RDB loader for FULLRESYNC bootstrapping
- Interleaved 52‑bit geohash scores for GEO commands (decode for GEOPOS)
- Simple replication stream with GETACK/WAIThandling

## Publishing your own module (optional)

If you plan to import this as a Go module from GitHub, update `go.mod`:

1) Replace the module path with your GitHub repo path, e.g.:

```
module github.com/<your-username>/<your-repo>
```

2) Run `go mod tidy` after pushing the repo.

This change isn’t required to run the server locally.

## Attribution

Inspired by the CodeCrafters “Build Your Own Redis” challenge. See https://codecrafters.io for the original course and materials.

## License

MIT
