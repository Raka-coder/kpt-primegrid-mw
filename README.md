# primegrid

`primegrid` is a master-worker prime-counting system built with Python and gRPC. A
master splits an inclusive range into half-open chunks, dispatches those chunks
dynamically to any number of workers, verifies each result against known prime
counts, and streams progress back to clients.

The project implements the requirements in `docs/01-PRD.md`,
`docs/02-ARCHITECTURE.md`, `docs/03-RULES.md`, and
`docs/04-IMPLEMENTATION_PLAN.md`.

## What is implemented

| Requirement | Implementation |
|---|---|
| At least 4 RPCs with one streaming RPC | 10 RPCs across `MasterService` and `WorkerService`; `WatchJob` is server streaming |
| Many concurrent clients without corrupted state | Per-job locks, a thread-safe chunk queue, separate worker/dedup registries, and exactly-once chunk aggregation |
| Timeout, retry, and request IDs | Deadlines on every outgoing call, bounded exponential backoff for `DEADLINE_EXCEEDED` and `UNAVAILABLE`, and `request_id` deduplication for `SubmitJob` |
| Demo on at least two devices | Manual LAN/Wi-Fi deployment with the master on Laptop A and one or more workers on Laptop B; see `docs/PENGUJIAN.md` |
| Benchmark 1/5/20 clients and speedup | Scenario A/B/C orchestrator writes raw CSV files and a plotting script produces PNG charts |
| Git history | Small, logical commits with conventional prefixes; each team member should commit under their own Git identity |

## Architecture

```text
                +--------------------------------------+
   clients ---->|  MasterService                       |
                |  job registry + per-job locks        |---- gRPC ----> Worker 1 :50061
   workers ---->|  worker registry + dedup table       |---- gRPC ----> Worker 2 :50062
                |  chunk queue + dispatcher per worker |---- gRPC ----> Worker 3 :50063
                |  health monitor + progress notifier  |
                +--------------------------------------+
```

The master never performs prime computation. Each worker is a separate process
so CPU-bound work is not limited by the GIL. A worker computes one chunk at a
time; parallelism comes from running multiple worker processes.

### RPC contract

`MasterService`:

- `SubmitJob` (unary, idempotent through `request_id`)
- `GetJobStatus` (unary)
- `WatchJob` (server streaming progress)
- `GetResult` (unary)
- `CancelJob` (unary, idempotent)
- `ListWorkers` (unary)
- `RegisterWorker` (unary, upsert)
- `Heartbeat` (unary)

`WorkerService`:

- `ProcessChunk` (unary, pure and retry-safe)
- `Ping` (unary)

The canonical definition is `proto/primegrid/primegrid.proto`. Generated stubs
are written into `src/primegrid/` and can always be regenerated.

## Repository layout

```text
primegrid/
├── proto/primegrid/primegrid.proto
├── src/primegrid/
│   ├── common/          configuration, logging, retry
│   ├── master/          job manager, dispatcher, registry, servicer, server
│   ├── worker/          algorithms, servicer, registrar, server
│   └── client/          API, CLI, benchmark orchestrator
├── tests/               unit, integration, concurrency, failure, regression
├── bench/               plotting script
├── results/             raw benchmark CSV and charts
└── docs/                PRD, architecture, rules, implementation plan, demo and testing guides
```

## Prerequisites

- Python 3.10 or newer (verified with Python 3.14.7)
- `grpcio`, `grpcio-tools`, `pytest`, and optionally `matplotlib`
- Two Windows laptops on the same Wi-Fi/LAN for the multi-machine demo
- At least three usable CPU cores for meaningful speedup measurements

## Manual installation

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements-dev.txt
python scripts/generate_proto.py
```

On Linux/macOS use `source .venv/bin/activate` and `python` instead of the
Windows launcher. Installing the package in editable mode makes the console
scripts available:

```powershell
python -m pip install -e .
```

## Run manually on two laptops

Contoh topologi:

| Laptop | Peran | Contoh IP |
|---|---|---|
| A | Master | `192.168.1.10` |
| B | Worker | `192.168.1.20` |

Kedua laptop harus berada di Wi-Fi/LAN yang sama, bisa saling `ping`, dan
menggunakan profil jaringan Windows `Private`. Ganti IP contoh dengan hasil
`ipconfig`.

### Laptop A: master

```powershell
cd C:\primegrid
.\.venv\Scripts\Activate.ps1
python -m primegrid.master --host 0.0.0.0 --port 50050
```

Bind `0.0.0.0` membuat master menerima koneksi dari Wi-Fi/LAN. Jangan memakai
`localhost` untuk mode dua laptop.

### Laptop B: worker

```powershell
cd C:\primegrid
.\.venv\Scripts\Activate.ps1
python -m primegrid.worker `
    --master 192.168.1.10:50050 `
    --host 0.0.0.0 `
    --port 50061 `
    --advertise-host 192.168.1.20 `
    --advertise-port 50061 `
    --worker-id laptop-b-1
```

`--advertise-host` adalah IP Laptop B yang dapat dijangkau master. Nilai ini
bukan `0.0.0.0` dan bukan `localhost`.

### Client

```powershell
python -m primegrid.client --master 192.168.1.10:50050 workers
python -m primegrid.client --master 192.168.1.10:50050 submit `
    --start 1 --end 1000000 --chunk-size 100000 --mode sieve
python -m primegrid.client --master 192.168.1.10:50050 watch --job-id <job-id>
python -m primegrid.client --master 192.168.1.10:50050 result --job-id <job-id>
```

Hasil yang diharapkan untuk rentang 1.000.000: `prime_count=78498` dan
`max_prime=999983`.

`SubmitJob` bersifat at-most-once per `request_id`:

```powershell
python -m primegrid.client --master 192.168.1.10:50050 submit `
    --start 1 --end 1000000 --request-id demo-001
python -m primegrid.client --master 192.168.1.10:50050 submit `
    --start 1 --end 1000000 --request-id demo-001
```

Panggilan kedua mengembalikan `job_id` yang sama dengan `deduplicated=true`.

### Firewall Windows

Jalankan sebagai Administrator di Laptop A:

```powershell
New-NetFirewallRule -DisplayName "primegrid-master-tcp-50050" `
    -Direction Inbound -Action Allow -Protocol TCP -LocalPort 50050 `
    -Profile Private,Domain -RemoteAddress LocalSubnet
```

Jalankan sebagai Administrator di Laptop B:

```powershell
New-NetFirewallRule -DisplayName "primegrid-worker-tcp-50061" `
    -Direction Inbound -Action Allow -Protocol TCP -LocalPort 50061 `
    -Profile Private,Domain -RemoteAddress LocalSubnet
```

Verifikasi dari kedua arah:

```powershell
Test-NetConnection 192.168.1.10 -Port 50050
Test-NetConnection 192.168.1.20 -Port 50061
```

Panduan lengkap, termasuk dua worker untuk failover, uji 20 client, benchmark,
dan troubleshooting ada di `docs/PENGUJIAN.md`.

## Tests

```powershell
python -m pytest
```

Tes default mengecualikan regresi `trial` full-range. Jalankan tes lambat secara
terpisah:

```powershell
python -m pytest -m slow
```

Default tests cover:

- chunk coverage, half-open boundaries, and remainder handling
- both algorithms against PRD reference values
- concurrent deduplication of `SubmitJob`
- exactly-once chunk aggregation, requeue, stale leases, cancellation
- worker registration, stale detection, heartbeat revival
- three-worker end-to-end job with `WatchJob` streaming
- 20 concurrent clients with correct results
- worker loss during a job
- gRPC status-code mapping
- full 1-10,000,000 sieve regression (`664,579`, largest prime `9,999,991`)

The intentionally slow full-range trial-division regression is opt-in:

```powershell
python -m pytest -m slow
```

## Benchmark

The benchmark orchestrator launches real master and worker processes. This is
required because CPU-bound threads in one Python process would share one GIL.

```powershell
primegrid-bench --scenario all `
    --worker-counts 1,2,3 --client-counts 1,5,20 `
    --repeat 3 --warmup 1 `
    --start 1 --end 10000000 --chunk-size 100000 --mode trial `
    --b-start 1 --b-end 1000000 --b-chunk-size 10000 `
    --output-dir results

python bench/plot_results.py --input results
```

Outputs:

- `results/scenario_a.csv` - T1/TN, speedup, efficiency, correctness
- `results/scenario_b_jobs.csv` - client concurrency, job latency/throughput
- `results/scenario_b_status.csv` - `GetJobStatus` latency/throughput
- `results/scenario_c.csv` - duplicate submit, worker loss, fault injection
- `results/scenario_a.png` and `results/scenario_b.png`
- `results/environment.txt` - host, Python, CPU count, timestamp

One warm-up run is discarded and every measured configuration is repeated at
least three times by default. Speedup is computed against the mean T1 baseline.

### Quick smoke run

```powershell
primegrid-bench --scenario A --worker-counts 1,2,3 `
    --repeat 1 --warmup 0 --start 1 --end 200000 `
    --chunk-size 10000 --mode trial --output-dir results/smoke
```

### Why speedup is not linear

The report should discuss all of these:

- `trial` chunks near 10,000,000 are more expensive than early chunks, so load
  balance depends on dynamic dispatch and chunk size.
- gRPC serialization, queueing, and master bookkeeping are serial overheads
  (Amdahl's law).
- The host may have fewer physical cores than logical CPUs.
- In the two-laptop demo, Worker B may be limited by its physical CPU.
- Python execution speed and per-process startup are not part of the measured
  job, but RPC round-trip and scheduling are.

## Configuration

Every operational parameter is environment-driven. See `.env.example` for a
complete list. Important variables:

| Variable | Default | Purpose |
|---|---:|---|
| `PRIMEGRID_MASTER_HOST` | `0.0.0.0` | Master bind address |
| `PRIMEGRID_MASTER_PORT` | `50050` | Master port |
| `PRIMEGRID_MASTER_ADDRESS` | `localhost:50050` | Address used by workers/clients |
| `PRIMEGRID_WORKER_ID` | generated | Unique worker ID |
| `PRIMEGRID_WORKER_HOST` | `0.0.0.0` | Worker bind address |
| `PRIMEGRID_WORKER_PORT` | `50061` | Worker bind port |
| `PRIMEGRID_WORKER_ADVERTISE_HOST` | `localhost` | Address advertised to master |
| `PRIMEGRID_WORKER_ADVERTISE_PORT` | `50061` | Port advertised to master |
| `PRIMEGRID_HEARTBEAT_INTERVAL` | `2.0` | Heartbeat interval in seconds |
| `PRIMEGRID_WORKER_DEAD_AFTER` | `6.0` | Heartbeat age before DEAD |
| `PRIMEGRID_PROCESS_DEADLINE` | `60.0` | Deadline for `ProcessChunk` |
| `PRIMEGRID_RPC_DEADLINE` | `3.0` | Deadline for lightweight RPCs |
| `PRIMEGRID_MAX_RETRIES` | `3` | Maximum attempts for retryable calls |
| `PRIMEGRID_CHUNK_ATTEMPT_LIMIT` | `3` | Requeue budget before a job fails |
| `PRIMEGRID_DEFAULT_CHUNK_SIZE` | `100000` | Chunk size when client sends 0 |
| `PRIMEGRID_DEDUP_TTL` | `3600.0` | `request_id` dedup retention |
| `PRIMEGRID_FAULT_DELAY_PROBABILITY` | `0.0` | Worker delay injection |
| `PRIMEGRID_FAULT_DELAY_MAX_MS` | `0` | Maximum injected delay |
| `PRIMEGRID_FAULT_ERROR_PROBABILITY` | `0.0` | Worker transient failure injection |

CLI flags (`--host`, `--port`, `--master`, `--worker-id`, etc.) override the
corresponding environment values.

## Concurrency and failure design

- The job registry has a short lock for lookup/creation only.
- Each job has its own lock and condition variable for progress and aggregation.
- The worker registry and dedup table have independent locks.
- The chunk queue is `queue.Queue`, so blocking `get` is safe without a manual
  lock.
- No outgoing RPC is made while a master lock is held.
- A chunk result is accepted only if its `chunk_id` is not already in the job's
  `done_set`, so retries cannot double-count.
- Failed worker leases are requeued. A stale result from an earlier lease is
  harmless because aggregation is keyed by `(job_id, chunk_id)`.
- `SubmitJob` checks and writes the dedup table while holding its lock, which
  prevents two simultaneous submissions with the same `request_id` from both
  creating a job.
- The health monitor marks stale workers DEAD, requeues their assigned chunks,
  and lets surviving workers finish the job.
- A worker heartbeat can revive a DEAD worker; a fresh dispatcher is started.

## Demo guide

See `docs/DEMO.md` for a five-minute demo sequence, including worker-loss,
idempotency, retry injection, and the 20-client load test.

## Known limitations

- Master state is in memory. A master restart loses jobs and the dedup table.
- The master is a single point of failure.
- Channels use insecure gRPC; TLS/mTLS is not configured.
- Results contain prime count and maximum prime, not the full prime list.
- At-most-once submit is guaranteed only for the master process lifetime.

These are acceptable for the course scope and are documented in
`docs/02-ARCHITECTURE.md` section 10.

## Git workflow

Use feature/fix branches and small conventional commits (`feat:`, `fix:`,
`test:`, `docs:`, `chore:`, `bench:`). Configure each team member's own name and
email before committing so `git shortlog -sn` reflects the real distribution of
work.
