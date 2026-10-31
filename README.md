# primegrid

`primegrid` adalah sistem penghitungan bilangan prima master-worker berbasis
Python dan gRPC. Master memecah rentang inklusif menjadi chunk half-open,
mendistribusikannya secara dinamis ke sejumlah worker, memverifikasi setiap
hasil terhadap jumlah prima yang diketahui, dan mengalirkan progres ke client.

Proyek ini mengimplementasikan kebutuhan pada `docs/01-PRD.md`,
`docs/02-ARCHITECTURE.md`, `docs/03-RULES.md`, dan
`docs/04-IMPLEMENTATION_PLAN.md`.

## Fitur yang diimplementasikan

| Kebutuhan | Implementasi |
|---|---|
| Minimal 4 RPC dengan satu streaming RPC | 10 RPC pada `MasterService` dan `WorkerService`; `WatchJob` adalah server streaming |
| Banyak client konkuren tanpa state rusak | Lock per job, antrean chunk thread-safe, registry worker/dedup terpisah, dan agregasi chunk exactly-once |
| Timeout, retry, dan request ID | Deadline pada setiap panggilan keluar, exponential backoff terbatas untuk `DEADLINE_EXCEEDED` dan `UNAVAILABLE`, serta deduplikasi `request_id` pada `SubmitJob` |
| Demo minimal dua perangkat | Deployment manual LAN/Wi-Fi dengan master di Laptop A dan satu atau lebih worker di Laptop B; lihat `docs/PENGUJIAN.md` |
| Benchmark 1/5/20 client dan speedup | Orkestrator skenario A/B/C menulis CSV mentah dan skrip plotting menghasilkan grafik PNG |
| Riwayat Git | Commit kecil dan logis dengan prefiks konvensional; setiap anggota tim sebaiknya melakukan commit dengan identitas Git masing-masing |

## Arsitektur

```text
                +--------------------------------------+
   clients ---->|  MasterService                       |
                |  job registry + per-job locks        |---- gRPC ----> Worker 1 :50061
   workers ---->|  worker registry + dedup table       |---- gRPC ----> Worker 2 :50062
                |  chunk queue + dispatcher per worker |---- gRPC ----> Worker 3 :50063
                |  health monitor + progress notifier  |
                +--------------------------------------+
```

Master tidak pernah melakukan komputasi bilangan prima. Setiap worker adalah
proses terpisah sehingga pekerjaan CPU-bound tidak dibatasi GIL. Satu worker
menghitung satu chunk pada satu waktu; paralelisme diperoleh dari menjalankan
beberapa proses worker.

### Kontrak RPC

`MasterService`:

- `SubmitJob` (unary, idempoten melalui `request_id`)
- `GetJobStatus` (unary)
- `WatchJob` (server streaming progres)
- `GetResult` (unary)
- `CancelJob` (unary, idempoten)
- `ListWorkers` (unary)
- `RegisterWorker` (unary, upsert)
- `Heartbeat` (unary)

`WorkerService`:

- `ProcessChunk` (unary, murni dan aman di-retry)
- `Ping` (unary)

Definisi kanonis ada di `proto/primegrid/primegrid.proto`. Stub hasil generate
ditulis ke `src/primegrid/` dan dapat dibuat ulang kapan saja.

## Struktur repositori

```text
primegrid/
├── proto/primegrid/primegrid.proto
├── src/primegrid/
│   ├── common/          konfigurasi, logging, retry, helper jaringan
│   ├── master/          job manager, dispatcher, registry, servicer, server
│   ├── worker/          algoritma, servicer, registrar, server
│   └── client/          API, CLI, orkestrator benchmark
├── tests/               unit, integrasi, konkurensi, kegagalan, regresi
├── bench/               skrip plotting
├── results/             CSV mentah dan grafik benchmark
└── docs/                PRD, arsitektur, aturan, rencana implementasi, demo, pengujian
```

## Prasyarat

- Python 3.10 atau lebih baru (terverifikasi dengan Python 3.14.7)
- `grpcio`, `grpcio-tools`, `pytest`, dan opsional `matplotlib`
- Dua laptop Windows pada Wi-Fi/LAN yang sama untuk demo multi-mesin
- Minimal tiga core CPU yang dapat dipakai untuk pengukuran speedup yang bermakna

## Instalasi manual

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements-dev.txt
python scripts/generate_proto.py
```

Di Linux/macOS gunakan `source .venv/bin/activate` dan `python` sebagai ganti
launcher Windows. Memasang paket dalam mode editable membuat console script
tersedia:

```powershell
python -m pip install -e .
```

## Menjalankan manual di dua laptop

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
    --master-ip 192.168.1.10 `
    --master-port 50050 `
    --host 0.0.0.0 `
    --port 50061 `
    --worker-id laptop-b-1
```

Worker mendeteksi IP LAN-nya secara otomatis dari rute menuju master. Jika
deteksi otomatis gagal, isi secara eksplisit dengan IP Laptop B:

```powershell
python -m primegrid.worker `
    --master-ip 192.168.1.10 `
    --master-port 50050 `
    --port 50061 `
    --advertise-host 192.168.1.20 `
    --worker-id laptop-b-1
```

Nilai `--advertise-host` harus IP Laptop B yang dapat dijangkau master, bukan
`0.0.0.0` dan bukan `localhost`.

### Client

```powershell
python -m primegrid.client --master-ip 192.168.1.10 --master-port 50050 workers
python -m primegrid.client --master-ip 192.168.1.10 --master-port 50050 submit `
    --start 1 --end 1000000 --chunk-size 100000 --mode sieve
python -m primegrid.client --master-ip 192.168.1.10 --master-port 50050 watch --job-id <job-id>
python -m primegrid.client --master-ip 192.168.1.10 --master-port 50050 result --job-id <job-id>
```

Hasil yang diharapkan untuk rentang 1.000.000: `prime_count=78498` dan
`max_prime=999983`.

`SubmitJob` bersifat at-most-once per `request_id`:

```powershell
python -m primegrid.client --master-ip 192.168.1.10 --master-port 50050 submit `
    --start 1 --end 1000000 --request-id demo-001
python -m primegrid.client --master-ip 192.168.1.10 --master-port 50050 submit `
    --start 1 --end 1000000 --request-id demo-001
```

Panggilan kedua mengembalikan `job_id` yang sama dengan `deduplicated=true`.

Bentuk lama `--master 192.168.1.10:50050` masih didukung, tetapi
`--master-ip`/`--master-port` adalah bentuk yang dianjurkan.

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

## Tes

```powershell
python -m pytest
```

Tes default mengecualikan regresi `trial` full-range. Jalankan tes lambat secara
terpisah:

```powershell
python -m pytest -m slow
```

Tes default mencakup:

- cakupan chunk, batas half-open, dan penanganan sisa pembagian
- kedua algoritma terhadap nilai referensi PRD
- deduplikasi `SubmitJob` secara konkuren
- agregasi chunk exactly-once, requeue, lease basi, pembatalan
- registrasi worker, deteksi worker mati, kebangkitan melalui heartbeat
- job end-to-end dengan tiga worker dan streaming `WatchJob`
- 20 client konkuren dengan hasil yang benar
- kehilangan worker saat job berjalan
- pemetaan status code gRPC
- regresi penuh sieve 1-10.000.000 (`664.579`, prima terbesar `9.999.991`)

Regresi full-range trial-division yang sengaja lambat bersifat opt-in:

```powershell
python -m pytest -m slow
```

## Benchmark

Orkestrator benchmark menjalankan proses master dan worker sungguhan. Ini
diperlukan karena thread CPU-bound dalam satu proses Python akan berbagi satu
GIL.

```powershell
primegrid-bench --scenario all `
    --worker-counts 1,2,3 --client-counts 1,5,20 `
    --repeat 3 --warmup 1 `
    --start 1 --end 10000000 --chunk-size 100000 --mode trial `
    --b-start 1 --b-end 1000000 --b-chunk-size 10000 `
    --output-dir results

python bench/plot_results.py --input results
```

Output:

- `results/scenario_a.csv` - T1/TN, speedup, efisiensi, kebenaran
- `results/scenario_b_jobs.csv` - konkurensi client, latensi/throughput job
- `results/scenario_b_status.csv` - latensi/throughput `GetJobStatus`
- `results/scenario_c.csv` - submit duplikat, kehilangan worker, injeksi fault
- `results/scenario_a.png` dan `results/scenario_b.png`
- `results/environment.txt` - host, Python, jumlah CPU, timestamp

Satu warm-up dibuang dan setiap konfigurasi terukur diulang minimal tiga kali
secara default. Speedup dihitung terhadap mean baseline T1.

### Smoke run cepat

```powershell
primegrid-bench --scenario A --worker-counts 1,2,3 `
    --repeat 1 --warmup 0 --start 1 --end 200000 `
    --chunk-size 10000 --mode trial --output-dir results/smoke
```

### Mengapa speedup tidak linear

Laporan perlu membahas semua hal berikut:

- Chunk `trial` di dekat 10.000.000 lebih mahal daripada chunk awal, sehingga
  keseimbangan beban bergantung pada dispatch dinamis dan ukuran chunk.
- Serialisasi gRPC, antrean, dan pembukuan master adalah overhead serial
  (hukum Amdahl).
- Host bisa memiliki core fisik lebih sedikit daripada CPU logis.
- Pada demo dua laptop, Worker B dapat dibatasi CPU fisiknya.
- Kecepatan eksekusi Python dan startup per proses tidak termasuk dalam job yang
  diukur, tetapi round-trip RPC dan penjadwalan termasuk.

## Konfigurasi

Setiap parameter operasional dikendalikan melalui environment. Lihat
`.env.example` untuk daftar lengkap. Variabel penting:

| Variabel | Default | Kegunaan |
|---|---:|---|
| `PRIMEGRID_MASTER_HOST` | `0.0.0.0` | Alamat bind master |
| `PRIMEGRID_MASTER_PORT` | `50050` | Port master, juga port default worker/client |
| `PRIMEGRID_MASTER_IP` | tidak ada | IP/host master untuk worker/client; wajib di laptop lain |
| `PRIMEGRID_MASTER_ADDRESS` | kosong | Bentuk legacy `host:port`; jika diisi menimpa IP/port |
| `PRIMEGRID_WORKER_ID` | dibuat otomatis | ID unik worker |
| `PRIMEGRID_WORKER_HOST` | `0.0.0.0` | Alamat bind worker |
| `PRIMEGRID_WORKER_PORT` | `50061` | Port bind worker |
| `PRIMEGRID_WORKER_ADVERTISE_HOST` | kosong | IP LAN yang diiklankan; kosong = deteksi otomatis |
| `PRIMEGRID_WORKER_ADVERTISE_PORT` | `0` | Port yang diiklankan; `0` = ikuti port bind |
| `PRIMEGRID_HEARTBEAT_INTERVAL` | `2.0` | Interval heartbeat dalam detik |
| `PRIMEGRID_WORKER_DEAD_AFTER` | `6.0` | Umur heartbeat sebelum worker dianggap DEAD |
| `PRIMEGRID_PROCESS_DEADLINE` | `60.0` | Deadline untuk `ProcessChunk` |
| `PRIMEGRID_RPC_DEADLINE` | `3.0` | Deadline untuk RPC ringan |
| `PRIMEGRID_MAX_RETRIES` | `3` | Maksimum percobaan untuk panggilan yang dapat di-retry |
| `PRIMEGRID_CHUNK_ATTEMPT_LIMIT` | `3` | Batas requeue sebelum job gagal |
| `PRIMEGRID_DEFAULT_CHUNK_SIZE` | `100000` | Ukuran chunk saat client mengirim 0 |
| `PRIMEGRID_DEDUP_TTL` | `3600.0` | Masa retensi dedup `request_id` |
| `PRIMEGRID_FAULT_DELAY_PROBABILITY` | `0.0` | Probabilitas injeksi delay worker |
| `PRIMEGRID_FAULT_DELAY_MAX_MS` | `0` | Delay maksimum yang diinjeksi |
| `PRIMEGRID_FAULT_ERROR_PROBABILITY` | `0.0` | Probabilitas injeksi kegagalan sementara worker |

Flag CLI (`--host`, `--port`, `--master-ip`, `--master-port`, `--worker-id`,
dan seterusnya) menimpa nilai environment terkait. Tidak ada alamat
`localhost` yang di-hardcode; worker dan client gagal dengan pesan jelas jika
alamat master tidak diberikan.

## Desain konkurensi dan kegagalan

- Registry job memakai lock singkat hanya untuk lookup/pembuatan.
- Setiap job memiliki lock dan condition variable sendiri untuk progres dan
  agregasi.
- Registry worker dan tabel dedup memiliki lock yang independen.
- Antrean chunk adalah `queue.Queue`, sehingga `get` yang memblokir aman tanpa
  lock manual.
- Tidak ada RPC keluar yang dilakukan saat lock master dipegang.
- Hasil chunk diterima hanya jika `chunk_id` belum ada di `done_set` job,
  sehingga retry tidak menghitung ganda.
- Lease worker yang gagal akan di-requeue. Hasil basi dari lease sebelumnya
  tidak berbahaya karena agregasi dikunci oleh `(job_id, chunk_id)`.
- `SubmitJob` memeriksa dan menulis tabel dedup sambil memegang lock-nya,
  sehingga dua pengiriman simultan dengan `request_id` yang sama tidak dapat
  membuat dua job.
- Health monitor menandai worker basi sebagai DEAD, me-requeue chunk yang
  ditugaskan, dan membiarkan worker yang masih hidup menyelesaikan job.
- Heartbeat worker dapat membangkitkan worker DEAD; dispatcher baru akan
  dijalankan.

## Panduan demo

Lihat `docs/DEMO.md` untuk urutan demo lima menit, termasuk kehilangan worker,
idempotensi, injeksi retry, dan uji beban 20 client.

## Keterbatasan yang diketahui

- State master berada di memori. Restart master menghilangkan job dan tabel
  dedup.
- Master adalah single point of failure.
- Channel gRPC menggunakan koneksi insecure; TLS/mTLS belum dikonfigurasi.
- Hasil berisi jumlah prima dan prima maksimum, bukan daftar lengkap prima.
- Jaminan at-most-once submit hanya berlaku selama masa hidup proses master.

Hal-hal ini dapat diterima untuk lingkup mata kuliah dan didokumentasikan pada
`docs/02-ARCHITECTURE.md` bagian 10.

## Alur kerja Git

Gunakan branch fitur/perbaikan dan commit konvensional kecil (`feat:`, `fix:`,
`test:`, `docs:`, `chore:`, `bench:`). Konfigurasikan nama dan email setiap
anggota tim sebelum melakukan commit agar `git shortlog -sn` mencerminkan
distribusi pekerjaan yang sebenarnya.
