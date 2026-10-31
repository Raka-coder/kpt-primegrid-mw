# ARSITEKTUR — primegrid (Master–Worker gRPC)

Dokumen ini menjelaskan komponen, alur panggilan RPC, model data, strategi concurrency, dan toleransi kegagalan. Isinya bisa langsung dijadikan dasar bab "Deskripsi studi kasus dan diagram arsitektur" pada laporan.

---

## 1. Gambaran Umum

```
                          ┌──────────────────────────────┐
                          │            MASTER            │
 ┌──────────┐  gRPC       │  MasterService (port 50050)  │       gRPC        ┌───────────┐
 │ Client 1 │────────────▶│  ┌────────────────────────┐  │◀──────────────────│ Worker 1  │
 └──────────┘             │  │ Job Registry           │  │  Register/Heartbeat│ :50061    │
 ┌──────────┐             │  │ Chunk Queue            │  │                   └───────────┘
 │ Client N │────────────▶│  │ Worker Registry        │  │  ProcessChunk     ┌───────────┐
 └──────────┘             │  │ Dedup Table            │  │──────────────────▶│ Worker 2  │
                          │  │ Dispatcher Threads     │  │                   │ :50062    │
                          │  └────────────────────────┘  │                   └───────────┘
                          │                              │  ProcessChunk     ┌───────────┐
                          └──────────────────────────────┘──────────────────▶│ Worker 3  │
                                                                              │ :50063    │
                                                                              └───────────┘
```

Master berperan ganda:
- **Server** untuk client (`SubmitJob`, `GetJobStatus`, `WatchJob`, `GetResult`, `CancelJob`, `ListWorkers`) dan untuk worker (`RegisterWorker`, `Heartbeat`).
- **Client** terhadap worker (`ProcessChunk`, `Ping`).

Worker juga berperan ganda: server untuk `ProcessChunk` / `Ping`, client untuk `RegisterWorker` / `Heartbeat`.

## 2. Komponen

### 2.1 Master
| Modul internal | Tanggung jawab |
|----------------|----------------|
| **API layer (servicer)** | Menerima RPC dari client dan worker, validasi input, memetakan error ke status code gRPC |
| **Job Manager** | Membuat job, memecah rentang jadi chunk, menyimpan status, menghitung progres |
| **Chunk Queue** | Antrian thread-safe berisi chunk berstatus PENDING (termasuk chunk yang dikembalikan) |
| **Dispatcher** | Satu thread per worker hidup; mengambil chunk dari antrian, memanggil `ProcessChunk`, melaporkan hasil ke Aggregator |
| **Aggregator** | Menerima hasil chunk, menerapkan dedup `(job_id, chunk_id)`, memperbarui total, memicu penyelesaian job |
| **Worker Registry** | Daftar worker, status (ALIVE / DEAD), waktu heartbeat terakhir |
| **Health Monitor** | Thread berkala yang menandai worker DEAD dan memicu pengembalian chunk |
| **Dedup Table** | Peta `request_id → job_id` untuk `SubmitJob`, dengan TTL |
| **Progress Notifier** | Mekanisme pemberitahuan (condition/event) agar `WatchJob` mengirim pembaruan tanpa polling sibuk |

### 2.2 Worker
| Modul internal | Tanggung jawab |
|----------------|----------------|
| **API layer** | Menerima `ProcessChunk` dan `Ping`, validasi rentang |
| **Compute engine** | Dua algoritma: `sieve` (segmented sieve, untuk verifikasi) dan `trial` (pembagian percobaan, untuk benchmark) |
| **Registrar** | Mendaftar ke master saat start (dengan retry), mengirim heartbeat berkala di thread terpisah |
| **Fault injector (opsional, diaktifkan lewat konfigurasi)** | Menyuntik keterlambatan atau kegagalan acak untuk demo skenario C |

### 2.3 Client (CLI)
| Perintah | RPC yang dipakai |
|----------|------------------|
| `submit` | `SubmitJob` (dengan `request_id` dibuat sekali per niat pengguna, dipakai ulang tiap retry) |
| `status` | `GetJobStatus` |
| `watch` | `WatchJob` |
| `result` | `GetResult` |
| `cancel` | `CancelJob` |
| `workers` | `ListWorkers` |
| `bench` | Menjalankan skenario uji A dan B secara otomatis |

## 3. Model Data

### 3.1 Job
| Field | Keterangan |
|-------|------------|
| `job_id` | ID unik (UUID) |
| `request_id` | ID dari client untuk dedup |
| `start`, `end` | Rentang inklusif dari sisi pengguna; dikonversi ke chunk setengah terbuka |
| `mode` | `sieve` / `trial` |
| `chunk_size` | Ukuran chunk |
| `status` | `PENDING` → `RUNNING` → `COMPLETED` / `FAILED` / `CANCELLED` |
| `total_chunks`, `done_chunks` | Untuk progres |
| `prime_count`, `max_prime` | Hasil teragregasi |
| `done_set` | Himpunan `chunk_id` yang sudah dihitung (kunci dedup agregasi) |
| `created_at`, `started_at`, `finished_at` | Untuk pengukuran waktu |
| `per_worker_stats` | Jumlah chunk dan waktu hitung per worker |
| `error` | Pesan bila gagal |

### 3.2 Chunk
| Field | Keterangan |
|-------|------------|
| `chunk_id` | Nomor urut dalam job |
| `range` | `[a, b)` |
| `state` | `PENDING` → `ASSIGNED` → `DONE` |
| `attempts` | Jumlah percobaan; melebihi batas → job `FAILED` |
| `assigned_to` | `worker_id` saat ini |

### 3.3 Worker
`worker_id`, `address`, `cores`, `status` (ALIVE/DEAD), `last_heartbeat`, `chunks_done`.

## 4. Alur Utama

### 4.1 Registrasi worker
1. Worker start, membuka server `WorkerService`.
2. Worker memanggil `RegisterWorker` ke master (retry dengan backoff sampai berhasil atau menyerah setelah batas waktu).
3. Master menyimpan/memperbarui entri (upsert pada `worker_id`) dan membuat/menyalakan dispatcher untuk worker itu.
4. Worker mengirim `Heartbeat` tiap interval tetap.

### 4.2 Pengajuan dan eksekusi job
1. Client memanggil `SubmitJob(request_id, start, end, ...)`.
2. Master memeriksa Dedup Table: bila `request_id` sudah ada → kembalikan `job_id` lama; bila belum → buat job, pecah chunk, masukkan ke Chunk Queue, catat dedup.
3. Master membalas `job_id` segera (job berjalan asinkron).
4. Dispatcher tiap worker mengambil chunk, memanggil `ProcessChunk` dengan deadline.
5. Worker menghitung dan mengembalikan `(prime_count, max_prime, durasi)`.
6. Aggregator memeriksa `chunk_id` belum ada di `done_set`; bila belum → tambahkan hasil dan tandai selesai; bila sudah → abaikan (duplikat).
7. Ketika semua chunk selesai, job menjadi `COMPLETED` dan `finished_at` dicatat.
8. Client memantau lewat `WatchJob` / `GetJobStatus`, lalu mengambil hasil lewat `GetResult`.

```
Client        Master                     Worker k
  │ SubmitJob    │                           │
  │─────────────▶│ dedup? buat job+chunks    │
  │◀─────────────│ job_id                    │
  │ WatchJob     │                           │
  │─────────────▶│  Dispatcher ProcessChunk  │
  │              │──────────────────────────▶│ hitung
  │◀── progres ──│◀──────── hasil ───────────│
  │◀── progres ──│   ... (berulang)          │
  │◀── COMPLETED │                           │
```

### 4.3 Worker mati di tengah job
1. Heartbeat berhenti **atau** `ProcessChunk` berakhir `UNAVAILABLE` / `DEADLINE_EXCEEDED` setelah retry habis.
2. Dispatcher/Health Monitor menandai worker `DEAD`.
3. Chunk `ASSIGNED` milik worker itu dikembalikan ke Chunk Queue, `attempts` bertambah.
4. Worker lain mengambil chunk tersebut.
5. Bila worker hidup kembali dan mendaftar lagi, ia kembali mendapat chunk.
6. Bila hasil dari worker "mati" ternyata tiba terlambat, Aggregator mengabaikannya karena `chunk_id` sudah dihitung (atau menerimanya bila ia lebih dulu — hanya satu yang diterima).

### 4.4 Pembatalan job
`CancelJob` menandai job `CANCELLED`; dispatcher berhenti mengambil chunk job itu; hasil yang masih berjalan dibuang. Pembatalan berulang tidak mengubah apa pun (idempoten).

## 5. Pembagian Chunk dan Algoritma

- Rentang pengguna `[start, end]` dikonversi ke `[start, end+1)`, lalu dipotong menjadi chunk `[a, b)` berukuran `chunk_size`. Setengah terbuka menjamin tidak ada angka terhitung dua kali.
- **Mode `sieve`**: segmented sieve; dipakai sebagai **pembanding kebenaran** dan untuk uji beban client yang ringan.
- **Mode `trial`**: pembagian percobaan oleh bilangan ganjil hingga akar kuadrat; sengaja berat CPU agar speedup terlihat; angka besar lebih mahal sehingga beban per chunk **tidak merata** → alasan memakai pembagian dinamis.
- Ukuran chunk bawaan disarankan 100.000 (≈ 100 chunk untuk 10 juta). Nilai final ditentukan dari eksperimen kecil dan dicatat di laporan.

## 6. Concurrency

| Sumber konflik | Mekanisme | Alasan |
|----------------|-----------|--------|
| Registri job (buat/baca dari banyak thread RPC) | Lock global pendek hanya untuk pembuatan/pencarian job | Operasi singkat; kontensi rendah |
| State per job (progres, total, `done_set`) | **Lock per job** | Job berbeda tidak saling menghambat; mencegah race pada agregasi |
| Chunk Queue | Antrian thread-safe bawaan pustaka standar | Aman tanpa lock manual dan mendukung blocking get dengan timeout |
| Worker Registry | Lock khusus registri worker | Heartbeat sering; dipisah dari lock job |
| Dedup Table | Lock + pengecekan-lalu-tulis dalam satu bagian kritis | Mencegah dua `SubmitJob` bersamaan dengan `request_id` sama lolos keduanya |
| `WatchJob` | Condition/event per job | Menghindari polling sibuk |

Aturan emas:
- Tidak ada panggilan RPC keluar (jaringan) **saat memegang lock**.
- Urutan pengambilan lock konsisten (registri → job) untuk mencegah deadlock.
- Komputasi berat berjalan di worker, bukan di master.
- Di worker, **satu chunk pada satu waktu**; proses worker terpisah per worker untuk menghindari batasan GIL.

## 7. Toleransi Kegagalan

| Kegagalan | Deteksi | Penanganan |
|-----------|---------|------------|
| Worker lambat / jaringan lambat | Deadline per `ProcessChunk` | Retry backoff; setelah habis, chunk dikembalikan ke antrian |
| Worker mati | Heartbeat terlewat dan/atau `UNAVAILABLE` | Tandai DEAD, kembalikan chunk, worker lain melanjutkan |
| Balasan hilang (hasil sudah dihitung tetapi tak sampai) | Timeout di master | Chunk dikirim ulang; aman karena perhitungan murni dan agregasi dedup |
| Client mengirim ulang `SubmitJob` | `request_id` sama | Kembalikan `job_id` lama (at-most-once) |
| Client terputus saat `WatchJob` | Status konteks RPC tidak aktif | Hentikan streaming; job tetap berjalan |
| Semua worker mati | Tidak ada worker ALIVE | Job menunggu sampai batas waktu, lalu `FAILED` dengan pesan jelas |
| Chunk gagal berulang kali | `attempts` melewati batas | Job `FAILED` (mencegah loop tak berujung) |
| Master restart | — | State hilang (keterbatasan, lihat bagian 10) |

### Parameter bawaan (semua dapat dikonfigurasi lewat environment)
| Parameter | Nilai awal yang disarankan |
|-----------|----------------------------|
| Interval heartbeat | 2 detik |
| Worker dianggap DEAD | 3 heartbeat terlewat (≈ 6 detik) |
| Deadline `ProcessChunk` | Cukup besar untuk chunk terberat (mis. 30–60 detik), ukur dulu |
| Deadline RPC ringan | 2–3 detik |
| Retry maksimal | 3 percobaan |
| Backoff | 0,5 s × 2^(percobaan−1) (ditambah jitter kecil) |
| Batas `attempts` chunk | 3 |
| TTL entri dedup | Beberapa menit hingga jam, dengan pembersihan berkala |

Status code yang boleh di-retry: `DEADLINE_EXCEEDED`, `UNAVAILABLE`. Status seperti `INVALID_ARGUMENT` dan `NOT_FOUND` **tidak** di-retry.

## 8. Matriks Idempotensi

| RPC | Idempoten | Jika terulang |
|-----|-----------|---------------|
| `SubmitJob` | Tidak | Dedup `request_id` → `job_id` sama |
| `RegisterWorker` | Ya | Upsert |
| `Heartbeat` | Ya | Perbarui waktu |
| `ProcessChunk` | Ya (murni) | Hasil sama; agregasi hanya menerima sekali |
| `CancelJob` | Ya | Sudah dibatalkan → tidak ada perubahan |
| `GetJobStatus`, `GetResult`, `ListWorkers`, `WatchJob`, `Ping` | Ya (baca saja) | Aman |

## 9. Deployment

### 9.1 Topologi demo (memenuhi "≥ 2 container/komputer")
- 1 container **master**
- 3 container **worker** (`worker1..3`), dibatasi 1 CPU masing-masing agar pengukuran adil
- 1 container **client/benchmark**
- Satu jaringan container buatan sendiri; worker menemukan master lewat nama layanan, master menemukan worker lewat alamat yang dilaporkan saat registrasi
- Orkestrasi dengan berkas compose yang berjalan di Docker maupun Podman

### 9.2 Topologi alternatif (2 komputer fisik)
Master + client di komputer 1; worker di komputer 2 (atau tersebar). Ganti `localhost` dengan IP master dan buka port di firewall.

### 9.3 Port
| Komponen | Port |
|----------|------|
| Master | 50050 |
| Worker | 50061, 50062, 50063 (atau satu port yang sama bila tiap worker punya container sendiri) |

## 10. Keterbatasan dan Perbaikan yang Diusulkan (untuk bab analisis)

| Keterbatasan | Dampak | Perbaikan |
|--------------|--------|-----------|
| State master in-memory | Master restart → job dan dedup hilang; jaminan at-most-once putus | Simpan job, chunk, dan dedup di penyimpanan persisten (mis. SQLite/Redis) |
| Master tunggal (SPOF) | Master mati → sistem berhenti | Replikasi master dengan leader election |
| Koneksi tanpa TLS | Data bisa disadap | TLS/mTLS pada channel gRPC |
| Hasil hanya count dan max | Tidak mengembalikan daftar prima | Streaming daftar prima bila diperlukan |
| Heartbeat berbasis waktu | Bisa salah deteksi saat jaringan lambat | Gabungkan dengan hasil panggilan langsung |

## 11. Struktur Repositori yang Direkomendasikan

```
primegrid/
├── README.md
├── proto/primegrid.proto
├── generated/              # keluaran protoc (boleh di-gitignore, dibuat ulang lewat skrip)
├── src/primegrid/
│   ├── common/             # konfigurasi, logging, util retry/backoff
│   ├── master/             # servicer, job manager, queue, dispatcher, aggregator, registry
│   ├── worker/             # servicer, compute engine, registrar
│   └── client/             # CLI
├── bench/                  # skrip skenario A, B, C + pembuat grafik
├── tests/                  # uji unit dan integrasi
├── deploy/                 # Dockerfile, compose
├── docs/                   # PRD, arsitektur, rules, plan, diagram
└── results/                # CSV hasil ukur dan grafik
```
