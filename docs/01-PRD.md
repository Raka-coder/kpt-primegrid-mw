# PRD — Sistem Terdistribusi Master–Worker Berbasis gRPC

**Mata kuliah:** Komputasi Paralel dan Terdistribusi
**Topik:** Remote Procedure Calls (RPC) — Tugas Proyek Mini, Studi Kasus 2
**Nama proyek:** `primegrid` (nama kerja, boleh diganti)
**Bahasa / pustaka:** Python 3.10+, `grpcio`, `grpcio-tools`
**Anggota kelompok:** (isi nama, NIM, peran: Anggota A / B / C)

---

## 1. Ringkasan

Membangun sistem terdistribusi di mana sebuah **master** membagi pekerjaan menghitung bilangan prima pada rentang **1 – 10.000.000** menjadi potongan-potongan kecil (*chunk*), mengirimkannya ke **3 worker atau lebih** secara paralel melalui gRPC, menggabungkan hasilnya, lalu mengukur **speedup**.

Sistem juga harus tahan terhadap kegagalan (worker mati, jaringan lambat, permintaan terduplikasi) dan aman dipakai banyak client secara bersamaan.

## 2. Tujuan

1. Memenuhi seluruh **Ketentuan Umum** tugas (lihat bagian 3) dan rubrik penilaian.
2. Menunjukkan pemahaman konsep RPC: stub, marshalling, IDL, binding, timeout, retry, semantik at-most-once, concurrency.
3. Membuktikan lewat pengukuran bahwa pembagian kerja ke banyak worker mempercepat komputasi (speedup > 1) dan menjelaskan penyebab speedup yang tidak linear.

## 3. Pemetaan Ketentuan Umum → Kebutuhan

| # | Ketentuan Umum | Wujud di proyek | Bukti yang dikumpulkan |
|---|----------------|-----------------|------------------------|
| 1 | `.proto` minimal 4 RPC, ≥ 1 streaming | 9 RPC di 2 service (lihat bagian 6), 1 server streaming (`WatchJob`) | Isi `.proto` + penjelasan tiap RPC di laporan |
| 2 | Banyak client bersamaan tanpa data rusak | Lock pada state master, antrian chunk thread-safe, agregasi hanya sekali per chunk | Uji beban N client; hasil selalu sama dengan nilai referensi |
| 3 | Timeout + retry; operasi non-idempoten dilindungi ID permintaan | Deadline di semua panggilan, exponential backoff, `request_id` pada `SubmitJob`, dedup chunk pada master | Log demo retry + tabel idempotensi |
| 4 | Demo di ≥ 2 komputer / 2 container | Master + 3 worker + client sebagai container terpisah (opsional: 2 laptop fisik) | Video demo + tangkapan layar |
| 5 | Uji kinerja 1, 5, 20 client; hitung speedup | Skenario uji A (speedup) dan B (client bersamaan) | Tabel + grafik + analisis |
| 6 | Git dengan riwayat commit tiap anggota | Repositori Git, branch per fitur, commit tiap anggota | Tautan repositori, `git shortlog` |

## 4. Peran Pengguna

| Peran | Deskripsi | Interaksi |
|-------|-----------|-----------|
| **Client (pengaju job)** | Pengguna / skrip CLI yang meminta penghitungan prima | Memanggil RPC pada master |
| **Master (koordinator)** | Server yang menerima job, memecah chunk, mendistribusikan, menggabungkan | Server untuk client dan worker; client untuk worker |
| **Worker** | Server yang menghitung prima untuk satu chunk | Server untuk master; client untuk master saat registrasi/heartbeat |
| **Operator** | Anggota yang menjalankan demo dan uji kinerja | Menjalankan container, mematikan worker untuk demo kegagalan |

## 5. Kebutuhan Fungsional

### 5.1 Job dan pembagian kerja
- **FR-01** Client dapat mengirim job dengan parameter: `start`, `end`, `chunk_size` (opsional, ada nilai bawaan), dan `mode` algoritma (`sieve` atau `trial`).
- **FR-02** Master memecah rentang menjadi chunk **setengah terbuka `[a, b)`** tanpa tumpang tindih dan tanpa celah; chunk terakhir menyesuaikan sisa.
- **FR-03** Master memberi `job_id` unik dan menjalankan job secara asinkron; `SubmitJob` langsung kembali.
- **FR-04** Master mengirim chunk ke ≥ 3 worker **secara paralel**; worker yang selesai langsung mendapat chunk berikutnya (pembagian dinamis).
- **FR-05** Master menggabungkan hasil: total jumlah prima, prima terbesar yang ditemukan, dan statistik per worker (jumlah chunk, total waktu hitung).
- **FR-06** Client dapat melihat status job (`GetJobStatus`), memantau progres langsung (`WatchJob`, streaming), dan mengambil hasil akhir (`GetResult`).
- **FR-07** Client dapat membatalkan job (`CancelJob`).

### 5.2 Manajemen worker
- **FR-08** Worker mendaftar ke master saat start (`RegisterWorker`) dengan `worker_id`, alamat, dan jumlah core.
- **FR-09** Worker mengirim `Heartbeat` berkala; master menandai worker **DEAD** bila beberapa heartbeat berturut-turut terlewat.
- **FR-10** Client dapat melihat daftar worker beserta statusnya (`ListWorkers`).
- **FR-11** Worker bisa bergabung di tengah job berjalan dan langsung ikut mengambil chunk.

### 5.3 Toleransi kegagalan
- **FR-12** Semua panggilan RPC memakai **deadline (timeout)**.
- **FR-13** Panggilan gagal dengan `DEADLINE_EXCEEDED` / `UNAVAILABLE` diulang dengan **exponential backoff** dengan batas percobaan.
- **FR-14** Bila worker mati saat memproses chunk, chunk **dikembalikan ke antrian** dan dikerjakan worker lain; job tetap selesai dengan hasil benar.
- **FR-15** Chunk yang gagal melebihi batas percobaan membuat job berstatus `FAILED` dengan pesan jelas.
- **FR-16** `SubmitJob` dilindungi `request_id`: pengiriman ulang dengan ID sama **tidak** membuat job kedua (at-most-once).
- **FR-17** Hasil ganda untuk chunk yang sama (akibat retry atau worker lambat) **hanya dihitung sekali**.

### 5.4 Concurrency
- **FR-18** Master melayani banyak client dan banyak job aktif sekaligus tanpa data rusak.
- **FR-19** Pemilihan mekanisme (lock, antrian thread-safe, struktur data) **dijelaskan alasannya** di laporan.

### 5.5 Pengukuran dan verifikasi
- **FR-20** Tersedia skrip benchmark otomatis untuk skenario A (speedup) dan B (client bersamaan).
- **FR-21** Hasil dibandingkan dengan nilai referensi yang diketahui (lihat bagian 8) untuk membuktikan kebenaran.

## 6. Daftar RPC (Antarmuka Layanan)

Satu file `.proto`, paket `primegrid`, dua service.

**Service `MasterService`** (dipanggil client dan worker)

| RPC | Tipe | Fungsi | Idempoten? | Perlindungan |
|-----|------|--------|-----------|--------------|
| `SubmitJob` | Unary | Membuat job baru | **Tidak** | `request_id` + tabel dedup |
| `GetJobStatus` | Unary | Status dan progres job | Ya | — |
| `WatchJob` | **Server streaming** | Alirkan progres sampai job selesai | Ya (read-only) | Deadline total + deteksi client putus |
| `GetResult` | Unary | Hasil akhir job | Ya | — |
| `CancelJob` | Unary | Batalkan job | Ya | — |
| `ListWorkers` | Unary | Daftar worker dan status | Ya | — |
| `RegisterWorker` | Unary | Worker mendaftar | Ya (kunci `worker_id`) | Upsert |
| `Heartbeat` | Unary | Tanda hidup worker | Ya | — |

**Service `WorkerService`** (dipanggil master)

| RPC | Tipe | Fungsi | Idempoten? | Perlindungan |
|-----|------|--------|-----------|--------------|
| `ProcessChunk` | Unary | Hitung prima pada `[a, b)` | Ya (fungsi murni) | Master hanya menerima hasil pertama per `(job_id, chunk_id)` |
| `Ping` | Unary | Cek kesehatan worker | Ya | — |

Total **10 RPC**, 1 di antaranya streaming. Penambahan streaming kedua (misalnya client streaming untuk mengirim banyak job) bersifat opsional dan tidak diwajibkan.

## 7. Kebutuhan Non-Fungsional

| Kode | Kebutuhan | Target |
|------|-----------|--------|
| NFR-01 | Kebenaran | Hasil selalu sama dengan nilai referensi pada semua uji |
| NFR-02 | Kinerja | Speedup 3 worker > 1,8× pada beban 1–10.000.000 mode `trial` (target realistis, jelaskan bila tidak tercapai) |
| NFR-03 | Ketahanan | Job selesai benar walau 1 dari 3 worker dimatikan di tengah proses |
| NFR-04 | Portabilitas | Seluruh sistem berjalan dengan satu perintah compose dan juga manual dengan venv |
| NFR-05 | Keterbacaan | Kode memiliki README, struktur modul jelas, penamaan konsisten |
| NFR-06 | Observabilitas | Log terstruktur dengan `job_id`, `chunk_id`, `worker_id`, durasi |
| NFR-07 | Reproduksibilitas | Hasil benchmark bisa diulang dengan skrip dan konfigurasi yang tercatat |

## 8. Nilai Referensi Kebenaran

Gunakan untuk verifikasi otomatis (π(n) = jumlah prima ≤ n):

| n | π(n) |
|---|------|
| 10.000 | 1.229 |
| 100.000 | 9.592 |
| 1.000.000 | 78.498 |
| 10.000.000 | 664.579 |

Prima terbesar ≤ 10.000.000 adalah **9.999.991**.

## 9. Skenario Uji Kinerja

### Skenario A — Speedup (Ketentuan 5, studi kasus 2)
- Beban: rentang 1–10.000.000, mode `trial`, ukuran chunk tetap.
- Variasi: 1, 2, 3 (dan 4 bila ada) worker.
- Ukur: waktu total job (`T_N`). **Speedup = T₁ ÷ T_N**, efisiensi = speedup ÷ N.
- Ulangi minimal 3 kali per konfigurasi, laporkan rata-rata dan simpangan.
- Opsional: variasi ukuran chunk (misalnya 50k, 100k, 500k) untuk menunjukkan trade-off overhead RPC vs keseimbangan beban.

### Skenario B — Client bersamaan (Ketentuan 5)
- Variasi: **1, 5, dan 20 client** bersamaan, 3 worker aktif.
- Dua pengukuran:
  1. **Job penuh** dengan rentang lebih kecil (misalnya 1–1.000.000): latensi rata-rata submit→selesai dan throughput (job/detik).
  2. **RPC ringan** `GetJobStatus`: latensi rata-rata dan throughput (req/detik).
- Setiap hasil juga diverifikasi benar (tidak ada data rusak).

### Skenario C — Kegagalan (untuk demo dan rubrik toleransi kegagalan)
1. Matikan satu worker saat job berjalan → job tetap selesai benar.
2. Kirim `SubmitJob` dua kali dengan `request_id` sama → hanya satu job.
3. Suntik keterlambatan acak di worker → retry terjadi, hasil tidak terhitung ganda.

## 10. Kriteria Penerimaan

- [ ] `.proto` ≥ 4 RPC, ada streaming, penamaan konsisten
- [ ] Semua fitur wajib berjalan di ≥ 2 container/komputer berbeda
- [ ] Hasil 1–10.000.000 = 664.579 pada semua konfigurasi worker dan semua uji beban
- [ ] Tidak ada race condition, dibuktikan uji beban 20 client
- [ ] Timeout, retry, dan deduplikasi berjalan dan terdokumentasi di demo
- [ ] Tabel dan grafik kinerja lengkap dengan analisis penyebab
- [ ] Laporan PDF, README, repositori Git dengan commit semua anggota, video demo ≤ 5 menit
- [ ] Berkas dikumpulkan sebagai `Kelompok<no>_ProyekRPC.zip`

## 11. Di Luar Cakupan

- Autentikasi, TLS, dan otorisasi (koneksi *insecure* cukup untuk tugas ini; sebutkan sebagai keterbatasan).
- Penyimpanan persisten state master (in-memory cukup; sebutkan konsekuensinya dan usulan perbaikan di laporan).
- Master berreplika / failover master.
- Antarmuka web.

## 12. Risiko dan Mitigasi

| Risiko | Dampak | Mitigasi |
|--------|--------|----------|
| GIL Python membuat thread dalam satu proses worker tidak paralel | Speedup tidak terukur | Setiap worker = **proses/container terpisah**, satu chunk berjalan per worker pada satu waktu |
| Jumlah core mesin < jumlah worker | Speedup jenuh | Catat jumlah core, batasi CPU per container, jelaskan di analisis |
| Algoritma sieve terlalu cepat sehingga overhead RPC mendominasi | Speedup tampak buruk | Pakai mode `trial` untuk benchmark speedup; sieve dipakai sebagai verifikator |
| Beban tidak merata antar chunk (angka besar lebih mahal) | Worker menganggur | Pembagian dinamis berbasis antrian, chunk kecil |
| Worker mati meninggalkan chunk menggantung | Job macet | Heartbeat + deadline + reassign |
| Hasil ganda dari retry | Hitungan prima salah | Dedup per `(job_id, chunk_id)` pada agregasi |
| Perbedaan kontribusi antar anggota | Penilaian riwayat commit | Pembagian tugas tertulis, commit kecil dan sering |

## 13. Pembagian Tugas Anggota (usulan, sesuaikan)

| Anggota | Fokus | Output |
|---------|-------|--------|
| A | `.proto`, master (job, chunk queue, agregasi, dedup) | modul master, uji unit |
| B | Worker, algoritma, heartbeat, klien CLI | modul worker, client |
| C | Containerisasi, benchmark, grafik, laporan, video | compose, skrip benchmark, laporan |

Bila hanya 2 orang, gabungkan B dan C; bila 3 orang, semua anggota tetap wajib mengerjakan minimal satu modul kode agar riwayat commit merata.
