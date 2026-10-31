# RULES — Aturan Pengembangan primegrid

Aturan ini mengikat semua anggota kelompok dan alat bantu (termasuk asisten AI) yang dipakai membangun proyek. Jika ada konflik, urutan prioritas: **Ketentuan Umum tugas > PRD > Arsitektur > Rules ini**.

---

## 1. Aturan Mutlak (tidak boleh dilanggar)

1. Semua komunikasi antar komponen memakai **gRPC**. Dilarang memakai socket mentah, HTTP manual, atau XML-RPC untuk jalur utama.
2. Antarmuka hanya didefinisikan di **satu file `.proto`**. Dilarang menulis ulang atau mengedit manual kode hasil generate.
3. Master harus memanggil **minimal 3 worker secara paralel** (bukan berurutan).
4. Seluruh state bersama harus dilindungi mekanisme sinkronisasi. **Tidak boleh ada state yang dimodifikasi dari banyak thread tanpa perlindungan.**
5. Setiap panggilan RPC keluar wajib memiliki **deadline**. Dilarang memanggil tanpa timeout.
6. Operasi non-idempoten (`SubmitJob`) wajib memakai `request_id` yang **dibuat sekali** per niat pengguna dan **dipakai ulang di setiap retry**. Dilarang membuat ID baru per percobaan.
7. Hasil per chunk hanya boleh dihitung **sekali** (dedup `(job_id, chunk_id)`).
8. Tidak ada panggilan jaringan saat memegang lock.
9. Hasil komputasi harus selalu bisa diverifikasi dengan nilai referensi (PRD bagian 8). Fitur dianggap selesai hanya jika lolos verifikasi.
10. Dilarang menyalin kode dari kelompok lain. Referensi dan penggunaan alat bantu (termasuk AI) **wajib dicantumkan di laporan**.

## 2. Stack dan Dependensi

| Hal | Aturan |
|-----|--------|
| Bahasa | Python 3.10+ |
| Dependensi runtime | Hanya `grpcio` dan `grpcio-tools`; selebihnya pustaka standar |
| Dependensi pengembangan | `pytest`; `matplotlib` hanya untuk membuat grafik (di folder `bench/`) |
| Dependensi tak terikat | Semua versi dipatok di `requirements.txt` |
| Lingkungan | Virtual environment; dua laptop fisik untuk demo |
| Penambahan pustaka baru | Harus disepakati kelompok dan dicatat alasannya |

## 3. Aturan Desain `.proto`

- Paket `primegrid`; dua service: `MasterService` dan `WorkerService`.
- Nama service dan message: `PascalCase`; nama RPC: kata kerja + objek (`SubmitJob`); nama field: `snake_case`.
- Setiap RPC punya message request dan response sendiri (tidak berbagi message yang membuat kontrak ambigu).
- Setiap message request non-idempoten memiliki field `request_id`.
- Status job dan worker memakai **enum**, bukan string bebas.
- Nomor field **tidak boleh diubah atau dipakai ulang** setelah ditetapkan; field baru memakai nomor baru (menjaga backward compatibility Protobuf).
- Jangan menambah RPC atau field yang tidak dipakai (rubrik menilai kerapian: "tidak ada yang tidak perlu").
- Streaming dipakai hanya di tempat yang tepat: `WatchJob` (server streaming progres).
- Setiap RPC didokumentasikan lewat komentar singkat di `.proto` dan tabel di laporan.

## 4. Aturan Penulisan Kode

### 4.1 Umum
- Ikuti PEP 8; panjang baris maksimal 100 karakter.
- Type hints pada fungsi publik; docstring singkat berbahasa Indonesia atau Inggris (satu bahasa konsisten).
- Satu tanggung jawab per modul; tidak ada file raksasa yang mencampur master, worker, dan client.
- Tidak ada nilai magis di dalam kode: semua parameter (port, timeout, interval, ukuran chunk, batas retry) berada di modul konfigurasi dan dapat diubah lewat **environment variable**.
- Tidak ada `print` untuk log; gunakan modul logging.
- Tidak ada variabel global yang dapat diubah kecuali dibungkus kelas yang membawa lock-nya sendiri.

### 4.2 Penanganan error
- Server memetakan error ke status code gRPC yang tepat:
  | Situasi | Status code |
  |---------|-------------|
  | Input tidak valid (rentang terbalik, chunk_size ≤ 0, rentang > batas) | `INVALID_ARGUMENT` |
  | Job / worker tidak ditemukan | `NOT_FOUND` |
  | Tidak ada worker tersedia | `FAILED_PRECONDITION` atau `UNAVAILABLE` |
  | Job sudah dibatalkan / tidak dalam status yang sesuai | `FAILED_PRECONDITION` |
  | Bug tak terduga | `INTERNAL` (dengan log lengkap) |
- Client hanya me-retry `DEADLINE_EXCEEDED` dan `UNAVAILABLE`; status lain langsung dilaporkan.
- Tidak boleh ada `except` kosong yang menelan error.

### 4.3 Concurrency
- Gunakan lock per job untuk state job; lock terpisah untuk registri worker dan dedup.
- Bagian kritis sekecil mungkin; hindari I/O dan log berat di dalamnya.
- Urutan lock: registri → job. Tidak boleh dibalik.
- Antrian chunk memakai struktur thread-safe dari pustaka standar.
- Thread latar (heartbeat, health monitor, dispatcher) harus bisa dihentikan dengan rapi (event berhenti), bukan dibiarkan menggantung.
- Worker: satu chunk per waktu per proses. Paralelisme berasal dari **banyak proses worker**, bukan banyak thread di satu proses.

### 4.4 Algoritma
- Rentang chunk selalu setengah terbuka `[a, b)`.
- Mode `sieve` dan `trial` harus menghasilkan angka identik untuk rentang yang sama; ini diuji otomatis.
- Mode `trial` ditujukan untuk benchmark speedup; jangan "dioptimalkan" sampai terlalu cepat hingga tujuan pengukuran hilang (catat versi algoritma yang dipakai saat benchmark).

## 5. Aturan Logging dan Observabilitas

- Format log satu baris, memuat: waktu, level, komponen (`master` / `worker-<id>` / `client`), `job_id`, `chunk_id`, `worker_id`, dan durasi bila relevan.
- Peristiwa wajib dicatat: job dibuat / selesai / gagal, chunk dikirim / selesai / dikembalikan, retry (percobaan ke-berapa dan alasannya), worker mendaftar / DEAD / hidup kembali, permintaan duplikat terdeteksi.
- Log yang dipakai sebagai bukti demo harus mudah dibaca di tangkapan layar (tidak terlalu berisik).
- Level bawaan INFO; DEBUG dapat diaktifkan lewat environment.

## 6. Aturan Pengujian

| Jenis | Cakupan minimum |
|-------|-----------------|
| Unit | Pemecahan chunk (tanpa celah/tumpang tindih, kasus rentang kecil, kasus tidak habis dibagi); kedua algoritma terhadap nilai referensi kecil (10.000 → 1.229, 100.000 → 9.592); dedup agregasi; dedup `SubmitJob` |
| Integrasi | Master + ≥ 3 worker di satu mesin: hasil 1–1.000.000 = 78.498 |
| Konkurensi | 20 client mengirim job bersamaan; setiap hasil benar; tidak ada exception/deadlock |
| Kegagalan | Worker dimatikan saat job berjalan → hasil tetap benar; `SubmitJob` ganda ber-`request_id` sama → satu job; keterlambatan disuntik → tidak ada hitungan ganda |
| Regresi hasil akhir | 1–10.000.000 = 664.579 dan prima terbesar 9.999.991 |

Aturan tambahan:
- Uji harus deterministik; hindari `sleep` panjang tanpa alasan.
- Setiap bug yang ditemukan dibuatkan uji penjaganya.
- Klaim "tidak ada race condition" hanya boleh ditulis di laporan bila ada uji beban yang membuktikannya.

## 7. Aturan Benchmark

1. Benchmark dijalankan dengan konfigurasi tercatat: spesifikasi mesin (CPU, jumlah core, RAM), versi Python, jumlah worker, ukuran chunk, mode, batas CPU per container.
2. Setiap konfigurasi diulang **minimal 3 kali**; laporkan rata-rata dan simpangan baku. Satu kali jalan awal dibuang sebagai pemanasan.
3. Pengukuran waktu memakai jam monotonik resolusi tinggi, dari sisi client (submit sampai selesai) dan dari sisi master (kerja murni).
4. Hasil mentah disimpan sebagai CSV di `results/`; grafik dibuat dari CSV oleh skrip (bukan disunting manual).
5. Skenario A: speedup = T₁ ÷ T_N, plus efisiensi. Skenario B: 1, 5, 20 client; latensi rata-rata dan throughput. Skenario C: bukti kegagalan.
6. Jangan menjalankan aplikasi berat lain saat benchmark.
7. Analisis wajib menjelaskan **penyebab**: overhead RPC, ukuran chunk, beban tak merata, batas core fisik, hukum Amdahl, bagian serial di master.
8. Jangan menyembunyikan hasil buruk; jelaskan mengapa terjadi.

## 8. Aturan Git

| Hal | Aturan |
|-----|--------|
| Branch | `main` stabil; fitur di `feat/<nama>`, perbaikan di `fix/<nama>` |
| Merge | Lewat pull request / review antar anggota minimal 1 orang |
| Commit | Kecil dan sering; satu perubahan logis per commit; **setiap anggota harus punya commit atas namanya sendiri** |
| Pesan commit | Awalan jenis: `feat:`, `fix:`, `test:`, `docs:`, `chore:`, `bench:`; kalimat imperatif singkat |
| Identitas | Nama dan email Git terkonfigurasi benar di setiap perangkat anggota |
| Larangan | Tidak ada force-push ke `main`; tidak ada commit berkas besar, venv, atau rahasia |
| `.gitignore` | Venv, cache Python, berkas generate (bila dibuat ulang lewat skrip), hasil mentah besar |
| Tag | `v0.1` (kerangka), `v0.5` (fungsional), `v1.0` (final dikumpulkan) |

## 9. Aturan Dokumentasi

- `README.md` wajib memuat: deskripsi singkat, prasyarat, instalasi, cara generate stub, cara menjalankan master / worker / client (manual dan container), cara menjalankan uji dan benchmark, contoh keluaran, struktur proyek, anggota.
- Perintah di README harus **benar-benar dicoba** pada mesin bersih sebelum pengumpulan.
- Diagram arsitektur dibuat dan disertakan dalam `docs/` dan laporan.
- Setiap keputusan desain penting (mis. alasan pemilihan lock, pembagian dinamis) dicatat singkat dalam dokumen atau laporan.

## 10. Aturan Laporan dan Pengumpulan

Isi laporan proyek (urut sesuai ketentuan tugas):
1. Deskripsi studi kasus dan diagram arsitektur (komponen dan alur RPC)
2. Isi `.proto` dan penjelasan setiap RPC
3. Strategi concurrency dan toleransi kegagalan (termasuk **alasan** pemilihan mekanisme)
4. Hasil pengujian kinerja dan analisisnya (tabel + grafik + penyebab)
5. Pembagian tugas anggota
6. Tautan repositori Git dan video demo (maksimal 5 menit)

Tambahan yang disarankan: bagian keterbatasan dan perbaikan (state in-memory, SPOF, tanpa TLS) serta pernyataan penggunaan referensi/alat bantu.

Format berkas:
- `Kelompok<no>_ProyekRPC.zip` berisi laporan PDF dan kode sumber.
- Kode harus berjalan mengikuti README.
- Semua anggota menguasai seluruh sistem (presentasi dinilai per kelompok *dan* pemahaman anggota).

## 11. Aturan Demo

Urutan demo yang disarankan (≤ 5 menit untuk video):
1. Tunjukkan topologi: ≥ 2 laptop berjalan, `ListWorkers` menampilkan worker yang terdaftar.
2. Jalankan job 1–10.000.000, tunjukkan progres lewat `WatchJob`, hasil = 664.579.
3. Tunjukkan tabel speedup (1 vs 3 worker).
4. Matikan satu worker di tengah job; tunjukkan log reassign dan hasil tetap benar.
5. Kirim `SubmitJob` dua kali dengan `request_id` sama; tunjukkan hanya satu job.
6. Tunjukkan uji 20 client bersamaan tanpa hasil rusak.

## 12. Daftar Periksa Definition of Done (per fitur)

- [ ] Sesuai PRD dan arsitektur
- [ ] Ada deadline, penanganan error, dan log
- [ ] Ada uji (unit/integrasi) yang lolos
- [ ] Hasil terverifikasi dengan nilai referensi
- [ ] README/dokumen diperbarui
- [ ] Sudah di-review dan di-merge lewat pull request
