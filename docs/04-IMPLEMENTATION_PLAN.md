# IMPLEMENTATION PLAN — primegrid (1 Minggu)

Rencana kerja bertahap untuk kelompok 2–3 orang. Setiap fase memiliki tujuan, tugas, keluaran, dan **kriteria selesai** yang bisa diperiksa. Jangan lanjut ke fase berikutnya sebelum kriteria selesai fase sekarang terpenuhi.

Rujukan: `01-PRD.md`, `02-ARCHITECTURE.md`, `03-RULES.md`.

---

## Ringkasan Jadwal

| Hari | Fase | Fokus | Penanggung jawab utama |
|------|------|-------|------------------------|
| 1 | 0–1 | Setup repositori, kontrak `.proto`, kerangka | Semua |
| 2 | 2 | Algoritma + worker + master inti (alur dasar) | A (master), B (worker) |
| 3 | 3 | Job lengkap: streaming, status, registrasi, heartbeat | A, B |
| 4 | 4 | Concurrency, dedup, timeout/retry, reassign | A, B |
| 5 | 5 | Containerisasi + uji integrasi + uji konkurensi | C (+ semua) |
| 6 | 6 | Benchmark A, B, C + grafik | C (+ semua) |
| 7 | 7 | Laporan, README, video demo, pengumpulan | Semua |

Sisakan **buffer setengah hari** di hari 6–7; benchmark dan laporan hampir selalu memakan waktu lebih lama dari perkiraan.

---

## Fase 0 — Persiapan (Hari 1, pagi)

**Tujuan:** semua anggota punya lingkungan dan repositori yang sama.

Tugas:
1. Buat repositori Git; tambahkan semua anggota; atur identitas Git masing-masing.
2. Buat struktur direktori sesuai arsitektur (bagian 11), `.gitignore`, `requirements.txt` dengan versi dipatok.
3. Salin dokumen PRD, arsitektur, rules, dan plan ke `docs/`.
4. Setiap anggota menyiapkan venv dan memastikan `grpcio` dan `grpcio-tools` terpasang (Python ≥ 3.10).
5. Sepakati pembagian tugas dan simpan di `docs/`.
6. Tulis skrip generate stub dari `.proto`.

**Kriteria selesai:**
- [ ] Semua anggota berhasil clone, membuat venv, menjalankan skrip generate
- [ ] Tiap anggota sudah membuat minimal satu commit

---

## Fase 1 — Kontrak Antarmuka (Hari 1, siang–sore)

**Tujuan:** `.proto` final sebelum kode ditulis, karena mengubahnya belakangan merusak semua modul.

Tugas:
1. Rancang message dan enum: status job, status worker, status chunk.
2. Definisikan 10 RPC sesuai PRD bagian 6 (8 di `MasterService`, 2 di `WorkerService`), dengan `WatchJob` sebagai server streaming.
3. Pastikan `SubmitJob` memuat `request_id`; `ProcessChunk` memuat `job_id`, `chunk_id`, rentang, mode.
4. Tinjau bersama menurut aturan `.proto` di `03-RULES.md` bagian 3.
5. Generate stub; bekukan kontrak (tag `v0.1`).

**Kriteria selesai:**
- [ ] `.proto` lolos tinjauan semua anggota
- [ ] Stub berhasil dihasilkan tanpa galat
- [ ] Tidak ada RPC atau field yang tidak punya tujuan jelas

---

## Fase 2 — Inti Komputasi dan Alur Dasar (Hari 2)

**Tujuan:** satu job sederhana berjalan end-to-end dengan 1 master dan 1 worker.

Tugas — Compute engine (Anggota B):
1. Implementasi mode `sieve` (segmented, rentang setengah terbuka).
2. Implementasi mode `trial` (pembagian percobaan hingga akar).
3. Uji kedua mode terhadap nilai referensi PRD bagian 8 (10.000, 100.000, 1.000.000).
4. Ukur waktu kasar `trial` pada 1–10.000.000 di mesin kalian; catat untuk menentukan ukuran chunk dan deadline.

Tugas — Worker (B):
5. Servicer `ProcessChunk` dan `Ping` dengan validasi input.
6. Konfigurasi via environment (port, id, alamat master).

Tugas — Master (A):
7. Pemecah chunk (uji unit: tanpa celah, tanpa tumpang tindih, sisa chunk terakhir).
8. `SubmitJob`, `GetJobStatus`, `GetResult` versi dasar.
9. Worker terdaftar lewat konfigurasi statis dulu (registrasi dinamis di fase 3).
10. Dispatcher sederhana untuk 1 worker dan agregasi hasil.

**Kriteria selesai:**
- [ ] Job 1–1.000.000 menghasilkan 78.498 via client CLI sederhana
- [ ] Kedua algoritma identik pada rentang uji
- [ ] Ukuran chunk dan deadline awal sudah ditetapkan dari data nyata

---

## Fase 3 — Fitur Lengkap Master–Worker (Hari 3)

**Tujuan:** semua RPC fungsional dan 3+ worker bekerja paralel.

Tugas:
1. `RegisterWorker` dan `Heartbeat`; Worker Registry; `ListWorkers`.
2. Worker: registrar dengan retry, thread heartbeat.
3. Dispatcher satu thread per worker, mengambil chunk dari antrian bersama (pembagian dinamis).
4. `WatchJob` (server streaming) memakai mekanisme condition/event; berhenti saat job selesai atau client putus.
5. `CancelJob`.
6. Statistik per worker (jumlah chunk, waktu hitung).
7. Client CLI: `submit`, `status`, `watch`, `result`, `cancel`, `workers`.
8. Jalankan 3 worker (3 proses) di satu mesin.

**Kriteria selesai:**
- [ ] 3 worker terdaftar dan tampil di `ListWorkers`
- [ ] Job 1–10.000.000 mode `trial` = 664.579, prima terbesar 9.999.991
- [ ] `WatchJob` menampilkan progres bertahap
- [ ] Log menunjukkan chunk dibagi ke ketiga worker

---

## Fase 4 — Concurrency, Timeout, Retry, Kegagalan (Hari 4)

**Tujuan:** memenuhi ketentuan 2 dan 3 dan rubrik concurrency/toleransi kegagalan.

Tugas — Concurrency (A):
1. Terapkan lock sesuai arsitektur bagian 6: registri, per-job, registri worker, dedup.
2. Aggregator dengan `done_set` sehingga chunk hanya dihitung sekali.
3. Tinjau seluruh kode: tidak ada RPC keluar saat memegang lock; urutan lock konsisten.

Tugas — Toleransi kegagalan (A + B):
4. Deadline pada semua panggilan (client → master, master → worker, worker → master).
5. Util retry dengan exponential backoff dan jitter; hanya untuk `DEADLINE_EXCEEDED` / `UNAVAILABLE`.
6. `SubmitJob` dengan `request_id`: client membuatnya sekali, dipakai ulang tiap retry; master dedup dengan TTL.
7. Health Monitor: tandai worker DEAD; kembalikan chunk `ASSIGNED` ke antrian; hitung `attempts`; job `FAILED` bila melebihi batas.
8. Fault injector di worker (keterlambatan/kegagalan acak, aktif lewat konfigurasi) untuk demo.
9. Penanganan client putus pada `WatchJob`.

**Kriteria selesai:**
- [ ] Mematikan 1 worker di tengah job → job selesai, hasil 664.579
- [ ] `SubmitJob` dua kali dengan `request_id` sama → satu job
- [ ] Keterlambatan disuntik → retry terlihat di log, tidak ada hitungan ganda
- [ ] 20 client mengirim job bersamaan → semua hasil benar, tanpa exception

---

## Fase 5 — Containerisasi dan Pengujian (Hari 5)

**Tujuan:** memenuhi ketentuan 4 (demo di ≥ 2 container/komputer) dan memiliki rangkaian uji otomatis.

Tugas — Container (C):
1. Satu image untuk master/worker/client (entry point dibedakan lewat perintah) dengan versi Python dipatok.
2. Berkas compose: 1 master, 3 worker (batas CPU masing-masing 1), 1 client/benchmark, jaringan bersama.
3. Pastikan worker menemukan master lewat nama layanan dan master menemukan worker lewat alamat yang dilaporkan.
4. Uji topologi alternatif 2 komputer fisik (opsional, menambah nilai demo).

Tugas — Pengujian (semua):
5. Lengkapi uji unit, integrasi, konkurensi, kegagalan sesuai `03-RULES.md` bagian 6.
6. Jalankan seluruh uji dari mesin bersih (clone baru) untuk memastikan README benar.

**Kriteria selesai:**
- [ ] Satu perintah compose menjalankan seluruh sistem
- [ ] Job lintas container menghasilkan 664.579
- [ ] Seluruh uji otomatis lulus
- [ ] README terverifikasi di mesin bersih

---

## Fase 6 — Benchmark dan Analisis (Hari 6)

**Tujuan:** menghasilkan data untuk rubrik "Analisis kinerja" dan "Concurrency & konsistensi".

Tugas:
1. Catat spesifikasi mesin dan konfigurasi (jumlah core, RAM, versi Python, batas CPU container, ukuran chunk, mode).
2. **Skenario A (speedup):** 1, 2, 3 (+4 bila memungkinkan) worker; rentang 1–10.000.000, mode `trial`; 1 kali pemanasan + minimal 3 ulangan; hitung rata-rata, simpangan baku, speedup, efisiensi.
3. **Skenario B (client bersamaan):** 1, 5, 20 client; (i) job penuh rentang kecil: latensi rata-rata dan throughput; (ii) `GetJobStatus`: latensi dan throughput. Verifikasi kebenaran tiap hasil.
4. **Skenario C (kegagalan):** rekam bukti (log/tangkapan layar) untuk tiga kasus PRD.
5. Opsional: variasi ukuran chunk (mis. 50k / 100k / 500k) untuk menunjukkan trade-off overhead vs keseimbangan beban.
6. Simpan CSV mentah; buat grafik dengan skrip (speedup vs jumlah worker beserta garis ideal, throughput vs jumlah client, latensi vs jumlah client).
7. Tulis analisis penyebab: overhead RPC dan serialisasi, bagian serial di master (Amdahl), beban chunk tak merata, jumlah core fisik, kontensi lock, thread pool master.

**Kriteria selesai:**
- [ ] Tabel dan grafik skenario A dan B lengkap, dari CSV
- [ ] Setiap angka disertai penjelasan penyebab
- [ ] Bukti skenario C tersimpan
- [ ] Spesifikasi mesin dan konfigurasi tercatat

---

## Fase 7 — Laporan, Demo, Pengumpulan (Hari 7)

**Tujuan:** berkas lengkap dan tepat format.

Tugas:
1. **Laporan PDF** mengikuti struktur di `03-RULES.md` bagian 10 (deskripsi + diagram arsitektur, `.proto` dan penjelasan RPC, strategi concurrency dan kegagalan beserta alasan, hasil dan analisis, pembagian tugas, tautan repositori dan video). Tambahkan bagian keterbatasan dan pernyataan alat bantu/referensi.
2. **Video demo ≤ 5 menit** sesuai urutan di `03-RULES.md` bagian 11; latih sekali sebelum merekam.
3. Finalisasi README; tag `v1.0`.
4. Periksa `git shortlog -sn` — semua anggota punya kontribusi.
5. Kemas `Kelompok<no>_ProyekRPC.zip` (laporan PDF + kode sumber).
6. Latihan presentasi: **setiap anggota** harus bisa menjelaskan seluruh sistem, bukan hanya bagiannya.

**Kriteria selesai:**
- [ ] Semua butir "Kriteria Penerimaan" di PRD bagian 10 tercentang
- [ ] Nama berkas dan isi ZIP sesuai ketentuan
- [ ] Dikumpulkan sebelum sesi presentasi

---

## Pemetaan Rubrik → Fase

| Aspek rubrik (bobot) | Dipenuhi di fase | Bukti utama |
|----------------------|------------------|-------------|
| Desain antarmuka `.proto` (15%) | 1 | `.proto` rapi, 10 RPC, 1 streaming, tinjauan |
| Fungsionalitas (25%) | 2, 3, 5 | Demo 3 worker lintas container, hasil benar |
| Concurrency & konsistensi (20%) | 4, 5, 6 | Uji 20 client, hasil selalu 664.579 |
| Toleransi kegagalan (15%) | 4, 6 | Demo worker mati, retry, dedup |
| Analisis kinerja (15%) | 6 | Tabel, grafik, penjelasan penyebab |
| Laporan & presentasi (10%) | 7 | Laporan, diagram, semua anggota menguasai |

## Daftar Risiko Jadwal

| Risiko | Antisipasi |
|--------|------------|
| Fase 4 memakan waktu lebih lama (debug concurrency) | Mulai uji konkurensi sejak fase 3; sisakan buffer |
| Mode `trial` terlalu lama/terlalu cepat di mesin kalian | Kalibrasi di fase 2; sesuaikan ukuran rentang uji berulang, tetapi tetap tunjukkan hasil penuh 1–10.000.000 |
| Mesin punya core lebih sedikit dari jumlah worker | Catat, jelaskan di analisis; gunakan 2 komputer bila memungkinkan |
| Kontribusi commit tidak merata | Pembagian tugas tertulis di fase 0; tinjau `git shortlog` tiap hari |
| README tidak berjalan di mesin lain | Uji di clone bersih pada fase 5 dan sebelum mengumpulkan |

## Catatan Penggunaan Dokumen Ini dengan Asisten AI

Bila memakai asisten AI untuk menghasilkan kode, berikan **keempat dokumen** sebagai konteks, kerjakan **per fase** (bukan sekaligus), dan minta asisten memverifikasi kriteria selesai tiap fase. Ingat: penggunaan alat bantu **wajib dicantumkan di laporan**, dan setiap anggota harus memahami kode yang dikumpulkan.
