# Panduan Pengujian Server dan Client

Panduan ini mencakup pengujian otomatis, pengujian manual pada satu laptop,
dan pengujian dua laptop fisik melalui Wi-Fi/LAN. Contoh IP yang dipakai:

| Perangkat | Peran | Contoh IP |
|---|---|---|
| Laptop A | Master | `192.168.1.10` |
| Laptop B | Worker | `192.168.1.20` |

Ganti IP tersebut dengan hasil `ipconfig` di masing-masing laptop.

> Catatan CLI: panduan ini memakai bentuk yang sudah berjalan sekarang,
> yaitu `--master <ip>:<port>`. Setelah refactor `--master-ip`,
> bentuk `--master-ip <ip> --master-port <port>` juga tersedia dan opsi
> `--master` tetap kompatibel.

## 1. Pengujian otomatis yang sudah tersedia

Jalankan dari root repo:

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

Hasil terakhir: `54 passed, 1 deselected`.

Untuk regresi full-range 1 sampai 10.000.000 dengan algoritma `trial`
(memakan waktu sekitar 95 detik):

```powershell
.\.venv\Scripts\python.exe -m pytest -q -m slow
```

Hasil terakhir: `1 passed, 54 deselected`.

Cakupan tes utama:

| File | Yang diuji |
|---|---|
| `tests/test_integration.py` | 3 worker, 20 client paralel, dedup, kehilangan worker |
| `tests/test_client_api.py` | client submit, retry request ID, streaming, result |
| `tests/test_rpc_validation.py` | status code gRPC, validasi input, cancel idempoten |
| `tests/test_job_manager.py` | chunking, exactly-once aggregation, requeue, cancel |
| `tests/test_worker_registry.py` | registrasi, heartbeat, worker mati/hidup lagi |
| `tests/test_algorithms.py` | sieve dan trial terhadap nilai referensi |
| `tests/test_reference_regression.py` | full-range sieve dan trial 10 juta |
| `tests/test_config.py` | konfigurasi env dan validasi |
| `tests/test_retry.py` | retry hanya untuk error sementara |
| `tests/test_benchmark_helpers.py` | helper benchmark, CSV, dan environment |

Jalankan pemeriksaan format dan lint jika `ruff` tersedia:

```powershell
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m ruff format --check .
```

## 2. Prasyarat dua laptop

Kedua laptop harus:

- berada di jaringan yang sama, misalnya Wi-Fi rumah/kantor atau hotspot;
- bisa saling `ping`;
- menggunakan profil jaringan Windows `Private`;
- tidak terkena AP/client isolation pada Wi-Fi;
- memiliki Python 3.10 atau lebih baru;
- memiliki repo dan dependency yang sama.

Cek IP di masing-masing laptop:

```powershell
ipconfig
```

Atau lebih ringkas:

```powershell
Get-NetIPAddress -AddressFamily IPv4 |
    Where-Object { $_.IPAddress -notlike "127.*" } |
    Format-Table InterfaceAlias, IPAddress, PrefixLength -AutoSize
```

Cek koneksi dasar dari B ke A:

```powershell
ping 192.168.1.10
```

Jika `ping` gagal, jangan lanjut ke langkah gRPC. Periksa Wi-Fi, subnet,
hotspot, dan AP isolation terlebih dahulu.

Cek profil jaringan:

```powershell
Get-NetConnectionProfile
```

Jika profil `Public`, ubah ke `Private` di PowerShell sebagai Administrator:

```powershell
Set-NetConnectionProfile -InterfaceAlias "Wi-Fi" -NetworkCategory Private
```

Sesuaikan `InterfaceAlias` dengan nama koneksi yang muncul di
`Get-NetConnectionProfile`.

## 3. Firewall Windows

Jalankan PowerShell sebagai Administrator di **Laptop A**:

```powershell
New-NetFirewallRule `
    -DisplayName "primegrid-master-tcp-50050" `
    -Direction Inbound `
    -Action Allow `
    -Protocol TCP `
    -LocalPort 50050 `
    -Profile Private,Domain `
    -RemoteAddress LocalSubnet
```

Jalankan PowerShell sebagai Administrator di **Laptop B**:

```powershell
New-NetFirewallRule `
    -DisplayName "primegrid-worker-tcp-50061" `
    -Direction Inbound `
    -Action Allow `
    -Protocol TCP `
    -LocalPort 50061 `
    -Profile Private,Domain `
    -RemoteAddress LocalSubnet
```

Jika Laptop B menjalankan worker kedua pada port `50062`, buka juga:

```powershell
New-NetFirewallRule `
    -DisplayName "primegrid-worker-tcp-50062" `
    -Direction Inbound `
    -Action Allow `
    -Protocol TCP `
    -LocalPort 50062 `
    -Profile Private,Domain `
    -RemoteAddress LocalSubnet
```

Alternatif CMD, dijalankan sebagai Administrator:

```cmd
netsh advfirewall firewall add rule name="primegrid-master-tcp-50050" dir=in action=allow protocol=TCP localport=50050 profile=private,domain remoteip=localsubnet
netsh advfirewall firewall add rule name="primegrid-worker-tcp-50061" dir=in action=allow protocol=TCP localport=50061 profile=private,domain remoteip=localsubnet
```

Verifikasi aturan:

```powershell
Get-NetFirewallRule -DisplayName "primegrid-*" |
    Format-Table DisplayName, Enabled, Direction, Action -AutoSize
```

Verifikasi port dari **Laptop B ke Laptop A**:

```powershell
Test-NetConnection 192.168.1.10 -Port 50050
```

Verifikasi port dari **Laptop A ke Laptop B**:

```powershell
Test-NetConnection 192.168.1.20 -Port 50061
```

Keduanya harus menghasilkan `TcpTestSucceeded : True`. Jika `False`, periksa
urutan: IP benar, master/worker sudah berjalan, profil Private, firewall rule,
dan AP isolation.

Hapus aturan jika sudah selesai:

```powershell
Remove-NetFirewallRule -DisplayName "primegrid-*"
```

## 4. Instalasi di kedua laptop

Salin/clone repo ke kedua laptop, lalu jalankan di masing-masing laptop:

```powershell
cd C:\primegrid
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements-dev.txt
python scripts/generate_proto.py
python -m pip install -e .
```

Verifikasi entry point:

```powershell
python -m primegrid.master --help
python -m primegrid.worker --help
python -m primegrid.client --help
```

## 5. Uji lokal di satu laptop

Tes ini memastikan instalasi dan proto valid sebelum mencoba dua laptop.
Gunakan tiga terminal di Laptop A.

Terminal 1, master:

```powershell
.\.venv\Scripts\Activate.ps1
python -m primegrid.master --host 127.0.0.1 --port 50050
```

Terminal 2, worker:

```powershell
.\.venv\Scripts\Activate.ps1
python -m primegrid.worker `
    --master 127.0.0.1:50050 `
    --host 127.0.0.1 `
    --port 50061 `
    --advertise-host 127.0.0.1 `
    --advertise-port 50061 `
    --worker-id local-worker-1
```

Terminal 3, client:

```powershell
.\.venv\Scripts\Activate.ps1

python -m primegrid.client --master 127.0.0.1:50050 workers

python -m primegrid.client --master 127.0.0.1:50050 submit `
    --start 1 --end 100000 --chunk-size 10000 --mode sieve `
    --request-id local-test-001
```

Catat `job_id`, lalu:

```powershell
python -m primegrid.client --master 127.0.0.1:50050 status --job-id <job_id>
python -m primegrid.client --master 127.0.0.1:50050 watch --job-id <job_id>
python -m primegrid.client --master 127.0.0.1:50050 result --job-id <job_id>
```

Hasil yang diharapkan:

```text
prime_count=9592 max_prime=99991
```

## 6. Uji dua laptop: Master di A, Worker di B

### Laptop A, terminal master

```powershell
cd C:\primegrid
.\.venv\Scripts\Activate.ps1
python -m primegrid.master --host 0.0.0.0 --port 50050
```

`0.0.0.0` berarti master menerima koneksi dari semua interface, termasuk
Wi-Fi/LAN. Jangan mengganti dengan `localhost` untuk mode dua laptop.

### Laptop B, terminal worker

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

`--advertise-host` harus berisi IP Laptop B yang dapat dijangkau Laptop A.
Jangan memakai `0.0.0.0` atau `localhost` untuk nilai advertise.

### Laptop A, terminal client

```powershell
cd C:\primegrid
.\.venv\Scripts\Activate.ps1

python -m primegrid.client --master 192.168.1.10:50050 workers
```

Hasil yang diharapkan:

```text
id=laptop-b-1 address=192.168.1.20:50061 ... status=ALIVE
```

Jika daftar worker kosong, lihat log Laptop B. Worker akan mencoba registrasi
ulang jika master belum siap atau koneksi sempat putus.

### Submit job lintas laptop

Dari Laptop A atau B:

```powershell
python -m primegrid.client --master 192.168.1.10:50050 submit `
    --start 1 --end 1000000 --chunk-size 100000 --mode sieve `
    --request-id dua-laptop-001
```

Catat `job_id`, lalu jalankan:

```powershell
python -m primegrid.client --master 192.168.1.10:50050 watch --job-id <job_id>
python -m primegrid.client --master 192.168.1.10:50050 result --job-id <job_id>
```

Hasil yang diharapkan untuk rentang 1.000.000:

```text
prime_count=78498 max_prime=999983
```

Contoh referensi lengkap:

| Rentang | prime_count | max_prime |
|---|---:|---:|
| 1 sampai 100.000 | 9592 | 99991 |
| 1 sampai 1.000.000 | 78498 | 999983 |
| 1 sampai 10.000.000 | 664579 | 9999991 |

## 7. Uji idempotensi SubmitJob

Kirim dua kali dengan `request_id` yang sama:

```powershell
python -m primegrid.client --master 192.168.1.10:50050 submit `
    --start 1 --end 100000 --mode sieve --request-id dedup-demo-001

python -m primegrid.client --master 192.168.1.10:50050 submit `
    --start 1 --end 100000 --mode sieve --request-id dedup-demo-001
```

Hasil yang diharapkan:

- `job_id` kedua sama dengan yang pertama;
- output kedua berisi `deduplicated=true`;
- tidak ada job baru atau perhitungan ganda.

## 8. Uji failover dengan dua worker di Laptop B

Untuk menunjukkan pemulihan saat worker mati, jalankan worker kedua di
Laptop B pada port `50062`.

Terminal worker 1:

```powershell
python -m primegrid.worker `
    --master 192.168.1.10:50050 `
    --host 0.0.0.0 `
    --port 50061 `
    --advertise-host 192.168.1.20 `
    --advertise-port 50061 `
    --worker-id laptop-b-1
```

Terminal worker 2:

```powershell
python -m primegrid.worker `
    --master 192.168.1.10:50050 `
    --host 0.0.0.0 `
    --port 50062 `
    --advertise-host 192.168.1.20 `
    --advertise-port 50062 `
    --worker-id laptop-b-2
```

Dari Laptop A, submit job yang cukup lama:

```powershell
python -m primegrid.client --master 192.168.1.10:50050 submit `
    --start 1 --end 10000000 --chunk-size 100000 --mode trial `
    --request-id failover-demo-001
```

Watch job:

```powershell
python -m primegrid.client --master 192.168.1.10:50050 watch --job-id <job_id>
```

Saat job berjalan, hentikan worker 1 dengan `Ctrl+C` di terminalnya. Worker 2
harus mengambil chunk yang di-requeue dan job tetap selesai:

```text
prime_count=664579 max_prime=9999991
```

Jika hanya ada satu worker, mematikan worker tersebut akan membuat job
kehabisan worker. Ini normal; gunakan dua worker untuk demonstrasi failover.

## 9. Uji 20 client paralel

### Opsi A: benchmark bawaan (satu mesin)

Benchmark ini menjalankan master dan worker lokal sebagai proses terpisah:

```powershell
python -m primegrid.client.benchmark --scenario B `
    --client-counts 1,5,20 --repeat 3 `
    --b-start 1 --b-end 1000000 --b-chunk-size 10000 `
    --status-requests 100 --output-dir results
```

Cek:

```powershell
Import-Csv results\scenario_b_jobs.csv |
    Format-Table clients, jobs, wall_s, throughput_jobs_s, correct_jobs -AutoSize
```

### Opsi B: 20 client dari Laptop A ke master Laptop A

Jalankan di PowerShell 7, dengan worker di Laptop B sudah aktif:

```powershell
$python = "C:\primegrid\.venv\Scripts\python.exe"
$master = "192.168.1.10:50050"

$output = 1..20 | ForEach-Object -Parallel {
    $id = "remote-load-$_"
    & $using:python -m primegrid.client --master $using:master submit `
        --start 1 --end 100000 --chunk-size 10000 --mode sieve `
        --request-id $id
} -ThrottleLimit 20

$output
$jobIds = $output | ForEach-Object {
    if ($_ -match "job_id=(\S+)") { $Matches[1] }
}
$jobIds
```

Tunggu setiap job selesai dan cek hasilnya:

```powershell
foreach ($id in $jobIds) {
    do {
        $line = & $python -m primegrid.client --master $master status --job-id $id
        Start-Sleep -Milliseconds 200
    } until ($line -match "status=(COMPLETED|FAILED|CANCELLED)")

    & $python -m primegrid.client --master $master result --job-id $id |
        Select-String "prime_count=9592"
}
```

Hasil yang diharapkan: 20 baris `prime_count=9592`, tanpa error.

## 10. Benchmark speedup penuh

Benchmark ini menjalankan 1, 2, dan 3 worker lokal pada satu laptop. Gunakan
ini untuk laporan speedup dan throughput, bukan untuk membuktikan koneksi
dua laptop:

```powershell
python -m primegrid.client.benchmark --scenario all `
    --worker-counts 1,2,3 --client-counts 1,5,20 `
    --repeat 3 --warmup 1 `
    --start 1 --end 10000000 --chunk-size 100000 --mode trial `
    --b-start 1 --b-end 1000000 --b-chunk-size 10000 `
    --status-requests 100 --output-dir results

python bench\plot_results.py --input results
```

File hasil:

- `results\scenario_a.csv`
- `results\scenario_b_jobs.csv`
- `results\scenario_b_status.csv`
- `results\scenario_c.csv`
- `results\scenario_a.png`
- `results\scenario_b.png`

## 11. Troubleshooting

| Gejala | Penyebab umum | Tindakan |
|---|---|---|
| `failed to connect to all addresses` | Master belum jalan, IP salah, firewall, beda subnet | Cek `Test-NetConnection <IP_A> -Port 50050`, pastikan master bind `0.0.0.0` |
| `workers` kosong | Worker tidak bisa menjangkau master | Lihat log worker, cek firewall A, cek IP master |
| Worker `ALIVE` tapi job macet | Master tidak bisa menjangkau worker | `Test-NetConnection <IP_B> -Port 50061`, pastikan `--advertise-host` berisi IP B |
| `address already in use` | Port dipakai proses lain | Hentikan proses lama atau ganti `--port` |
| `cannot assign requested address` | IP bind bukan milik laptop | Gunakan `0.0.0.0` untuk bind, IP LAN hanya untuk advertise |
| Koneksi hanya satu arah | AP isolation atau profil Public | Matikan AP isolation, ubah profil ke Private, buka kedua port |
| Job `FAILED` setelah worker mati | Tidak ada worker lain | Jalankan minimal dua worker untuk uji failover |
| Job `trial` 10 juta lambat | Trial division near-10M mahal | Gunakan `sieve`, atau uji `trial` dengan rentang lebih kecil |
| `ping` gagal | Beda jaringan/subnet | Samakan Wi-Fi, cek IP dan gateway |

Perintah diagnosa cepat:

```powershell
Get-NetConnectionProfile
Get-NetIPAddress -AddressFamily IPv4
Test-NetConnection 192.168.1.10 -Port 50050
Test-NetConnection 192.168.1.20 -Port 50061
Get-NetFirewallRule -DisplayName "primegrid-*"
```

## 12. Checklist demo dua laptop

- [ ] Laptop A dan B berada di jaringan yang sama.
- [ ] `ping` A ke B dan B ke A berhasil.
- [ ] Profil jaringan `Private`.
- [ ] Firewall TCP `50050` di A terbuka untuk `LocalSubnet`.
- [ ] Firewall TCP `50061` di B terbuka untuk `LocalSubnet`.
- [ ] `Test-NetConnection` A ke B port `50061` berhasil.
- [ ] `Test-NetConnection` B ke A port `50050` berhasil.
- [ ] Master A berjalan dengan bind `0.0.0.0:50050`.
- [ ] Worker B berjalan dengan `--advertise-host` berisi IP B.
- [ ] `workers` menampilkan worker B sebagai `ALIVE`.
- [ ] Job 100.000 selesai dengan `prime_count=9592`.
- [ ] Job 1.000.000 selesai dengan `prime_count=78498`.
- [ ] Submit dengan `request_id` yang sama menghasilkan `deduplicated=true`.
- [ ] Failover dua worker berhasil.
- [ ] 20 client paralel selesai tanpa error.
