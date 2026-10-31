# Demo Guide: Dua Laptop Fisik

Panduan ini adalah versi singkat untuk demo 5 menit. Panduan pengujian lengkap
ada di `docs/PENGUJIAN.md`.

Topologi:

| Laptop | Peran | Contoh IP |
|---|---|---|
| A | Master | `192.168.1.10` |
| B | Worker | `192.168.1.20` |

Kedua laptop harus berada di Wi-Fi/LAN yang sama, bisa saling `ping`, dan
menggunakan profil jaringan Windows `Private`.

## 0. Persiapan

Di kedua laptop:

```powershell
cd C:\primegrid
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements-dev.txt
python scripts/generate_proto.py
python -m pip install -e .
ipconfig
```

Jalankan PowerShell sebagai Administrator di Laptop A:

```powershell
New-NetFirewallRule -DisplayName "primegrid-master-tcp-50050" `
    -Direction Inbound -Action Allow -Protocol TCP -LocalPort 50050 `
    -Profile Private,Domain -RemoteAddress LocalSubnet
```

Jalankan PowerShell sebagai Administrator di Laptop B:

```powershell
New-NetFirewallRule -DisplayName "primegrid-worker-tcp-50061" `
    -Direction Inbound -Action Allow -Protocol TCP -LocalPort 50061 `
    -Profile Private,Domain -RemoteAddress LocalSubnet
```

Verifikasi dari Laptop B:

```powershell
Test-NetConnection 192.168.1.10 -Port 50050
```

Verifikasi dari Laptop A:

```powershell
Test-NetConnection 192.168.1.20 -Port 50061
```

Keduanya harus `TcpTestSucceeded : True`.

## 1. Jalankan master di Laptop A

```powershell
python -m primegrid.master --host 0.0.0.0 --port 50050
```

## 2. Jalankan worker di Laptop B

```powershell
python -m primegrid.worker `
    --master 192.168.1.10:50050 `
    --host 0.0.0.0 `
    --port 50061 `
    --advertise-host 192.168.1.20 `
    --advertise-port 50061 `
    --worker-id laptop-b-1
```

## 3. Tunjukkan topologi dua laptop

Dari Laptop A:

```powershell
python -m primegrid.client --master 192.168.1.10:50050 workers
```

Hasil yang diharapkan:

```text
id=laptop-b-1 address=192.168.1.20:50061 ... status=ALIVE
```

## 4. Jalankan job referensi

```powershell
python -m primegrid.client --master 192.168.1.10:50050 submit `
    --start 1 --end 1000000 --chunk-size 100000 --mode sieve `
    --request-id demo-dua-laptop-001
```

Catat `job_id`, lalu:

```powershell
python -m primegrid.client --master 192.168.1.10:50050 watch --job-id <job-id>
python -m primegrid.client --master 192.168.1.10:50050 result --job-id <job-id>
```

Hasil yang diharapkan:

```text
prime_count=78498 max_prime=999983
```

Untuk referensi 10 juta:

```powershell
python -m primegrid.client --master 192.168.1.10:50050 submit `
    --start 1 --end 10000000 --chunk-size 100000 --mode sieve `
    --request-id demo-dua-laptop-10m
```

Hasil yang diharapkan: `prime_count=664579` dan `max_prime=9999991`.

## 5. Tunjukkan idempotensi

```powershell
python -m primegrid.client --master 192.168.1.10:50050 submit `
    --start 1 --end 100000 --mode sieve --request-id demo-dedup-001

python -m primegrid.client --master 192.168.1.10:50050 submit `
    --start 1 --end 100000 --mode sieve --request-id demo-dedup-001
```

Panggilan kedua harus menghasilkan `job_id` yang sama dan
`deduplicated=true`.

## 6. Tunjukkan failover worker

Jalankan worker kedua di Laptop B pada port `50062`:

```powershell
python -m primegrid.worker `
    --master 192.168.1.10:50050 `
    --host 0.0.0.0 `
    --port 50062 `
    --advertise-host 192.168.1.20 `
    --advertise-port 50062 `
    --worker-id laptop-b-2
```

Buka firewall port `50062` di Laptop B jika belum:

```powershell
New-NetFirewallRule -DisplayName "primegrid-worker-tcp-50062" `
    -Direction Inbound -Action Allow -Protocol TCP -LocalPort 50062 `
    -Profile Private,Domain -RemoteAddress LocalSubnet
```

Submit job `trial` 10 juta dari Laptop A, lalu hentikan worker pertama dengan
`Ctrl+C` saat job berjalan. Worker kedua harus menyelesaikan chunk yang
di-requeue dan job tetap `COMPLETED`.

## 7. Tunjukkan 20 client paralel

Jalankan benchmark bawaan dari Laptop A:

```powershell
python -m primegrid.client.benchmark --scenario B `
    --client-counts 1,5,20 --repeat 3 `
    --b-start 1 --b-end 1000000 --b-chunk-size 10000 `
    --status-requests 100 --output-dir results

Import-Csv results\scenario_b_jobs.csv |
    Format-Table clients, jobs, wall_s, throughput_jobs_s, correct_jobs -AutoSize
```

Untuk uji 20 client langsung ke master dua laptop, lihat `docs/PENGUJIAN.md`
bagian 9.

## 8. Cleanup

Hentikan master dan worker dengan `Ctrl+C`. Hapus aturan firewall jika demo
selesai:

```powershell
Remove-NetFirewallRule -DisplayName "primegrid-*"
```
