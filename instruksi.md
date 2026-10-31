"Batal dulu pembuatan Docker Compose. Saya ingin mengubah strategi deployment sesuai Ketentuan Umum #4 ('Demo dijalankan pada minimal 2 komputer').
Saya akan melakukan demo menggunakan 2 Laptop Fisik (Laptop A sebagai Master, Laptop B sebagai Worker), bukan container.
Tolong bantu saya menyesuaikan kode dan instruksi untuk skenario 2 Laptop Fisik ini:
Konfigurasi IP Dinamis: Pastikan master_server.py bind ke 0.0.0.0 dan worker_client.py menerima argumen --master-ip dari CLI/Env Var. Jangan hardcode localhost.
Instruksi Firewall: Berikan perintah PowerShell/CMD yang tepat untuk membuka port gRPC (50050) di Windows agar Laptop B bisa mengakses Laptop A.
Panduan Demo: Buat langkah-langkah singkat bagaimana cara menjalankan Master di Laptop A dan Worker di Laptop B menggunakan venv biasa (tanpa Docker).
Update README: Sesuaikan bagian 'Cara Menjalankan' di README agar mencakup mode 'Manual Multi-Machine' ini.
Abaikan sementara bagian Docker/Podman. Fokus agar sistem berjalan stabil via WiFi/LAN antar dua laptop."