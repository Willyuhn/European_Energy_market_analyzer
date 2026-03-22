# Hostinger VPS Deploy (Docker + Nginx + HTTPS)

This project is a Python/FastAPI app (`app.py`). Hostinger “Node.js Websites” cannot deploy it, but a Hostinger **VPS** can.

## Prerequisites

- A Hostinger VPS (Ubuntu/Debian)
- A domain pointing to your VPS public IP (A/AAAA record)
- MySQL database reachable from the VPS (Hostinger DB, managed DB, or another VPS)

## 1) Connect to the VPS

```bash
ssh root@YOUR_VPS_IP
```

## 2) Install Docker + Compose plugin

```bash
apt-get update
apt-get install -y ca-certificates curl gnupg
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo $VERSION_CODENAME) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

## 3) Clone your repo

```bash
mkdir -p /opt/enerlyzer
cd /opt/enerlyzer
git clone https://github.com/Willyuhn/European_Energy_market_analyzer.git .
```

## 4) Configure environment variables

Create `.env` next to `docker-compose.yml`:

```bash
cp .env.example .env
nano .env
```

You must set at least:
- `DB_HOST`
- `DB_USER`
- `DB_PASSWORD`

Optional:
- `DB_PORT` (default 3306)
- `DB_NAME` (default `energy_market`)

## 5) Start the app

```bash
docker compose up -d --build
docker compose ps
docker logs -n 100 enerlyzer
```

Health check:

```bash
curl -sS http://127.0.0.1:8080/health
```

## 6) Install Nginx

```bash
apt-get install -y nginx
```

Copy the template config and edit `YOUR_DOMAIN`:

```bash
cp /opt/enerlyzer/deploy/nginx/enerlyzer.conf /etc/nginx/sites-available/enerlyzer.conf
nano /etc/nginx/sites-available/enerlyzer.conf
ln -sf /etc/nginx/sites-available/enerlyzer.conf /etc/nginx/sites-enabled/enerlyzer.conf
nginx -t
systemctl reload nginx
```

Now test:

```bash
curl -I http://YOUR_DOMAIN/health
```

## 7) Enable HTTPS (Let’s Encrypt)

```bash
apt-get install -y certbot python3-certbot-nginx
certbot --nginx -d YOUR_DOMAIN
```

## 8) Updating (manual)

```bash
cd /opt/enerlyzer
git pull
docker compose up -d --build
```

