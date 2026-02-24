# LeadGen — Oracle Cloud Free Tier Deployment Guide

## Architecture Overview

```
User → Oracle Cloud ARM VM (free) → Nginx + SSL → Docker → Flask + Gunicorn → SQLite + Chromium
                                                                    ↕
                                                              Stripe Webhooks
```

## Why Oracle Cloud Free Tier?

| Resource      | Free Tier Allocation          |
| ------------- | ----------------------------- |
| **CPU**       | 4 ARM-based OCPUs (Ampere A1) |
| **RAM**       | 24 GB                         |
| **Storage**   | 200 GB block storage          |
| **Bandwidth** | 10 TB/month outbound          |
| **Cost**      | $0 forever (Always Free)      |

This is more than enough for Chrome/Selenium scraping + Flask web server.

---

## Quick Deploy (One Command)

```bash
ssh ubuntu@YOUR_OCI_IP
curl -sL https://raw.githubusercontent.com/YOUR_USERNAME/leadgen/main/deploy.sh | bash -s -- https://github.com/YOUR_USERNAME/leadgen.git
```

Or step-by-step:

---

## Step-by-Step Deployment

### 1. Create an Oracle Cloud Instance

1. Go to [Oracle Cloud Console](https://cloud.oracle.com/)
2. **Compute → Instances → Create Instance**
3. Settings:
   - **Image:** Ubuntu 22.04 or 24.04 (Canonical)
   - **Shape:** VM.Standard.A1.Flex (ARM) — **4 OCPUs, 24 GB RAM**
   - **Boot volume:** 50 GB (minimum)
   - **Network:** Create a new VCN or use existing
   - **SSH key:** Add your public key
4. Click **Create**

### 2. Open Ports in OCI Security List

**This is critical and often missed.** Oracle Cloud blocks all traffic by default.

1. Go to **Networking → Virtual Cloud Networks** → your VCN
2. Click **Security Lists** → **Default Security List**
3. **Add Ingress Rules:**

   | Source CIDR | Protocol | Dest Port | Description |
   | ----------- | -------- | --------- | ----------- |
   | 0.0.0.0/0   | TCP      | 80        | HTTP        |
   | 0.0.0.0/0   | TCP      | 443       | HTTPS       |

### 3. SSH and Run Deploy Script

```bash
ssh ubuntu@YOUR_OCI_IP

# Download and run the deploy script
git clone https://github.com/YOUR_USERNAME/leadgen.git /opt/leadgen
cd /opt/leadgen
bash deploy.sh
```

The script will:

- Install Docker & Docker Compose
- Generate a random secret key
- Open iptables firewall ports
- Build and start the Docker container

### 4. Configure Environment

```bash
sudo nano /opt/leadgen/.env
```

Fill in your Stripe keys (if using payment):

```
STRIPE_SECRET_KEY=sk_live_...
STRIPE_WEBHOOK_SECRET=whsec_...
STRIPE_PRICE_ID_PRO=price_...
```

Then restart:

```bash
cd /opt/leadgen && sudo docker compose restart
```

### 5. Set Up Nginx + SSL

```bash
sudo apt install -y nginx certbot python3-certbot-nginx

# Create Nginx config
sudo tee /etc/nginx/sites-available/leadgen > /dev/null <<'EOF'
server {
    listen 80;
    server_name yourdomain.com;

    client_max_body_size 50M;

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 300s;
        proxy_connect_timeout 300s;
        proxy_send_timeout 300s;
    }
}
EOF

sudo ln -sf /etc/nginx/sites-available/leadgen /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t && sudo systemctl restart nginx

# Get SSL certificate (point your domain to the OCI IP first)
sudo certbot --nginx -d yourdomain.com
```

### 6. Verify

```bash
curl http://localhost:5000/health
# → {"status": "healthy", "timestamp": "..."}

curl https://yourdomain.com/health
# → Same response over SSL
```

---

## Stripe Integration (Automated License Keys)

### Setup Steps:

1. **Create a Stripe account** at https://stripe.com
2. **Create a Product + Price** in Stripe Dashboard:
   - Product: "LeadGen Pro License"
   - Price: One-time payment (e.g., $49)
   - Copy the Price ID (starts with `price_`)
3. **Create a Webhook** in Stripe Dashboard → Developers → Webhooks:
   - Endpoint URL: `https://yourdomain.com/api/stripe/webhook`
   - Events: `checkout.session.completed`, `invoice.payment_succeeded`
   - Copy the Webhook Signing Secret (starts with `whsec_`)
4. **Add to .env:**
   ```
   STRIPE_SECRET_KEY=sk_live_...
   STRIPE_WEBHOOK_SECRET=whsec_...
   STRIPE_PRICE_ID_PRO=price_...
   ```

### Payment Flow:

```
User clicks "Buy" → POST /api/stripe/create-checkout → Stripe Checkout page
User pays → Stripe webhook → /api/stripe/webhook → Auto-generates license key
User returns → /activate?payment=success → Account auto-activated
```

---

## Environment Variables Reference

| Variable                | Required     | Description                               |
| ----------------------- | ------------ | ----------------------------------------- |
| `LEADGEN_SECRET_KEY`    | **Yes**      | 64-char hex string for session signing    |
| `LEADGEN_DB_PATH`       | No           | SQLite path (default: `./leadgen.db`)     |
| `LEADGEN_OUTPUT_DIR`    | No           | CSV output dir (default: `./output`)      |
| `FLASK_ENV`             | Yes          | Set to `production`                       |
| `STRIPE_SECRET_KEY`     | For payments | Stripe secret key                         |
| `STRIPE_WEBHOOK_SECRET` | For payments | Stripe webhook signing secret             |
| `STRIPE_PRICE_ID_PRO`   | For payments | Stripe Price ID for Pro plan              |
| `ALLOWED_ORIGINS`       | No           | CORS origins (default: `*`)               |
| `RATELIMIT_STORAGE_URI` | No           | Rate limit storage (default: `memory://`) |

---

## Security Features

- **bcrypt** password hashing (auto-migrates legacy SHA-256 hashes)
- **Rate limiting** on auth endpoints (5/min register, 10/min login)
- **CSRF protection** via Flask-WTF (API routes exempt — protected by SameSite cookies)
- **Security headers** (HSTS, X-Content-Type-Options, X-Frame-Options, etc.)
- **Password strength** requirements (8+ chars, uppercase, lowercase, number)
- **Session fixation** prevention (session regenerated on login)
- **Non-root** Docker container user
- **Health check** endpoint at `/health`

---

## Monitoring

Check container health:

```bash
docker compose ps
docker compose logs -f leadgen
curl http://localhost:5000/health
```

---

## Backups

SQLite database is at `/app/data/leadgen.db` inside the container (Docker volume `leadgen-data`).

```bash
# Backup
sudo docker cp leadgen-app:/app/data/leadgen.db ./backup_$(date +%Y%m%d).db

# Set up daily cron backup
sudo mkdir -p /opt/backups
echo "0 3 * * * docker cp leadgen-app:/app/data/leadgen.db /opt/backups/leadgen_\$(date +\%Y\%m\%d).db" | sudo crontab -
```

---

## Updating the App

```bash
cd /opt/leadgen
sudo git pull
sudo docker compose up -d --build
```

---

## Oracle Cloud Troubleshooting

### "Connection refused" / "Site can't be reached"

1. **Check OCI Security List** — this is the #1 issue. Go to VCN → Security Lists → add ingress rules for ports 80 and 443.
2. **Check iptables:** `sudo iptables -L INPUT -n --line-numbers` — ports 80/443 should be ACCEPT.
3. **Check the container:** `sudo docker compose ps` — status should be "Up (healthy)".

### Docker build fails on ARM

The Dockerfile uses `chromium` from apt (ARM64-native). If the build fails:

```bash
# Verify architecture
uname -m   # should show aarch64

# Test chromium manually
sudo docker run --rm python:3.11-slim bash -c "apt-get update && apt-get install -y chromium && chromium --version"
```

### Chromium crashes in container

```bash
# Check shared memory
sudo docker compose exec leadgen df -h /dev/shm
# Should show 2GB. If not, ensure shm_size: "2gb" in docker-compose.yml.

# Check logs
sudo docker compose logs --tail 50 leadgen
```

### Out of memory

Oracle Free Tier gives 24GB RAM, but if Docker is constrained:

```bash
# Check actual usage
sudo docker stats --no-stream
```

### SSL certificate renewal

Certbot auto-renews, but verify:

```bash
sudo certbot renew --dry-run
```
