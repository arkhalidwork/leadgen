#!/usr/bin/env bash
# ============================================================
# LeadGen — Oracle Cloud Free Tier Setup Script
# Run this on a fresh OCI ARM instance (Ubuntu 22.04/24.04)
# Usage:  ssh ubuntu@YOUR_IP  then  bash deploy.sh
# ============================================================

set -euo pipefail

APP_DIR="/opt/leadgen"
REPO_URL="${1:-}"   # Pass repo URL as first argument, or clone manually

echo "================================================"
echo " LeadGen — Oracle Cloud Deployment"
echo "================================================"
echo ""

# --- 1. System updates ---
echo "[1/7] Updating system packages..."
sudo apt-get update -qq && sudo apt-get upgrade -y -qq

# --- 2. Install Docker ---
if ! command -v docker &>/dev/null; then
    echo "[2/7] Installing Docker..."
    curl -fsSL https://get.docker.com | sudo sh
    sudo usermod -aG docker "$USER"
    sudo systemctl enable docker
    echo "  → Docker installed. You may need to log out/in for group changes."
else
    echo "[2/7] Docker already installed."
fi

# --- 3. Install Docker Compose plugin ---
if ! docker compose version &>/dev/null 2>&1; then
    echo "[3/7] Installing Docker Compose plugin..."
    sudo apt-get install -y -qq docker-compose-plugin
else
    echo "[3/7] Docker Compose already installed."
fi

# --- 4. Clone or update repo ---
if [ -n "$REPO_URL" ]; then
    if [ -d "$APP_DIR" ]; then
        echo "[4/7] Updating existing repo..."
        cd "$APP_DIR" && sudo git pull
    else
        echo "[4/7] Cloning repo..."
        sudo git clone "$REPO_URL" "$APP_DIR"
    fi
    cd "$APP_DIR"
else
    if [ -d "$APP_DIR" ]; then
        echo "[4/7] Using existing repo at $APP_DIR"
        cd "$APP_DIR"
    else
        echo "[4/7] No repo URL provided and $APP_DIR doesn't exist."
        echo "  → Clone your repo manually: git clone <url> $APP_DIR"
        echo "  → Or run: bash deploy.sh https://github.com/YOU/leadgen.git"
        exit 1
    fi
fi

# --- 5. Create .env if missing ---
if [ ! -f .env ]; then
    echo "[5/7] Creating .env from template..."
    sudo cp .env.example .env
    # Generate a random secret key
    SECRET_KEY=$(python3 -c "import secrets; print(secrets.token_hex(32))")
    sudo sed -i "s/change-me-to-a-random-64-char-hex-string/$SECRET_KEY/" .env
    echo "  → .env created with auto-generated secret key."
    echo "  → EDIT .env to add your Stripe keys: sudo nano $APP_DIR/.env"
else
    echo "[5/7] .env already exists, skipping."
fi

# --- 6. Open firewall ports ---
echo "[6/7] Configuring firewall (iptables)..."
# Oracle Cloud uses iptables, not ufw. Port 80/443 needed for web traffic.
sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport 80 -j ACCEPT 2>/dev/null || true
sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport 443 -j ACCEPT 2>/dev/null || true
sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport 5000 -j ACCEPT 2>/dev/null || true
sudo netfilter-persistent save 2>/dev/null || true
echo "  → Ports 80, 443, 5000 opened."
echo "  → IMPORTANT: Also open ports 80 & 443 in OCI Security List (VCN console)."

# --- 7. Build and start ---
echo "[7/7] Building and starting LeadGen..."
sudo docker compose up -d --build

echo ""
echo "================================================"
echo " Deployment complete!"
echo "================================================"
echo ""
echo " App running at:  http://$(curl -s ifconfig.me):5000"
echo ""
echo " Next steps:"
echo "   1. Edit .env with Stripe keys:  sudo nano $APP_DIR/.env"
echo "   2. Set up Nginx + SSL (see below)"
echo "   3. Open ports 80/443 in OCI VCN Security List"
echo ""
echo " --- Nginx + SSL setup ---"
echo "   sudo apt install -y nginx certbot python3-certbot-nginx"
echo "   # Then create Nginx config (see DEPLOYMENT.md)"
echo "   sudo certbot --nginx -d yourdomain.com"
echo ""
echo " --- Useful commands ---"
echo "   sudo docker compose logs -f      # View logs"
echo "   sudo docker compose restart      # Restart app"
echo "   sudo docker compose down          # Stop app"
echo "   curl http://localhost:5000/health # Health check"
echo ""
