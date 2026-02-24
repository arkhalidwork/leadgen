FROM python:3.11-slim

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive
# Production defaults
ENV FLASK_ENV=production
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Install Chromium (works on both amd64 AND arm64 — required for Oracle Cloud ARM)
# Also installs matching chromedriver so Selenium can find it automatically
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver \
    curl \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libxss1 \
    libxtst6 \
    libx11-xcb1 \
    libxcb-dri3-0 \
    libpango-1.0-0 \
    libcairo2 \
    xdg-utils \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Tell Selenium where to find Chromium and chromedriver
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver
# Prevent Selenium Manager from trying to download Chrome/chromedriver
ENV SE_AVOID_BROWSER_DOWNLOAD=true

# Create a non-root user for security
RUN groupadd -r leadgen && useradd -r -g leadgen -d /app -s /sbin/nologin leadgen

# Set working directory
WORKDIR /app

# Copy requirements first for layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy ONLY production code (no desktop/build files)
COPY app.py scraper.py linkedin_scraper.py instagram_scraper.py web_crawler.py ./
COPY templates/ templates/
COPY static/ static/

# Create output and data directories
RUN mkdir -p output data && chown -R leadgen:leadgen /app

# Switch to non-root user
USER leadgen

# Expose port
EXPOSE 5000

# Health check for container orchestrators
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:5000/health || exit 1

CMD ["gunicorn", \
     "--bind", "0.0.0.0:5000", \
     "--workers", "2", \
     "--threads", "4", \
     "--timeout", "300", \
     "--access-logfile", "-", \
     "--error-logfile", "-", \
     "--log-level", "info", \
     "app:app"]
