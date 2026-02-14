# LeadGen — Google Maps Lead Generator

A web application that scrapes Google Maps to generate business leads. Enter a keyword and location, and the tool will find businesses, extract their contact details, and export everything as a clean CSV file.

## Features

- **Web Interface** — Clean dark-themed UI to enter search parameters
- **Google Maps Scraping** — Automated Selenium-based scraping with full scroll loading
- **Real-time Progress** — Live progress bar and status updates while scraping
- **Data Cleaning** — Deduplication and formatting of scraped data
- **CSV Export** — Download results as a CSV with business name, owner, phone, website, address, rating, reviews, and category
- **Stop/Resume** — Cancel a running job at any time
- **Filter Results** — Search and filter results in the browser

## Prerequisites

- **Python 3.10+**
- **Google Chrome** browser installed
- ChromeDriver is installed automatically via `webdriver-manager`

## Setup

```bash
# 1. Navigate to project directory
cd LeadGen

# 2. Create a virtual environment
python3 -m venv venv
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt
```

## Usage

```bash
# Start the web server
python app.py
```

Open your browser to **http://localhost:5000**

1. Enter a **business type / keyword** (e.g., "restaurants", "dentists", "plumbers")
2. Enter a **location** (e.g., "New York", "London", "Mumbai")
3. Click **Go** — the scraper will search Google Maps for `{keyword} in {place}`
4. Watch the progress bar as it scrolls through results and scrapes each listing
5. View results in the table, filter them, and **Download CSV**

## Output CSV Columns

| Column        | Description                     |
| ------------- | ------------------------------- |
| Business Name | Name of the business            |
| Owner Name    | Owner/proprietor (if available) |
| Phone         | Phone number                    |
| Website       | Business website URL            |
| Address       | Full address                    |
| Rating        | Google Maps rating (1-5)        |
| Reviews       | Number of reviews               |
| Category      | Business category               |

## Project Structure

```
LeadGen/
├── app.py                 # Flask backend & API routes
├── scraper.py             # Google Maps scraper module
├── requirements.txt       # Python dependencies
├── templates/
│   └── index.html         # Frontend HTML template
├── static/
│   ├── css/
│   │   └── style.css      # Custom styles
│   └── js/
│       └── app.js         # Frontend logic
├── output/                # Generated CSV files
└── README.md
```

## API Endpoints

| Method | Endpoint             | Description           |
| ------ | -------------------- | --------------------- |
| GET    | `/`                  | Main web interface    |
| POST   | `/api/scrape`        | Start a scraping job  |
| GET    | `/api/status/<id>`   | Check job progress    |
| GET    | `/api/results/<id>`  | Get completed results |
| GET    | `/api/download/<id>` | Download CSV          |
| POST   | `/api/stop/<id>`     | Stop a running job    |

## Disclaimer

This tool is intended for **educational and authorized use only**. Always respect Google's Terms of Service and robots.txt. Use responsibly and ensure compliance with all applicable laws and regulations regarding web scraping and data collection.
