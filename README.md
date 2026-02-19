# Spanish Business Portal Scrapers

Standalone multi-portal scraper for Spanish business directories. Outputs JSON lines (one company per line) — no database required.

## Thought Process

We needed to scrape multiple Spanish business portals to build a B2B lead database. After researching ~15 portals, we narrowed to 6 worth scraping based on data quality vs engineering effort:

| Portal | What We Get | How It Works | Anti-Bot |
|--------|------------|-------------|----------|
| **Empresite** | Name, address, CNAE, phone, email, employee filter | curl_cffi (HTTP only, no browser) | Medium — Chrome TLS fingerprint bypass |
| **Europages** | Name, employees, website, description, sector | Playwright (browser) | AWS WAF — auto-resolves in ~20s |
| **Empresia** | CIF, CNAE, employees, phone, directors, address | Playwright (browser) | None — uses jQuery autocomplete search |
| **PaginasAmarillas** | Phone, address, website, category | Playwright (browser) | Incapsula — needs manual CAPTCHA solve |
| **Einforma** | CIF, CNAE, legal form, address | Playwright (browser) | Robot block — needs manual solve |
| **LibreBOR** | CIF, CNAE, directors, incorporation date | Playwright (browser) | Cloudflare — needs manual solve |

**Portals we skipped** (not worth the effort):
- Axesor — paywalled, needs paid API
- Kompass — heavy Cloudflare, limited free data
- Cylex/Hotfrog/Vulka — low-value thin directories
- DNB/Infobel — enterprise subscription only
- Infocif — unreliable, frequently down

### Key technical decisions

1. **curl_cffi for Empresite** — It mimics Chrome's TLS/JA3 fingerprint at the HTTP level, so no browser needed. 10x faster than Playwright. This is the best candidate for Cloudflare Workers deployment since it's just HTTP requests.

2. **Persistent browser profiles for everything else** — Playwright with `launch_persistent_context()` saves cookies between runs. After solving a CAPTCHA once, you can scrape for hours without being challenged again. The profiles live in `~/.leadgen/chrome-profile-{portal}/`.

3. **Search-based discovery** — Most portals block their listing/directory pages but leave search functional (blocking search = breaking the site for real users). Europages uses URL-based search (`/es/search?q=...&location=...`), Empresia uses jQuery UI autocomplete.

4. **5+ second delays** — Boss tested manually with a Chrome extension and found 5s per page avoids rate limits. All scrapers default to 4-7s random delays.

5. **Manual CAPTCHA resolution** — For Cloudflare/Incapsula/robot blocks, the scraper pauses and waits for you to solve it in the headed browser window. It checks every 5s if the challenge is gone and auto-resumes. Timeout is 5 minutes.

### Cloudflare Workers deployment

The **Empresite** scraper is the best candidate for Workers:
- Pure HTTP requests (no browser)
- Just needs `fetch()` with the right headers + TLS fingerprint
- IP auto-rotation from Cloudflare eliminates rate limiting
- The other 5 portals need Playwright, which requires **Cloudflare Browser Rendering** (enterprise plan)

## Setup

```bash
pip install -r requirements.txt
playwright install chromium
```

## Usage

```bash
# Empresite — HTTP only, fastest, no browser needed
python scrape.py --portal empresite --region BARCELONA --limit 100 -o results.json

# With detail pages (adds CNAE, phone, email — slower, 2 requests per company)
python scrape.py --portal empresite --region BARCELONA --details --limit 50 -o results.json

# Europages — B2B marketplace, employee counts + websites
python scrape.py --portal europages --region BARCELONA --limit 50 -o results.json

# Empresia — official BORME registry data (CIF, CNAE, employees, directors)
python scrape.py --portal empresia --region BARCELONA --limit 50 -o results.json

# PaginasAmarillas — Yellow Pages, best for phone numbers (needs manual CAPTCHA)
python scrape.py --portal paginasamarillas --region BARCELONA --limit 50

# Einforma — registry data (needs manual robot block solve)
python scrape.py --portal einforma --region BARCELONA --limit 50

# LibreBOR — BORME data (needs manual Cloudflare solve)
python scrape.py --portal librebor --region BARCELONA --limit 50

# Custom delays (default: 4-7s between requests)
python scrape.py --portal empresite --region BARCELONA --delay-min 5 --delay-max 10

# Headless mode (works for Empresia, Europages; blocked by others)
python scrape.py --portal empresia --region BARCELONA --limit 20 --headless
```

## Output Format

JSON Lines — one company per line:

```json
{"legal_name": "ACCENTURE SL", "city": "Madrid", "cif": "B79217790", "cnae_code": "6202", "phone": "915966000", "employee_count": "3631", "source_portal": "empresia"}
{"legal_name": "LINGUAVOX SL", "city": "Barcelona", "employee_count": "5-9", "website_url": "https://www.linguavox.net/", "source_portal": "europages"}
```

### Fields

| Field | Description | Which portals |
|-------|------------|---------------|
| `legal_name` | Company legal name (uppercased) | All |
| `city` | City name | All |
| `province` | Province | All |
| `region` | Region | All |
| `cif` | Spanish tax ID (CIF/NIF) | Empresia, LibreBOR, Einforma |
| `cnae_code` | CNAE activity code | Empresite (details), Empresia, LibreBOR |
| `phone` | Phone number | Empresite (details), PaginasAmarillas, Empresia |
| `email` | Email address | Empresite (details) |
| `website_url` | Company website | Europages, PaginasAmarillas, Empresite (details) |
| `domain` | Domain extracted from website | Europages, PaginasAmarillas |
| `employee_count` | Employee count or range | Europages, Empresia |
| `industry` | Sector/activity description | Empresite, Europages, Empresia |
| `address` | Street address | Empresite, PaginasAmarillas, Empresia |
| `summary` | Business description | Empresite, Europages, Empresia |
| `source_portal` | Which portal the data came from | All |
| `source_url` | URL of the company page | All |

## Available Regions

`BARCELONA`, `MADRID`, `VALENCIA`, `SEVILLA`, `MALAGA`, `ALICANTE`, `ZARAGOZA`, `BILBAO`, `VIZCAYA`, `GIRONA`, `TARRAGONA`, `LLEIDA`, `MURCIA`, `CADIZ`, `GRANADA`, `CORDOBA`, `ASTURIAS`, `CANTABRIA`, `NAVARRA`, `PONTEVEDRA`, and more.
