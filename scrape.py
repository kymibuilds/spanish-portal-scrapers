#!/usr/bin/env python3
"""
Standalone multi-portal scraper — no database required.

Outputs JSON lines (one company per line) to stdout or a file.
Designed to be run independently or deployed behind Cloudflare Workers/tunnels.

Usage:
    python scrape_standalone.py --portal empresite --region BARCELONA --limit 10
    python scrape_standalone.py --portal europages --region BARCELONA --limit 50 --output results.json
    python scrape_standalone.py --portal paginasamarillas --region BARCELONA --limit 20

All portals:
    empresite       - curl_cffi, employee filters, CNAE/phone from detail pages
    europages       - Playwright, B2B data (employees, website, description)
    paginasamarillas - Playwright, contact data (phone, address, website)
    einforma        - Playwright, registry data (CIF, CNAE, legal form)
    empresia        - Playwright, BORME data (CIF, CNAE, employees, directors)
    librebor        - Playwright, BORME API (CIF, CNAE, incorporation)

Requirements:
    pip install curl-cffi beautifulsoup4 lxml playwright
    playwright install chromium
"""
import argparse
import asyncio
import json
import logging
import random
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

PROFILE_BASE = Path.home() / ".leadgen"
MIN_DELAY = 4.0  # Minimum seconds between requests
MAX_DELAY = 7.0  # Maximum seconds between requests


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

async def human_delay(min_s: float = MIN_DELAY, max_s: float = MAX_DELAY):
    await asyncio.sleep(random.uniform(min_s, max_s))


def has_challenge(content: str) -> bool:
    lower = content.lower()
    indicators = [
        "challenge-platform", "just a moment", "cf-challenge", "cf_chl_opt",
        "incapsula", "_incapsula_resource", "incident_id",
        "human verification", "awswaf",
        "g-recaptcha-response", 'class="g-recaptcha"', "captcha-delivery",
        "hcaptcha-box", "capado_robots", "control robots",
    ]
    return any(i in lower for i in indicators)


async def wait_for_challenge(page, timeout: int = 300) -> bool:
    content = await page.content()
    if not has_challenge(content):
        return True

    logger.warning(
        f"Bot challenge detected! Please solve it in the browser window. "
        f"Waiting up to {timeout}s..."
    )
    elapsed = 0
    while elapsed < timeout:
        await asyncio.sleep(5)
        elapsed += 5
        content = await page.content()
        if not has_challenge(content):
            logger.info("Challenge resolved! Resuming.")
            return True
        if elapsed % 30 == 0:
            logger.info(f"  Still waiting... ({elapsed}s)")
    logger.error(f"Challenge not resolved within {timeout}s.")
    return False


async def launch_browser(portal: str, headless: bool = False):
    from playwright.async_api import async_playwright

    profile_dir = PROFILE_BASE / f"chrome-profile-{portal}"
    profile_dir.mkdir(parents=True, exist_ok=True)

    pw = await async_playwright().start()
    context = await pw.chromium.launch_persistent_context(
        user_data_dir=str(profile_dir),
        headless=headless,
        channel="chromium",
        locale="es-ES",
        timezone_id="Europe/Madrid",
        viewport={"width": 1366, "height": 768},
        args=[
            "--disable-blink-features=AutomationControlled",
            "--disable-features=IsolateOrigins,site-per-process",
        ],
    )
    page = context.pages[0] if context.pages else await context.new_page()
    return pw, context, page


# ---------------------------------------------------------------------------
# Empresite (curl_cffi)
# ---------------------------------------------------------------------------

def _cffi_fetch(url, method="GET", max_retries=3, **kwargs):
    from curl_cffi import requests as curl_requests
    headers = {
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.google.com/",
        **kwargs.pop("headers", {}),
    }
    for attempt in range(max_retries):
        resp = curl_requests.request(method, url, impersonate="chrome", headers=headers, **kwargs)
        if resp.status_code == 429 and attempt < max_retries - 1:
            wait = 30 * (2 ** attempt)
            logger.warning(f"Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue
        return resp
    return resp


async def scrape_empresite(region, city, limit, **kwargs):
    from bs4 import BeautifulSoup

    BASE = "https://empresite.eleconomista.es"
    total = 0
    companies = []

    if city:
        cities = [(city, city)]
    else:
        # Get city list
        resp = _cffi_fetch(f"{BASE}/provincia/{region}/")
        if resp.status_code != 200:
            logger.error(f"Failed to get cities: {resp.status_code}")
            return companies
        soup = BeautifulSoup(resp.text, "lxml")
        cities = []
        for link in soup.select("a[href*='/localidad/']"):
            m = re.search(r"/localidad/([^/]+)/", link["href"])
            if m:
                cities.append((link.get_text(strip=True).split("(")[0].strip(), m.group(1)))

    emp_range = f"{kwargs.get('employee_min', 10)}-{kwargs.get('employee_max', 200)}"

    for city_name, city_slug in cities:
        if limit and total >= limit:
            break

        page = 1
        while True:
            if limit and total >= limit:
                break

            base_path = f"/localidad/{city_slug}/"
            if page > 1:
                base_path += f"PgNum-{page}/"
            url = f"{BASE}{base_path}?testfiltros=1&emp_empleados_number={emp_range}"

            await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
            logger.info(f"Empresite: {city_slug} page {page}")

            try:
                resp = _cffi_fetch(url, method="POST", headers={
                    "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
                    "Referer": f"{BASE}/localidad/{city_slug}/",
                })
                if resp.status_code != 200:
                    break
            except Exception as e:
                logger.error(f"Request failed: {e}")
                break

            soup = BeautifulSoup(resp.text, "lxml")
            cards = soup.select("div.cardCompanyBox")
            if not cards:
                break

            for card in cards:
                if limit and total >= limit:
                    break

                name_meta = card.select_one('meta[itemprop="name"]')
                if not name_meta:
                    continue
                legal_name = name_meta.get("content", "").strip()
                if not legal_name:
                    continue

                link_el = card.select_one("h3 a")
                detail_url = ""
                if link_el:
                    detail_url = link_el["href"]
                    if not detail_url.startswith("http"):
                        detail_url = urljoin(BASE, detail_url)

                desc_el = card.select_one("span.line-clamp-2")
                addr_el = card.select_one('span[itemprop="address"]')

                company = {
                    "legal_name": legal_name.upper(),
                    "city": city_slug.split("-")[0].replace("-", " ").title(),
                    "province": region.title(),
                    "region": region.title(),
                    "address": addr_el.get_text(strip=True) if addr_el else "",
                    "summary": desc_el.get_text(strip=True)[:500] if desc_el else "",
                    "source_portal": "empresite",
                    "source_url": detail_url,
                }

                # Optionally scrape detail page
                if kwargs.get("details") and detail_url:
                    await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
                    try:
                        dresp = _cffi_fetch(detail_url)
                        if dresp.status_code == 200:
                            m = re.search(r"'CNAE'\s*:\s*'(\d+)'.*?'GRUPO_SECTOR'\s*:\s*'([^']*)'", dresp.text)
                            if m:
                                company["cnae_code"] = m.group(1)
                                company["industry"] = m.group(2)
                            dsoup = BeautifulSoup(dresp.text, "lxml")
                            ph = dsoup.select_one('span[itemprop="telephone"], a[href^="tel:"]')
                            if ph:
                                phone = re.sub(r"[^\d+]", "", ph.get("content") or ph.get_text(strip=True))
                                if len(phone) >= 9:
                                    company["phone"] = phone
                            em = dsoup.select_one('a[href^="mailto:"]')
                            if em:
                                company["email"] = em["href"].replace("mailto:", "")
                            web = dsoup.select_one('a[itemprop="url"][href*="http"]')
                            if web and "empresite" not in web["href"]:
                                company["website_url"] = web["href"]
                                company["domain"] = urlparse(web["href"]).netloc.replace("www.", "")
                    except Exception:
                        pass

                companies.append(company)
                total += 1

            if len(cards) < 30:
                break
            if page >= 40:
                break
            page += 1

    return companies


# ---------------------------------------------------------------------------
# Europages (Playwright)
# ---------------------------------------------------------------------------

EUROPAGES_SEARCH_TERMS = [
    "servicios", "fabricante", "industrial", "construccion", "tecnologia",
    "alimentacion", "transporte", "consultoria", "energia", "textil",
    "quimico", "metalurgia", "farmaceutico", "ingenieria", "maquinaria",
]

async def scrape_europages(region, city, limit, headless=False, **kwargs):
    pw, context, page = await launch_browser("europages", headless)
    companies = []
    seen_urls = set()
    location = city.title() if city else region.title()
    BASE = "https://www.europages.es"

    try:
        for term in EUROPAGES_SEARCH_TERMS:
            if limit and len(companies) >= limit:
                break

            page_num = 1
            while True:
                if limit and len(companies) >= limit:
                    break

                await human_delay()
                url = f"{BASE}/es/search?q={term}&location={location}"
                if page_num > 1:
                    url += f"&page={page_num}"

                logger.info(f"Europages: '{term}' page {page_num} in {location}")
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                except Exception as e:
                    logger.error(f"Navigation failed: {e}")
                    break

                if not await wait_for_challenge(page):
                    break

                await human_delay(2, 4)
                content = await page.content()
                raw_links = re.findall(r'href="(/es/company/[^"]+)"', content)
                slugs = []
                for link in raw_links:
                    base = re.sub(r"/products/.*", "", link)
                    if base not in seen_urls:
                        seen_urls.add(base)
                        slugs.append(base)

                if not slugs:
                    break

                for slug in slugs:
                    if limit and len(companies) >= limit:
                        break
                    await human_delay()
                    try:
                        await page.goto(f"{BASE}{slug}", wait_until="domcontentloaded", timeout=30000)
                    except Exception:
                        continue
                    if not await wait_for_challenge(page, timeout=120):
                        continue

                    await human_delay(1, 3)
                    text = await page.inner_text("body")

                    name = None
                    for sel in ["h1", "h2"]:
                        el = await page.query_selector(sel)
                        if el:
                            raw = (await el.inner_text()).strip()
                            if raw.upper().startswith("SOBRE "):
                                raw = raw[6:].strip()
                            if raw:
                                name = raw
                                break
                    if not name:
                        continue

                    company = {
                        "legal_name": name.upper(),
                        "source_portal": "europages",
                        "source_url": f"{BASE}{slug}",
                        "region": region.title(),
                        "province": region.title(),
                        "city": region.title(),
                    }

                    emp = re.search(r"Empleados:\s*([\d\s\-–]+)", text)
                    if emp:
                        company["employee_count"] = emp.group(1).strip()

                    web_el = await page.query_selector("a[href*='http']:has-text('Visitar')")
                    if web_el:
                        href = await web_el.get_attribute("href")
                        if href and "europages" not in href:
                            company["website_url"] = href
                            company["domain"] = urlparse(href).netloc.replace("www.", "")

                    founded = re.search(r"Fundada:\s*(\d{4})", text)
                    addr = re.search(r"([\w\s/.,-]+\d{4,5})\s*\n?\s*España", text)
                    if addr:
                        company["address"] = addr.group(1).strip()

                    desc_el = await page.query_selector("div[class*='description'], div[class*='about'] p")
                    if desc_el:
                        desc = (await desc_el.inner_text()).strip()
                        if desc:
                            company["summary"] = desc[:500]

                    phone_el = await page.query_selector("a[href^='tel:']")
                    if phone_el:
                        ph = re.sub(r"[^\d+]", "", (await phone_el.get_attribute("href")).replace("tel:", ""))
                        if len(ph) >= 9:
                            company["phone"] = ph

                    companies.append(company)
                    logger.debug(f"  Saved: {company['legal_name']}")

                has_next = await page.query_selector("a[rel='next'], [aria-label='Next']")
                if not has_next or page_num >= 10:
                    break
                page_num += 1
    finally:
        await context.close()
        await pw.stop()

    return companies


# ---------------------------------------------------------------------------
# PaginasAmarillas (Playwright)
# ---------------------------------------------------------------------------

PA_CATEGORIES = [
    "empresas", "construccion", "consultoria", "informatica",
    "transporte", "alimentacion", "industria", "servicios",
    "ingenieria", "comercio", "inmobiliaria", "abogados",
]

PA_PROVINCES = {
    "BARCELONA": "barcelona", "MADRID": "madrid", "VALENCIA": "valencia",
    "SEVILLA": "sevilla", "MALAGA": "malaga", "ALICANTE": "alicante",
    "ZARAGOZA": "zaragoza", "BILBAO": "vizcaya", "MURCIA": "murcia",
}

async def scrape_paginasamarillas(region, city, limit, headless=False, **kwargs):
    pw, context, page = await launch_browser("paginasamarillas", headless)
    companies = []
    seen = set()
    BASE = "https://www.paginasamarillas.es"
    province = PA_PROVINCES.get(region.upper(), region.lower())

    try:
        logger.info("PaginasAmarillas: navigating to homepage...")
        await page.goto(BASE, wait_until="domcontentloaded", timeout=30000)
        await human_delay(3, 5)
        if not await wait_for_challenge(page):
            return companies

        logger.info("PaginasAmarillas: access granted!")

        for cat in PA_CATEGORIES:
            if limit and len(companies) >= limit:
                break
            pnum = 1
            while True:
                if limit and len(companies) >= limit:
                    break
                await human_delay()
                url = f"{BASE}/search/{cat}/all-ma/{province}/all-is/{province}/all-ba/all-pu/all-nc/{pnum}"
                logger.info(f"PaginasAmarillas: '{cat}' page {pnum}")
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                except Exception:
                    break
                if not await wait_for_challenge(page, timeout=120):
                    break
                await human_delay(2, 4)

                listings = await page.query_selector_all(
                    "div.listado-item, div.search-result, article, [data-name]"
                )
                if not listings:
                    break

                new = 0
                for li in listings:
                    if limit and len(companies) >= limit:
                        break
                    name_el = await li.query_selector("h2 a, h2 span, [itemprop='name']")
                    if not name_el:
                        continue
                    name = (await name_el.inner_text()).strip()
                    if not name or name.upper() in seen:
                        continue
                    seen.add(name.upper())

                    c = {"legal_name": name.upper(), "source_portal": "paginasamarillas",
                         "region": region.title(), "province": region.title(), "city": region.title()}

                    ph_el = await li.query_selector("a[href^='tel:'], [itemprop='telephone']")
                    if ph_el:
                        ph = re.sub(r"[^\d+]", "", (await ph_el.get_attribute("href") or await ph_el.inner_text()).replace("tel:", ""))
                        if len(ph) >= 9:
                            c["phone"] = ph

                    addr_el = await li.query_selector("[itemprop='address'], span.address")
                    if addr_el:
                        c["address"] = (await addr_el.inner_text()).strip()
                        city_el = await addr_el.query_selector("[itemprop='addressLocality']")
                        if city_el:
                            c["city"] = (await city_el.inner_text()).strip().title()

                    web_el = await li.query_selector("a[data-type='web'], a.web")
                    if web_el:
                        href = await web_el.get_attribute("href")
                        if href and "paginasamarillas" not in href and href.startswith("http"):
                            c["website_url"] = href
                            c["domain"] = urlparse(href).netloc.replace("www.", "")

                    companies.append(c)
                    new += 1

                if new == 0:
                    break
                has_next = await page.query_selector("a.next, a[rel='next']")
                if not has_next or pnum >= 20:
                    break
                pnum += 1
    finally:
        await context.close()
        await pw.stop()

    return companies


# ---------------------------------------------------------------------------
# Einforma (Playwright)
# ---------------------------------------------------------------------------

EINFORMA_PROVINCES = {
    "BARCELONA": "barcelona", "MADRID": "madrid", "VALENCIA": "valencia",
    "SEVILLA": "sevilla", "MALAGA": "malaga", "ALICANTE": "alicante",
    "ZARAGOZA": "zaragoza", "BILBAO": "vizcaya", "MURCIA": "murcia",
}

async def scrape_einforma(region, city, limit, headless=False, **kwargs):
    pw, context, page = await launch_browser("einforma", headless)
    companies = []
    BASE = "https://www.einforma.com"
    province = EINFORMA_PROVINCES.get(region.upper(), region.lower())

    try:
        logger.info("Einforma: navigating to homepage...")
        await page.goto(BASE, wait_until="domcontentloaded", timeout=30000)
        await human_delay(3, 5)

        # Accept cookies
        try:
            btn = await page.query_selector("button#onetrust-accept-btn-handler")
            if btn:
                await btn.click()
                await page.wait_for_timeout(2000)
        except Exception:
            pass

        # Load listing page
        await page.goto(f"{BASE}/informes-empresas/{province}.html", wait_until="domcontentloaded", timeout=30000)
        await human_delay(3, 5)
        if not await wait_for_challenge(page):
            return companies

        logger.info("Einforma: access granted!")

        pnum = 1
        while True:
            if limit and len(companies) >= limit:
                break

            if pnum > 1:
                await human_delay()
                url = f"{BASE}/informes-empresas/{province}-{pnum}.html"
                logger.info(f"Einforma: page {pnum}")
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                except Exception:
                    break
                if not await wait_for_challenge(page, timeout=120):
                    break
                await human_delay(2, 4)

            content = await page.content()
            links = re.findall(r'href="(/informes-empresa/[^"]+)"', content)
            rows = await page.query_selector_all("table tbody tr, .empresa-item, .result-row")

            if rows:
                for row in rows:
                    if limit and len(companies) >= limit:
                        break
                    name_el = await row.query_selector("a[href*='/informes-empresa/'], td:first-child a")
                    if not name_el:
                        continue
                    name = (await name_el.inner_text()).strip()
                    if not name:
                        continue
                    c = {"legal_name": name.upper(), "source_portal": "einforma",
                         "region": region.title(), "province": region.title(), "city": region.title()}
                    href = await name_el.get_attribute("href")
                    if href:
                        c["source_url"] = href if href.startswith("http") else urljoin(BASE, href)

                    cif_el = await row.query_selector(".cif, td:nth-child(2)")
                    if cif_el:
                        m = re.search(r"[A-Z]\d{7,8}", (await cif_el.inner_text()).strip())
                        if m:
                            c["cif"] = m.group(0)
                    companies.append(c)
            elif links:
                for link in links:
                    if limit and len(companies) >= limit:
                        break
                    name_part = re.search(r'/informes-empresa/([^/]+)', link)
                    if name_part:
                        raw = name_part.group(1).replace("-", " ").strip()
                        if raw:
                            companies.append({
                                "legal_name": raw.upper(),
                                "source_portal": "einforma",
                                "source_url": urljoin(BASE, link),
                                "region": region.title(), "province": region.title(),
                                "city": region.title(),
                            })
            else:
                break

            has_next = await page.query_selector("a[rel='next'], a:has-text('Siguiente')")
            if not has_next:
                break
            pnum += 1
    finally:
        await context.close()
        await pw.stop()

    return companies


# ---------------------------------------------------------------------------
# Empresia (Playwright)
# ---------------------------------------------------------------------------

EMPRESIA_SEARCH_TERMS = [
    "SL", "SA", "SLU", "SOCIEDAD LIMITADA",
    "CONSULTING", "SERVICIOS", "TECNOLOGIA", "CONSTRUCCION",
    "INGENIERIA", "ALIMENTACION", "TRANSPORTE", "INDUSTRIAL",
    "GESTION", "COMERCIAL", "SOLUCIONES", "GRUPO",
]

async def scrape_empresia(region, city, limit, headless=False, **kwargs):
    pw, context, page = await launch_browser("empresia", headless)
    companies = []
    seen = set()
    BASE = "https://www.empresia.es"
    location = (city or region).upper()

    try:
        await page.goto(BASE, wait_until="domcontentloaded", timeout=20000)
        await human_delay(2, 4)

        for term in EMPRESIA_SEARCH_TERMS:
            if limit and len(companies) >= limit:
                break

            query = f"{term} {location}"
            logger.info(f"Empresia: searching '{query}'")
            await human_delay()

            await page.goto(BASE, wait_until="domcontentloaded", timeout=20000)
            await human_delay(1, 2)

            search = await page.query_selector("input.ui-autocomplete-input, input[type='search'], input[type='text']")
            if not search:
                continue

            await search.click()
            await search.fill("")
            await search.type(query, delay=50)
            await page.wait_for_timeout(3000)

            suggestions = await page.query_selector_all(".ui-autocomplete .ui-menu-item, .ui-autocomplete li")
            if not suggestions:
                continue

            texts = []
            for s in suggestions:
                t = (await s.inner_text()).strip()
                if t:
                    texts.append(t)

            logger.info(f"  Got {len(texts)} suggestions")

            for text in texts:
                if limit and len(companies) >= limit:
                    break

                await human_delay()
                await page.goto(BASE, wait_until="domcontentloaded", timeout=20000)
                await human_delay(1, 2)

                search = await page.query_selector("input.ui-autocomplete-input, input[type='search'], input[type='text']")
                if not search:
                    continue
                await search.click()
                await search.fill("")
                await search.type(text[:30], delay=50)
                await page.wait_for_timeout(3000)

                item = await page.query_selector(".ui-autocomplete .ui-menu-item a, .ui-autocomplete li a, .ui-autocomplete li")
                if not item:
                    continue
                await item.click()
                await page.wait_for_timeout(3000)

                if "/empresa/" not in page.url:
                    continue

                slug = page.url.rstrip("/").split("/")[-1]
                if slug in seen:
                    continue
                seen.add(slug)

                body = await page.inner_text("body")
                name_m = re.search(r"Datos de (.+?)(?:\n|$)", body)
                name = name_m.group(1).strip() if name_m else None
                if not name:
                    h1 = await page.query_selector("h1")
                    if h1:
                        name = (await h1.inner_text()).strip()
                if not name:
                    continue

                c = {
                    "legal_name": name.upper(),
                    "source_portal": "empresia",
                    "source_url": page.url,
                    "region": region.title(), "province": region.title(), "city": region.title(),
                }

                cif_m = re.search(r"CIF\s*\n\s*([A-Z]\d{7,8})", body)
                if cif_m:
                    c["cif"] = cif_m.group(1)

                cnae_m = re.search(r"CNAE\s+(\d{3,4})\s*[-–]\s*(.+?)(?:\n|$)", body)
                if cnae_m:
                    c["cnae_code"] = cnae_m.group(1)
                    c["industry"] = cnae_m.group(2).strip()[:256]

                phone_m = re.search(r"(\d{9})\s+\d{9}", body)
                if phone_m:
                    c["phone"] = phone_m.group(1)

                emp_m = re.search(r"[Nn]úmero empleados\s*\n?\s*(\d[\d.]*)", body)
                if emp_m:
                    c["employee_count"] = emp_m.group(1).replace(".", "")

                addr_m = re.search(r"((?:CALLE|PASEO|AVENIDA|PLAZA|C/|CL |PG )[^\n]+?\([A-Z]+\))", body, re.I)
                if addr_m:
                    c["address"] = addr_m.group(1).strip()
                    city_m = re.search(r"\(([^)]+)\)\s*$", addr_m.group(1))
                    if city_m:
                        c["city"] = city_m.group(1).strip().title()

                obj_m = re.search(r"Objeto social\s*\n\s*(.+?)(?:\nCNAE|\nCIF|\nFecha)", body, re.S)
                if obj_m:
                    c["summary"] = obj_m.group(1).strip()[:500]

                # Website — skip Axesor and partner links
                excluded = ("empresia", "axesor", "einforma", "infocif", "google", "facebook")
                web_els = await page.query_selector_all("a[href^='http']")
                for wel in web_els:
                    href = await wel.get_attribute("href")
                    if href and not any(d in href.lower() for d in excluded):
                        wtext = await wel.inner_text()
                        if "www" in href or "http" in wtext.lower():
                            c["website_url"] = href
                            c["domain"] = urlparse(href).netloc.replace("www.", "")
                            break

                companies.append(c)
                logger.info(f"  Saved: {c['legal_name']} (CIF: {c.get('cif', 'N/A')})")
    finally:
        await context.close()
        await pw.stop()

    return companies


# ---------------------------------------------------------------------------
# LibreBOR (Playwright)
# ---------------------------------------------------------------------------

LIBREBOR_PROVINCES = {
    "BARCELONA": "barcelona", "MADRID": "madrid", "VALENCIA": "valencia",
    "SEVILLA": "sevilla", "MALAGA": "malaga", "ALICANTE": "alicante",
    "ZARAGOZA": "zaragoza", "BILBAO": "bizkaia", "MURCIA": "murcia",
}

async def scrape_librebor(region, city, limit, headless=False, **kwargs):
    pw, context, page = await launch_browser("librebor", headless)
    companies = []
    BASE = "https://librebor.me"
    province = LIBREBOR_PROVINCES.get(region.upper(), region.lower())

    try:
        logger.info("LibreBOR: navigating to homepage...")
        await page.goto(BASE, wait_until="domcontentloaded", timeout=30000)
        await human_delay(3, 5)
        if not await wait_for_challenge(page):
            return companies

        logger.info("LibreBOR: access granted!")

        pnum = 1
        while True:
            if limit and len(companies) >= limit:
                break

            await human_delay()
            api_url = f"{BASE}/borme/api/v1/empresa/provincia/{province}/?page={pnum}"
            logger.info(f"LibreBOR: API page {pnum}")

            try:
                await page.goto(api_url, wait_until="domcontentloaded", timeout=30000)
            except Exception:
                break

            if not await wait_for_challenge(page, timeout=120):
                break

            try:
                body = await page.inner_text("body")
                data = json.loads(body)
            except (json.JSONDecodeError, Exception):
                # HTML fallback
                content = await page.content()
                links = re.findall(r'href="(/borme/empresa/[^"]+)"', content)
                if not links:
                    break
                for link in links:
                    if limit and len(companies) >= limit:
                        break
                    await human_delay()
                    try:
                        await page.goto(f"{BASE}{link}", wait_until="domcontentloaded", timeout=20000)
                    except Exception:
                        continue
                    if not await wait_for_challenge(page, timeout=60):
                        continue
                    text = await page.inner_text("body")
                    h1 = await page.query_selector("h1")
                    if not h1:
                        continue
                    name = (await h1.inner_text()).strip()
                    if not name:
                        continue
                    c = {"legal_name": name.upper(), "source_portal": "librebor",
                         "source_url": f"{BASE}{link}", "city": region.title(),
                         "province": region.title(), "region": region.title()}
                    cif_m = re.search(r"CIF:\s*([A-Z]\d{7,8})", text)
                    if cif_m:
                        c["cif"] = cif_m.group(1)
                    cnae_m = re.search(r"CNAE:\s*(\d{3,4})", text)
                    if cnae_m:
                        c["cnae_code"] = cnae_m.group(1)
                    companies.append(c)
                pnum += 1
                continue

            results = data.get("results", [])
            if not results:
                break
            for item in results:
                if limit and len(companies) >= limit:
                    break
                name = item.get("name", "").strip()
                if not name:
                    continue
                c = {"legal_name": name.upper(), "source_portal": "librebor",
                     "city": region.title(), "province": region.title(), "region": region.title()}
                if item.get("url"):
                    c["source_url"] = item["url"]
                if item.get("cif"):
                    c["cif"] = item["cif"]
                if item.get("cnae") and len(str(item["cnae"])) <= 20:
                    c["cnae_code"] = str(item["cnae"])
                companies.append(c)

            if not data.get("next"):
                break
            pnum += 1
    finally:
        await context.close()
        await pw.stop()

    return companies


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

SCRAPERS = {
    "empresite": scrape_empresite,
    "europages": scrape_europages,
    "paginasamarillas": scrape_paginasamarillas,
    "einforma": scrape_einforma,
    "empresia": scrape_empresia,
    "librebor": scrape_librebor,
}


def main():
    parser = argparse.ArgumentParser(description="Standalone multi-portal scraper")
    parser.add_argument("--portal", required=True, choices=SCRAPERS.keys())
    parser.add_argument("--region", default="BARCELONA", help="Province (e.g. BARCELONA, MADRID)")
    parser.add_argument("--city", default=None, help="Specific city slug")
    parser.add_argument("--limit", type=int, default=None, help="Max companies to scrape")
    parser.add_argument("--output", "-o", default=None, help="Output file (default: stdout)")
    parser.add_argument("--headless", action="store_true", help="Run browser headless (may get blocked)")
    parser.add_argument("--details", action="store_true", help="Scrape detail pages (empresite)")
    parser.add_argument("--employee-min", type=int, default=10)
    parser.add_argument("--employee-max", type=int, default=200)
    parser.add_argument("--delay-min", type=float, default=4.0, help="Min delay between requests (seconds)")
    parser.add_argument("--delay-max", type=float, default=7.0, help="Max delay between requests (seconds)")
    args = parser.parse_args()

    global MIN_DELAY, MAX_DELAY
    MIN_DELAY = args.delay_min
    MAX_DELAY = args.delay_max

    scraper = SCRAPERS[args.portal]
    companies = asyncio.run(scraper(
        region=args.region.upper(),
        city=args.city.upper() if args.city else None,
        limit=args.limit,
        headless=args.headless,
        details=args.details,
        employee_min=args.employee_min,
        employee_max=args.employee_max,
    ))

    # Output as JSON lines
    out = open(args.output, "w") if args.output else sys.stdout
    try:
        for company in companies:
            # Remove empty values
            company = {k: v for k, v in company.items() if v}
            out.write(json.dumps(company, ensure_ascii=False) + "\n")
    finally:
        if args.output:
            out.close()

    logger.info(f"Done: {len(companies)} companies scraped from {args.portal}")


if __name__ == "__main__":
    main()
