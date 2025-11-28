#!/usr/bin/env python3

import argparse
import datetime
import logging
import os
import re
import sys
import time
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, IntPrompt, Confirm
from rich import print as rprint
from rich.text import Text
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict, field
from typing import Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================================================
# LOGGING
# =========================================================

os.makedirs("logs", exist_ok=True)
os.makedirs("output", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/scraper.log", encoding="utf8")
    ]
)
log = logging.getLogger("scraper")

console = Console()

# =========================================================
# BANNER
# =========================================================
def show_banner():
    banner = Text()
    banner.append("\n")
    banner.append(" " * 15 + "╔" + "═" * 50 + "╗\n")
    banner.append(" " * 15 + "║" + " " * 50 + "║\n")
    banner.append(" " * 15 + "║   ███╗   ███╗  █████╗  █████╗ ███████╗ ██████╗ █████╗ ██████╗ \n")
    banner.append(" " * 15 + "║   ████╗ ████║ ██╔══██╗ ██╔══██╗ ██╔══██╗ ██╔════╝ ██╔══██╗ ║\n")
    banner.append(" " * 15 + "║   ██╔████╔██║ ███████║ ███████║ ███████║ ██████╗  ██████╔╝ ║\n")
    banner.append(" " * 15 + "║   ██║╚██╔╝██║ ██╔══██║ ██╔══██║ ╚════██║ ██╔═══╝  ██╔══██╗ ║\n")
    banner.append(" " * 15 + "║   ██║ ╚═╝ ██║ ██║  ██║ ██║  ██║ ███████║ ██████╗  ██║  ██║ ║\n")
    banner.append(" " * 15 + "║   ╚═╝     ╚═╝ ╚═╝  ╚═╝ ╚═╝  ╚═╝ ╚══════╝ ╚═════╝  ╚═╝  ╚═╝ ║\n")
    banner.append(" " * 15 + "║" + " " * 50 + "║\n")
    banner.append(" " * 15 + "╚" + "═" * 50 + "╝\n\n")
    
    title = Text("MapScrap", style="bold cyan")
    author = Text("by MrHumildad", style="bold magenta")
    
    banner.append(" " * 22)
    banner.append(title)
    banner.append("\n" + " " * 27)
    banner.append(author)
    banner.append("\n\n")

    console.print(banner, justify="center")

# =========================================================
# USER INPUT
# =========================================================
def get_user_inputs():
    console.clear()
    show_banner()

    console.print(Panel(
        "[bold green]Welcome to MapScrap![/bold green]\nConfigure your scraping session below.",
        title="MapScrap Configuration",
        style="bold blue"
    ))

    search_query = Prompt.ask(" [cyan]Search query[/cyan]", default="cafes in Barcelona")
    total_results = IntPrompt.ask(" [cyan]How many results?[/cyan]", default=20)
    workers = IntPrompt.ask(" [cyan]Number of workers (threads)[/cyan]", default=10)
    headless = Confirm.ask(" [yellow]Run in headless mode?[/yellow]", default=True)

    console.print("\n[bold green]Starting MapScrap...[/bold green]\n")
    return {
        "search": search_query,
        "total": total_results,
        "workers": workers,
        "headless": headless
    }

# =========================================================
# EMAIL SCRAPER UTILITIES
# =========================================================

EMAIL_PATHS = [
    "", "contact", "contacto", "contacte", "contacts",
    "about", "about-us", "info", "information"
]

EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE
)

def fetch_html(url: str) -> Optional[str]:
    if not url or not url.startswith("http"):
        url = "https://" + url.lstrip("https:/")
    try:
        return requests.get(url, timeout=12, verify=False, headers={"User-Agent": "Mozilla/5.0"}).text
    except:
        return None


def extract_email(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")

    # 1) mailto
    for a in soup.find_all("a", href=True):
        h = a["href"].lower()
        if h.startswith("mailto:"):
            email = h.replace("mailto:", "").split("?")[0]
            if EMAIL_REGEX.search(email):
                return email

    # 2) raw regex
    m = EMAIL_REGEX.search(soup.get_text(" "))
    return m.group(0) if m else None


def smart_email_scrape(website: str) -> dict:
    """
    MapScrap 2025 — Extrae TODO:
    email, IG, WhatsApp, FB, TikTok, LinkedIn
    """
    result = {
        "email": "", "email_source": "",
        "instagram_username": "", "instagram_url": "",
        "whatsapp_number": "", "facebook_url": "",
        "tiktok_username": "", "tiktok_url": "",
        "linkedin_url": ""
    }

    if not website:
        return result

    base = website.strip().rstrip("/")
    if not base.startswith("http"):
        base = "https://" + base.replace("https://", "").replace("http://", "")

    log.info(f"Extracting ALL contacts from {base}")

    html = fetch_html(base)
    if not html:
        return result

    soup = BeautifulSoup(html, "html.parser")

    # 1. Email clásico
    email = extract_email(html)
    if email and not result["email"]:
        result["email"] = email
        result["email_source"] = base
        log.info(f"  Email en web → {email}")

    # 2. Recorrer todos los <a href>
    for a in soup.find_all("a", href=True):
        href = a["href"].lower().strip()
        url = a["href"]

        # ── Instagram ──
        if "instagram.com" in href and not result["instagram_username"]:
            m = re.search(r"instagram\.com/([a-zA-Z0-9._]+)", href)
            if m:
                username = m.group(1).split("?")[0].rstrip("/")
                if username and username != "p" and not username.startswith(("explore", "accounts", "reel")) and 3 <= len(username) <= 30:
                    result["instagram_username"] = username
                    result["instagram_url"] = f"https://instagram.com/{username}"
                    log.info(f"  Instagram → @{username}")

                    # Bonus: email en bio de IG
                    ig_html = fetch_html(result["instagram_url"])
                    if ig_html and not result["email"]:
                        bio_mail = EMAIL_REGEX.search(ig_html)
                        if bio_mail:
                            result["email"] = bio_mail.group(0)
                            result["email_source"] = result["instagram_url"]
                            log.info(f"  EMAIL EN BIO IG → {result['email']}")

        # ── TikTok ──
        if "tiktok.com" in href and not result["tiktok_username"]:
            m = re.search(r"tiktok\.com/@([a-zA-Z0-9._]+)", href)
            if m:
                username = m.group(1).split("?")[0]
                if 1 <= len(username) <= 40:
                    result["tiktok_username"] = username
                    result["tiktok_url"] = f"https://tiktok.com/@{username}"
                    log.info(f"  TikTok → @{username}")

        # ── WhatsApp ──
        if ("wa.me" in href or "api.whatsapp.com" in href) and not result["whatsapp_number"]:
            m = re.search(r"wa\.me/(\d+)|phone=(\d+)", href)
            if m:
                phone = m.group(1) or m.group(2)
                result["whatsapp_number"] = phone
                log.info(f"  WhatsApp → {phone}")

        # ── Facebook ──
        if "facebook.com" in href and not result["facebook_url"]:
            clean = url.split("?")[0].split("#")[0].rstrip("/")
            if clean.count("/") >= 3:  # es un perfil/página real
                result["facebook_url"] = clean
                log.info(f"  Facebook → {clean}")

        # ── LinkedIn ──
        if "linkedin.com" in href and not result["linkedin_url"]:
            if "/in/" in href or "/company/" in href:
                clean = url.split("?")[0].rstrip("/")
                result["linkedin_url"] = clean
                log.info(f"  LinkedIn → {clean}")

    return result

# =========================================================
# DATA MODELS
# =========================================================

@dataclass
class Business:
    name: str = ""
    address: str = ""
    domain: str = ""
    website: str = ""
    phone_number: str = ""
    category: str = ""
    location: str = ""
    reviews_count: Optional[int] = None
    reviews_average: Optional[float] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    email: str = ""
    email_source: str = ""
    instagram_username: str = ""
    instagram_url: str = ""
    whatsapp_number: str = ""
    facebook_url: str = ""
    tiktok_username: str = ""        # ← NUEVO
    tiktok_url: str = ""             # ← NUEVO
    linkedin_url: str = ""           # ← NUEVO
    def __hash__(self):
        return hash((self.name, self.website, self.domain, self.phone_number))


@dataclass
class BusinessList:
    businesses: list = field(default_factory=list)
    seen: set = field(default_factory=set)
    date: str = datetime.datetime.now().strftime("%Y-%m-%d")

    @property
    def folder(self):
        out = os.path.join("output", self.date)
        os.makedirs(out, exist_ok=True)
        return out

    def add(self, b: Business):
        key = hash(b)
        if key not in self.seen:
            self.seen.add(key)
            self.businesses.append(b)

    def df(self) -> pd.DataFrame:
        return pd.json_normalize((asdict(x) for x in self.businesses))


# =========================================================
# PLAYWRIGHT SAFE HELPERS
# =========================================================

def safe_text(page, selector: str) -> str:
    """
    Playwright-safe text getter that doesn't raise strict mode errors.
    """
    try:
        loc = page.locator(selector)
        if loc.count():
            return loc.nth(0).inner_text().strip()
    except Exception:
        pass
    return ""


def safe_attr(page, selector: str, attr: str) -> Optional[str]:
    try:
        loc = page.locator(selector)
        if loc.count():
            return loc.nth(0).get_attribute(attr)
    except Exception:
        pass
    return None


def extract_coordinates(url: str):
    if "/@" not in url:
        return None, None
    try:
        coords = url.split("/@")[1].split("/")[0].split(",")
        return float(coords[0]), float(coords[1])
    except:
        return None, None


# =========================================================
# GOOGLE MAPS SCRAPER (STABLE 2025)
# =========================================================

def scrape_gmaps(search: str, total: int, headless: bool) -> BusinessList:
    log.info(f"Iniciando MapScrap ULTRA 2025 → {search}")
    bl = BusinessList()

    with sync_playwright() as p:
        # Lanzar con argumentos anti-detección
        browser = p.chromium.launch(
            headless=headless,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--window-position=0,0",
                "--ignore-certificate-errors",
                "--ignore-certificate-errors-spki-list",
                "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ]
        )

        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            timezone_id="America/New_York"
        )
        context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => false})")

        page = context.new_page()

        try:
            page.goto("https://www.google.com/maps", timeout=90000)
            page.wait_for_load_state("domcontentloaded")
            log.info("Google Maps cargado")

   # RECHAZAR COOKIES 2025 — FUNCIONA EN TODA ESPAÑA
            log.info("Intentando rechazar cookies...")

            # 1. Esperar a que aparezca el banner (puede tardar)
            page.wait_for_timeout(8000)

            # 2. Lista de selectores que funcionan en español 2025
            reject_selectors = [
                'button:has-text("Rechazar todo")',
                'button:has-text("Rechazar Todo")',
                'button:has-text("Reject all")',
                '[aria-label*="Rechazar todo" i]',
                '[aria-label*="Reject all" i]',
                'span:has-text("Rechazar todo")',
                '//span[contains(text(), "Rechazar todo")]',
                '//button[contains(., "Rechazar todo")]',
                '[jsaction*="reject"]',
                'form[action*="consent.google"] button:nth-child(2)',  # el segundo botón suele ser "Rechazar todo"
            ]

            rejected = False
            for sel in reject_selectors:
                try:
                    if sel.startswith("//"):
                        page.click(sel, timeout=6000)
                    else:
                        page.click(sel, timeout=6000)
                    log.info(f"Cookies rechazadas con selector: {sel}")
                    rejected = True
                    page.wait_for_timeout(3000)
                    break
                except Exception as e:
                    continue

            # 3. SI NO FUNCIONA NINGUNO → FORZAR EN EL IFRAME (el truco definitivo)
            if not rejected:
                try:
                    page.wait_for_selector('iframe[src*="consent.google.com"]', timeout=10000)
                    iframe = page.frames[-1]  # el último iframe suele ser el de consentimiento
                    iframe.click('button:has-text("Rechazar todo")', timeout=8000)
                    log.info("Cookies rechazadas DENTRO DEL IFRAME (truco español 2025)")
                    rejected = True
                except:
                    pass

            if not rejected:
                log.warning("No se pudo rechazar cookies automáticamente — continuamos igual (a veces no pasa nada)")
            else:
                log.info("Cookies rechazadas con éxito")
            # MÉTODO INFALIBLE PARA ENCONTRAR EL CAMPO DE BÚSQUEDA
            search_box = None
            selectors_to_try = [
                '#searchboxinput',
                'input[placeholder*="Search Google Maps" i]',
                'input[aria-label*="Search Google Maps" i]',
                'input[role="combobox"]',
                'input[type="text"]',
                '//input[contains(@placeholder, "Search")]',
                'input'
            ]

            for selector in selectors_to_try:
                try:
                    if selector.startswith("//"):
                        search_box = page.wait_for_selector(selector, timeout=8000)
                    else:
                        search_box = page.wait_for_selector(selector, timeout=8000)
                    if search_box and search_box.is_visible():
                        log.info(f"Campo de búsqueda encontrado con: {selector}")
                        break
                except:
                    continue

            if not search_box:
                log.error("No se encontró el campo de búsqueda. Posible bloqueo.")
                browser.close()
                return bl

            # ESCRIBIR Y BUSCAR
            search_box.click()
            page.wait_for_timeout(1000)
            search_box.fill("")
            page.wait_for_timeout(500)
            search_box.fill(search)
            page.wait_for_timeout(1000)
            page.keyboard.press("Enter")
            log.info(f"Búsqueda realizada: {search}")

            # ESPERAR RESULTADOS
            page.wait_for_timeout(8000)
            try:
                page.wait_for_selector('a[href*="maps/place"]', timeout=20000)
            except:
                log.error("No hay resultados o Google bloqueó")
                browser.close()
                return bl

            # SCROLL AUTOMÁTICO HASTA TENER SUFICIENTES
            scroll_pause = 2500
            last_height = page.evaluate("document.body.scrollHeight")

            while len(bl.businesses) < total:
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(scroll_pause)
                
                links = page.locator('a[href*="maps/place"]').all()
                if len(links) >= total:
                    links = links[:total]
                    break

                new_height = page.evaluate("document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height

            # EXTRAER CADA NEGOCIO
            links = page.locator('a[href*="maps/place"]').all()[:total]
            log.info(f"Encontrados {len(links)} enlaces. Extrayendo datos...")

            for i, link in enumerate(links, 1):
                try:
                    link.click(force=True)
                    page.wait_for_timeout(4000)

                    b = Business()

                    # NOMBRE
                    try:
                        b.name = page.locator('h1').first.inner_text(timeout=7000).strip()
                    except:
                        b.name = "Nombre no encontrado"

                    # DIRECCIÓN
                    try:
                        b.address = page.locator('//button[@data-item-id="address"]//div[contains(@class,"fontBody")]').first.inner_text(timeout=4000).strip()
                    except:
                        b.address = ""

                    # WEB
                    try:
                        web_text = page.locator('//a[@data-item-id="authority"]//div[contains(@class,"fontBody")]').first.inner_text(timeout=3000).strip()
                        b.website = web_text if web_text.startswith("http") else "https://" + web_text
                        b.domain = b.website.split("/")[2].replace("www.", "") if "http" in b.website else web_text
                    except:
                        b.website = ""
                        b.domain = ""

                    # TELÉFONO
                    try:
                        b.phone_number = page.locator('//button[contains(@data-item-id,"phone")]//div[contains(@class,"fontBody")]').first.inner_text(timeout=3000).strip()
                    except:
                        b.phone_number = ""

                    # COORDENADAS
                    b.latitude, b.longitude = extract_coordinates(page.url)

                    # CATEGORÍA
                    if " in " in search.lower():
                        b.category, b.location = [x.strip().title() for x in search.lower().split(" in ")]
                    else:
                        b.category = search.title()

                    bl.add(b)
                    log.info(f"  {i}/{len(links)} → {b.name} | {b.website or 'sin web'} | {b.phone_number or 'sin tel'}")

                except Exception as e:
                    log.error(f"Error en negocio {i}: {e}")
                    continue

        except Exception as e:
            log.error(f"Error crítico: {e}")

        finally:
            browser.close()

    log.info(f"EXTRACCIÓN COMPLETA: {len(bl.businesses)} negocios")
    return bl


# =========================================================
# PIPELINE
# =========================================================

def run_pipeline(search: str, total: int, headless: bool, workers: int):
    bl = scrape_gmaps(search, total, headless)

    df = bl.df()
    log.info(f"📊 GMaps scraped: {len(df)} businesses")

    if len(df) == 0:
        out = os.path.join(bl.folder, f"{search.replace(' ', '_')}_FINAL.csv")
        df.to_csv(out, index=False)
        log.warning(f"No results. Saved empty CSV: {out}")
        return

    # parallel email scraping → AHORA ES CONTACT SCRAPING
    websites = df["website"].fillna("").tolist()
    contact_results = [{} for _ in websites]

    log.info(f"Extracting Instagram, WhatsApp, emails... ({workers} workers)")

    with ThreadPoolExecutor(max_workers=workers) as ex:
        future_to_idx = {
            ex.submit(smart_email_scrape, w): i
            for i, w in enumerate(websites) if w
        }

        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                data = future.result()
                contact_results[idx] = data
            except:
                contact_results[idx] = {}

    # Aplicar todos los datos al DataFrame
    for key in ["email", "email_source", "instagram_username", "instagram_url",
                "whatsapp_number", "facebook_url"]:
        df[key] = [contact_results[i].get(key, "") for i in range(len(df))]

    out = f"{re.sub(r'[^A-Za-z0-9_-]', '_', search)}_FINAL.csv"


    df.to_csv(out, index=False)
    #log the data collected in table
    log.info(df)
    log.info(f"💾 Saved {out}")


# =========================================================
# CLI
# =========================================================


console = Console()

if __name__ == "__main__":
    try:
        args = get_user_inputs()

        log.info(f"Starting scraper → {args['search']} | Results: {args['total']} | Workers: {args['workers']} | Headless: {args['headless']}")

        run_pipeline(
            search=args["search"],
            total=args["total"],
            headless=args["headless"],
            workers=args["workers"]
        )

        rprint("\n[bold green]🎉 All done! Check your output folder.[/bold green]")
        log.info("Scraping finished successfully.")

    except KeyboardInterrupt:
        rprint("\n\n[yellow]Scraping cancelled by user. See you later! 👋[/yellow]")
        sys.exit(0)
    except Exception as e:
        rprint(f"\n[bold red]Unexpected error:[/bold red] {e}")
        log.error(f"Crash: {traceback.format_exc()}")
        sys.exit(1)
    
