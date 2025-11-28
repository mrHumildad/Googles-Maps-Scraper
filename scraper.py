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


def smart_email_scrape(website: str) -> Tuple[Optional[str], Optional[str]]:
    """Check homepage + internal contacts."""
    if not website:
        return None, None

    base = website.strip().rstrip("/")
    log.info(f"🔍 Searching email in {base}")

    for p in EMAIL_PATHS:
        url = base if p == "" else f"{base}/{p}"
        """ log.info(f"  Trying {url}") """

        html = fetch_html(url)
        if not html:
            continue

        email = extract_email(html)
        if email:
            log.info(f"  ✅ Found email {email} at {url}")
            return email, url

    log.info(f"  ❌ No email found for {base}")
    return None, None


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
    log.info(f"🚀 Scraping Google Maps for: {search}")

    bl = BusinessList()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page(locale="en-GB")

        # Load maps
        page.goto("https://www.google.com/maps", timeout=30000)
        time.sleep(1)
        # --- CODE TO CLICK THE REJECT ALL BUTTON ---
        print("Attempting to click 'Reject all' cookie button...")
        
        # 1. Wait for the button to be visible/enabled
        # Playwright recommends using `page.wait_for_selector` for robustness.
        # The selector is based on the aria-label which is specific.
        try:
            reject_button_selector = '[aria-label="Reject all"]'
            page.wait_for_selector(reject_button_selector, state="visible", timeout=5000)
            
            # 2. Click the button
            page.click(reject_button_selector)
            print("'Reject all' button clicked successfully.")
            
        except Exception as e:
            # Handle the case where the cookie dialog doesn't appear 
            # (e.g., if you're already logged in or have been cookied)
            print(f"Cookie button not found or could not be clicked: {e}")
        # Search
        page.fill("#searchboxinput", search)
        page.keyboard.press("Enter")
        page.wait_for_timeout(3000)

        # Scroll panel
        feed = '//div[@role="feed"]'
        try:
            page.wait_for_selector(feed, timeout=8000)
        except:
            log.error("No feed container found. Zero results.")
            return bl

        panel = page.locator(feed).nth(0)
        last = 0
        stable = 0

        while True:
            try:
                panel.evaluate("el => el.scrollTop = el.scrollHeight")
            except:
                page.evaluate("window.scrollBy(0, 1500)")

            page.wait_for_timeout(1800)

            count = page.locator('//a[contains(@href, "/maps/place/")]').count()
            log.info(f"  Listings loaded: {count}")

            if count >= total:
                break

            if count == last:
                stable += 1
                if stable >= 3:
                    break
            else:
                last = count
                stable = 0

        links = page.locator('//a[contains(@href, "/maps/place/")]').all()[:total]

        # Loop listings
        for i, link in enumerate(links):
            try:
                link.click()
                page.wait_for_timeout(2200)

                b = Business()

                b.name = safe_text(page, 'h1.DUwDvf')
                b.address = safe_text(page, '//button[@data-item-id="address"]//div[contains(@class,"Io6YTe")]')

                dom = safe_text(page, '//a[@data-item-id="authority"]//div[contains(@class,"Io6YTe")]')
                if dom:
                    b.domain = dom
                    b.website = "https://" + dom

                b.phone_number = safe_text(page, '//button[contains(@data-item-id,"phone")]//div[contains(@class,"Io6YTe")]')

                # reviews count
                rc = safe_text(page, '//span[@class="UY7F9"]')
                rc = rc.replace(",", "")
                b.reviews_count = int(rc) if rc.isdigit() else None

                # reviews average
                aria = safe_attr(page, '//div[contains(@aria-label, "stars")]', 'aria-label')
                if aria:
                    nums = re.findall(r"\d+(\.\d+)?", aria.replace(",", "."))
                    b.reviews_average = float(nums[0]) if nums else None

                # coords
                b.latitude, b.longitude = extract_coordinates(page.url)

                # category / location
                if " in " in search:
                    b.category, b.location = search.split(" in ", 1)
                else:
                    b.category = search

                bl.add(b)

                log.info(f"  ✔ {i+1}/{len(links)} {b.name} — {b.website or 'no-site'}")

            except Exception as e:
                log.error(f"Error on listing {i+1}: {e}")
                continue

        browser.close()

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

    # parallel email scraping
    websites = df["website"].fillna("").tolist()
    results = [None] * len(websites)

    log.info("⚡ Email scraping using %d workers", workers)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        future_map = {
            ex.submit(smart_email_scrape, w): i
            for i, w in enumerate(websites)
        }

        for fut in as_completed(future_map):
            idx = future_map[fut]
            try:
                results[idx] = fut.result()
            except:
                results[idx] = (None, None)

    df["email"] = [r[0] for r in results]
    df["email_source"] = [r[1] for r in results]

    out = f"{re.sub(r'[^A-Za-z0-9_-]', '_', search)}_FINAL.csv"


    df.to_csv(out, index=False)
    #log the data collected in table
    log.info(df)
    log.info(f"💾 Saved {out}")


# =========================================================
# CLI
# =========================================================

""" def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("-s", "--search", required=True)
    ap.add_argument("-t", "--total", type=int, default=20)
    ap.add_argument("-w", "--workers", type=int, default=10)
    ap.add_argument("--headless", action="store_true")
    ap.add_argument("--no-headless", dest="headless", action="store_false")
    ap.set_defaults(headless=True)
    return ap.parse_args()


if __name__ == "__main__":
    args = parse_args() """
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
    
