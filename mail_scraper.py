import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import time

EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE
)


def fetch_html(url):
    print(f"\n🌐 Fetching URL: {url}")

    # Ensure proper scheme
    if not url.startswith("http"):
        url = "https://" + url
        print(f"🔧 Fixed URL → {url}")

    try:
        response = requests.get(url, timeout=10, headers={
            "User-Agent": "Mozilla/5.0"
        })
        print(f"🔍 HTTP status: {response.status_code}")

        if response.status_code >= 400:
            print("❌ Bad status code, skipping.")
            return None

        return response.text

    except Exception as e:
        print(f"💥 Request failed: {e}")
        return None


def extract_email_from_html(html):
    """Search HTML for mailto: first, then fallback regex."""
    print("🔎 Searching for email in HTML…")

    soup = BeautifulSoup(html, "html.parser")
    text_content = soup.get_text(separator=" ")

    # 1) MAILTO scan
    for link in soup.find_all("a", href=True):
        href = link["href"].lower()
        if "mailto:" in href:
            email = href.replace("mailto:", "").split("?")[0]
            print(f"📬 MAILTO found → {email}")
            return email

    print("⚠️ No mailto: found. Trying regex…")

    # 2) Regex fallback search
    match = EMAIL_REGEX.search(text_content)
    if match:
        print(f"📧 REGEX email found → {match.group(0)}")
        return match.group(0)

    print("❌ No email found on this page.")
    return None


def main():
    print("🚀 Starting EMAIL SCRAPER")

    df = pd.read_csv("GMaps Data/2025-11-25/promotora_espectaculos_in_Barcelona.csv")
    print(f"📄 CSV loaded with {len(df)} rows.")

    results = []

    for index, row in df.iterrows():
        print("\n============================")
        print(f"🔍 ROW {index}")
        print("============================")

        website = "" if pd.isna(row.get("website")) else str(row.get("website")).strip()

        print(f"🌐 Website: {website}")

        if not website or website.strip() == "":
            print("⚠️ No website field. Skipping.")
            results.append(None)
            continue

        html = fetch_html(website)
        if html is None:
            print("❌ No HTML retrieved. Skipping.")
            results.append(None)
            continue

        email = extract_email_from_html(html)
        results.append(email)

        print(f"✅ FINAL EMAIL for row {index}: {email}")
        time.sleep(1)  # polite delay

    df["email"] = results

    out_file = "scraped_with_emails.csv"
    df.to_csv(out_file, index=False)
    print(f"\n💾 Saved results to {out_file}")


if __name__ == "__main__":
    main()
