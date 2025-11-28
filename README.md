# MapScrap 2025  
**by MrHumildad**

![MapScrap](https://img.shields.io/badge/MapScrap-2025-00d4ff?style=for-the-badge&logo=googlemaps&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat&logo=python&logoColor=white)
![Playwright](https://img.shields.io/badge/Playwright-2025-45ba4b?style=flat&logo=playwright&logoColor=white)

**The most powerful Google Maps lead scraper of 2025** — built for real-world results in Spain, LatAm, USA and beyond.

Extracts hundreds of verified businesses with:
- Name, address, phone, website
- Email (website + Instagram bio)
- Instagram, TikTok, WhatsApp, Facebook, LinkedIn
- Coordinates, reviews & rating

Perfect for gyms, restaurants, clinics, bars, real estate, beauty salons…

---

### Tech Stack (2025 Edition)

| Technology               | Purpose                                          |
|--------------------------|--------------------------------------------------|
| **Python 3.11+**         | Core language                                    |
| **Playwright**           | Reliable, fast browser automation                |
| **playwright-stealth**   | Bypass Google bot detection (EU/Spain fix)       |
| **BeautifulSoup4**       | Parse websites & social profiles                 |
| **Rich**                 | Beautiful interactive terminal UI                |
| **Pandas**               | Data processing & clean CSV export               |
| **ThreadPoolExecutor**   | Parallel contact extraction (email + social)     |
| **Requests**             | Fast HTTP fallback (SSL-tolerant)                |

---

### Key Features

- Works 100% in 2025 (Spain, USA, LatAm)
- Handles Spanish “Rechazar todo” consent iframe
- Instagram & TikTok bio email extraction
- Auto-detects WhatsApp, LinkedIn, Facebook
- Geolocation spoofing (Madrid, Miami, etc.)
- Headless or visible mode
- Smart deduplication & infinite scroll
- 20+ column CSV ready for cold outreach

---

### Quick Start

```bash
# 1. Clone or download
git clone https://github.com/mrhumildad/mapscrap-2025.git
cd mapscrap-2025

# 2. Install
pip install playwright rich pandas beautifulsoup4 playwright-stealth lxml

# 3. Install browser
playwright install chromium

# 4. Run
python scraper.py