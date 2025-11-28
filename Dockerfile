FROM python:3.11-slim

# Install Playwright dependencies
RUN apt-get update && apt-get install -y \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 \
    libxkbcommon0 libasound2 libxcomposite1 libxdamage1 \
    libxrandr2 libgbm1 libpango-1.0-0 libpangocairo-1.0-0 \
    libatspi2.0-0 libcairo2 libgtk-3-0 wget unzip curl && \
    apt-get clean

WORKDIR /app


RUN pip install --no-cache-dir pandas requests beautifulsoup4 playwright

RUN playwright install --with-deps chromium

COPY . /app

ENTRYPOINT ["python", "scraper.py", "--headless"]

CMD ["-s", "bars in Galatina", "-t", "50"]
