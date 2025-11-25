import pandas as pd
import smtplib
import dns.resolver
import argparse
import socket
import sys
import re

# ------------------------------------------------------------------------
# Extract domain from URL
# ------------------------------------------------------------------------

def domain_from_url(url):
    try:
        if "://" in url:
            url = url.split("://")[1]
        return url.split("/")[0].replace("www.", "")
    except:
        return None

# ------------------------------------------------------------------------
# Get MX records for domain
# ------------------------------------------------------------------------

def get_mx_records(domain):
    try:
        answers = dns.resolver.resolve(domain, 'MX')
        return [str(r.exchange).rstrip('.') for r in answers]
    except Exception as e:
        print(f"❌ MX lookup failed for {domain}: {e}")
        return []

# ------------------------------------------------------------------------
# SMTP verify (RCPT TO handshake)
# ------------------------------------------------------------------------

def smtp_check(email, mx_records):
    for mx in mx_records:
        try:
            print(f"📡 Connecting to {mx} to verify {email}…")

            server = smtplib.SMTP(timeout=8)
            server.connect(mx)
            server.helo("example.com")
            server.mail("probe@example.com")

            code, msg = server.rcpt(email)
            server.quit()

            if code in [250, 251]:
                print(f"✅ VERIFIED: {email}")
                return True
            else:
                print(f"❌ Rejected: {email} -> {code}")
        except Exception as e:
            print(f"⚠️ SMTP error on {mx}: {e}")

    return False

# ------------------------------------------------------------------------
# Generate candidate emails
# ------------------------------------------------------------------------

CANDIDATES = ["info", "contact", "hello", "admin"]

def guess_candidates(domain):
    return [f"{c}@{domain}" for c in CANDIDATES]

# ------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file_path", type=str, required=True)
    args = parser.parse_args()

    df = pd.read_csv(args.file_path)

    if "website" not in df.columns or "email" not in df.columns:
        print("❌ CSV must contain 'website' and 'email' columns")
        sys.exit()

    df["verified_email"] = ""

    print("\n🔍 Starting REAL email verification…\n")

    for i, row in df.iterrows():
        website = row["website"]
        existing_email = row["email"]

        if isinstance(existing_email, str) and "@" in existing_email:
            print(f"⏭️ Row {i}: already has email → {existing_email}")
            df.loc[i, "verified_email"] = existing_email
            continue

        # Extract domain
        domain = domain_from_url(website)
        if not domain:
            print(f"⚠️ No valid domain in website: {website}")
            continue

        print(f"\n==============================")
        print(f"🌐 Row {i}: checking domain {domain}")
        print("==============================")

        # Get MX records
        mx_records = get_mx_records(domain)
        if not mx_records:
            print("❌ No MX records found — skipping.")
            continue
        # Skip Google MX
        if any("google.com" in mx.lower() for mx in mx_records):
            print("⚠️ Google MX detected — cannot verify. Leaving email as None.")
            continue
        # Try candidates
        for cand in guess_candidates(domain):
            if smtp_check(cand, mx_records):
                df.loc[i, "verified_email"] = cand
                break

    # Save result
    out_path = args.file_path.replace(".csv", "_verified.csv")
    df.to_csv(out_path, index=False)
    print(f"\n🎉 DONE! Saved: {out_path}\n")


if __name__ == "__main__":
    main()
