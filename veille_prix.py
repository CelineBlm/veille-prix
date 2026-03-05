"""
veille_prix.py — Veille concurrentielle maplatine.com
Architecture : Tavily (search) + BeautifulSoup (extraction) + gspread (sheets)
Zero Claude API — extraction via JSON-LD / Meta / regex avec timeout par requete
"""

import os, re, json, time, logging, gspread
from datetime import datetime
from google.oauth2.service_account import Credentials
import requests
from bs4 import BeautifulSoup

# ── LOGGING ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# ── CONFIGURATION ─────────────────────────────────────────────
TAVILY_API_KEY  = os.environ.get("TAVILY_API_KEY", "tvly-XXXX")
GOOGLE_JSON     = os.environ.get("GOOGLE_JSON")           # JSON service account (GitHub Secret)
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "")  # ID du Google Sheet (GitHub Secret)

MAX_URLS        = 6    # resultats Tavily par produit
REQUEST_TIMEOUT = 8    # secondes max par requete HTTP (timeout dur)
DELAY_BETWEEN   = 0.3  # secondes entre requetes

# Patterns URL a rejeter (listing/categorie)
URL_BLACKLIST = [
    r"/marque/", r"/brand/", r"/categorie/", r"/category/", r"/collection/",
    r"/recherche", r"/search", r"srsltid=", r"\?q=", r"\?s=",
    r"/blog/", r"/forum/", r"/avis/", r"/guide/", r"/news/",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ── GOOGLE SHEETS ─────────────────────────────────────────────
def get_gspread_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    if GOOGLE_JSON:
        creds = Credentials.from_service_account_info(json.loads(GOOGLE_JSON), scopes=scopes)
    else:
        creds = Credentials.from_service_account_file("google_credentials.json", scopes=scopes)
    return gspread.authorize(creds)


def read_products(client):
    sh   = client.open_by_key(GOOGLE_SHEET_ID)
    ws   = sh.get_worksheet(0)        # Catalogue = onglet index 0
    rows = ws.get_all_values()

    products = []
    # ligne 0 = titre, ligne 1 = en-tetes -> donnees a partir de ligne 2
    for row in rows[2:]:
        if len(row) < 2:
            continue
        ref     = str(row[0]).strip()
        libelle = str(row[1]).strip()
        if not ref or not libelle:
            continue
        prix_mpl = str(row[2]).strip() if len(row) > 2 else ""
        try:
            prix_mpl = float(
                prix_mpl.replace("€", "").replace("\xa0", "")
                        .replace(" ", "").replace(",", ".")
            ) if prix_mpl else None
        except ValueError:
            prix_mpl = None
        products.append({"ref": ref, "libelle": libelle, "prix_mpl": prix_mpl})

    log.info(f"{len(products)} produits charges")
    return products


def write_rows(client, rows: list):
    sh = client.open_by_key(GOOGLE_SHEET_ID)
    ws = sh.get_worksheet(1)          # Historique Prix = onglet index 1
    if rows:
        ws.append_rows(rows, value_input_option="USER_ENTERED")
        log.info(f"  -> {len(rows)} ligne(s) ecrite(s)")


# ── TAVILY : RECHERCHE ────────────────────────────────────────
def tavily_search(libelle: str) -> list:
    try:
        resp = requests.post(
            "https://api.tavily.com/search",
            json={
                "api_key":             TAVILY_API_KEY,
                "query":               f"{libelle} prix acheter france",
                "search_depth":        "basic",
                "max_results":         12,
                "include_raw_content": False,
                "include_answer":      False,
                "exclude_domains": [
                    "maplatine.com", "facebook.com", "instagram.com",
                    "youtube.com", "wikipedia.org", "reddit.com",
                    "pinterest.com", "leboncoin.fr", "vinted.fr",
                    "tiktok.com", "twitter.com",
                ],
            },
            timeout=15,
        )
        if resp.status_code != 200:
            return []

        results = resp.json().get("results", [])
        items   = []
        seen    = set()

        for r in results:
            url = r.get("url", "")
            if not url or not is_product_url(url):
                continue
            domain = extract_domain(url)
            if domain in seen:
                continue
            seen.add(domain)
            items.append({
                "url":     url,
                "title":   r.get("title", ""),
                "snippet": r.get("content", ""),
            })
            if len(items) >= MAX_URLS:
                break

        return items
    except Exception as e:
        log.warning(f"  Tavily erreur: {e}")
        return []


# ── FILTRAGE URL ──────────────────────────────────────────────
def is_product_url(url: str) -> bool:
    u = url.lower()
    for pattern in URL_BLACKLIST:
        if re.search(pattern, u):
            return False
    return True


# ── EXTRACTION PRIX ───────────────────────────────────────────
def extract_price(item: dict):
    """
    Cascade :
    1. Snippet Tavily (instantane, zero requete)
    2. Titre (parfois contient le prix)
    3. Scraping page avec timeout dur de REQUEST_TIMEOUT secondes
    """
    p = price_from_text(item["snippet"])
    if p:
        return p

    p = price_from_text(item["title"])
    if p:
        return p

    return scrape_page(item["url"])


def price_from_text(text: str):
    """Extrait un prix depuis un texte court (snippet/titre)."""
    if not text:
        return None
    patterns = [
        r"(\d{1,5})[,.](\d{2})\s*€",
        r"(\d{1,5})\s*€",
        r"€\s*(\d{1,5}[,.]\d{2})",
        r"(\d{1,5}[,.]\d{2})\s*EUR",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            raw = m.group(0).replace("€", "").replace("EUR", "").strip()
            p   = clean_price(raw)
            if p:
                return p
    return None


def scrape_page(url: str):
    """Scrape une page avec timeout dur — jamais de blocage."""
    try:
        resp = requests.get(
            url,
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT,   # timeout par requete
            allow_redirects=True,
        )
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # 1. JSON-LD
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data  = json.loads(script.string or "")
                nodes = data if isinstance(data, list) else [data]
                for node in nodes:
                    p = price_from_node(node)
                    if p:
                        return p
                    for sub in node.get("@graph", []):
                        p = price_from_node(sub)
                        if p:
                            return p
            except Exception:
                continue

        # 2. Meta tags
        for prop in ["product:price:amount", "og:price:amount"]:
            tag = soup.find("meta", property=prop)
            if tag and tag.get("content"):
                p = clean_price(tag["content"])
                if p:
                    return p

        # 3. itemprop price
        tag = soup.find(attrs={"itemprop": "price"})
        if tag:
            p = clean_price(tag.get("content") or tag.get_text())
            if p:
                return p

        return None

    except requests.exceptions.Timeout:
        log.debug(f"  Timeout {REQUEST_TIMEOUT}s depasse sur {url}")
        return None
    except Exception:
        return None


def price_from_node(node: dict):
    if "offers" in node:
        off = node["offers"]
        if isinstance(off, list):
            off = off[0]
        if isinstance(off, dict) and "price" in off:
            return clean_price(str(off["price"]))
    if node.get("@type") == "Offer" and "price" in node:
        return clean_price(str(node["price"]))
    return None


def clean_price(s: str):
    if not s:
        return None
    s = str(s).replace("€", "").replace("EUR", "").replace("\xa0", "").strip()
    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        val = float(s)
        return val if 1 < val < 150_000 else None
    except ValueError:
        return None


# ── UTILITAIRES ───────────────────────────────────────────────
def extract_domain(url: str) -> str:
    try:
        return url.split("/")[2].replace("www.", "")
    except Exception:
        return url


def detect_platform(url: str) -> str:
    u = url.lower()
    for key, name in {
        "amazon": "amazon", "fnac": "fnac", "darty": "darty",
        "boulanger": "boulanger", "cdiscount": "cdiscount",
        "son-video": "son-video", "hifilink": "hifilink",
        "ebay": "ebay", "ldlc": "ldlc", "rakuten": "rakuten",
        "cultura": "cultura", "sono-elec": "sono-elec",
    }.items():
        if key in u:
            return name
    return "autre"


# ── MAIN ──────────────────────────────────────────────────────
def main():
    today = datetime.now().strftime("%d/%m/%Y")
    log.info(f"=== Veille prix demarree -- {today} ===")

    client   = get_gspread_client()
    products = read_products(client)
    buffer   = []

    for i, p in enumerate(products, 1):
        log.info(f"[{i}/{len(products)}] {p['ref']} -- {p['libelle'][:55]}")

        items = tavily_search(p["libelle"])
        log.info(f"  {len(items)} URL(s) trouvee(s)")

        for item in items:
            prix = extract_price(item)
            if not prix:
                continue

            ecart_eur = round(prix - p["prix_mpl"], 2) if p["prix_mpl"] else ""
            ecart_pct = round((ecart_eur / p["prix_mpl"]) * 100, 2) if p["prix_mpl"] else ""
            domain    = extract_domain(item["url"])
            sign      = "+" if isinstance(ecart_pct, float) and ecart_pct > 0 else ""
            log.info(f"  + {domain} -- {prix} EUR ({sign}{ecart_pct if isinstance(ecart_pct, float) else '?'}%)")

            buffer.append([
                today, p["ref"], p["libelle"],
                p["prix_mpl"] or "",
                domain,
                detect_platform(item["url"]),
                item["title"],
                prix, ecart_eur, ecart_pct,
                item["url"],
            ])
            time.sleep(DELAY_BETWEEN)

        if len(buffer) >= 20:
            write_rows(client, buffer)
            buffer = []

    if buffer:
        write_rows(client, buffer)

    log.info("=== Veille terminee ===")


if __name__ == "__main__":
    main()
