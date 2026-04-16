#!/usr/bin/env python3
from __future__ import annotations

import io
import json
import math
import urllib.request
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


API_URL = "http://127.0.0.1:3200/api/exchange/overview?timeframe=all"
ASSET_DIR = Path("/root/pacifica-flow/public/assets/volume-rank")
CANVAS = 96

COINGECKO_IDS = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
    "HYPE": "hyperliquid",
    "BNB": "binancecoin",
    "XRP": "ripple",
    "ZEC": "zcash",
    "ENA": "ethena",
    "ASTER": "aster",
    "PUMP": "pump-fun",
    "SUI": "sui",
    "DOGE": "dogecoin",
    "PAXG": "pax-gold",
    "KBONK": "bonk",
    "FARTCOIN": "fartcoin",
    "AAVE": "aave",
    "KPEPE": "pepe",
    "AVAX": "avalanche-2",
    "LINK": "chainlink",
    "LTC": "litecoin",
    "LIT": "litentry",
    "UNI": "uniswap",
    "WLFI": "world-liberty-financial",
    "PENGU": "pudgy-penguins",
    "XMR": "monero",
    "CRV": "curve-dao-token",
    "TAO": "bittensor",
    "LDO": "lido-dao",
    "MON": "mon-protocol",
    "TRUMP": "official-trump",
    "NEAR": "near",
    "BCH": "bitcoin-cash",
    "VIRTUAL": "virtual-protocol",
    "ADA": "cardano",
    "ZK": "zksync",
    "PIPPIN": "pippin",
    "JUP": "jupiter-exchange-solana",
    "STRK": "starknet",
    "ARB": "arbitrum",
    "ICP": "internet-computer",
    "CRCL": "circle-xstock",
    "WIF": "dogwifcoin",
    "WLD": "worldcoin-wld",
    "ZRO": "layerzero",
    "MEGA": "make-europe-great-again",
}

# Existing local custom assets that should be preserved.
CUSTOM_ONLY = {"XPL"}

EQUITY_LOGO_URLS = {
    "NVDA": "https://commons.wikimedia.org/wiki/Special:Redirect/file/NVIDIA%20logo.svg?width=256",
    "TSLA": "https://commons.wikimedia.org/wiki/Special:Redirect/file/Tesla_logo.png",
    "GOOGL": "https://commons.wikimedia.org/wiki/Special:Redirect/file/Google_2015_logo.svg?width=512",
    "PLTR": "https://commons.wikimedia.org/wiki/Special:Redirect/file/Palantir_Technologies_logo.svg?width=512",
    "HOOD": "https://commons.wikimedia.org/wiki/Special:Redirect/file/Robinhood%20Logo.png",
    "BP": "https://commons.wikimedia.org/wiki/Special:Redirect/file/Bp_logo89.svg?width=512",
}

FOREX_PAIRS = {
    "EURUSD": (("EUR", (33, 86, 171), (255, 204, 0)), ("USD", (178, 34, 52), (255, 255, 255))),
    "USDJPY": (("USD", (178, 34, 52), (255, 255, 255)), ("JPY", (255, 255, 255), (188, 0, 45))),
}

FALLBACK_LABELS = {
    "USDJPY": "UJ",
    "EURUSD": "EU",
    "XAU": "Au",
    "XAG": "Ag",
    "CL": "CL",
    "NVDA": "NV",
    "TSLA": "TS",
    "GOOGL": "GO",
    "PLTR": "PL",
    "COPPER": "Cu",
    "NATGAS": "NG",
    "SP500": "S5",
    "BP": "BP",
    "URNM": "UM",
    "HOOD": "HD",
    "PLATINUM": "Pt",
    "2Z": "2Z",
}

FALLBACK_HUES = {
    "USDJPY": (73, 143, 182),
    "EURUSD": (93, 156, 200),
    "XAU": (183, 154, 79),
    "XAG": (148, 157, 171),
    "CL": (94, 131, 154),
    "NVDA": (84, 170, 105),
    "TSLA": (198, 89, 92),
    "GOOGL": (98, 137, 201),
    "PLTR": (101, 117, 138),
    "COPPER": (189, 124, 84),
    "NATGAS": (84, 138, 182),
    "SP500": (86, 141, 183),
    "BP": (88, 162, 122),
    "URNM": (110, 144, 115),
    "HOOD": (82, 175, 146),
    "PLATINUM": (150, 162, 170),
    "2Z": (112, 146, 189),
}

INSTRUMENT_BADGES = {
    "CL": ("CL", "Oil"),
    "COPPER": ("Cu", "Copper"),
    "NATGAS": ("NG", "NatGas"),
    "SP500": ("S&P", "500"),
    "XAU": ("Au", "Gold"),
    "XAG": ("Ag", "Silver"),
    "PLATINUM": ("Pt", "Platinum"),
    "CRCL": ("CRCL", "Circle"),
}


def fetch_json(url: str):
    with urllib.request.urlopen(url, timeout=30) as response:
        return json.load(response)


def fetch_bytes(url: str) -> bytes:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 PacificaFlow/1.0"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read()


def ensure_dir():
    ASSET_DIR.mkdir(parents=True, exist_ok=True)


def load_symbols() -> list[str]:
    payload = fetch_json(API_URL)
    rows = payload.get("volumeRank") or []
    return [str(row.get("symbol") or "").upper() for row in rows if row.get("symbol")]


def load_market_images() -> dict[str, str]:
    ids = sorted({coin_id for coin_id in COINGECKO_IDS.values() if coin_id})
    if not ids:
        return {}
    url = (
        "https://api.coingecko.com/api/v3/coins/markets"
        f"?vs_currency=usd&ids={','.join(ids)}"
    )
    rows = fetch_json(url)
    images = {}
    for row in rows:
        coin_id = row.get("id")
        image = row.get("image")
        if coin_id and image:
            images[coin_id] = image
    return images


def save_png(symbol: str, data: bytes):
    path = ASSET_DIR / f"{symbol.lower()}.png"
    path.write_bytes(data)


def build_fallback_png(symbol: str) -> bytes:
    hue = FALLBACK_HUES.get(symbol, (98, 150, 192))
    label = FALLBACK_LABELS.get(symbol, symbol[:3].upper())

    image = Image.new("RGBA", (CANVAS, CANVAS), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image)

    cx = cy = CANVAS // 2
    outer = 44
    inner = 38
    glow = Image.new("RGBA", (CANVAS, CANVAS), (0, 0, 0, 0))
    glow_draw = ImageDraw.Draw(glow)
    glow_draw.ellipse(
        (cx - outer, cy - outer, cx + outer, cy + outer),
        fill=(*hue, 40),
    )
    image.alpha_composite(glow)

    draw.ellipse(
        (cx - inner, cy - inner, cx + inner, cy + inner),
        fill=(12, 25, 38, 188),
        outline=(214, 234, 246, 82),
        width=2,
    )
    draw.ellipse(
        (cx - inner + 6, cy - inner + 6, cx + inner - 6, cy + inner - 6),
        fill=(*hue, 42),
    )

    try:
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 24)
    except Exception:
        font = ImageFont.load_default()

    try:
        bbox = draw.textbbox((0, 0), label, font=font)
        w = bbox[2] - bbox[0]
        h = bbox[3] - bbox[1]
    except Exception:
        w, h = draw.textsize(label, font=font)
    draw.text(
        (cx - w / 2, cy - h / 2 - 1),
        label,
        font=font,
        fill=(245, 250, 253, 235),
    )

    output = io.BytesIO()
    image.save(output, format="PNG")
    return output.getvalue()


def build_forex_badge_png(symbol: str) -> bytes:
    pairs = FOREX_PAIRS.get(symbol)
    if not pairs:
        return build_fallback_png(symbol)

    image = Image.new("RGBA", (CANVAS, CANVAS), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image)

    # soft shadow / glass chip
    draw.rounded_rectangle((10, 18, 86, 78), radius=22, fill=(11, 27, 42, 128))
    draw.rounded_rectangle((12, 20, 84, 76), radius=20, fill=(20, 46, 68, 180), outline=(228, 241, 248, 72), width=2)

    left = (14, 24, 48, 72)
    right = (48, 24, 82, 72)
    (lcode, l1, l2), (rcode, r1, r2) = pairs

    draw.rounded_rectangle(left, radius=16, fill=(*l1, 230))
    draw.rectangle((left[0], left[1] + 16, left[2], left[1] + 32), fill=(*l2, 230))
    draw.rounded_rectangle(right, radius=16, fill=(*r1, 230))
    draw.rectangle((right[0], right[1] + 16, right[2], right[1] + 32), fill=(*r2, 230))

    try:
        code_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 13)
    except Exception:
        code_font = ImageFont.load_default()

    for rect, code in ((left, lcode), (right, rcode)):
        try:
            bbox = draw.textbbox((0, 0), code, font=code_font)
            w = bbox[2] - bbox[0]
            h = bbox[3] - bbox[1]
        except Exception:
            w, h = draw.textsize(code, font=code_font)
        draw.text(
            ((rect[0] + rect[2] - w) / 2, (rect[1] + rect[3] - h) / 2 - 1),
            code,
            font=code_font,
            fill=(248, 251, 253, 238),
        )

    output = io.BytesIO()
    image.save(output, format="PNG")
    return output.getvalue()


def build_instrument_badge_png(symbol: str) -> bytes:
    primary, secondary = INSTRUMENT_BADGES.get(symbol, (symbol[:3].upper(), ""))
    hue = FALLBACK_HUES.get(symbol, (98, 150, 192))

    image = Image.new("RGBA", (CANVAS, CANVAS), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image)

    draw.rounded_rectangle((8, 8, 88, 88), radius=24, fill=(11, 26, 38, 118))
    draw.rounded_rectangle(
        (10, 10, 86, 86),
        radius=22,
        fill=(14, 34, 50, 188),
        outline=(224, 240, 248, 70),
        width=2,
    )
    draw.rounded_rectangle((16, 16, 80, 80), radius=18, fill=(*hue, 46))

    try:
        primary_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 21 if len(primary) <= 3 else 16)
        secondary_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 11)
    except Exception:
        primary_font = ImageFont.load_default()
        secondary_font = ImageFont.load_default()

    try:
        bbox = draw.textbbox((0, 0), primary, font=primary_font)
        pw = bbox[2] - bbox[0]
        ph = bbox[3] - bbox[1]
    except Exception:
        pw, ph = draw.textsize(primary, font=primary_font)
    draw.text(
        ((CANVAS - pw) / 2, 28 - ph / 2),
        primary,
        font=primary_font,
        fill=(246, 250, 252, 240),
    )

    if secondary:
        try:
            bbox = draw.textbbox((0, 0), secondary, font=secondary_font)
            sw = bbox[2] - bbox[0]
            sh = bbox[3] - bbox[1]
        except Exception:
            sw, sh = draw.textsize(secondary, font=secondary_font)
        draw.text(
            ((CANVAS - sw) / 2, 58 - sh / 2),
            secondary,
            font=secondary_font,
            fill=(228, 238, 244, 196),
        )

    output = io.BytesIO()
    image.save(output, format="PNG")
    return output.getvalue()


def main():
    ensure_dir()
    symbols = load_symbols()
    image_lookup = load_market_images()
    written = []

    for symbol in symbols:
        path = ASSET_DIR / f"{symbol.lower()}.png"
        if path.exists():
            continue
        if symbol in FOREX_PAIRS:
            save_png(symbol, build_forex_badge_png(symbol))
            written.append((symbol, "forex_badge"))
            continue
        if symbol in INSTRUMENT_BADGES:
            save_png(symbol, build_instrument_badge_png(symbol))
            written.append((symbol, "instrument_badge"))
            continue
        if symbol in EQUITY_LOGO_URLS:
            try:
                save_png(symbol, fetch_bytes(EQUITY_LOGO_URLS[symbol]))
                written.append((symbol, "equity_logo"))
                continue
            except Exception:
                pass
        coin_id = COINGECKO_IDS.get(symbol)
        if coin_id and coin_id in image_lookup and symbol not in CUSTOM_ONLY:
            try:
                save_png(symbol, fetch_bytes(image_lookup[coin_id]))
                written.append((symbol, "coingecko"))
                continue
            except Exception:
                pass
        save_png(symbol, build_fallback_png(symbol))
        written.append((symbol, "fallback"))

    print(f"symbols={len(symbols)}")
    print(f"assets={len(list(ASSET_DIR.glob('*.png')))}")
    for symbol, source in written:
        print(f"{symbol}: {source}")


if __name__ == "__main__":
    main()
