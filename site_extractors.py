from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

from engine import Deal

PRICE_RE = re.compile(r"£\s?\d[\d,]*(?:\.\d{2})?")
STAR_RE = re.compile(r"(?:^|\b)([1-5](?:\.5)?)\s*(?:star|\*)", re.I)
REVIEW_PATTERNS = [
    r"(\d\.\d)\s*/\s*10",
    r"rated\s+(\d\.\d)",
    r"review score\s+(\d\.\d)",
    r"excellent\s+(\d\.\d)",
    r"tripadvisor traveller rating[^\d]*(\d\.\d)",
    r"tripadvisor[^\d]{0,20}(\d\.\d)",
    r"trustscore[^\d]{0,20}(\d\.\d)",
]
BOARD_OPTIONS = [
    "ultra all inclusive", "all inclusive plus", "all inclusive",
    "full board plus", "full board", "half board plus", "half board",
    "bed and breakfast", "breakfast included", "breakfast", "self catering", "room only",
]
ROOM_WORDS = [
    "double room", "family room", "family suite", "suite", "studio", "apartment", "1 bedroom apartment",
    "2 bedroom apartment", "two bedroom apartment", "interconnecting room", "interconnecting", "sea view", "garden view",
    "swim up", "junior suite", "villa", "bungalow",
]
AIRPORT_WORDS = ["newcastle", "manchester", "leeds bradford", "glasgow", "birmingham", "bristol", "edinburgh"]

HOTEL_STOPWORDS = {
    "book", "book now", "price", "holiday", "holidays", "deal", "deals", "search", "more details",
    "flight", "flights", "transfer", "transfers", "baggage", "bag", "bags", "pool", "beach", "board",
    "included", "package", "packages", "low deposit", "deposit", "save", "offer", "offers", "breaks",
    "late deals", "all inclusive holidays", "family holidays", "cheap holidays", "our rating", "tripadvisor",
    "free child place", "pay monthly", "flexible payment", "protected", "atol", "abta", "more beach for your buck",
}

SITE_PROFILES: Dict[str, Dict[str, Any]] = {
    "jet2": {
        "name": "Jet2 Holidays",
        "package_positive": ["22kg baggage included", "10kg hand luggage included", "return transfers included", "free child place", "which? recommended provider"],
        "package_negative": ["£60pp deposit", "deposit", "pp"],
        "price_floor": 900,
        "base_conf": 0.84,
        "extractor": "site_specific.jet2.v3",
        "field_patterns": {
            "hotel": [r'"HotelName"\s*:\s*"([^"\\]{3,120})"', r'"hotelName"\s*:\s*"([^"\\]{3,120})"'],
            "resort": [r'"ResortName"\s*:\s*"([^"\\]{2,120})"', r'"resortName"\s*:\s*"([^"\\]{2,120})"'],
            "rating": [r'"RatingPlus"\s*:\s*"?([1-5](?:\.5)?)"?', r'"TripAdvisorRating"\s*:\s*"?([1-5](?:\.5)?)"?'],
            "board": [r'"BoardBasis"\s*:\s*"([^"\\]{3,60})"'],
        },
    },
    "tui": {
        "name": "TUI Holidays",
        "package_positive": ["luggage and transfers", "free kids' place", "tui blue", "a la carte", "hotel + flights"],
        "package_negative": ["from £", "pp", "deposit"],
        "price_floor": 900,
        "base_conf": 0.82,
        "extractor": "site_specific.tui.v3",
        "field_patterns": {
            "hotel": [r'"hotelName"\s*:\s*"([^"\\]{3,120})"', r'"name"\s*:\s*"([^"\\]{3,120}hotel[^"\\]{0,60})"'],
            "resort": [r'"destinationName"\s*:\s*"([^"\\]{2,120})"', r'"resortName"\s*:\s*"([^"\\]{2,120})"'],
            "rating": [r'"rating"\s*:\s*"?([1-5](?:\.5)?)"?', r'"tripAdvisorRating"\s*:\s*"?([1-5](?:\.5)?)"?'],
            "board": [r'"boardBasis"\s*:\s*"([^"\\]{3,60})"'],
        },
    },
    "loveholidays": {
        "name": "loveholidays",
        "package_positive": ["atol protected", "hotel", "flights", "all inclusive", "beach"],
        "package_negative": ["£19 per person", "low deposit", "pp", "per person", "monthly"],
        "price_floor": 850,
        "base_conf": 0.80,
        "extractor": "site_specific.loveholidays.v3",
        "field_patterns": {
            "hotel": [r'"hotelName"\s*:\s*"([^"\\]{3,120})"', r'"name"\s*:\s*"([^"\\]{3,120}(?:hotel|resort|apartments|suites)[^"\\]{0,40})"'],
            "resort": [r'"destinationName"\s*:\s*"([^"\\]{2,120})"', r'"resortName"\s*:\s*"([^"\\]{2,120})"'],
            "rating": [r'"starRating"\s*:\s*"?([1-5](?:\.5)?)"?', r'"reviewScore"\s*:\s*"?([1-9](?:\.\d)?)"?'],
            "board": [r'"boardBasis"\s*:\s*"([^"\\]{3,60})"'],
        },
    },
    "onthebeach": {
        "name": "On the Beach",
        "package_positive": ["secure trust account", "package holiday", "hotel", "transfer", "luggage", "beach"],
        "package_negative": ["£19pp deposits", "pp", "per person", "monthly"],
        "price_floor": 850,
        "base_conf": 0.80,
        "extractor": "site_specific.onthebeach.v3",
        "field_patterns": {
            "hotel": [r'"hotelName"\s*:\s*"([^"\\]{3,120})"', r'"name"\s*:\s*"([^"\\]{3,120}(?:hotel|resort|apartments|suites)[^"\\]{0,40})"'],
            "resort": [r'"resortName"\s*:\s*"([^"\\]{2,120})"', r'"destinationName"\s*:\s*"([^"\\]{2,120})"'],
            "rating": [r'"starRating"\s*:\s*"?([1-5](?:\.5)?)"?', r'"reviewRating"\s*:\s*"?([1-9](?:\.\d)?)"?'],
            "board": [r'"boardBasis"\s*:\s*"([^"\\]{3,60})"'],
        },
    },
    "travelsupermarket": {
        "name": "TravelSupermarket Packages",
        "package_positive": ["package holiday", "hotel", "board basis", "tripadvisor", "beach"],
        "package_negative": ["from £", "pp", "per person", "low deposit"],
        "price_floor": 850,
        "base_conf": 0.78,
        "extractor": "site_specific.travelsupermarket.v3",
        "field_patterns": {
            "hotel": [r'"hotelName"\s*:\s*"([^"\\]{3,120})"', r'"name"\s*:\s*"([^"\\]{3,120}(?:hotel|resort|apartments|suites)[^"\\]{0,40})"'],
            "resort": [r'"destinationName"\s*:\s*"([^"\\]{2,120})"', r'"resortName"\s*:\s*"([^"\\]{2,120})"'],
            "rating": [r'"starRating"\s*:\s*"?([1-5](?:\.5)?)"?', r'"reviewScore"\s*:\s*"?([1-9](?:\.\d)?)"?'],
            "board": [r'"boardBasis"\s*:\s*"([^"\\]{3,60})"'],
        },
    },
    "google flights": {"name": "Google Flights", "price_floor": 40, "base_conf": 0.64, "extractor": "site_specific.google_flights.v3"},
    "skyscanner": {"name": "Skyscanner", "price_floor": 40, "base_conf": 0.62, "extractor": "site_specific.skyscanner.v3"},
    "kayak": {"name": "KAYAK", "price_floor": 40, "base_conf": 0.60, "extractor": "site_specific.kayak.v3"},
    "momondo": {"name": "Momondo", "price_floor": 40, "base_conf": 0.60, "extractor": "site_specific.momondo.v4"},
    "expedia": {
        "name": "Expedia Packages",
        "package_positive": ["package", "hotel + flight", "member price", "beach", "all inclusive"],
        "package_negative": ["per person", "pp", "deposit", "pay monthly"],
        "price_floor": 850,
        "base_conf": 0.74,
        "extractor": "site_specific.expedia.v4",
        "field_patterns": {
            "hotel": [r'"name"\s*:\s*"([^"\]{3,120}(?:hotel|resort|apartments|suites)[^"\]{0,40})"'],
            "resort": [r'"regionName"\s*:\s*"([^"\]{2,120})"', r'"destinationName"\s*:\s*"([^"\]{2,120})"'],
            "rating": [r'"starRating"\s*:\s*"?([1-5](?:\.5)?)"?', r'"guestRating"\s*:\s*"?([1-9](?:\.\d)?)"?'],
            "board": [r'"mealPlan"\s*:\s*"([^"\]{3,60})"', r'"boardBasis"\s*:\s*"([^"\]{3,60})"'],
        },
    },
}


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def _lines(text: str) -> List[str]:
    raw = [re.sub(r"\s+", " ", x).strip(" -–|:\t") for x in text.splitlines()]
    return [x for x in raw if x]


def _find_board(text: str) -> str:
    tl = text.lower()
    for option in BOARD_OPTIONS:
        if option in tl:
            return option.title()
    return ""


def _find_review(text: str) -> Optional[float]:
    for p in REVIEW_PATTERNS:
        m = re.search(p, text, re.I)
        if m:
            try:
                return float(m.group(1))
            except Exception:
                pass
    return None


def _find_stars(text: str) -> Optional[float]:
    m = STAR_RE.search(text)
    if m:
        try:
            return float(m.group(1))
        except Exception:
            return None
    return None


def _find_beach_minutes(text: str) -> Optional[int]:
    tl = text.lower()
    if any(k in tl for k in ["beachfront", "on the beach", "by the beach", "steps from the beach"]):
        return 0
    for p in [
        r"(\d{1,2})\s*(?:min|minute)[a-z]*\s*(?:walk)?\s*(?:to|from)?\s*the\s*beach",
        r"beach\s*(?:is)?\s*(\d{1,2})\s*(?:min|minute)",
        r"(\d{1,2})\s*minutes?\s*to\s*beach",
        r"(\d{2,4})\s*m\s*from\s*the\s*beach",
    ]:
        m = re.search(p, text, re.I)
        if m:
            try:
                val = int(m.group(1))
                return max(0, round(val / 80)) if val > 99 else val
            except Exception:
                pass
    return None


def _find_transfers(text: str) -> Optional[bool]:
    tl = text.lower()
    if "transfers not included" in tl or "no transfers" in tl:
        return False
    if any(k in tl for k in ["shared transfer included", "transfers included", "return transfers included", "coach transfer included", "private transfer included"]):
        return True
    return None


def _find_bags(text: str) -> Optional[bool]:
    tl = text.lower()
    if any(x in tl for x in ["bags not included", "baggage not included", "hand baggage only"]):
        return False
    if any(x in tl for x in ["baggage included", "bags included", "22kg bag", "23kg bag", "hold baggage included", "10kg hand luggage included"]):
        return True
    return None


def _find_room_type(text: str) -> str:
    tl = text.lower()
    for word in ROOM_WORDS:
        if word in tl:
            return word.title()
    return ""




def _find_child_pricing_note(text: str) -> str:
    tl = text.lower()
    patterns = [
        r"(free child place[^|.;]{0,90})",
        r"(free kids'? place[^|.;]{0,90})",
        r"(kids go free[^|.;]{0,90})",
        r"(child discount[^|.;]{0,90})",
        r"(child price[^|.;]{0,90})",
        r"(free child[^|.;]{0,90})",
    ]
    for pat in patterns:
        m = re.search(pat, tl, re.I)
        if m:
            return _clean(m.group(1)).title()
    return ""


def _find_infant_cost_note(text: str) -> str:
    tl = text.lower()
    patterns = [
        r"(infant[^|.;]{0,90}(?:included|free|taxes only|tax only|charges apply|supplement applies|fee applies))",
        r"((?:under 2|under 1)[^|.;]{0,90}(?:included|free|taxes only|tax only|charges apply|supplement applies))",
        r"(baby[^|.;]{0,90}(?:included|free|taxes only|charges apply))",
    ]
    for pat in patterns:
        m = re.search(pat, tl, re.I)
        if m:
            return _clean(m.group(1)).title()
    return ""


def _find_family_room(text: str) -> str:
    room = _find_room_type(text)
    tl = text.lower()
    if room:
        return room
    for pat in [
        r"(family room[^|.;]{0,40})",
        r"(family suite[^|.;]{0,40})",
        r"(interconnecting room[^|.;]{0,40})",
        r"((?:one|two|1|2) bedroom apartment[^|.;]{0,40})",
        r"(apartment[^|.;]{0,40})",
        r"(suite[^|.;]{0,40})",
    ]:
        m = re.search(pat, tl, re.I)
        if m:
            return _clean(m.group(1)).title()
    return ""

def _find_airport(text: str) -> str:
    tl = text.lower()
    for word in AIRPORT_WORDS:
        if word in tl:
            return word.title()
    return ""


def _flags(text: str) -> Dict[str, Optional[bool]]:
    tl = text.lower()
    return {
        "pool": True if any(k in tl for k in ["pool", "swimming pool", "outdoor pool", "indoor pool", "kids' pool", "children's pool"]) else (False if "no pool" in tl else None),
        "bags_included": _find_bags(text),
        "transfers_included": _find_transfers(text),
        "free_child_place": True if any(k in tl for k in ["free child place", "free child places", "free kids' place", "free child"] ) else None,
        "free_cancellation": True if "free cancellation" in tl else (False if "non-refundable" in tl else None),
    }


def _looks_like_hotel_name(text: str) -> bool:
    if not text or len(text) < 4 or len(text) > 120:
        return False
    tl = text.lower().strip()
    if any(bad in tl for bad in HOTEL_STOPWORDS):
        return False
    if re.search(r"\b(transfer|baggage|deposit|beach|price|from £|from|only £|return flights?)\b", tl):
        return False
    alpha_count = len(re.findall(r"[A-Za-z]", text))
    return alpha_count >= 4


def _extract_hotel(chunk: str) -> str:
    chunk = _clean(chunk)
    patterns = [
        r"([A-Z][A-Za-z0-9&'’\-\s]{2,80}?(?:Hotel|Resort|Apartments|Suites|Village|Spa|Beach Resort|Beach Hotel))\b",
        r"hotel\s*[:\-]?\s*([A-Z][A-Za-z0-9&'’\-\s]{4,80})",
        r"^([A-Z][A-Za-z0-9&'’\-\s]{5,80})\s+(?:All Inclusive|Half Board|Bed and Breakfast|Self Catering|Room Only)\b",
    ]
    for p in patterns:
        m = re.search(p, chunk, re.I | re.M)
        if m:
            candidate = _clean(m.group(1))
            if _looks_like_hotel_name(candidate):
                return candidate
    for line in chunk.split(" | ")[:6]:
        if 5 <= len(line) <= 90 and re.match(r"^[A-Z][A-Za-z0-9&'’\-\s]+$", line):
            if _looks_like_hotel_name(line):
                return _clean(line)
    return ""


def _find_nights(text: str) -> Optional[int]:
    for p in [r"\b(\d{1,2})\s*nights?\b", r"for\s*(\d{1,2})\s*nights?\b"]:
        m = re.search(p, text, re.I)
        if m:
            try:
                val = int(m.group(1))
                if 2 <= val <= 21:
                    return val
            except Exception:
                pass
    return None


def _find_dates(text: str) -> str:
    for p in [r"\b\d{4}-\d{2}-\d{2}\b", r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b", r"\b\d{1,2}\s+[A-Z][a-z]{2,8}\s+\d{4}\b"]:
        m = re.search(p, text)
        if m:
            return m.group(0)
    return ""


def _price_value(s: str) -> Optional[float]:
    try:
        return float(s.replace("£", "").replace(",", "").strip())
    except Exception:
        return None


def _price_local_context(chunk: str, price_token: str) -> str:
    idx = chunk.find(price_token)
    if idx < 0:
        return chunk
    return chunk[max(0, idx - 140): min(len(chunk), idx + len(price_token) + 220)]


def _normalise_hotel_name(name: str) -> str:
    name = _clean(name).strip(" -|:")
    name = re.sub(r"\b(our|hotel details|show more|loading)$", "", name, flags=re.I).strip()
    name = re.sub(r"\s{2,}", " ", name)
    return name


def _site_profile(site_name: str) -> Dict[str, Any]:
    lower = site_name.lower()
    compact = re.sub(r"[^a-z0-9]+", "", lower)
    for key, profile in SITE_PROFILES.items():
        key_compact = re.sub(r"[^a-z0-9]+", "", key.lower())
        if key in lower or key_compact in compact:
            return profile
    return {"name": site_name, "price_floor": 850, "base_conf": 0.56, "extractor": "generic.rules.v3", "package_positive": [], "package_negative": [], "field_patterns": {}}


def _price_quality(price: float, chunk: str, site_name: str, meta: Dict[str, Any], profile: Dict[str, Any], price_token: str = "") -> Tuple[float, str]:
    tl = chunk.lower()
    score = 0.50
    reasons: List[str] = []

    if any(k in tl for k in ["holiday", "package", "hotel + flight", "total price", "total holiday price", "total holiday cost"]):
        score += 0.18; reasons.append("package cue")
    token_context = tl
    if price_token:
        ix = chunk.lower().find(price_token.lower())
        if ix >= 0:
            token_context = chunk.lower()[max(0, ix - 32): min(len(chunk), ix + len(price_token) + 48)]
    if any(k in token_context for k in ["per person", "pp", "/pp"]):
        score -= 0.30; reasons.append("per-person cue")
    if any(k in token_context for k in ["deposit", "low deposit", "today only deposit", "monthly", "per month", "pay monthly"]):
        score -= 0.42; reasons.append("deposit/monthly cue")
    if any(k in tl for k in ["save £", "you save", "discount", "was £", "before discount"]):
        score -= 0.14; reasons.append("promo cue")
    if "from £" in tl or "from only £" in tl:
        score -= 0.12; reasons.append("from-price cue")
    if any(k in tl for k in profile.get("package_positive", [])):
        score += 0.12; reasons.append("site package cue")
    if any(k in tl for k in profile.get("package_negative", [])):
        score -= 0.12; reasons.append("site noise cue")

    floor = profile.get("price_floor", 850)
    if price < 250:
        score -= 0.55; reasons.append("too low")
    elif meta.get("site_type") == "package" and price < floor:
        score -= 0.30; reasons.append("below site floor")
    elif price >= 1400:
        score += 0.12; reasons.append("plausible total")

    if any(k in tl for k in ["all inclusive", "breakfast", "beach", "pool", "transfer", "baggage", "luggage", "board basis"]):
        score += 0.10; reasons.append("detail-rich chunk")

    site_lower = site_name.lower()
    if site_lower in ["google flights", "skyscanner", "kayak"] and price < 40:
        score -= 0.60; reasons.append("implausible flight")
    return max(0.0, min(1.0, score)), ", ".join(reasons)


def _candidate_chunks(text: str) -> List[str]:
    lines = _lines(text)
    chunks: List[str] = []
    price_idxs = [i for i, line in enumerate(lines) if PRICE_RE.search(line)]
    for idx in price_idxs[:60]:
        start = max(0, idx - 10)
        end = min(len(lines), idx + 10)
        chunk = " | ".join(lines[start:end])
        if len(chunk) > 35:
            chunks.append(chunk)
    cleaned = _clean(text)
    for m in PRICE_RE.finditer(cleaned[:220000]):
        start = max(0, m.start() - 360)
        end = min(len(cleaned), m.end() + 720)
        chunk = cleaned[start:end]
        if len(chunk) > 35:
            chunks.append(chunk)
        if len(chunks) >= 110:
            break
    out: List[str] = []
    seen = set()
    for c in chunks:
        key = c[:180]
        if key in seen:
            continue
        seen.add(key)
        out.append(c)
    return out


def _structured_json_objects(html: str) -> Iterable[Dict[str, Any]]:
    if not html:
        return []
    objs: List[Dict[str, Any]] = []
    for match in re.finditer(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.I | re.S):
        raw = match.group(1).strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                objs.append(parsed)
            elif isinstance(parsed, list):
                objs.extend(x for x in parsed if isinstance(x, dict))
        except Exception:
            continue
    return objs


def _site_field_value(profile: Dict[str, Any], field: str, html: str) -> str:
    for pat in profile.get("field_patterns", {}).get(field, []):
        m = re.search(pat, html, re.I)
        if m:
            val = _clean(m.group(1)).encode("utf-8", "ignore").decode("unicode_escape", "ignore")
            if field != "hotel" or _looks_like_hotel_name(val):
                return _normalise_hotel_name(val) if field == "hotel" else val
    return ""


def _deal_from_structured(obj: Dict[str, Any], site_name: str, url: str, meta: Dict[str, Any], batch: str, mode: str, extractor_used: str, conf: float) -> Optional[Deal]:
    typ = obj.get("@type")
    if isinstance(typ, list):
        typ = " ".join(map(str, typ))
    typ = str(typ or "")
    name = _clean(str(obj.get("name") or obj.get("headline") or ""))
    if name and not _looks_like_hotel_name(name):
        name = ""
    offer = obj.get("offers") if isinstance(obj.get("offers"), dict) else {}
    aggregate_offer = obj.get("aggregateOffer") if isinstance(obj.get("aggregateOffer"), dict) else {}
    price_candidates = [
        obj.get("price"), offer.get("price"), aggregate_offer.get("lowPrice"), aggregate_offer.get("highPrice"),
    ]
    price_total = None
    for raw in price_candidates:
        if raw is None:
            continue
        try:
            price_total = float(str(raw).replace(",", ""))
            break
        except Exception:
            continue
    if meta.get("site_type") == "package" and price_total is not None and price_total < 700:
        return None
    rating = None
    aggregate = obj.get("aggregateRating") if isinstance(obj.get("aggregateRating"), dict) else {}
    for raw in [aggregate.get("ratingValue"), obj.get("starRating"), obj.get("reviewRating")]:
        if raw is None:
            continue
        try:
            rating = float(raw)
            break
        except Exception:
            continue
    address = obj.get("address") if isinstance(obj.get("address"), dict) else {}
    destination = _clean(str(obj.get("addressLocality") or address.get("addressLocality") or meta.get("destination_city", "")))
    country = _clean(str(address.get("addressCountry") or meta.get("destination_country", "")))
    notes = f"Structured data type: {typ or 'unknown'}"
    return Deal(
        source_site=site_name,
        source_url=url,
        deal_type=meta.get("site_type", "package"),
        destination=destination,
        country=country,
        departure_date=meta.get("depart_iso", ""),
        nights=meta.get("nights"),
        hotel_name=name,
        board_basis="",
        price_total_gbp=price_total,
        review_score=rating,
        pool=None,
        beach_minutes=None,
        bags_included=None,
        transfers_included=None,
        free_child_place=None,
        family_room_type="",
        child_pricing_note="",
        infant_cost_note="",
        free_cancellation=None,
        notes=notes,
        extraction_confidence=conf,
        ai_summary="Structured data extraction",
        scan_batch=batch,
        scan_mode=mode,
        verified=False,
        extractor_used=extractor_used,
        provider_rank=1,
    )


def _site_candidate_blocks(text: str, html: str, site_name: str, profile: Dict[str, Any]) -> List[str]:
    chunks = _candidate_chunks(text)
    field_values: List[str] = []
    for fld in ["hotel", "resort", "rating", "board"]:
        val = _site_field_value(profile, fld, html)
        if val:
            field_values.append(val)
    if field_values:
        joined = " | ".join(field_values)
        for chunk in chunks[:30]:
            if any(v.lower() in chunk.lower() for v in field_values if len(v) > 2):
                chunks.insert(0, f"{joined} | {chunk}")
                break
    return chunks


def _extract_site_specific(text: str, html: str, site_name: str, url: str, meta: Dict[str, Any], batch: str, mode: str, profile: Dict[str, Any]) -> List[Deal]:
    deals: List[Deal] = []
    seen = set()
    rank = 0
    site_type = meta.get("site_type", "package")
    default_destination = meta.get("destination_city", "")
    default_country = meta.get("destination_country", "")
    default_nights = meta.get("nights")
    default_depart = meta.get("depart_iso", "")

    for obj in _structured_json_objects(html):
        d = _deal_from_structured(obj, site_name, url, meta, batch, mode, profile["extractor"] + ".jsonld", min(0.97, profile["base_conf"] + 0.08))
        if d and d.price_total_gbp:
            if site_type == "flight":
                d.deal_type = "flight"
                d.hotel_name = d.hotel_name or f"{default_destination} flights"
            deals.append(d)

    chunks = _site_candidate_blocks(text, html, site_name, profile)
    for chunk in chunks:
        prices = PRICE_RE.findall(chunk)
        if not prices:
            continue
        hotel = _normalise_hotel_name(_site_field_value(profile, "hotel", html) or _extract_hotel(chunk))
        resort = _site_field_value(profile, "resort", html)
        board = _site_field_value(profile, "board", html) or _find_board(chunk)
        beach = _find_beach_minutes(chunk)
        review = _find_review(chunk)
        rating_field = _site_field_value(profile, "rating", html)
        if review is None and rating_field:
            try:
                review = float(rating_field)
            except Exception:
                pass
        stars = _find_stars(chunk)
        room = _find_family_room(chunk)
        airport = _find_airport(chunk)
        child_note = _find_child_pricing_note(chunk)
        infant_note = _find_infant_cost_note(chunk)
        nights = _find_nights(chunk) or default_nights
        dep = default_depart or _find_dates(chunk)
        flags = _flags(chunk)

        best_price: Optional[float] = None
        best_q = -1.0
        best_note = ""
        for p in prices[:7]:
            pv = _price_value(p)
            if pv is None:
                continue
            local_chunk = _price_local_context(chunk, p)
            q, why = _price_quality(pv, local_chunk, site_name, meta, profile, price_token=p)
            if q > best_q:
                best_q, best_price, best_note = q, pv, why
        if best_price is None or best_q < 0.30:
            continue

        low_detail = not any([hotel, board, beach is not None, flags["pool"], review is not None, stars is not None])
        if site_type == "package" and best_price < profile.get("price_floor", 850) and low_detail:
            continue

        rank += 1
        notes = chunk[:620]
        detail_bits = []
        if room:
            detail_bits.append(room)
        if airport:
            detail_bits.append(f"Airport cue: {airport}")
        if resort:
            detail_bits.append(f"Resort cue: {resort}")
        if stars is not None:
            detail_bits.append(f"{stars:g}* cue")
        if best_note:
            detail_bits.append(f"Price confidence: {best_note}")
        if detail_bits:
            notes = f"{notes} || {'; '.join(detail_bits)}"

        destination = default_destination or resort
        d = Deal(
            source_site=site_name,
            source_url=url,
            deal_type=site_type,
            destination=destination,
            country=default_country,
            departure_date=dep,
            nights=nights,
            hotel_name=hotel,
            board_basis=board,
            price_total_gbp=best_price,
            review_score=review,
            pool=flags["pool"],
            beach_minutes=beach,
            bags_included=flags["bags_included"],
            transfers_included=flags["transfers_included"],
            free_child_place=flags["free_child_place"],
            family_room_type=room,
            child_pricing_note=child_note,
            infant_cost_note=infant_note,
            free_cancellation=flags["free_cancellation"],
            notes=notes,
            extraction_confidence=round(min(0.97, max(0.40, profile["base_conf"] * 0.72 + best_q * 0.48)), 3),
            ai_summary=f"{profile['name']} tuned extraction",
            scan_batch=batch,
            scan_mode=mode,
            verified=False,
            extractor_used=profile["extractor"],
            provider_rank=rank,
        )
        if site_type == "flight":
            d.deal_type = "flight"
            d.hotel_name = d.hotel_name or f"{default_destination} flights"
            d.destination = default_destination

        key = ((d.source_site or "").lower(), (d.hotel_name or "").lower()[:60], d.price_total_gbp, (d.departure_date or ""), d.nights)
        if key in seen:
            continue
        seen.add(key)
        deals.append(d)

    return deals


def _extract_generic(text: str, site_name: str, url: str, meta: Dict[str, Any], batch: str, mode: str) -> List[Deal]:
    profile = _site_profile(site_name)
    return _extract_site_specific(text, "", site_name, url, meta, batch, mode, profile)


def extract_for_site(site_name: str, text: str, url: str, meta: Dict[str, Any], batch: str, mode: str, html: str = "") -> List[Deal]:
    profile = _site_profile(site_name)
    html = html or ""
    lower = site_name.lower()
    compact = re.sub(r"[^a-z0-9]+", "", lower)
    if any((key in lower) or (re.sub(r"[^a-z0-9]+", "", key.lower()) in compact) for key in SITE_PROFILES.keys()):
        return _extract_site_specific(text, html, site_name, url, meta, batch, mode, profile)
    return _extract_generic(text, site_name, url, meta, batch, mode)
