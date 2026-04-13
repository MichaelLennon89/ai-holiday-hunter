from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse

from engine import (
    apply_price_deltas,
    apply_strategy_overlay,
    dedupe_deals,
    generate_urls_preview,
    load_config,
    load_previous_history,
    package_profile_text,
    save_history_snapshot,
    score_deals,
    write_outputs,
    effective_price,
)
from site_extractors import extract_for_site

ROOT = Path(__file__).resolve().parent
RESULTS = ROOT / "results"
RESULTS.mkdir(exist_ok=True)

COOKIE_TEXTS = [
    "accept", "accept all", "agree", "allow all", "got it", "continue", "consent",
    "accept cookies", "allow cookies", "i agree", "ok", "okay",
]

PROVIDER_CONFIG: Dict[str, Dict[str, Any]] = {
    "jet2 holidays": {
        "result_link_selectors": [
            "a[href*='/hotel/']", "a[href*='/hotels/']", "a[href*='/indulgent-escapes/']",
            "[data-testid*='hotel'] a[href]", "article a[href]",
        ],
        "href_hints": ["/hotel/", "/hotels/", "/indulgent-escapes/", "/villa/"],
        "next_texts": ["next", "show more", "load more"],
        "max_links": 12,
        "max_pages": 3,
        "near_booking_texts": ["view rooms", "room options", "select room", "continue", "book now"],
        "search_button_texts": ["search", "find holidays", "show holidays", "see deals"],
        "filter_texts": ["pool", "near beach", "free child", "family"],
        "checkout_texts": ["view rooms", "select room", "continue", "price breakdown", "summary", "continue to payment"],
        "family_filter_texts": ["free child", "free kids", "family room", "apartment", "all inclusive"],
        "room_texts": ["family room", "suite", "apartment", "interconnecting"],
        "basket_texts": ["price breakdown", "holiday summary", "extras", "continue to payment"],
    },
    "tui holidays": {
        "result_link_selectors": [
            "a[href*='/destinations/']", "a[href*='/accommodation/']", "a[href*='/f/']",
            "[data-testid*='hotel'] a[href]", "article a[href]",
        ],
        "href_hints": ["/accommodation/", "/f/", "/destinations/", "/bookaccommodation"],
        "next_texts": ["next", "show more", "load more results"],
        "max_links": 12,
        "max_pages": 3,
        "near_booking_texts": ["view rooms", "continue", "room options", "book now"],
        "search_button_texts": ["search", "show holidays", "see holidays", "find deals"],
        "filter_texts": ["pool", "beach", "free kids", "family"],
        "checkout_texts": ["view rooms", "continue", "select room", "summary", "continue to payment"],
        "family_filter_texts": ["free kids", "family", "all inclusive"],
        "room_texts": ["family room", "suite", "apartment", "interconnecting"],
        "basket_texts": ["price breakdown", "holiday summary", "extras", "continue to payment"],
    },
    "loveholidays": {
        "result_link_selectors": [
            "a[href*='/holidays/']", "a[href*='/property/']", "a[href*='/hotel/']",
            "article a[href]", "[data-testid*='hotel'] a[href]",
        ],
        "href_hints": ["/property/", "/hotel/", "/holidays/", "/search/"],
        "next_texts": ["next", "show more", "load more"],
        "max_links": 12,
        "max_pages": 3,
        "near_booking_texts": ["view details", "select room", "continue", "book now"],
        "search_button_texts": ["search", "show deals", "find holidays", "see results"],
        "filter_texts": ["pool", "beach", "free child", "family"],
        "checkout_texts": ["view details", "select room", "continue", "summary", "continue to payment"],
    },
    "on the beach": {
        "result_link_selectors": [
            "a[href*='/hotels/']", "a[href*='/hotel/']", "article a[href]", "[data-testid*='hotel'] a[href]",
        ],
        "href_hints": ["/hotel/", "/hotels/", "/holidays/"],
        "next_texts": ["next", "show more", "load more"],
        "max_links": 12,
        "max_pages": 3,
        "near_booking_texts": ["view hotel", "room options", "select room", "continue", "book now"],
        "search_button_texts": ["search", "show deals", "find holidays", "see results"],
        "filter_texts": ["pool", "beach", "free child", "family"],
        "checkout_texts": ["view details", "select room", "continue", "summary", "continue to payment"],
    },
    "travelsupermarket packages": {
        "result_link_selectors": [
            "a[href*='holidays']", "a[href*='/holiday/']", "article a[href]", "[data-testid*='result'] a[href]",
        ],
        "href_hints": ["holiday", "/hotel/", "/holidays/", "/package/"],
        "next_texts": ["next", "show more", "load more"],
        "max_links": 12,
        "max_pages": 3,
        "near_booking_texts": ["view deal", "deal details", "continue", "book now"],
        "search_button_texts": ["search", "find holidays", "show results"],
        "filter_texts": ["pool", "beach", "family"],
        "checkout_texts": ["view deal", "continue", "price breakdown", "summary"],
    },
    "expedia packages": {
        "result_link_selectors": [
            "a[href*='/Hotel-Search']", "a[href*='/Packages']", "main a[href]", "article a[href]",
        ],
        "href_hints": ["Hotel-Search", "Packages", "package", "hotel"],
        "next_texts": ["next", "show more"],
        "max_links": 8,
        "max_pages": 2,
        "near_booking_texts": ["select", "choose room", "reserve", "book now", "continue"],
        "search_button_texts": ["search", "show deals", "find packages"],
        "filter_texts": ["pool", "beach", "family"],
        "checkout_texts": ["view deal", "continue", "price breakdown", "summary"],
    },
    "google flights": {
        "result_link_selectors": ["a[href*='travel/flights']", "div[role='main'] a[href]", "main a[href]"],
        "href_hints": ["travel/flights", "flights?", "/booking/", "?hl="],
        "next_texts": ["next", "more flights"],
        "max_links": 8,
        "max_pages": 2,
        "near_booking_texts": ["select", "continue", "booking options"],
        "search_button_texts": ["search"],
        "filter_texts": [],
    },
    "skyscanner": {
        "result_link_selectors": ["a[href*='/transport/flights/']", "a[href*='skyscanner']", "main a[href]"],
        "href_hints": ["/transport/flights/", "/book/", "details", "day-view"],
        "next_texts": ["next", "show more"],
        "max_links": 8,
        "max_pages": 2,
        "near_booking_texts": ["select", "details", "continue", "book"],
        "search_button_texts": ["search"],
        "filter_texts": [],
    },
    "kayak": {
        "result_link_selectors": ["a[href*='/flights/']", "a[href*='kayak']", "main a[href]", "article a[href]"],
        "href_hints": ["/flights/", "booking", "details", "offers"],
        "next_texts": ["next", "show more"],
        "max_links": 8,
        "max_pages": 2,
        "near_booking_texts": ["view deal", "select", "details", "continue"],
        "search_button_texts": ["search"],
        "filter_texts": [],
    },
    "momondo": {
        "result_link_selectors": ["a[href*='/flight-search/']", "main a[href]", "article a[href]"],
        "href_hints": ["/flight-search/", "booking", "details", "offers"],
        "next_texts": ["next", "show more"],
        "max_links": 8,
        "max_pages": 2,
        "near_booking_texts": ["view deal", "select", "details", "continue"],
        "search_button_texts": ["search"],
        "filter_texts": [],
    },
}

FILTER_OUT_URL_BITS = [
    "login", "signin", "register", "privacy", "cookie", "terms", "about", "contact", "help",
    "blog", "careers", "jobs", "faq", "insurance", "car-hire", "car-rental", "cruises", "villa",
]


ATTRIBUTE_JS = r'''
(payload) => {
  const terms = (payload.terms || []).map(x => String(x).toLowerCase());
  const value = payload.value == null ? '' : String(payload.value);
  const isSelectPreferred = !!payload.preferSelect;
  const elems = [...document.querySelectorAll('input, textarea, select')];
  const labelText = (el) => {
    const labels = [];
    if (el.labels) {
      for (const l of el.labels) labels.push(l.innerText || l.textContent || '');
    }
    return labels.join(' ');
  };
  const textBag = (el) => [
    el.name, el.id, el.placeholder,
    el.getAttribute('aria-label'),
    el.getAttribute('data-testid'),
    el.getAttribute('data-qa'),
    el.getAttribute('title'),
    labelText(el),
  ].filter(Boolean).join(' ').toLowerCase();
  for (const el of elems) {
    const bag = textBag(el);
    if (!terms.some(t => bag.includes(t))) continue;
    try {
      if (el.tagName === 'SELECT' || isSelectPreferred) {
        const opts = [...(el.options || [])];
        const wanted = value.toLowerCase();
        let chosen = null;
        for (const opt of opts) {
          const target = `${opt.value} ${opt.label} ${opt.text}`.toLowerCase();
          if (target === wanted || target.includes(wanted)) {
            chosen = opt.value;
            break;
          }
        }
        if (!chosen && opts.some(o => String(o.value) === value)) chosen = value;
        if (!chosen && opts.length) chosen = opts[0].value;
        if (chosen != null) {
          el.value = chosen;
          el.dispatchEvent(new Event('input', { bubbles: true }));
          el.dispatchEvent(new Event('change', { bubbles: true }));
          return true;
        }
      }
      if (el.tagName !== 'SELECT') {
        el.removeAttribute('readonly');
        el.focus();
        el.value = value;
        el.dispatchEvent(new Event('input', { bubbles: true }));
        el.dispatchEvent(new Event('change', { bubbles: true }));
        el.dispatchEvent(new KeyboardEvent('keydown', { bubbles: true, key: 'Enter' }));
        el.dispatchEvent(new KeyboardEvent('keyup', { bubbles: true, key: 'Enter' }));
        return true;
      }
    } catch (e) {}
  }
  return false;
}
'''


def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()



def _site_key(site_name: str) -> str:
    return site_name.strip().lower()



def _profile(site_name: str) -> Dict[str, Any]:
    return PROVIDER_CONFIG.get(_site_key(site_name), {})



def click_cookie_buttons(page) -> None:
    for text in COOKIE_TEXTS:
        try:
            loc = page.get_by_role("button", name=re.compile(fr"^{re.escape(text)}$", re.I))
            if loc.count() > 0:
                loc.first.click(timeout=1200)
                time.sleep(0.5)
        except Exception:
            pass
    for selector in ["button[id*='accept']", "button[class*='accept']", "[aria-label*='accept']"]:
        try:
            loc = page.locator(selector)
            if loc.count() > 0:
                loc.first.click(timeout=800)
                time.sleep(0.4)
        except Exception:
            pass



def dismiss_overlays(page) -> None:
    click_cookie_buttons(page)
    for text in ["close", "not now", "maybe later", "skip", "no thanks"]:
        try:
            loc = page.get_by_role("button", name=re.compile(fr"^{re.escape(text)}$", re.I))
            if loc.count() > 0:
                loc.first.click(timeout=800)
                time.sleep(0.3)
        except Exception:
            pass



def slow_scroll(page, loops: int = 4) -> None:
    for _ in range(loops):
        try:
            page.mouse.wheel(0, 1600)
        except Exception:
            pass
        time.sleep(1.0)



def expand_page(page, site_name: str) -> None:
    texts = ["show more", "load more", "more results", "view more", "see more"]
    texts.extend(_profile(site_name).get("next_texts", []))
    seen: Set[str] = set()
    for text in texts:
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        try:
            loc = page.get_by_role("button", name=re.compile(re.escape(text), re.I))
            count = min(2, loc.count())
            for idx in range(count):
                try:
                    loc.nth(idx).click(timeout=1200)
                    time.sleep(1.0)
                except Exception:
                    pass
        except Exception:
            pass



def _good_url(url: str, base_url: str, site_name: str) -> bool:
    if not url:
        return False
    full = urljoin(base_url, url)
    parsed = urlparse(full)
    if parsed.scheme not in {"http", "https"}:
        return False
    low = full.lower()
    if any(bit in low for bit in FILTER_OUT_URL_BITS):
        return False
    host = (parsed.netloc or "").lower()
    site_low = site_name.lower()
    if "google flights" in site_low:
        return "google.com" in host
    if "kayak" in site_low:
        return "kayak." in host
    if "skyscanner" in site_low:
        return "skyscanner" in host
    if "momondo" in site_low:
        return "momondo" in host
    return True



def collect_candidate_links(page, site_name: str, base_url: str) -> List[str]:
    profile = _profile(site_name)
    urls: List[str] = []
    hints = [h.lower() for h in profile.get("href_hints", [])]

    def add(href: Optional[str]):
        if not href:
            return
        full = urljoin(base_url, href)
        low = full.lower()
        if not _good_url(full, base_url, site_name):
            return
        if hints and not any(h in low for h in hints):
            return
        if full not in urls:
            urls.append(full)

    selectors = profile.get("result_link_selectors", []) + ["main a[href]", "article a[href]", "a[href]"]
    for selector in selectors:
        try:
            loc = page.locator(selector)
            count = min(80, loc.count())
            for i in range(count):
                try:
                    add(loc.nth(i).get_attribute("href"))
                except Exception:
                    pass
        except Exception:
            pass
        if len(urls) >= profile.get("max_links", 6) * 2:
            break
    return urls[: profile.get("max_links", 6)]




def _money_values(text: str) -> List[float]:
    vals = []
    for m in re.finditer(r"£\s?([0-9]{2,5}(?:,[0-9]{3})*(?:\.\d{2})?)", text or "", re.I):
        try:
            vals.append(float(m.group(1).replace(',', '')))
        except Exception:
            pass
    return vals


def _extract_discount(text: str) -> tuple[Optional[str], Optional[float]]:
    for m in re.finditer(r"((?:save|discount|promo|offer)[^£]{0,40}£\s?([0-9]{1,4}(?:,[0-9]{3})?(?:\.\d{2})?))", text or "", re.I):
        try:
            return clean_text(m.group(1)), float(m.group(2).replace(',', ''))
        except Exception:
            return clean_text(m.group(1)), None
    return None, None


def _choose_price_from_context(text: str) -> tuple[Optional[float], str, float]:
    body = clean_text(text or '')
    best_total = None
    best_score = -10.0
    best_stage = 'headline'
    for m in re.finditer(r"£\s?([0-9]{2,5}(?:,[0-9]{3})*(?:\.\d{2})?)", body, re.I):
        try:
            value = float(m.group(1).replace(',', ''))
        except Exception:
            continue
        context = body[max(0, m.start()-140): min(len(body), m.end()+180)].lower()
        score = 0.0
        stage = 'headline'
        if value < 100:
            score -= 6
        if value < 500:
            score -= 3
        if any(bad in context for bad in ['deposit', 'from £', 'pp', 'per person', 'per adult', 'monthly', '/mo', 'finance']):
            score -= 4
        if any(good in context for good in ['to pay now', 'payment summary', 'review booking', 'booking summary', 'card details', 'secure booking', 'pay today']):
            score += 8
            stage = 'pre-payment'
        elif any(good in context for good in ['total price', 'holiday price', 'total holiday price', 'price breakdown', 'basket total', 'total cost', 'amount due', 'total due', 'grand total']):
            score += 6
            stage = 'basket'
        if any(good in context for good in ['room total', 'flight + hotel', 'selected room', 'your holiday', 'summary', 'includes discount', 'discount applied', 'traveller details', 'guest details']):
            score += 3
            if stage == 'headline':
                stage = 'room-selection'
        if value >= 850:
            score += min(3.0, value / 1500.0)
        if score > best_score:
            best_score = score
            best_total = value
            best_stage = stage
    conf = 0.97 if best_stage == 'pre-payment' else (0.92 if best_stage == 'basket' else (0.78 if best_stage == 'room-selection' else (0.58 if best_total is not None else 0.0)))
    return best_total, best_stage, conf


def _basket_inclusions(text: str) -> str:
    tl = (text or '').lower()
    bits = []
    for cue, label in [
        ('bags included', 'bags'), ('baggage included', 'bags'), ('transfers included', 'transfers'),
        ('free cancellation', 'free cancellation'), ('breakfast', 'breakfast'), ('all inclusive', 'all inclusive'),
        ('free child', 'free child signal'), ('family room', 'family room')
    ]:
        if cue in tl and label not in bits:
            bits.append(label)
    return ', '.join(bits[:6])


def _annotate_pricing(deals, text: str, stage_label: str, step_count: int):
    basket_price, basket_stage, conf = _choose_price_from_context(text)
    discount_note, discount_savings = _extract_discount(text)
    inclusions = _basket_inclusions(text)
    for d in deals:
        if d.headline_price_gbp is None and d.price_total_gbp is not None:
            d.headline_price_gbp = d.price_total_gbp
        if basket_price is not None and (d.basket_price_gbp is None or conf > (d.true_price_confidence or 0)):
            d.basket_price_gbp = basket_price
            d.checkout_stage = basket_stage or stage_label
            d.checkout_step_count = max(d.checkout_step_count or 0, step_count)
            d.pricing_completeness = 'pre-payment' if basket_stage == 'pre-payment' else ('near-final' if basket_stage in {'basket', 'room-selection'} else 'headline+')
            d.true_price_confidence = conf
        elif not d.pricing_completeness:
            d.pricing_completeness = 'headline' if d.price_total_gbp is not None else 'unknown'
            d.true_price_confidence = d.true_price_confidence or (0.55 if d.price_total_gbp is not None else 0.0)
        if discount_note and not d.discount_note:
            d.discount_note = discount_note
        if discount_savings is not None and d.discount_savings_gbp is None:
            d.discount_savings_gbp = discount_savings
        if inclusions:
            d.basket_inclusions = inclusions
        d.pricing_truth_label = 'Pre-payment price' if (d.pricing_completeness == 'pre-payment' or d.checkout_stage == 'pre-payment') else ('Basket-like price' if (d.basket_price_gbp is not None and (d.true_price_confidence or 0) >= 0.88) else ('Room-stage price' if d.basket_price_gbp is not None else ('Headline price' if d.price_total_gbp is not None else 'Unknown price')))
        d.price_reference_gbp = d.basket_price_gbp if d.basket_price_gbp is not None else d.price_total_gbp
    return deals

def _write_capture(slug: str, text: str, html: str) -> Dict[str, str]:
    txt_path = RESULTS / f"{slug}.txt"
    html_path = RESULTS / f"{slug}.html"
    txt_path.write_text(text[:220000], encoding="utf-8")
    html_path.write_text(html[:500000], encoding="utf-8")
    return {"txt": txt_path.name, "html": html_path.name}



def _annotate_extraction(deals, saved: Dict[str, str], journey_applied: str = "", journey_depth: int = 0, near_booking_stage: str = "", meta: Optional[Dict[str, Any]] = None):
    meta = meta or {}
    for d in deals:
        d.raw_text_file = saved.get("txt", "")
        d.screenshot_file = saved.get("html", "")
        if journey_applied:
            d.journey_applied = journey_applied
        if journey_depth:
            d.journey_depth = max(d.journey_depth or 0, journey_depth)
        if near_booking_stage:
            d.near_booking_stage = near_booking_stage
        if meta.get("search_route_id"):
            d.search_route_id = meta.get("search_route_id")
        if meta.get("provider_priority_band"):
            d.provider_priority_band = meta.get("provider_priority_band")
        if meta.get("search_variant"):
            d.search_variant = meta.get("search_variant")
        d.search_passes = max(d.search_passes or 0, int(meta.get("search_passes", 1) or 1))
    return deals



def llm_extract(text: str, site_name: str, url: str, meta: Dict[str, Any], batch: str, mode: str, html: str = ""):
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return extract_for_site(site_name, text, url, meta, batch, mode, html=html)
    try:
        from openai import OpenAI
        from engine import Deal

        client = OpenAI(api_key=api_key)
        prompt = "Return only JSON with extracted holiday deals. Use null for unknowns. Text:\n" + text[:30000]
        resp = client.responses.create(model=os.getenv("HOLIDAY_AI_MODEL", "gpt-4.1-mini"), input=prompt)
        payload = json.loads(getattr(resp, "output_text", "").strip())
        out = []
        rank = 0
        for item in payload.get("deals", []):
            rank += 1
            out.append(Deal(
                source_site=site_name,
                source_url=url,
                deal_type=meta.get("site_type", "package"),
                destination=item.get("destination") or meta.get("destination_city", ""),
                country=item.get("country") or meta.get("destination_country", ""),
                departure_date=item.get("departure_date") or meta.get("depart_iso", ""),
                nights=item.get("nights") if item.get("nights") is not None else meta.get("nights"),
                hotel_name=item.get("hotel_name", "") or "",
                board_basis=item.get("board_basis", "") or "",
                price_total_gbp=item.get("price_total_gbp"),
                review_score=item.get("review_score"),
                pool=item.get("pool"),
                beach_minutes=item.get("beach_minutes"),
                bags_included=item.get("bags_included"),
                transfers_included=item.get("transfers_included"),
                free_child_place=item.get("free_child_place"),
                family_room_type=item.get("family_room_type", "") or "",
                child_pricing_note=item.get("child_pricing_note", "") or "",
                infant_cost_note=item.get("infant_cost_note", "") or "",
                free_cancellation=item.get("free_cancellation"),
                notes=item.get("notes", "") or "",
                extraction_confidence=item.get("extraction_confidence", 0.7),
                ai_summary=item.get("ai_summary", "") or "",
                scan_batch=batch,
                scan_mode=mode,
                verified=False,
                extractor_used="llm.primary",
                provider_rank=rank,
            ))
        return out or extract_for_site(site_name, text, url, meta, batch, mode, html=html)
    except Exception:
        return extract_for_site(site_name, text, url, meta, batch, mode, html=html)



def extract_current_page(page, site_name: str, url: str, meta: Dict[str, Any], batch: str, mode: str, slug_prefix: str, journey_applied: str = "", journey_depth: int = 0, near_booking_stage: str = ""):
    dismiss_overlays(page)
    slow_scroll(page, loops=3)
    expand_page(page, site_name)
    text = clean_text(page.locator("body").inner_text(timeout=12000))
    html = page.content()
    slug = re.sub(r"[^a-z0-9]+", "_", slug_prefix.lower()).strip("_")[:120]
    saved = _write_capture(slug, text, html)
    extracted = llm_extract(text, site_name, url, meta, batch, mode, html=html)
    extracted = _annotate_extraction(extracted, saved, journey_applied, journey_depth, near_booking_stage)
    return _annotate_pricing(extracted, text + "\n" + html[:120000], near_booking_stage or journey_applied or "landing", max(0, journey_depth))



def _click_role_text(page, texts: List[str], roles: Optional[List[str]] = None, timeout: int = 1200) -> bool:
    roles = roles or ["button", "link", "tab", "option"]
    for text in texts:
        for role in roles:
            try:
                loc = page.get_by_role(role, name=re.compile(re.escape(text), re.I))
                if loc.count() > 0:
                    loc.first.click(timeout=timeout)
                    time.sleep(0.8)
                    return True
            except Exception:
                pass
    return False



def _smart_fill(page, terms: List[str], value: str, prefer_select: bool = False) -> bool:
    try:
        if page.evaluate(ATTRIBUTE_JS, {"terms": terms, "value": value, "preferSelect": prefer_select}):
            time.sleep(0.4)
            return True
    except Exception:
        pass
    selectors = []
    for term in terms:
        selectors.extend([
            f"input[name*='{term}' i]", f"input[id*='{term}' i]", f"input[placeholder*='{term}' i]",
            f"input[aria-label*='{term}' i]", f"select[name*='{term}' i]", f"select[id*='{term}' i]",
        ])
    for selector in selectors:
        try:
            loc = page.locator(selector)
            if loc.count() > 0:
                if prefer_select or selector.startswith("select"):
                    try:
                        loc.first.select_option(label=value)
                    except Exception:
                        try:
                            loc.first.select_option(value=value)
                        except Exception:
                            continue
                else:
                    loc.first.fill(value, timeout=1500)
                    loc.first.press("Enter", timeout=500)
                time.sleep(0.4)
                return True
        except Exception:
            pass
    return False



def _set_dates(page, depart_iso: str, return_iso: str) -> bool:
    ok = False
    date_variants = {
        "depart": [depart_iso, datetime.fromisoformat(depart_iso).strftime("%d/%m/%Y"), datetime.fromisoformat(depart_iso).strftime("%d %b %Y")],
        "return": [return_iso, datetime.fromisoformat(return_iso).strftime("%d/%m/%Y"), datetime.fromisoformat(return_iso).strftime("%d %b %Y")],
    }
    for variant in date_variants["depart"]:
        ok = _smart_fill(page, ["depart", "departure", "outbound", "flying out", "start date", "check in"], variant) or ok
        if ok:
            break
    for variant in date_variants["return"]:
        ok = _smart_fill(page, ["return", "inbound", "flying back", "end date", "check out"], variant) or ok
        if ok:
            break
    return ok



def _set_party(page, group: Dict[str, Any]) -> bool:
    changed = False
    _click_role_text(page, ["rooms", "travellers", "travelers", "guests", "who's going", "party size", "passengers"], ["button", "link", "combobox"])
    changed = _smart_fill(page, ["adults", "adult"], str(group.get("adults", 2)), prefer_select=True) or changed
    changed = _smart_fill(page, ["children", "child"], str(group.get("children", 0)), prefer_select=True) or changed
    changed = _smart_fill(page, ["infants", "infant", "babies", "baby"], str(group.get("infants", 0)), prefer_select=True) or changed
    child_ages = list(group.get("child_ages") or [])
    if child_ages:
        changed = _smart_fill(page, ["child age", "age of child", "child 1 age", "child age 1"], str(child_ages[0]), prefer_select=True) or changed
    rooms = int(group.get("rooms", 0) or 0)
    if rooms:
        changed = _smart_fill(page, ["rooms", "room", "number of rooms"], str(rooms), prefer_select=True) or changed
    if group.get("infants", 0):
        infant_months = int(group.get("infant_age_months", 10) or 10)
        changed = _smart_fill(page, ["infant age", "baby age", "age of infant"], str(infant_months), prefer_select=True) or changed
    return changed



def _apply_result_filters(page, site_name: str) -> int:
    clicked = 0
    texts = _profile(site_name).get("filter_texts", [])
    for text in texts:
        if _click_role_text(page, [text], ["checkbox", "button", "link", "option"]):
            clicked += 1
    return clicked



def _brief_tokens(meta: Dict[str, Any]) -> Set[str]:
    text = " ".join([
        str(meta.get("destination_city", "")),
        str(meta.get("destination_country", "")),
        str(meta.get("party_text", "")),
        str(meta.get("overview_prompt", "")),
    ]).lower()
    return {t for t in re.findall(r"[a-z0-9]+", text) if t}


def _build_journey_variants(site_name: str, meta: Dict[str, Any]) -> List[Dict[str, Any]]:
    tokens = _brief_tokens(meta)
    orchestration = meta.get("orchestration") or {}
    depth = int(orchestration.get("deep_package_passes", 4) or 4)
    variants = [
        {"label": "exact-brief", "family_filters": True, "room_push": True, "basket_push": True},
        {"label": "family-saver", "family_filters": True, "room_push": True, "basket_push": True, "child_focus": True},
        {"label": "price-led", "family_filters": False, "room_push": True, "basket_push": True},
        {"label": "near-match", "family_filters": False, "room_push": False, "basket_push": True, "relaxed": True},
    ]
    if "all" in tokens and "inclusive" in tokens:
        variants.insert(1, {"label": "all-inclusive", "family_filters": True, "board_focus": "all inclusive", "room_push": True, "basket_push": True})
    return variants[:max(1, depth)]


def _tap_terms(page, texts: List[str], roles: List[str] | None = None, limit: int = 2) -> int:
    clicked = 0
    roles = roles or ["button", "link", "checkbox", "option"]
    for text in texts:
        for role in roles:
            try:
                loc = page.get_by_role(role, name=re.compile(re.escape(text), re.I))
                count = min(limit, loc.count())
                for i in range(count):
                    try:
                        loc.nth(i).click(timeout=1200)
                        clicked += 1
                        time.sleep(0.6)
                    except Exception:
                        pass
            except Exception:
                pass
            if clicked >= limit:
                return clicked
    return clicked


def _drive_result_intelligence(page, site_name: str, variant: Dict[str, Any]) -> List[str]:
    profile = _profile(site_name)
    notes: List[str] = []
    if variant.get("family_filters"):
        count = _tap_terms(page, profile.get("family_filter_texts", []) or ["free child", "family", "apartment", "suite"], limit=3)
        if count:
            notes.append(f"family_filters:{count}")
    if variant.get("board_focus"):
        count = _tap_terms(page, [variant["board_focus"]], limit=1)
        if count:
            notes.append("board_focus")
    if variant.get("child_focus"):
        count = _tap_terms(page, ["free child", "free kids", "kids go free", "child place"], limit=2)
        if count:
            notes.append(f"child_focus:{count}")
    if variant.get("room_push"):
        count = _tap_terms(page, profile.get("room_texts", []) or ["family room", "suite", "apartment"], limit=2)
        if count:
            notes.append(f"room_push:{count}")
    if variant.get("relaxed"):
        count = _tap_terms(page, ["sort by price", "cheapest", "best value"], limit=1)
        if count:
            notes.append("relaxed_price_view")
    return notes


def _push_checkout_controls(page, site_name: str) -> List[str]:
    profile = _profile(site_name)
    notes: List[str] = []
    count = _tap_terms(page, profile.get("basket_texts", []) or [], roles=["button", "link"], limit=3)
    if count:
        notes.append(f"basket_push:{count}")
    return notes


def run_search_journey(page, site_name: str, meta: Dict[str, Any], batch: str, mode: str, slug_base: str):
    profile = _profile(site_name)
    if not profile or meta.get("site_type") != "package":
        return []

    all_rows = []
    dismiss_overlays(page)
    _click_role_text(page, ["holidays", "search", "search holidays"], ["button", "link", "tab"])
    group = meta.get("group") or {}
    base_notes: List[str] = []

    if _smart_fill(page, ["destination", "to", "where", "going to", "arrival airport"], meta.get("destination_city", "")):
        base_notes.append("destination")
    if _smart_fill(page, ["departure airport", "flying from", "from", "airport"], meta.get("origin_airport", "")):
        base_notes.append("origin")
    if _set_dates(page, meta.get("depart_iso", ""), meta.get("return_iso", "")):
        base_notes.append("dates")
    if _set_party(page, group):
        base_notes.append("party")

    if _click_role_text(page, profile.get("search_button_texts", ["search"]), ["button", "link"]):
        base_notes.append("search_submit")
        try:
            page.wait_for_load_state("domcontentloaded", timeout=12000)
        except Exception:
            pass
        time.sleep(4)

    filter_count = _apply_result_filters(page, site_name)
    if filter_count:
        base_notes.append(f"filters:{filter_count}")
        time.sleep(2)

    if not base_notes:
        return []

    variants = _build_journey_variants(site_name, meta)
    for idx, variant in enumerate(variants, start=1):
        variant_notes = list(base_notes)
        variant_notes.append(f"variant:{variant.get('label','base')}")
        variant_notes.extend(_drive_result_intelligence(page, site_name, variant))
        if variant.get("basket_push"):
            variant_notes.extend(_push_checkout_controls(page, site_name))
        extracted = extract_current_page(
            page,
            site_name,
            page.url,
            meta,
            batch,
            mode,
            f"{slug_base}_journey_results_{idx}",
            journey_applied="search_journey:" + ",".join(variant_notes),
            journey_depth=1 + idx,
            near_booking_stage="search-results",
        )
        all_rows.extend(extracted)
    return all_rows



def _drill_near_booking(page, site_name: str, meta: Dict[str, Any], batch: str, mode: str, slug_prefix: str):
    texts = _profile(site_name).get("near_booking_texts", [])
    if not texts:
        return []
    for text in texts:
        try:
            for role in ["button", "link"]:
                loc = page.get_by_role(role, name=re.compile(re.escape(text), re.I))
                if loc.count() == 0:
                    continue
                try:
                    with page.expect_navigation(wait_until="domcontentloaded", timeout=9000):
                        loc.first.click(timeout=1400)
                except Exception:
                    try:
                        loc.first.click(timeout=1400)
                        time.sleep(2.5)
                    except Exception:
                        continue
                time.sleep(3)
                return extract_current_page(
                    page,
                    site_name,
                    page.url,
                    meta,
                    batch,
                    mode,
                    f"{slug_prefix}_near_booking",
                    journey_applied="detail_drill",
                    journey_depth=2,
                    near_booking_stage=text,
                )
        except Exception:
            pass
    return []




def _payment_gate_visible(page) -> bool:
    try:
        body = clean_text(page.locator("body").inner_text(timeout=2000))[:12000].lower()
    except Exception:
        return False
    cues = ["card details", "payment details", "pay now", "secure booking", "review and pay", "payment summary", "to pay now"]
    return any(cue in body for cue in cues)


def _push_to_prepayment(page, site_name: str, meta: Dict[str, Any], batch: str, mode: str, slug_prefix: str):
    texts = [
        "continue to payment", "review booking", "payment summary", "review and pay",
        "traveller details", "guest details", "continue", "secure booking"
    ]
    out = []
    steps = 0
    for label in texts:
        if _payment_gate_visible(page):
            out.extend(extract_current_page(page, site_name, page.url, meta, batch, mode, f"{slug_prefix}_prepay_gate", journey_applied="prepayment_probe", journey_depth=5 + steps, near_booking_stage="pre-payment"))
            return out
        clicked = False
        for role in ["button", "link"]:
            try:
                loc = page.get_by_role(role, name=re.compile(re.escape(label), re.I))
                if loc.count() == 0:
                    continue
                try:
                    with page.expect_navigation(wait_until="domcontentloaded", timeout=8000):
                        loc.first.click(timeout=1500)
                except Exception:
                    try:
                        loc.first.click(timeout=1500)
                    except Exception:
                        continue
                clicked = True
                break
            except Exception:
                pass
        if not clicked:
            continue
        steps += 1
        time.sleep(2.5)
        out.extend(extract_current_page(page, site_name, page.url, meta, batch, mode, f"{slug_prefix}_prepay_{steps}", journey_applied="prepayment_probe", journey_depth=5 + steps, near_booking_stage="pre-payment"))
        if _payment_gate_visible(page) or steps >= 3:
            break
    return out



def _advance_checkout_flow(page, site_name: str, meta: Dict[str, Any], batch: str, mode: str, slug_prefix: str):
    profile = _profile(site_name)
    texts = profile.get('checkout_texts', []) or profile.get('near_booking_texts', [])
    if not texts:
        return []
    out = []
    steps = 0
    for label in texts[:6]:
        clicked = False
        for role in ['button', 'link']:
            try:
                loc = page.get_by_role(role, name=re.compile(re.escape(label), re.I))
                if loc.count() == 0:
                    continue
                try:
                    with page.expect_navigation(wait_until='domcontentloaded', timeout=9000):
                        loc.first.click(timeout=1500)
                except Exception:
                    try:
                        loc.first.click(timeout=1500)
                    except Exception:
                        continue
                clicked = True
                break
            except Exception:
                pass
        if not clicked:
            continue
        steps += 1
        time.sleep(3)
        _tap_terms(page, profile.get("room_texts", []) or ["family room", "suite", "apartment"], limit=2)
        _tap_terms(page, profile.get("family_filter_texts", []) or ["free child", "family"], limit=2)
        _tap_terms(page, profile.get("basket_texts", []) or ["summary", "price breakdown", "continue to payment"], roles=["button", "link"], limit=2)
        out.extend(extract_current_page(
            page, site_name, page.url, meta, batch, mode,
            f'{slug_prefix}_checkout_{steps}',
            journey_applied='checkout_orchestrator', journey_depth=2 + steps,
            near_booking_stage=f'checkout:{label}'
        ))
        if steps >= 3:
            break
    if not out or any((getattr(d, "pricing_completeness", "") == "pre-payment" or getattr(d, "checkout_stage", "") == "pre-payment") for d in out):
        return out
    out.extend(_push_to_prepayment(page, site_name, meta, batch, mode, slug_prefix))
    return out

def open_and_extract(context, url: str, site_name: str, meta: Dict[str, Any], batch: str, mode: str, slug_prefix: str):
    sub = context.new_page()
    try:
        sub.goto(url, wait_until="domcontentloaded", timeout=45000)
        time.sleep(2.5)
        deals = extract_current_page(sub, site_name, url, meta, batch, mode, slug_prefix, journey_applied="detail_open", journey_depth=1, near_booking_stage="detail")
        deeper = _drill_near_booking(sub, site_name, meta, batch, mode, slug_prefix)
        deals.extend(deeper)
        checkout = _advance_checkout_flow(sub, site_name, meta, batch, mode, slug_prefix)
        deals.extend(checkout)
        return deals
    except Exception:
        return []
    finally:
        sub.close()



def _click_candidates(page, site_name: str) -> int:
    clicked = 0
    for text in _profile(site_name).get("near_booking_texts", [])[:3]:
        try:
            for role in ["link", "button"]:
                loc = page.get_by_role(role, name=re.compile(re.escape(text), re.I))
                count = min(2, loc.count())
                for i in range(count):
                    try:
                        with page.expect_navigation(wait_until="domcontentloaded", timeout=8000):
                            loc.nth(i).click(timeout=1200)
                        time.sleep(2.0)
                        clicked += 1
                        if clicked >= 2:
                            return clicked
                        page.go_back(wait_until="domcontentloaded", timeout=8000)
                        time.sleep(1.2)
                    except Exception:
                        pass
        except Exception:
            pass
    return clicked



def run_provider_navigation(context, page, site_name: str, start_url: str, meta: Dict[str, Any], batch: str, mode: str, slug_base: str):
    profile = _profile(site_name)
    if not profile:
        return []

    deals = []
    visited_links: Set[str] = set()
    orchestration = meta.get("orchestration") or {}
    max_pages = min(4, max(profile.get("max_pages", 1), int(orchestration.get("site_revisit_depth", 3) or 3)))
    max_links = min(16, max(profile.get("max_links", 6), int(orchestration.get("target_candidates_per_site", 8) or 8)))

    for page_idx in range(max_pages):
        dismiss_overlays(page)
        slow_scroll(page, loops=4)
        expand_page(page, site_name)

        page_links = collect_candidate_links(page, site_name, page.url or start_url)
        for link_idx, link in enumerate(page_links[:max_links], start=1):
            if link in visited_links:
                continue
            visited_links.add(link)
            deeper = open_and_extract(
                context, link, site_name, meta, batch, mode,
                f"{slug_base}_detail_{page_idx+1}_{link_idx}"
            )
            deals.extend(deeper)

        if not page_links:
            _click_candidates(page, site_name)

        moved = False
        for text in profile.get("next_texts", []):
            try:
                for role in ["button", "link"]:
                    loc = page.get_by_role(role, name=re.compile(re.escape(text), re.I))
                    if loc.count() > 0:
                        try:
                            with page.expect_navigation(wait_until="domcontentloaded", timeout=10000):
                                loc.first.click(timeout=1500)
                        except Exception:
                            loc.first.click(timeout=1500)
                            time.sleep(2.0)
                        moved = True
                        break
                if moved:
                    break
            except Exception:
                pass
        if not moved:
            break
        time.sleep(2.0)
    return deals



def run_scan(mode: str = "balanced", headed: bool = True, limit: int = 120, max_flight_queries: int = 40):
    cfg = load_config()
    urls = generate_urls_preview(max_flight_queries=max_flight_queries)
    orchestration = cfg.get("orchestration", {}) or {}
    max_rows = int(orchestration.get("max_live_rows", orchestration.get("target_search_volume", limit)) or limit)
    effective_limit = min(limit or max_rows, max_rows)
    if max_rows > 0:
        urls = urls[:effective_limit]
    batch = datetime.now().strftime("%Y%m%d_%H%M%S")
    from playwright.sync_api import sync_playwright

    deals = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not headed)
        provider_contexts = {}

        for i, row in enumerate(urls[:effective_limit], start=1):
            site_name = row["site"]
            url = row["url"]
            meta = {
                "site_type": row["type"],
                "destination_city": row.get("destination", ""),
                "destination_country": row.get("country", ""),
                "depart_iso": row.get("date", ""),
                "return_iso": row.get("return_iso", row.get("return_date", "")),
                "nights": row.get("nights"),
                "origin_airport": cfg.get("origin_airport", "Newcastle"),
                "group": cfg.get("group", {}),
                "party_text": package_profile_text(cfg),
                "overview_prompt": cfg.get("overview_prompt", ""),
                "orchestration": cfg.get("orchestration", {}),
                "search_route_id": row.get("search_route_id", ""),
                "provider_priority_band": row.get("provider_priority_band", "support"),
                "search_variant": row.get("window_label", ""),
                "search_passes": row.get("search_budget", 1),
            }
            context = _load_or_create_context(browser, site_name, meta, provider_exec, provider_contexts)
            page = context.new_page()
            print(f"[{i}/{min(effective_limit, len(urls))}] {site_name} -> {url}")
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=50000)
                time.sleep(4)
                slug_base = re.sub(r"[^a-z0-9]+", "_", f"{batch}_{i}_{site_name}").strip("_")[:90]
                extracted = extract_current_page(page, site_name, url, meta, batch, mode, f"{slug_base}_landing", journey_applied="landing", journey_depth=0, near_booking_stage="landing")
                deals.extend(extracted)
                print(f"  extracted landing rows: {len(extracted)}")

                journey_rows = run_search_journey(page, site_name, meta, batch, mode, slug_base)
                deals.extend(journey_rows)
                if journey_rows:
                    print(f"  extracted search-journey rows: {len(journey_rows)}")

                deeper = run_provider_navigation(context, page, site_name, page.url or url, meta, batch, mode, slug_base)
                deals.extend(deeper)
                if deeper:
                    print(f"  extracted provider-detail rows: {len(deeper)}")
            except Exception as e:
                print(f"  failed: {e}")
            finally:
                page.close()
        for _ctx in provider_contexts.values():
            try:
                _ctx.close()
            except Exception:
                pass
        browser.close()

    deals = dedupe_deals(deals)
    prev = load_previous_history()
    score_deals(deals, cfg["weights"], mode, cfg)
    apply_price_deltas(deals, prev, cfg)
    deals.sort(key=lambda x: ((x.adjusted_buy_now_score or x.buy_now_score or 0), -(effective_price(x) or 999999)), reverse=True)
    apply_strategy_overlay(deals, cfg)
    write_outputs(deals, cfg)
    save_history_snapshot(deals)
    return len(deals)



_old_annotate_pricing = _annotate_pricing
_old_run_search_journey = run_search_journey
_old_run_provider_navigation = run_provider_navigation



EVIDENCE = RESULTS / "evidence"
EVIDENCE.mkdir(exist_ok=True)
SESSIONS = RESULTS / "sessions"
SESSIONS.mkdir(exist_ok=True)
TUNING = RESULTS / "tuning"
TUNING.mkdir(exist_ok=True)
BASKETS = RESULTS / "baskets"
BASKETS.mkdir(exist_ok=True)


PROVIDER_STATE_MACHINES = {
    "jet2 holidays": {
        "states": ["search", "results", "hotel", "rooms", "extras", "basket", "pre-payment"],
        "room_terms": ["family room", "suite", "apartment", "interconnecting", "two bedroom"],
        "basket_terms": ["price breakdown", "holiday summary", "continue", "continue to payment", "extras"],
        "summary_terms": ["to pay now", "payment summary", "holiday total", "amount due"]
    },
    "tui holidays": {
        "states": ["search", "results", "hotel", "rooms", "extras", "basket", "pre-payment"],
        "room_terms": ["family room", "suite", "interconnecting", "apartment"],
        "basket_terms": ["price breakdown", "continue", "continue to payment", "summary", "your holiday"],
        "summary_terms": ["to pay now", "payment summary", "holiday price", "amount due"]
    },
    "loveholidays": {
        "states": ["search", "results", "hotel", "rooms", "extras", "basket", "pre-payment"],
        "room_terms": ["family room", "suite", "apartment", "two bedroom"],
        "basket_terms": ["select room", "continue", "summary", "book now", "continue to payment"],
        "summary_terms": ["to pay now", "payment summary", "total holiday price", "amount due"]
    },
    "on the beach": {
        "states": ["search", "results", "hotel", "rooms", "extras", "basket", "pre-payment"],
        "room_terms": ["family room", "suite", "apartment", "interconnecting"],
        "basket_terms": ["room options", "continue", "summary", "book now", "continue to payment"],
        "summary_terms": ["to pay now", "payment summary", "total cost", "amount due"]
    },
}


def _safe_slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (value or "").lower()).strip("_")[:120]


def _provider_session_id(site_name: str, meta: Dict[str, Any]) -> str:
    parts = [site_name, str(meta.get('destination_city','')), str(meta.get('depart_iso','')), str(meta.get('return_iso','')), str((meta.get('group') or {}).get('rooms',0))]
    return _safe_slug("_".join(parts))[:80]


def _session_state_path(site_name: str, meta: Dict[str, Any]) -> Path:
    return SESSIONS / f"{_provider_session_id(site_name, meta)}.json"


def _load_or_create_context(browser, site_name: str, meta: Dict[str, Any], provider_exec: Dict[str, Any], cache: Dict[str, Any]):
    key = _provider_session_id(site_name, meta)
    existing = cache.get(key)
    if existing is not None:
        return existing
    kwargs = {"viewport": {"width": 1440, "height": 1100}}
    state_path = _session_state_path(site_name, meta)
    if provider_exec.get('sticky_sessions', True) and state_path.exists():
        kwargs['storage_state'] = str(state_path)
    ctx = browser.new_context(**kwargs)
    ctx.set_default_timeout(15000)
    cache[key] = ctx
    return ctx


def _save_context_state(context, site_name: str, meta: Dict[str, Any], provider_exec: Dict[str, Any]):
    if not provider_exec.get('sticky_sessions', True):
        return
    try:
        context.storage_state(path=str(_session_state_path(site_name, meta)))
    except Exception:
        pass


def _canonical_basket_payload(d, meta: Dict[str, Any], stage: str = "") -> Dict[str, Any]:
    return {
        "provider": d.source_site,
        "url": d.source_url,
        "search_route_id": d.search_route_id or meta.get('search_route_id',''),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "checkout_stage": d.checkout_stage or d.near_booking_stage or stage or d.provider_state,
        "pricing_truth_label": d.pricing_truth_label,
        "price_total_gbp": d.price_total_gbp,
        "headline_price_gbp": d.headline_price_gbp,
        "basket_price_gbp": d.basket_price_gbp,
        "price_reference_gbp": d.price_reference_gbp,
        "deposit_price_gbp": d.deposit_price_gbp,
        "due_now_gbp": d.due_now_gbp,
        "discount_note": d.discount_note,
        "discount_savings_gbp": d.discount_savings_gbp,
        "board_basis": d.board_basis,
        "room_type": d.family_room_type or d.basket_room_text,
        "rooms_requested": d.rooms_requested,
        "rooms_matched": d.rooms_matched,
        "bags_included": d.bags_included,
        "transfers_included": d.transfers_included,
        "baggage_summary": d.baggage_summary,
        "transfer_summary": d.transfer_summary,
        "free_child_place": d.free_child_place,
        "child_pricing_note": d.child_pricing_note,
        "infant_cost_note": d.infant_cost_note,
        "taxes_fees_note": d.taxes_fees_note,
        "beach_minutes": d.beach_minutes,
        "pool": d.pool,
        "estimated_temp_c": d.estimated_temp_c,
        "true_price_confidence": d.true_price_confidence,
        "provider_state": d.provider_state,
        "automation_status": d.automation_status or ("captured" if d.price_reference_gbp is not None else "partial"),
        "evidence": {
            "text": d.raw_text_file,
            "html": d.html_capture_file,
            "screenshot": d.screenshot_file,
            "session_id": d.session_id,
        },
        "meta": {
            "destination_city": meta.get('destination_city',''),
            "destination_country": meta.get('destination_country',''),
            "depart_iso": meta.get('depart_iso',''),
            "return_iso": meta.get('return_iso',''),
            "nights": meta.get('nights'),
            "party_text": meta.get('party_text',''),
            "search_variant": meta.get('search_variant',''),
            "provider_priority_band": meta.get('provider_priority_band',''),
        },
    }


def _write_basket_json(d, meta: Dict[str, Any], stage: str = "") -> str:
    name = _safe_slug(f"{d.source_site}_{d.hotel_name or d.destination}_{d.departure_date}_{d.nights}_{d.session_id or 'session'}")[:140] or 'basket'
    path = BASKETS / f"{name}.json"
    path.write_text(json.dumps(_canonical_basket_payload(d, meta, stage), indent=2), encoding='utf-8')
    return path.name


def _append_provider_trace(site_name: str, meta: Dict[str, Any], stage: str, event: str, details: Dict[str, Any]):
    path = TUNING / f"{_safe_slug(site_name)}_journey_trace.jsonl"
    row = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "site": site_name,
        "session_id": _provider_session_id(site_name, meta),
        "search_route_id": meta.get('search_route_id',''),
        "stage": stage,
        "event": event,
        "details": details,
    }
    with path.open('a', encoding='utf-8') as f:
        f.write(json.dumps(row) + "\n")


def _capture_evidence(page, slug: str, save_html: bool = True, save_screens: bool = True) -> Dict[str, str]:
    txt = clean_text(page.locator("body").inner_text(timeout=12000))
    html = page.content() if save_html else ""
    txt_path = EVIDENCE / f"{slug}.txt"
    txt_path.write_text(txt[:240000], encoding='utf-8')
    html_name = ""
    if save_html:
        html_path = EVIDENCE / f"{slug}.html"
        html_path.write_text(html[:650000], encoding='utf-8')
        html_name = html_path.name
    png_name = ""
    if save_screens:
        try:
            png_path = EVIDENCE / f"{slug}.png"
            page.screenshot(path=str(png_path), full_page=True)
            png_name = png_path.name
        except Exception:
            png_name = ""
    return {"txt": txt_path.name, "html": html_name, "png": png_name, "text": txt, "html_text": html}


def _extract_payment_numbers(text: str) -> Dict[str, Any]:
    tl = clean_text(text or '')
    out = {"deposit_price_gbp": None, "due_now_gbp": None, "taxes_fees_note": "", "baggage_summary": "", "transfer_summary": "", "rooms_matched": 0}
    patterns = {
        'deposit_price_gbp': [r'(?:deposit|low deposit|book for)\D{0,20}£\s?([0-9]{2,5}(?:,[0-9]{3})*(?:\.\d{2})?)'],
        'due_now_gbp': [r'(?:to pay now|pay now|amount due|due today)\D{0,20}£\s?([0-9]{2,5}(?:,[0-9]{3})*(?:\.\d{2})?)']
    }
    for key, pats in patterns.items():
        for pat in pats:
            m = re.search(pat, tl, re.I)
            if m:
                try:
                    out[key] = float(m.group(1).replace(',', ''))
                    break
                except Exception:
                    pass
    fees = re.search(r'((?:taxes|fees|resort fee|local tax)[^.|;]{0,120})', tl, re.I)
    if fees:
        out['taxes_fees_note'] = clean_text(fees.group(1))
    bags = re.search(r'((?:bags?|baggage)[^.|;]{0,120})', tl, re.I)
    if bags:
        out['baggage_summary'] = clean_text(bags.group(1))
    transfers = re.search(r'((?:transfer|coach|shuttle)[^.|;]{0,120})', tl, re.I)
    if transfers:
        out['transfer_summary'] = clean_text(transfers.group(1))
    room_hits = len(re.findall(r'family room|suite|apartment|interconnecting|two bedroom|1 bedroom|2 bedroom', tl, re.I))
    out['rooms_matched'] = room_hits
    return out


def _attach_payment_numbers(deals, text: str, meta: Dict[str, Any], state: str = ""):
    extracted = _extract_payment_numbers(text)
    requested_rooms = int(((meta.get('group') or {}).get('rooms', 0)) or 0)
    session_id = _provider_session_id(meta.get('source_site', '') or '', meta) if meta else ""
    for d in deals:
        for k, v in extracted.items():
            if getattr(d, k, None) in (None, '', 0):
                setattr(d, k, v)
        if requested_rooms:
            d.rooms_requested = requested_rooms
        if extracted.get('baggage_summary') and not d.basket_inclusions:
            d.basket_inclusions = extracted.get('baggage_summary','')
        d.session_id = d.session_id or session_id
        if state:
            d.provider_state = state
            d.automation_status = 'captured' if d.price_reference_gbp is not None else 'partial'
        try:
            d.basket_json_file = _write_basket_json(d, meta, state)
        except Exception:
            pass
    _append_provider_trace(meta.get('source_site', ''), meta, state or 'capture', 'page_captured', {
        'price_candidates_found': sum(1 for d in deals if d.price_reference_gbp is not None),
        'rooms_requested': requested_rooms,
        'due_now_seen': extracted.get('due_now_gbp'),
        'deposit_seen': extracted.get('deposit_price_gbp'),
    })
    return deals


def extract_current_page(page, site_name: str, url: str, meta: Dict[str, Any], batch: str, mode: str, slug_prefix: str, journey_applied: str = "", journey_depth: int = 0, near_booking_stage: str = ""):
    dismiss_overlays(page)
    slow_scroll(page, loops=3)
    expand_page(page, site_name)
    slug = _safe_slug(slug_prefix)
    exec_cfg = (load_config().get('provider_execution') or {})
    saved = _capture_evidence(page, slug, save_html=bool(exec_cfg.get('save_html', True)), save_screens=bool(exec_cfg.get('save_screenshots', True)))
    text = saved['text']
    html = saved['html_text']
    extracted = llm_extract(text, site_name, url, meta, batch, mode, html=html)
    meta = dict(meta or {})
    meta['source_site'] = site_name
    extracted = _annotate_extraction(extracted, saved, journey_applied, journey_depth, near_booking_stage, meta=meta)
    extracted = _annotate_pricing(extracted, text + "\n" + html[:120000], near_booking_stage or journey_applied or "landing", max(0, journey_depth))
    return _attach_payment_numbers(extracted, text + "\n" + html[:150000], meta, state=near_booking_stage or journey_applied or 'landing')


def _annotate_extraction(deals, saved: Dict[str, str], journey_applied: str = "", journey_depth: int = 0, near_booking_stage: str = "", meta: Optional[Dict[str, Any]] = None):
    meta = meta or {}
    for d in deals:
        d.raw_text_file = saved.get("txt", "")
        d.html_capture_file = saved.get("html", "")
        d.screenshot_file = saved.get("png", "")
        if journey_applied:
            d.journey_applied = journey_applied
        if journey_depth:
            d.journey_depth = max(d.journey_depth or 0, journey_depth)
        if near_booking_stage:
            d.near_booking_stage = near_booking_stage
            d.provider_state = near_booking_stage
        if meta.get("search_route_id"):
            d.search_route_id = meta.get("search_route_id")
        if meta.get("provider_priority_band"):
            d.provider_priority_band = meta.get("provider_priority_band")
        if meta.get("search_variant"):
            d.search_variant = meta.get("search_variant")
        d.search_passes = max(d.search_passes or 0, int(meta.get("search_passes", 1) or 1))
        d.session_id = d.session_id or _provider_session_id(meta.get('source_site', d.source_site or ''), meta)
        d.evidence_note = f"evidence/{saved.get('png','-')} | evidence/{saved.get('html','-')}"
    return deals


def _annotate_pricing(deals, text: str, stage_label: str, step_count: int):
    deals = _old_annotate_pricing(deals, text, stage_label, step_count)
    payment = _extract_payment_numbers(text)
    for d in deals:
        if payment.get('deposit_price_gbp') is not None and d.deposit_price_gbp is None:
            d.deposit_price_gbp = payment['deposit_price_gbp']
        if payment.get('due_now_gbp') is not None and d.due_now_gbp is None:
            d.due_now_gbp = payment['due_now_gbp']
        if payment.get('taxes_fees_note') and not d.taxes_fees_note:
            d.taxes_fees_note = payment['taxes_fees_note']
        if payment.get('baggage_summary') and not d.baggage_summary:
            d.baggage_summary = payment['baggage_summary']
        if payment.get('transfer_summary') and not d.transfer_summary:
            d.transfer_summary = payment['transfer_summary']
        if stage_label:
            d.provider_state = stage_label
    return deals


def _retryable_open(context, url: str, attempts: int = 2):
    last = None
    for _ in range(max(1, attempts)):
        page = context.new_page()
        try:
            page.goto(url, wait_until='domcontentloaded', timeout=50000)
            time.sleep(2.5)
            dismiss_overlays(page)
            return page
        except Exception as e:
            last = e
            try:
                page.close()
            except Exception:
                pass
            time.sleep(1.2)
    raise last or RuntimeError('unable to open page')


def _click_named(page, labels: List[str], roles: List[str] = None, max_clicks: int = 2) -> int:
    roles = roles or ['button','link','checkbox','option']
    clicks = 0
    for label in labels:
        for role in roles:
            try:
                loc = page.get_by_role(role, name=re.compile(re.escape(label), re.I))
                count = min(max_clicks, loc.count())
                for i in range(count):
                    try:
                        loc.nth(i).click(timeout=1600)
                        time.sleep(1.0)
                        clicks += 1
                    except Exception:
                        pass
                if clicks >= max_clicks:
                    return clicks
            except Exception:
                pass
    return clicks


def _run_provider_state_machine(page, site_name: str, meta: Dict[str, Any], batch: str, mode: str, slug_prefix: str):
    machine = PROVIDER_STATE_MACHINES.get(_site_key(site_name))
    if not machine:
        return []
    out = []
    # rooms / family / basket driven sequence
    sequences = [
        ('rooms', machine.get('room_terms', []), ['button','link','checkbox','option']),
        ('extras', ['bags included', 'baggage included', 'transfers included', 'free child', 'free kids', 'all inclusive'], ['button','link','checkbox','option']),
        ('basket', machine.get('basket_terms', []), ['button','link']),
        ('pre-payment', machine.get('summary_terms', []), ['button','link']),
    ]
    steps = 0
    for state, labels, roles in sequences:
        hit = _click_named(page, labels, roles=roles, max_clicks=2)
        if not hit:
            continue
        steps += 1
        _append_provider_trace(site_name, meta, state, 'state_clicks', {'clicks': hit})
        out.extend(extract_current_page(page, site_name, page.url, meta, batch, mode, f"{slug_prefix}_{state}_{steps}", journey_applied=f"state_machine:{state}", journey_depth=3+steps, near_booking_stage=state))
        if state == 'pre-payment' and _payment_gate_visible(page):
            break
    return out


def run_search_journey(page, site_name: str, meta: Dict[str, Any], batch: str, mode: str, slug_base: str):
    rows = _old_run_search_journey(page, site_name, meta, batch, mode, slug_base)
    if meta.get('site_type') == 'package':
        rows.extend(_run_provider_state_machine(page, site_name, meta, batch, mode, slug_base + '_sm'))
    return rows


def open_and_extract(context, url: str, site_name: str, meta: Dict[str, Any], batch: str, mode: str, slug_prefix: str):
    cfg = load_config()
    retries = int(((cfg.get('provider_execution') or {}).get('retry_attempts', 2)) or 2)
    sub = None
    try:
        sub = _retryable_open(context, url, attempts=retries)
        _append_provider_trace(site_name, meta, 'hotel', 'detail_open', {'url': url})
        deals = extract_current_page(sub, site_name, url, meta, batch, mode, slug_prefix, journey_applied='detail_open', journey_depth=1, near_booking_stage='hotel')
        deals.extend(_drill_near_booking(sub, site_name, meta, batch, mode, slug_prefix))
        deals.extend(_advance_checkout_flow(sub, site_name, meta, batch, mode, slug_prefix))
        deals.extend(_run_provider_state_machine(sub, site_name, meta, batch, mode, slug_prefix + '_state'))
        return deals
    except Exception:
        return []
    finally:
        if sub is not None:
            try:
                sub.close()
            except Exception:
                pass


def run_provider_navigation(context, page, site_name: str, start_url: str, meta: Dict[str, Any], batch: str, mode: str, slug_base: str):
    deals = _old_run_provider_navigation(context, page, site_name, start_url, meta, batch, mode, slug_base)
    # revisit promising detail pages for top providers when still shallow
    if meta.get('site_type') == 'package' and not any((d.pricing_completeness or '') == 'pre-payment' for d in deals):
        extra_links = collect_candidate_links(page, site_name, page.url or start_url)[:4]
        for idx, link in enumerate(extra_links, start=1):
            deals.extend(open_and_extract(context, link, site_name, meta, batch, mode, f"{slug_base}_revisit_{idx}"))
    return deals


def run_scan(mode: str = "balanced", headed: bool = True, limit: int = 120, max_flight_queries: int = 40):
    cfg = load_config()
    urls = generate_urls_preview(max_flight_queries=max_flight_queries)
    orchestration = cfg.get("orchestration", {}) or {}
    provider_exec = cfg.get('provider_execution', {}) or {}
    max_rows = int(orchestration.get("max_live_rows", orchestration.get("target_search_volume", limit)) or limit)
    effective_limit = min(limit or max_rows, max_rows)
    if max_rows > 0:
        urls = urls[:effective_limit]
    batch = datetime.now().strftime("%Y%m%d_%H%M%S")
    from playwright.sync_api import sync_playwright

    deals = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not headed)
        provider_contexts = {}

        for i, row in enumerate(urls[:effective_limit], start=1):
            site_name = row["site"]
            url = row["url"]
            meta = {
                "site_type": row["type"],
                "destination_city": row.get("destination", ""),
                "destination_country": row.get("country", ""),
                "depart_iso": row.get("date", ""),
                "return_iso": row.get("return_iso", row.get("return_date", "")),
                "nights": row.get("nights"),
                "origin_airport": cfg.get("origin_airport", "Newcastle"),
                "group": cfg.get("group", {}),
                "party_text": package_profile_text(cfg),
                "overview_prompt": cfg.get("overview_prompt", ""),
                "orchestration": cfg.get("orchestration", {}),
                "search_route_id": row.get("search_route_id", ""),
                "provider_priority_band": row.get("provider_priority_band", "support"),
                "search_variant": row.get("window_label", ""),
                "search_passes": row.get("search_budget", 1),
                "source_site": site_name,
            }
            context = _load_or_create_context(browser, site_name, meta, provider_exec, provider_contexts)
            page = context.new_page()
            print(f"[{i}/{min(effective_limit, len(urls))}] {site_name} -> {url}")
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=50000)
                time.sleep(4)
                slug_base = _safe_slug(f"{batch}_{i}_{site_name}")
                extracted = extract_current_page(page, site_name, url, meta, batch, mode, f"{slug_base}_landing", journey_applied="landing", journey_depth=0, near_booking_stage="landing")
                deals.extend(extracted)
                journey_rows = run_search_journey(page, site_name, meta, batch, mode, slug_base)
                deals.extend(journey_rows)
                deeper = run_provider_navigation(context, page, site_name, page.url or url, meta, batch, mode, slug_base)
                deals.extend(deeper)
                # save provider storage state for sticky sessions / evidence
                if provider_exec.get('sticky_sessions', True):
                    try:
                        _save_context_state(context, site_name, meta, provider_exec)
                    except Exception:
                        pass
            except Exception as e:
                print(f"  failed: {e}")
            finally:
                try:
                    page.close()
                except Exception:
                    pass
        for _ctx in provider_contexts.values():
            try:
                _ctx.close()
            except Exception:
                pass
        browser.close()

    deals = dedupe_deals(deals)
    prev = load_previous_history()
    score_deals(deals, cfg["weights"], mode, cfg)
    apply_price_deltas(deals, prev, cfg)
    deals.sort(key=lambda x: ((x.adjusted_buy_now_score or x.buy_now_score or 0), -(effective_price(x) or 999999)), reverse=True)
    apply_strategy_overlay(deals, cfg)
    write_outputs(deals, cfg)
    save_history_snapshot(deals)
    return len(deals)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["balanced", "cheapest", "best_value", "low_stress"], default="balanced")
    p.add_argument("--headless", action="store_true")
    p.add_argument("--limit", type=int, default=120)
    p.add_argument("--max-flight-queries", type=int, default=40)
    a = p.parse_args()
    count = run_scan(mode=a.mode, headed=not a.headless, limit=a.limit, max_flight_queries=a.max_flight_queries)
    print(f"Saved {count} deals")
