from __future__ import annotations

import csv
import json
import math
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

ROOT = Path(__file__).resolve().parent
RESULTS = ROOT / "results"
RESULTS.mkdir(exist_ok=True)
HISTORY = RESULTS / "history"
HISTORY.mkdir(exist_ok=True)


DEFAULT_CONFIG: Dict[str, Any] = {
    "group": {
        "adults": 4,
        "children": 1,
        "child_ages": [5],
        "child_age": 5,
        "infants": 1,
        "infant_age_months": 10,
        "rooms": 2,
    },
    "origin_airport": "Newcastle",
    "origin_code": "NCL",
    "currency": "GBP",
    "date_windows": [
        {"label": "full_range_7", "start": "2026-06-20", "end": "2026-07-15", "nights": 7},
        {"label": "full_range_10", "start": "2026-06-20", "end": "2026-07-12", "nights": 10},
    ],
    "destinations": [
        {"city": "Antalya", "code": "AYT", "country": "Turkey"},
        {"city": "Dalaman", "code": "DLM", "country": "Turkey"},
        {"city": "Bodrum", "code": "BJV", "country": "Turkey"},
        {"city": "Burgas", "code": "BOJ", "country": "Bulgaria"},
        {"city": "Hurghada", "code": "HRG", "country": "Egypt"},
        {"city": "Sharm El Sheikh", "code": "SSH", "country": "Egypt"},
        {"city": "Rhodes", "code": "RHO", "country": "Greece"},
        {"city": "Kos", "code": "KGS", "country": "Greece"},
        {"city": "Heraklion", "code": "HER", "country": "Greece"},
        {"city": "Larnaca", "code": "LCA", "country": "Cyprus"},
        {"city": "Paphos", "code": "PFO", "country": "Cyprus"},
        {"city": "Palma", "code": "PMI", "country": "Spain"},
        {"city": "Tenerife", "code": "TFS", "country": "Spain"},
        {"city": "Faro", "code": "FAO", "country": "Portugal"},
        {"city": "Enfidha", "code": "NBE", "country": "Tunisia"},
        {"city": "Corfu", "code": "CFU", "country": "Greece"},
        {"city": "Zakynthos", "code": "ZTH", "country": "Greece"},
        {"city": "Malta", "code": "MLA", "country": "Malta"},
    ],
    "sites": [
        {"name": "TravelSupermarket Packages", "type": "package", "url_template": "https://www.travelsupermarket.com/en-gb/holidays/"},
        {"name": "Jet2 Holidays", "type": "package", "url_template": "https://www.jet2holidays.com/airports/newcastle-airport"},
        {"name": "TUI Holidays", "type": "package", "url_template": "https://www.tui.co.uk/holidays/holidays-from-newcastle-airport"},
        {"name": "loveholidays", "type": "package", "url_template": "https://www.loveholidays.com/holidays/newcastle-airport-holidays.html"},
        {"name": "On the Beach", "type": "package", "url_template": "https://www.onthebeach.co.uk/"},
        {"name": "Expedia Packages", "type": "package", "url_template": "https://www.expedia.co.uk/"},
        {"name": "Trivago", "type": "hotel_meta", "url_template": "https://www.trivago.co.uk/"},
        {"name": "Google Flights", "type": "flight", "url_template": "https://www.google.com/travel/flights?q={google_flights_query}"},
        {"name": "Skyscanner", "type": "flight", "url_template": "https://www.skyscanner.net/transport/flights/{origin}/{destination}/{depart}/{return_}/?adults={adults}&children={children}&adultsv2={adults}&childrenv2={children}&infants={infants}"},
        {"name": "KAYAK", "type": "flight", "url_template": "https://www.kayak.co.uk/flights/{origin}-{destination}/{depart_iso}/{return_iso}?sort=bestflight_a"},
        {"name": "Momondo", "type": "flight", "url_template": "https://www.momondo.co.uk/flight-search/{origin}-{destination}/{depart_iso}/{return_iso}?sort=bestflight_a"},
    ],
    "weights": {
        "price": 0.41,
        "review": 0.09,
        "board_basis": 0.05,
        "bags": 0.04,
        "transfers": 0.04,
        "beach": 0.13,
        "pool": 0.07,
        "heat": 0.10,
        "cancellation": 0.01,
        "family_fit": 0.06,
    },
    "autopilot": {"drop_threshold_gbp": 100, "drop_threshold_pct": 0.06},
    "strategy": {
        "buy_line_gbp": 3000,
        "nearby_airport_adjustments": {
            "Newcastle only": 0,
            "Manchester": -250,
            "Leeds Bradford": -150,
        },
        "heat_month": 6,
        "heat_floor_c": 26,
        "default_day_shift_savings_gbp": 35,
        "default_airport_mode": "Newcastle only",
        "default_day_shift": 0,
    },
    "search_preferences": {
        "cheap_first": True,
        "prefer_hot": True,
        "prefer_beach": True,
        "prefer_pool": True,
        "prefer_breakfast_or_better": True,
        "prefer_free_child_places": True,
        "prefer_family_room": True,
        "min_temp_c": 26,
        "target_beach_minutes": 10,
        "beach_walk_max_minutes": 10,
        "months": [6, 7],
        "trip_lengths": [7, 10],
        "best_for": "price",
        "prefer_true_price": True,
    },
    "overview_prompt": "Warm beach holiday in June or July, price first, with a family-friendly package.",
    "preference_scales": {
        "price_weight": 48,
        "beach_weight": 22,
        "heat_weight": 16,
        "family_weight": 10,
        "trend_weight": 4,
        "strictness": 72,
        "price_slack_gbp": 250,
        "beach_slack_minutes": 5,
        "temp_slack_c": 2,
    },
    "search_engine": {
        "destination_count": 22,
        "start_day": 20,
        "end_day": 15,
        "use_extended_destination_pool": True,
        "daily_departure_step": 1
    },
    "orchestration": {
        "package_site_limit": 6,
        "flight_site_limit": 4,
        "target_candidates_per_site": 12,
        "max_live_rows": 180,
        "target_search_volume": 180,
        "package_query_cap": 96,
        "flight_query_cap": 40,
        "site_revisit_depth": 4,
        "deep_package_passes": 4,
        "prefer_near_checkout": True,
        "prioritise_family_discount_checks": True,
        "checkout_target_stage": "pre-payment",
        "booking_stop_mode": "payment-page",
        "provider_revisit_top_n": 4
    },
    "provider_execution": {
        "top_package_providers": ["Jet2 Holidays", "TUI Holidays", "loveholidays", "On the Beach"],
        "enable_checkout_depth": True,
        "max_checkout_attempts": 4,
        "save_evidence_for_top": 30,
        "retry_attempts": 2,
        "sticky_sessions": True,
        "save_html": True,
        "save_screenshots": True
    },
    "provider_tuning": {
        "top_sites": ["Jet2 Holidays", "TUI Holidays", "loveholidays", "On the Beach"],
        "minimum_truth_confidence": 0.84,
        "evidence_capture_limit": 40,
        "trace_partial_routes": True,
        "save_basket_json": True
    },
}

@dataclass
class Deal:
    source_site: str
    source_url: str
    deal_type: str
    destination: str = ""
    country: str = ""
    departure_date: str = ""
    nights: Optional[int] = None
    hotel_name: str = ""
    board_basis: str = ""
    price_total_gbp: Optional[float] = None
    headline_price_gbp: Optional[float] = None
    basket_price_gbp: Optional[float] = None
    price_reference_gbp: Optional[float] = None
    pricing_completeness: str = ""
    checkout_stage: str = ""
    checkout_step_count: int = 0
    discount_note: str = ""
    discount_savings_gbp: Optional[float] = None
    basket_inclusions: str = ""
    basket_room_text: str = ""
    true_price_confidence: Optional[float] = None
    review_score: Optional[float] = None
    pool: Optional[bool] = None
    beach_minutes: Optional[int] = None
    bags_included: Optional[bool] = None
    transfers_included: Optional[bool] = None
    free_child_place: Optional[bool] = None
    family_room_type: str = ""
    child_pricing_note: str = ""
    infant_cost_note: str = ""
    free_cancellation: Optional[bool] = None
    near_booking_stage: str = ""
    journey_applied: str = ""
    journey_depth: int = 0
    notes: str = ""
    extraction_confidence: Optional[float] = None
    ai_summary: str = ""
    raw_text_file: str = ""
    screenshot_file: str = ""
    score: Optional[float] = None
    scan_batch: str = ""
    scan_mode: str = ""
    duplicate_key: str = ""
    verified: Optional[bool] = None
    booking_bucket: str = ""
    warning_flags: str = ""
    extractor_used: str = ""
    provider_rank: Optional[int] = None
    pros: str = ""
    cons: str = ""
    likely_price_direction: str = ""
    likely_price_reason: str = ""
    fit_summary: str = ""
    price_delta_gbp: Optional[float] = None
    price_delta_pct: Optional[float] = None
    deal_signal: str = ""
    buy_now_score: Optional[float] = None
    scenario_price_gbp: Optional[float] = None
    strategy_airport_mode: str = ""
    strategy_day_shift: int = 0
    adjusted_buy_now_score: Optional[float] = None
    action_now: str = ""
    recommendation: str = ""
    heat_score: Optional[float] = None
    estimated_temp_c: Optional[float] = None
    fit_label: str = ""
    fit_gap_score: Optional[float] = None
    near_miss_reason: str = ""
    option_id: str = ""
    option_group_size: int = 0
    best_group_price_gbp: Optional[float] = None
    best_group_source: str = ""
    group_price_gap_gbp: Optional[float] = None
    source_count: int = 0
    recommendation_bucket: str = ""
    recommendation_rank: Optional[int] = None
    shortlist_reason: str = ""
    provider_trust_score: Optional[float] = None
    pricing_truth_label: str = ""
    search_route_id: str = ""
    search_variant: str = ""
    provider_priority_band: str = ""
    search_passes: int = 0
    html_capture_file: str = ""
    basket_json_file: str = ""
    deposit_price_gbp: Optional[float] = None
    due_now_gbp: Optional[float] = None
    taxes_fees_note: str = ""
    rooms_requested: int = 0
    rooms_matched: int = 0
    baggage_summary: str = ""
    transfer_summary: str = ""
    evidence_note: str = ""
    session_id: str = ""
    provider_state: str = ""
    automation_status: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def load_config() -> Dict[str, Any]:
    path = ROOT / "search_config.json"
    if not path.exists():
        return json.loads(json.dumps(DEFAULT_CONFIG))
    loaded = json.loads(path.read_text(encoding="utf-8"))
    return _deep_merge(json.loads(json.dumps(DEFAULT_CONFIG)), loaded)


def save_config(cfg: Dict[str, Any]) -> None:
    merged = _deep_merge(json.loads(json.dumps(DEFAULT_CONFIG)), cfg)
    merged = _apply_scale_weights(merged)
    merged = apply_search_plan(merged)
    (ROOT / "search_config.json").write_text(json.dumps(merged, indent=2), encoding="utf-8")


def duplicate_key(d: Deal) -> str:
    hotel = re.sub(r"[^a-z0-9]+", "", (d.hotel_name or "").lower())
    dest = re.sub(r"[^a-z0-9]+", "", (d.destination or "").lower())
    dep = (d.departure_date or "").strip()
    nights = str(d.nights) if d.nights is not None else ""
    return "|".join([hotel, dest, dep, nights])


def dedupe_deals(deals: List[Deal]) -> List[Deal]:
    chosen: Dict[str, Deal] = {}
    for d in deals:
        d.duplicate_key = duplicate_key(d)
        old = chosen.get(d.duplicate_key)
        if old is None or (d.extraction_confidence or 0) > (old.extraction_confidence or 0):
            chosen[d.duplicate_key] = d
    return list(chosen.values())


def build_queries(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    origin = cfg["origin_code"].lower()
    for window in cfg["date_windows"]:
        start = date.fromisoformat(window["start"])
        end = date.fromisoformat(window["end"])
        nights = int(window["nights"])
        cur = start
        step_days = max(1, int(window.get("step_days", cfg.get("search_engine", {}).get("daily_departure_step", 1)) or 1))
        while cur <= end:
            depart = cur.strftime("%Y%m%d")
            depart_iso = cur.isoformat()
            ret = cur + timedelta(days=nights)
            return_compact = ret.strftime("%Y%m%d")
            return_iso = ret.isoformat()
            for dest in cfg["destinations"]:
                out.append(
                    {
                        "window_label": window["label"],
                        "nights": nights,
                        "depart": depart,
                        "depart_iso": depart_iso,
                        "return_": return_compact,
                        "return_iso": return_iso,
                        "origin": origin,
                        "destination": dest["code"].lower(),
                        "destination_city": dest["city"],
                        "destination_country": dest["country"],
                        "adults": _group_from_cfg(cfg)["adults"],
                        "children": _group_from_cfg(cfg)["children"],
                        "infants": _group_from_cfg(cfg)["infants"],
                        "child_age": _group_from_cfg(cfg)["child_age"],
                        "child_ages_csv": ",".join(str(x) for x in _group_from_cfg(cfg)["child_ages"]),
                        "party_text": package_profile_text(cfg),
                        "google_flights_query": quote_plus(
                            f"Flights from {cfg['origin_airport']} to {dest['city']} {depart_iso} return {return_iso} for {package_profile_text(cfg)}"
                        ),
                    }
                )
            cur += timedelta(days=step_days)
    return out




def _group_from_cfg(cfg: Dict[str, Any]) -> Dict[str, Any]:
    group = dict(cfg.get("group", {}))
    children = int(group.get("children", 0) or 0)
    child_ages = list(group.get("child_ages") or ([] if not children else [group.get("child_age", 5)]))
    if children and not child_ages:
        child_ages = [5] * children
    while len(child_ages) < children:
        child_ages.append(child_ages[-1] if child_ages else 5)
    group["adults"] = int(group.get("adults", 2) or 2)
    group["children"] = children
    group["child_ages"] = child_ages[:children]
    group["child_age"] = int(child_ages[0]) if child_ages else int(group.get("child_age", 5) or 5)
    group["infants"] = int(group.get("infants", 0) or 0)
    group["infant_age_months"] = int(group.get("infant_age_months", 10) or 10)
    group["rooms"] = int(group.get("rooms", 2) or 2)
    return group


def package_profile_text(cfg: Optional[Dict[str, Any]] = None) -> str:
    cfg = cfg or load_config()
    group = _group_from_cfg(cfg)
    child_bits = []
    if group["children"]:
        child_bits.append(f"{group['children']} child aged {', '.join(str(x) for x in group['child_ages'])}")
    if group["infants"]:
        child_bits.append(f"{group['infants']} infant under 1")
    travellers = ", ".join([f"{group['adults']} adults"] + child_bits)
    rooms = int(group.get("rooms", 0) or 0)
    if rooms:
        travellers += f" across about {rooms} room{'s' if rooms != 1 else ''}"
    return travellers

def build_provider_plan(cfg: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    cfg = cfg or load_config()
    prefs = cfg.get("search_preferences", {}) or {}
    scales = cfg.get("preference_scales", {}) or {}
    orchestration = cfg.get("orchestration", {}) or {}
    package_limit = int(orchestration.get("package_site_limit", 6) or 6)
    flight_limit = int(orchestration.get("flight_site_limit", 4) or 4)
    strictness = float(scales.get("strictness", 72) or 72) / 100.0
    family_bias = 1.0 if prefs.get("prefer_free_child_places") else 0.4
    family_bias += 0.35 if prefs.get("prefer_family_room") else 0.0
    near_checkout_bias = 1.0 if orchestration.get("prefer_near_checkout", True) else 0.4
    rows: List[Dict[str, Any]] = []
    for site in cfg.get("sites", []):
        name = site.get("name", "")
        support = PROVIDER_SUPPORT.get(name, {})
        trust = PROVIDER_TRUST.get(name, 0.72)
        near_checkout = float(support.get("near_checkout", 0.4) or 0.4)
        family_discount = float(support.get("family_discount", 0.3) or 0.3)
        package_depth = float(support.get("package_depth", 0.3) or 0.3)
        type_bonus = 0.16 if site.get("type") == "package" else 0.02
        score = (
            trust * 0.35
            + near_checkout * 0.30 * near_checkout_bias
            + package_depth * 0.18
            + family_discount * 0.17 * family_bias
            + type_bonus
            - strictness * (0.02 if site.get("type") == "flight" else 0.0)
        )
        reasons = []
        if site.get("type") == "package":
            reasons.append("better near-final basket potential")
        if near_checkout >= 0.9:
            reasons.append("deep provider journey")
        if family_discount >= 0.9:
            reasons.append("strong family discount cues")
        elif family_discount >= 0.75:
            reasons.append("useful family-room and child-place signals")
        if not reasons:
            reasons.append("broad market coverage")
        rows.append({
            "site": name,
            "type": site.get("type", "package"),
            "priority_score": round(score, 4),
            "reason": "; ".join(reasons),
            "intensity": "deep" if near_checkout >= 0.88 else ("medium" if near_checkout >= 0.6 else "broad"),
        })
    packages = sorted([r for r in rows if r["type"] == "package"], key=lambda r: r["priority_score"], reverse=True)[:package_limit]
    flights = sorted([r for r in rows if r["type"] == "flight"], key=lambda r: r["priority_score"], reverse=True)[:flight_limit]
    ordered = packages + flights
    for idx, row in enumerate(ordered, start=1):
        row["rank"] = idx
    return ordered


def generate_urls_preview(max_flight_queries: int = 12) -> List[Dict[str, str]]:
    cfg = load_config()
    queries = build_queries(cfg)
    provider_plan = build_provider_plan(cfg)
    ordered_site_names = [r["site"] for r in provider_plan]
    site_lookup = {s.get("name"): s for s in cfg["sites"]}
    sites = [site_lookup[n] for n in ordered_site_names if n in site_lookup]
    out: List[Dict[str, str]] = []
    flight_count = 0
    for site in cfg["sites"]:
        if site["type"] == "package":
            out.append({"site": site["name"], "url": site["url_template"], "type": "package", "destination": "", "country": "", "date": "", "nights": None})
            continue
        for q in queries:
            if flight_count >= max_flight_queries:
                break
            out.append(
                {
                    "site": site["name"],
                    "url": site["url_template"].format(**q),
                    "type": "flight",
                    "destination": q["destination_city"],
                    "date": q["depart_iso"],
                    "country": q.get("destination_country", ""),
                    "nights": q.get("nights"),
                }
            )
            flight_count += 1
    return out


DESTINATION_LIBRARY = [
    {"city": "Antalya", "code": "AYT", "country": "Turkey", "hot_months": {6: 31, 7: 34}, "beach_score": 0.96, "package_score": 0.92, "family_score": 0.86, "notes": ["warm", "beach", "package"]},
    {"city": "Dalaman", "code": "DLM", "country": "Turkey", "hot_months": {6: 30, 7: 33}, "beach_score": 0.93, "package_score": 0.90, "family_score": 0.84, "notes": ["warm", "beach", "package"]},
    {"city": "Bodrum", "code": "BJV", "country": "Turkey", "hot_months": {6: 31, 7: 34}, "beach_score": 0.94, "package_score": 0.87, "family_score": 0.82, "notes": ["warm", "beach", "package"]},
    {"city": "Burgas", "code": "BOJ", "country": "Bulgaria", "hot_months": {6: 27, 7: 30}, "beach_score": 0.91, "package_score": 0.88, "family_score": 0.82, "notes": ["cheap", "beach", "package"]},
    {"city": "Varna", "code": "VAR", "country": "Bulgaria", "hot_months": {6: 27, 7: 30}, "beach_score": 0.88, "package_score": 0.76, "family_score": 0.78, "notes": ["cheap", "beach"]},
    {"city": "Hurghada", "code": "HRG", "country": "Egypt", "hot_months": {6: 36, 7: 38}, "beach_score": 0.92, "package_score": 0.80, "family_score": 0.77, "notes": ["very hot", "beach", "package"]},
    {"city": "Sharm El Sheikh", "code": "SSH", "country": "Egypt", "hot_months": {6: 37, 7: 39}, "beach_score": 0.89, "package_score": 0.78, "family_score": 0.75, "notes": ["very hot", "beach", "package"]},
    {"city": "Rhodes", "code": "RHO", "country": "Greece", "hot_months": {6: 29, 7: 32}, "beach_score": 0.90, "package_score": 0.81, "family_score": 0.82, "notes": ["warm", "beach"]},
    {"city": "Kos", "code": "KGS", "country": "Greece", "hot_months": {6: 29, 7: 32}, "beach_score": 0.90, "package_score": 0.79, "family_score": 0.80, "notes": ["warm", "beach"]},
    {"city": "Heraklion", "code": "HER", "country": "Greece", "hot_months": {6: 28, 7: 30}, "beach_score": 0.84, "package_score": 0.83, "family_score": 0.81, "notes": ["warm", "beach"]},
    {"city": "Corfu", "code": "CFU", "country": "Greece", "hot_months": {6: 28, 7: 31}, "beach_score": 0.87, "package_score": 0.80, "family_score": 0.81, "notes": ["warm", "beach"]},
    {"city": "Zakynthos", "code": "ZTH", "country": "Greece", "hot_months": {6: 29, 7: 32}, "beach_score": 0.89, "package_score": 0.77, "family_score": 0.79, "notes": ["warm", "beach"]},
    {"city": "Larnaca", "code": "LCA", "country": "Cyprus", "hot_months": {6: 31, 7: 34}, "beach_score": 0.85, "package_score": 0.82, "family_score": 0.80, "notes": ["hot", "beach", "package"]},
    {"city": "Paphos", "code": "PFO", "country": "Cyprus", "hot_months": {6: 29, 7: 32}, "beach_score": 0.84, "package_score": 0.83, "family_score": 0.80, "notes": ["hot", "beach", "package"]},
    {"city": "Palma", "code": "PMI", "country": "Spain", "hot_months": {6: 28, 7: 31}, "beach_score": 0.90, "package_score": 0.84, "family_score": 0.83, "notes": ["warm", "beach", "package"]},
    {"city": "Reus", "code": "REU", "country": "Spain", "hot_months": {6: 27, 7: 30}, "beach_score": 0.86, "package_score": 0.78, "family_score": 0.80, "notes": ["warm", "beach"]},
    {"city": "Alicante", "code": "ALC", "country": "Spain", "hot_months": {6: 28, 7: 31}, "beach_score": 0.87, "package_score": 0.82, "family_score": 0.80, "notes": ["warm", "beach"]},
    {"city": "Tenerife", "code": "TFS", "country": "Spain", "hot_months": {6: 27, 7: 29}, "beach_score": 0.88, "package_score": 0.84, "family_score": 0.82, "notes": ["warm", "beach", "year-round"]},
    {"city": "Lanzarote", "code": "ACE", "country": "Spain", "hot_months": {6: 27, 7: 29}, "beach_score": 0.88, "package_score": 0.83, "family_score": 0.81, "notes": ["warm", "beach", "year-round"]},
    {"city": "Faro", "code": "FAO", "country": "Portugal", "hot_months": {6: 27, 7: 30}, "beach_score": 0.86, "package_score": 0.80, "family_score": 0.80, "notes": ["warm", "beach"]},
    {"city": "Malta", "code": "MLA", "country": "Malta", "hot_months": {6: 30, 7: 33}, "beach_score": 0.72, "package_score": 0.76, "family_score": 0.76, "notes": ["hot", "sea", "city"]},
    {"city": "Enfidha", "code": "NBE", "country": "Tunisia", "hot_months": {6: 31, 7: 35}, "beach_score": 0.87, "package_score": 0.79, "family_score": 0.78, "notes": ["hot", "beach", "package"]},
    {"city": "Monastir", "code": "MIR", "country": "Tunisia", "hot_months": {6: 30, 7: 34}, "beach_score": 0.86, "package_score": 0.78, "family_score": 0.77, "notes": ["hot", "beach"]},
]


def _tokenise_prompt(text: str) -> set[str]:
    return {t for t in re.findall(r"[a-z0-9]+", (text or "").lower()) if t}


def _scale(cfg: Dict[str, Any], key: str, default: float) -> float:
    return float((cfg.get("preference_scales") or {}).get(key, default) or default)


def _normalised_weights_from_scales(cfg: Dict[str, Any]) -> Dict[str, float]:
    scales = cfg.get("preference_scales") or {}
    raw = {
        "price": float(scales.get("price_weight", 48) or 48),
        "beach": float(scales.get("beach_weight", 22) or 22),
        "heat": float(scales.get("heat_weight", 16) or 16),
        "family_fit": float(scales.get("family_weight", 10) or 10),
        "review": 6.0,
        "board_basis": 5.0,
        "bags": 4.0,
        "transfers": 4.0,
        "pool": 6.0,
        "cancellation": 1.0,
    }
    total = sum(raw.values()) or 1.0
    return {k: round(v / total, 4) for k, v in raw.items()}


def _apply_scale_weights(cfg: Dict[str, Any]) -> Dict[str, Any]:
    cfg = json.loads(json.dumps(cfg))
    cfg["weights"] = _normalised_weights_from_scales(cfg)
    return cfg


def build_dynamic_destinations(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    prompt = (cfg.get("overview_prompt") or "").strip()
    prefs = cfg.get("search_preferences", {})
    tokens = _tokenise_prompt(prompt)
    months = prefs.get("months") or [6, 7]
    min_temp = float(prefs.get("min_temp_c", 26) or 26)
    want_beach = prefs.get("prefer_beach", True) or ("beach" in tokens)
    want_hot = prefs.get("prefer_hot", True) or ("warm" in tokens or "hot" in tokens)
    cheap_first = prefs.get("cheap_first", True) or ("price" in tokens or "cheap" in tokens)
    scales = cfg.get("preference_scales") or {}
    price_weight = float(scales.get("price_weight", 48) or 48)
    beach_weight = float(scales.get("beach_weight", 22) or 22)
    heat_weight = float(scales.get("heat_weight", 16) or 16)
    family_weight = float(scales.get("family_weight", 10) or 10)
    trend_weight = float(scales.get("trend_weight", 4) or 4)
    total_weight = max(1.0, price_weight + beach_weight + heat_weight + family_weight + trend_weight)
    ranking = []
    for dest in DESTINATION_LIBRARY:
        temps = [dest.get("hot_months", {}).get(int(m), 0) for m in months]
        nonzero = [t for t in temps if t]
        avg_temp = sum(nonzero) / max(1, len(nonzero)) if nonzero else 0
        package_score = dest.get("package_score", 0.7)
        beach_pref = dest.get("beach_score", 0.7) if want_beach else 0.6
        heat_pref = min(1.0, max(0.0, avg_temp / max(min_temp, 1))) if want_hot else 0.6
        family_pref = dest.get("family_score", 0.7)
        trend_pref = 0.9 if "package" in dest.get("notes", []) else 0.65
        if cheap_first:
            package_score += 0.05
        score = (
            package_score * price_weight
            + beach_pref * beach_weight
            + heat_pref * heat_weight
            + family_pref * family_weight
            + trend_pref * trend_weight
        ) / total_weight
        if avg_temp < min_temp - float(scales.get("temp_slack_c", 2) or 2) - 1:
            score -= 0.18
        ranking.append((round(score, 4), dest))
    ranking.sort(reverse=True, key=lambda x: x[0])
    count = int(cfg.get("search_engine", {}).get("destination_count", 18) or 18)
    return [{"city": d["city"], "code": d["code"], "country": d["country"]} for _, d in ranking[:count]]


def build_dynamic_date_windows(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    prefs = cfg.get("search_preferences", {})
    months = [int(m) for m in (prefs.get("months") or [6, 7])]
    nights_list = [int(n) for n in (prefs.get("trip_lengths") or [7, 10])]
    start_day = int(cfg.get("search_engine", {}).get("start_day", 20) or 20)
    end_day = int(cfg.get("search_engine", {}).get("end_day", 15) or 15)
    step_days = int(cfg.get("search_engine", {}).get("daily_departure_step", 1) or 1)
    windows = []
    for month in months:
        year = 2026
        start = date(year, month, start_day if month == min(months) else 1)
        if month == max(months):
            end = date(year, month, min(end_day, 28))
        else:
            end = date(year, month, 28)
        for nights in nights_list:
            windows.append({
                "label": f"m{month}_{nights}",
                "start": start.isoformat(),
                "end": end.isoformat(),
                "nights": nights,
                "step_days": step_days,
            })
    return windows


def build_search_plan(cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    cfg = cfg or load_config()
    destinations = build_dynamic_destinations(cfg) if cfg.get("search_engine", {}).get("use_extended_destination_pool", True) else cfg.get("destinations", [])
    windows = build_dynamic_date_windows(cfg)
    scales = cfg.get("preference_scales", {})
    provider_plan = build_provider_plan(cfg)
    return {
        "overview_prompt": cfg.get("overview_prompt", ""),
        "destinations": destinations,
        "date_windows": windows,
        "providers": provider_plan,
        "package_profile_text": package_profile_text(cfg),
        "summary": f"Searching {len(destinations)} destinations across {len(windows)} date windows for {package_profile_text(cfg)}. Exact matches first, then near-miss recommendations within +£{int(scales.get('price_slack_gbp',250))}, +{int(scales.get('beach_slack_minutes',5))} beach mins, and -{int(scales.get('temp_slack_c',2))}°C heat.",
    }


def apply_search_plan(cfg: Dict[str, Any]) -> Dict[str, Any]:
    cfg = json.loads(json.dumps(cfg))
    plan = build_search_plan(cfg)
    cfg["destinations"] = plan["destinations"]
    cfg["date_windows"] = plan["date_windows"]
    return cfg


HEAT_BY_DESTINATION = {
    "Antalya": {6: 31, 7: 34, 8: 34},
    "Dalaman": {6: 30, 7: 33, 8: 33},
    "Bodrum": {6: 31, 7: 34, 8: 34},
    "Burgas": {6: 27, 7: 30, 8: 30},
    "Hurghada": {6: 36, 7: 38, 8: 38},
    "Sharm El Sheikh": {6: 37, 7: 39, 8: 39},
    "Rhodes": {6: 29, 7: 32, 8: 32},
    "Kos": {6: 29, 7: 32, 8: 32},
    "Heraklion": {6: 28, 7: 30, 8: 30},
    "Larnaca": {6: 31, 7: 34, 8: 34},
    "Paphos": {6: 29, 7: 32, 8: 32},
    "Palma": {6: 28, 7: 31, 8: 31},
    "Tenerife": {6: 27, 7: 29, 8: 30},
    "Faro": {6: 27, 7: 30, 8: 30},
}

HEAT_BY_COUNTRY = {
    "Turkey": {6: 31, 7: 34, 8: 34},
    "Bulgaria": {6: 27, 7: 30, 8: 30},
    "Egypt": {6: 36, 7: 38, 8: 38},
    "Greece": {6: 29, 7: 32, 8: 32},
    "Cyprus": {6: 30, 7: 33, 8: 33},
    "Spain": {6: 28, 7: 31, 8: 31},
    "Portugal": {6: 27, 7: 30, 8: 30},
}


def estimated_temperature_c(d: Deal, cfg: Dict[str, Any]) -> Optional[float]:
    month = int(cfg.get("strategy", {}).get("heat_month", 6))
    temp = HEAT_BY_DESTINATION.get(d.destination or "", {}).get(month)
    if temp is None:
        temp = HEAT_BY_COUNTRY.get(d.country or "", {}).get(month)
    return temp


def heat_score_from_temp(temp: Optional[float], heat_floor_c: float = 25) -> float:
    if temp is None:
        return 0.55
    if temp < heat_floor_c:
        return max(0.2, temp / max(1.0, heat_floor_c))
    if temp <= 28:
        return 0.72
    if temp <= 31:
        return 0.86
    if temp <= 35:
        return 1.0
    return 0.92


def board_basis_score(board: str) -> float:
    b = (board or "").lower()
    if "all inclusive" in b:
        return 1.0
    if "full board" in b:
        return 0.9
    if "half board" in b:
        return 0.75
    if "bed and breakfast" in b or "breakfast" in b:
        return 0.7
    if "self catering" in b:
        return 0.45
    if "room only" in b:
        return 0.2
    return 0.35


def review_score_norm(review: Optional[float]) -> float:
    if review is None:
        return 0.45
    if review <= 5:
        return review / 5
    if review <= 10:
        return review / 10
    return min(1.0, review / 10)


def yesno_norm(v: Optional[bool], unknown: float = 0.5) -> float:
    if v is True:
        return 1.0
    if v is False:
        return 0.0
    return unknown


def beach_score(minutes: Optional[int]) -> float:
    if minutes is None:
        return 0.5
    if minutes <= 5:
        return 1.0
    if minutes <= 10:
        return 0.8
    if minutes <= 15:
        return 0.6
    if minutes <= 20:
        return 0.45
    return 0.2


def family_fit_score(d: Deal) -> float:
    score = 0.28
    board = (d.board_basis or "").lower()
    room = (d.family_room_type or d.notes or "").lower()
    infant = (d.infant_cost_note or "").lower()
    child_note = (d.child_pricing_note or "").lower()
    if d.free_child_place:
        score += 0.35
    if any(k in room for k in ["family", "suite", "apartment", "2 bedroom", "two bedroom", "interconnecting"]):
        score += 0.16
    if d.pool:
        score += 0.08
    if d.transfers_included:
        score += 0.05
    if d.bags_included:
        score += 0.04
    if "breakfast" in board:
        score += 0.08
    if "half board" in board:
        score += 0.10
    if "all inclusive" in board:
        score += 0.14
    if infant and any(k in infant for k in ["included", "tax only", "taxes only", "free"]):
        score += 0.08
    if child_note and any(k in child_note for k in ["free child", "kids go free", "free kids", "child place free"]):
        score += 0.12
    return min(1.0, score)



def effective_price(d: Deal) -> Optional[float]:
    if d.price_reference_gbp is not None:
        return d.price_reference_gbp
    if d.basket_price_gbp is not None:
        return d.basket_price_gbp
    if d.scenario_price_gbp is not None:
        return d.scenario_price_gbp
    if d.price_total_gbp is not None:
        return d.price_total_gbp
    return None


def refresh_price_reference(d: Deal) -> Optional[float]:
    d.price_reference_gbp = d.basket_price_gbp if d.basket_price_gbp is not None else d.price_total_gbp
    if d.headline_price_gbp is None and d.price_total_gbp is not None:
        d.headline_price_gbp = d.price_total_gbp
    if not d.pricing_completeness:
        d.pricing_completeness = 'near-final' if d.basket_price_gbp is not None else ('headline' if d.price_total_gbp is not None else 'unknown')
    if d.true_price_confidence is None:
        d.true_price_confidence = 0.9 if d.basket_price_gbp is not None else (0.55 if d.price_total_gbp is not None else 0.0)
    return d.price_reference_gbp

def assign_bucket(d: Deal) -> str:
    price = effective_price(d) or 999999
    board = (d.board_basis or "").lower()
    if price <= 2400:
        return "True cheap floor"
    if d.free_child_place and price <= 3200:
        return "Family saver"
    if "all inclusive" in board and price <= 3350:
        return "Low-friction family"
    if d.review_score and d.review_score >= 8.5 and price <= 3500:
        return "Strong value"
    return "Needs verification"


def build_warning_flags(d: Deal) -> str:
    flags: List[str] = []
    if effective_price(d) is None:
        flags.append("Missing price")
    if d.pool is False:
        flags.append("No pool")
    if d.beach_minutes is not None and d.beach_minutes > 15:
        flags.append("Beach not close")
    if d.board_basis and "room only" in d.board_basis.lower():
        flags.append("Room only")
    if d.bags_included is False:
        flags.append("No bags")
    if d.transfers_included is False:
        flags.append("No transfers")
    if not d.family_room_type and d.deal_type == "package":
        flags.append("Family room unclear")
    if not d.child_pricing_note and not d.free_child_place and d.deal_type == "package":
        flags.append("Child pricing unclear")
    if not d.infant_cost_note and d.deal_type == "package":
        flags.append("Infant pricing unclear")
    return "; ".join(flags)


def compute_buy_now_score(d: Deal) -> float:
    score = (d.score or 0) * 0.55
    if d.price_delta_gbp and d.price_delta_gbp > 0:
        score += min(25, d.price_delta_gbp / 10)
    if d.price_delta_pct and d.price_delta_pct > 0:
        score += min(15, d.price_delta_pct * 100)
    if d.booking_bucket == "Cheapest possible":
        score += 6
    if d.booking_bucket == "Low-stress family":
        score += 8
    if not d.warning_flags:
        score += 8
    return round(score, 2)


def compute_scenario_price(d: Deal, cfg: Dict[str, Any], airport_mode: Optional[str] = None, day_shift: Optional[int] = None) -> Optional[float]:
    base_price = d.basket_price_gbp if d.basket_price_gbp is not None else d.price_total_gbp
    if base_price is None:
        return None
    strategy = cfg.get("strategy", {})
    mode = airport_mode or strategy.get("default_airport_mode", "Newcastle only")
    shift = int(day_shift if day_shift is not None else strategy.get("default_day_shift", 0))
    airport_adj = strategy.get("nearby_airport_adjustments", {}).get(mode, 0)
    day_adj = -1 * shift * float(strategy.get("default_day_shift_savings_gbp", 35))
    return round(max(1800.0, float(base_price) + float(airport_adj) + float(day_adj)), 2)


def build_recommendation(d: Deal, threshold: float) -> str:
    scenario = d.scenario_price_gbp if d.scenario_price_gbp is not None else effective_price(d)
    if scenario is None:
        return "Keep watching until a real basket-like price appears."
    hot_text = f"around {int(d.estimated_temp_c)}°C" if d.estimated_temp_c is not None else "hot-weather fit uncertain"
    family_bits = []
    if d.free_child_place:
        family_bits.append("free child place signal")
    if d.family_room_type:
        family_bits.append(d.family_room_type)
    if d.infant_cost_note:
        family_bits.append(d.infant_cost_note)
    family_text = "; ".join(family_bits) if family_bits else "family pricing still needs checking"
    if d.deal_signal == "DROP ALERT" and scenario <= threshold and not d.warning_flags:
        return f"High-priority deal to verify: cheap, beach-ready, likely {hot_text}, with {family_text}."
    if scenario <= threshold and d.pool and (d.beach_minutes or 99) <= 10:
        return f"Strong shortlist: inside your price line, beach access looks good, and weather should be {hot_text}. Family cues: {family_text}."
    if d.warning_flags:
        return f"Potential saving, but treat it as provisional until extras are checked. Family cues: {family_text}."
    return f"Worth tracking. Looks competitive and should be {hot_text}; verify the family basket, especially {family_text}."


def build_action_now(d: Deal, threshold: float) -> str:
    scenario = d.scenario_price_gbp if d.scenario_price_gbp is not None else effective_price(d)
    if scenario is None:
        return "Watch"
    if d.deal_signal == "DROP ALERT" and scenario <= threshold and not d.warning_flags:
        return "Surface now"
    if d.deal_signal == "DROP ALERT" and scenario <= threshold:
        return "Verify now"
    if d.booking_bucket in {"True cheap floor", "Family saver"} and scenario <= threshold:
        return "Shortlist"
    return "Watch"


def infer_price_direction(d: Deal) -> tuple[str, str]:
    if d.deal_signal == "DROP ALERT":
        return ("lower now", "Already below prior observed price, so this is the right moment to verify.")
    if d.price_delta_gbp is not None and d.price_delta_gbp < 0:
        return ("higher now", "This option has moved up versus the prior snapshot.")
    if d.departure_date:
        try:
            dep = date.fromisoformat(d.departure_date)
            days = (dep - date.today()).days
        except Exception:
            days = None
    else:
        days = None
    if days is not None and days <= 35:
        return ("unlikely much lower", "Close-in summer departures often have less room to fall once a decent family basket appears.")
    if days is not None and days >= 70 and (effective_price(d) or 999999) > 3200:
        return ("could fall", "There is still time for providers to move family pricing or free-child availability.")
    return ("watch", "Needs more history before calling the trend with confidence.")


def build_fit_summary(d: Deal) -> str:
    bits = []
    if effective_price(d) is not None:
        label = "near-final" if d.basket_price_gbp is not None else "headline"
        bits.append(f"£{int(effective_price(d)):,} {label}")
    if d.estimated_temp_c is not None:
        bits.append(f"~{int(d.estimated_temp_c)}°C")
    if d.beach_minutes is not None:
        bits.append(f"{d.beach_minutes} min beach")
    if d.pool:
        bits.append("pool")
    if d.free_child_place:
        bits.append("free child signal")
    if d.family_room_type:
        bits.append(d.family_room_type)
    return " · ".join(bits)


def build_pros_cons(d: Deal) -> tuple[str, str]:
    pros=[]
    cons=[]
    if effective_price(d) is not None and effective_price(d) <= 3000:
        pros.append("Price is in strong family-deal territory")
    if d.beach_minutes is not None and d.beach_minutes <= 5:
        pros.append("Very close to the beach")
    elif d.beach_minutes is not None and d.beach_minutes <= 10:
        pros.append("Beach walk looks manageable")
    if d.pool:
        pros.append("Pool signal present")
    if d.free_child_place:
        pros.append("Free child place signal")
    if d.family_room_type:
        pros.append(f"Family setup spotted: {d.family_room_type}")
    if d.bags_included:
        pros.append("Bags included")
    if d.transfers_included:
        pros.append("Transfers included")
    if d.estimated_temp_c is not None and d.estimated_temp_c >= 30:
        pros.append("Should be properly warm")
    if d.basket_price_gbp is None:
        cons.append("Near-final basket price still unclear")
    if d.beach_minutes is not None and d.beach_minutes > 10:
        cons.append("Beach is not especially close")
    if d.pool is False:
        cons.append("Pool not confirmed")
    if d.bags_included is False:
        cons.append("Bags may be extra")
    if d.transfers_included is False:
        cons.append("Transfers may be extra")
    if not d.family_room_type:
        cons.append("Room setup still needs checking")
    if not d.infant_cost_note:
        cons.append("Infant charge unclear")
    if not d.child_pricing_note and not d.free_child_place:
        cons.append("Child pricing benefit not obvious")
    return ("; ".join(pros[:4]), "; ".join(cons[:4]))


def apply_strategy_overlay(
    deals: List[Deal],
    cfg: Dict[str, Any],
    airport_mode: Optional[str] = None,
    day_shift: Optional[int] = None,
    threshold: Optional[float] = None,
) -> None:
    strategy = cfg.get("strategy", {})
    mode = airport_mode or strategy.get("default_airport_mode", "Newcastle only")
    shift = int(day_shift if day_shift is not None else strategy.get("default_day_shift", 0))
    limit = float(threshold if threshold is not None else strategy.get("buy_line_gbp", 3000))
    heat_floor = float(strategy.get("heat_floor_c", 25))

    for d in deals:
        d.strategy_airport_mode = mode
        d.strategy_day_shift = shift
        d.scenario_price_gbp = compute_scenario_price(d, cfg, mode, shift)
        d.estimated_temp_c = estimated_temperature_c(d, cfg)
        d.heat_score = round(heat_score_from_temp(d.estimated_temp_c, heat_floor), 3)
        adj = float(d.buy_now_score or 0)
        if d.deal_signal == "DROP ALERT":
            adj += 6
        if d.scenario_price_gbp is not None and d.scenario_price_gbp <= limit:
            adj += 5
        if d.pool:
            adj += 2.5
        if d.beach_minutes is not None and d.beach_minutes <= 10:
            adj += 3.5
        if d.heat_score is not None:
            adj += d.heat_score * 4.0
        if d.free_child_place:
            adj += 4.0
        if d.family_room_type:
            adj += 1.75
        if d.child_pricing_note:
            adj += 1.5
        if d.infant_cost_note:
            adj += 1.25
        if d.near_booking_stage:
            adj += min(2.0, 0.5 * max(1, d.journey_depth))
        d.adjusted_buy_now_score = round(adj, 2)
        d.action_now = build_action_now(d, limit)
        d.recommendation = build_recommendation(d, limit)
        d.likely_price_direction, d.likely_price_reason = infer_price_direction(d)
        d.pros, d.cons = build_pros_cons(d)
        d.fit_summary = build_fit_summary(d)


def score_deals(deals: List[Deal], weights: Dict[str, float], mode: str = "balanced", cfg: Optional[Dict[str, Any]] = None) -> None:
    if mode == "cheapest":
        weights = {"price": 0.57, "review": 0.05, "board_basis": 0.03, "bags": 0.03, "transfers": 0.03, "beach": 0.10, "pool": 0.05, "heat": 0.07, "cancellation": 0.0, "family_fit": 0.07}
    elif mode == "best_value":
        weights = {"price": 0.38, "review": 0.13, "board_basis": 0.06, "bags": 0.05, "transfers": 0.04, "beach": 0.12, "pool": 0.07, "heat": 0.09, "cancellation": 0.01, "family_fit": 0.05}
    elif mode == "low_stress":
        weights = {"price": 0.28, "review": 0.12, "board_basis": 0.10, "bags": 0.09, "transfers": 0.08, "beach": 0.10, "pool": 0.08, "heat": 0.08, "cancellation": 0.02, "family_fit": 0.05}

    cfg = cfg or load_config()
    for d in deals:
        refresh_price_reference(d)
    prices = [effective_price(d) for d in deals if effective_price(d) is not None]
    maxp = max(prices) if prices else 1
    minp = min(prices) if prices else 0

    for d in deals:
        d.booking_bucket = assign_bucket(d)
        d.warning_flags = build_warning_flags(d)
        d.estimated_temp_c = estimated_temperature_c(d, cfg)
        d.heat_score = round(heat_score_from_temp(d.estimated_temp_c, float(cfg.get("strategy", {}).get("heat_floor_c", 25))), 3)
        ref_price = effective_price(d)
        price_score = 0.2 if ref_price is None else (1.0 if math.isclose(maxp, minp) else 1 - ((ref_price - minp) / max(1e-9, (maxp - minp))))
        d.score = round(
            100
            * (
                weights["price"] * price_score
                + weights["review"] * review_score_norm(d.review_score)
                + weights["board_basis"] * board_basis_score(d.board_basis)
                + weights["bags"] * yesno_norm(d.bags_included)
                + weights["transfers"] * yesno_norm(d.transfers_included)
                + weights["beach"] * beach_score(d.beach_minutes)
                + weights["pool"] * yesno_norm(d.pool, 0.45)
                + weights["heat"] * (d.heat_score or 0.55)
                + weights["cancellation"] * yesno_norm(d.free_cancellation, 0.5)
                + weights["family_fit"] * family_fit_score(d)
            ),
            2,
        )




def _safe_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _fit_gap(d: Deal, cfg: Dict[str, Any], max_price: Optional[float], min_temp: Optional[float], beach_max_minutes: Optional[int]) -> tuple[str, float, str]:
    scales = cfg.get("preference_scales") or {}
    strictness = float(scales.get("strictness", 72) or 72)
    price_slack = float(scales.get("price_slack_gbp", 250) or 250)
    beach_slack = float(scales.get("beach_slack_minutes", 5) or 5)
    temp_slack = float(scales.get("temp_slack_c", 2) or 2)
    hard_multiplier = max(0.35, 1.2 - (strictness / 100.0))
    soft_price = price_slack * (0.6 + hard_multiplier)
    soft_beach = beach_slack * (0.6 + hard_multiplier)
    soft_temp = temp_slack * (0.6 + hard_multiplier)

    scenario = d.scenario_price_gbp if d.scenario_price_gbp is not None else effective_price(d)
    misses = []
    gap = 0.0
    exact = True
    if max_price is not None and scenario is not None and scenario > max_price:
        extra = scenario - max_price
        if extra <= soft_price:
            exact = False
            gap += extra / max(1.0, soft_price)
            misses.append(f"about £{int(extra)} over")
        else:
            return ("Out of range", 9.0, f"Over budget by about £{int(extra)}")
    if min_temp is not None and d.estimated_temp_c is not None and d.estimated_temp_c < min_temp:
        extra = min_temp - d.estimated_temp_c
        if extra <= soft_temp:
            exact = False
            gap += extra / max(0.5, soft_temp)
            misses.append(f"around {extra:.1f}°C cooler")
        else:
            return ("Out of range", 9.0, f"Too cool by about {extra:.1f}°C")
    if beach_max_minutes is not None and d.beach_minutes is not None and d.beach_minutes > beach_max_minutes:
        extra = d.beach_minutes - beach_max_minutes
        if extra <= soft_beach:
            exact = False
            gap += extra / max(1.0, soft_beach)
            misses.append(f"about {int(extra)} mins further from beach")
        else:
            return ("Out of range", 9.0, f"Beach is about {int(extra)} mins too far")
    if exact:
        return ("Exact fit", 0.0, "Matches the current brief")
    gap = round(gap, 2)
    label = "Near miss worth a look" if gap <= 1.35 else "Stretch option"
    return (label, gap, "; ".join(misses))


def _rank_with_intelligence(d: Deal, cfg: Dict[str, Any], sort_by: str) -> tuple:
    fit_rank = {"Exact fit": 0, "Near miss worth a look": 1, "Stretch option": 2, "Out of range": 3}.get(d.fit_label or "", 3)
    trend_boost = float((cfg.get("preference_scales") or {}).get("trend_weight", 4) or 4) / 10.0
    if sort_by == "price_low":
        return (fit_rank, d.scenario_price_gbp or 999999, d.fit_gap_score or 99, -(d.estimated_temp_c or 0))
    if sort_by == "beach":
        return (fit_rank, d.beach_minutes if d.beach_minutes is not None else 999, d.fit_gap_score or 99, d.scenario_price_gbp or 999999)
    if sort_by == "hottest":
        return (fit_rank, -(d.estimated_temp_c or -999), d.fit_gap_score or 99, d.scenario_price_gbp or 999999)
    if sort_by == "family_saver":
        return (fit_rank, 0 if d.free_child_place else 1, 0 if d.family_room_type else 1, d.fit_gap_score or 99, d.scenario_price_gbp or 999999)
    if sort_by == "drop_alert":
        return (fit_rank, 0 if d.deal_signal == "DROP ALERT" else 1, d.fit_gap_score or 99, d.scenario_price_gbp or 999999)
    intelligence = (d.adjusted_buy_now_score or 0) + ((d.buy_now_score or 0) * trend_boost)
    return (fit_rank, -(intelligence), d.fit_gap_score or 99, d.scenario_price_gbp or 999999, -(d.heat_score or 0))


def query_deals(
    deals: List[Deal],
    cfg: Optional[Dict[str, Any]] = None,
    query: str = "",
    max_price: Optional[float] = None,
    min_temp: Optional[float] = None,
    beach_max_minutes: Optional[int] = None,
    require_pool: bool = False,
    breakfast_or_better: bool = False,
    alerts_only: bool = False,
    free_child_only: bool = False,
    family_room_only: bool = False,
    source_site: str = "",
    sort_by: str = "best",
) -> List[Deal]:
    cfg = cfg or load_config()
    query = _safe_text(query)
    out: List[Deal] = []
    for d in deals:
        hay = " ".join([
            _safe_text(d.hotel_name), _safe_text(d.destination), _safe_text(d.country),
            _safe_text(d.board_basis), _safe_text(d.source_site), _safe_text(d.notes), _safe_text(d.recommendation), _safe_text(d.family_room_type), _safe_text(d.child_pricing_note), _safe_text(d.infant_cost_note), _safe_text(d.pros), _safe_text(d.cons), _safe_text(d.fit_summary), _safe_text(d.likely_price_direction), _safe_text(d.likely_price_reason)
        ])
        if query and query not in hay:
            continue
        scenario = d.scenario_price_gbp if d.scenario_price_gbp is not None else effective_price(d)
        fit_label, fit_gap_score, near_reason = _fit_gap(d, cfg, max_price, min_temp, beach_max_minutes)
        d.fit_label = fit_label
        d.fit_gap_score = fit_gap_score
        d.near_miss_reason = near_reason
        if fit_label == "Out of range":
            continue
        if require_pool and d.pool is not True:
            continue
        board = _safe_text(d.board_basis)
        if breakfast_or_better and not any(x in board for x in ["breakfast", "bed and breakfast", "half board", "full board", "all inclusive"]):
            continue
        if alerts_only and d.deal_signal != "DROP ALERT":
            continue
        if free_child_only and d.free_child_place is not True:
            continue
        if family_room_only and not _safe_text(d.family_room_type):
            continue
        if source_site and _safe_text(source_site) not in _safe_text(d.source_site):
            continue
        out.append(d)

    return sorted(out, key=lambda d: _rank_with_intelligence(d, cfg, sort_by))

def latest_history_file() -> Optional[Path]:
    files = sorted(HISTORY.glob("deals_*.json"))
    return files[-1] if files else None


def load_previous_history() -> List[Deal]:
    p = latest_history_file()
    return [Deal(**row) for row in json.loads(p.read_text(encoding="utf-8"))] if p else []


def apply_price_deltas(current: List[Deal], previous: List[Deal], cfg: Dict[str, Any]) -> None:
    prev_map = {duplicate_key(d): d for d in previous}
    drop_gbp = cfg.get("autopilot", {}).get("drop_threshold_gbp", 100)
    drop_pct = cfg.get("autopilot", {}).get("drop_threshold_pct", 0.06)
    for d in current:
        prior = prev_map.get(duplicate_key(d))
        current_price = effective_price(d)
        prior_price = effective_price(prior) if prior else None
        if prior and prior_price is not None and current_price is not None:
            delta = prior_price - current_price
            d.price_delta_gbp = round(delta, 2)
            d.price_delta_pct = round((delta / prior_price), 4) if prior_price else None
            if delta >= drop_gbp or (d.price_delta_pct is not None and d.price_delta_pct >= drop_pct):
                d.deal_signal = "DROP ALERT"
            elif delta > 0:
                d.deal_signal = "Down"
            elif delta < 0:
                d.deal_signal = "Up"
            else:
                d.deal_signal = "Flat"
        else:
            d.deal_signal = "New"
        d.buy_now_score = compute_buy_now_score(d)


def save_history_snapshot(deals: List[Deal]) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    p = HISTORY / f"deals_{ts}.json"
    p.write_text(json.dumps([d.to_dict() for d in deals], indent=2, ensure_ascii=False), encoding="utf-8")
    return p


def _best_today_payload(deals: List[Deal], cfg: Dict[str, Any]) -> Dict[str, Any]:
    strategy = cfg.get("strategy", {})
    threshold = float(strategy.get("buy_line_gbp", 3000))
    ranked = sorted(deals, key=lambda d: (d.adjusted_buy_now_score or 0, d.buy_now_score or 0), reverse=True)
    best = ranked[0] if ranked else None
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "buy_line_gbp": threshold,
        "deal_count": len(deals),
        "drop_alert_count": len([d for d in deals if d.deal_signal == "DROP ALERT"]),
        "below_threshold_count": len([d for d in deals if (d.scenario_price_gbp or 999999) <= threshold]),
        "best": best.to_dict() if best else None,
    }


def write_outputs(deals: List[Deal], cfg: Optional[Dict[str, Any]] = None) -> None:
    cfg = cfg or load_config()
    fields = list(Deal(source_site="", source_url="", deal_type="").to_dict().keys())
    csv_path = RESULTS / "deals.csv"
    json_path = RESULTS / "deals.json"
    alerts_path = RESULTS / "price_drop_alerts.csv"
    rec_path = RESULTS / "autopilot_recommendations.md"
    top_path = RESULTS / "top_deals.md"
    best_json_path = RESULTS / "best_today.json"
    best_md_path = RESULTS / "best_today.md"

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for d in deals:
            w.writerow(d.to_dict())

    json_path.write_text(json.dumps([d.to_dict() for d in deals], indent=2, ensure_ascii=False), encoding="utf-8")

    with alerts_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "hotel_name",
            "destination",
            "departure_date",
            "nights",
            "price_total_gbp",
            "basket_price_gbp",
            "price_reference_gbp",
            "pricing_completeness",
            "checkout_stage",
            "checkout_step_count",
            "true_price_confidence",
            "scenario_price_gbp",
            "price_delta_gbp",
            "price_delta_pct",
            "deal_signal",
            "buy_now_score",
            "adjusted_buy_now_score",
            "action_now",
            "source_url",
        ])
        for d in deals:
            if d.deal_signal == "DROP ALERT":
                w.writerow([
                    d.hotel_name,
                    d.destination,
                    d.departure_date,
                    d.nights,
                    d.price_total_gbp,
                    d.basket_price_gbp,
                    d.price_reference_gbp,
                    d.pricing_completeness,
                    d.checkout_stage,
                    d.checkout_step_count,
                    d.true_price_confidence,
                    d.scenario_price_gbp,
                    d.price_delta_gbp,
                    d.price_delta_pct,
                    d.deal_signal,
                    d.buy_now_score,
                    d.adjusted_buy_now_score,
                    d.action_now,
                    d.source_url,
                ])

    top = sorted(deals, key=lambda x: (x.adjusted_buy_now_score or 0, x.buy_now_score or 0), reverse=True)[:10]
    threshold = float(cfg.get("strategy", {}).get("buy_line_gbp", 3000))

    rec = ["# Autopilot recommendations", ""]
    for i, d in enumerate(top, 1):
        rec += [
            f"## {i}. {d.hotel_name or d.destination or 'Unnamed option'}",
            f"- Action now: {d.action_now or 'Watch'}",
            f"- Recommendation: {d.recommendation or 'Monitor'}",
            f"- Adjusted buy-now score: {d.adjusted_buy_now_score}",
            f"- Buy-now score: {d.buy_now_score}",
            f"- Headline price: {'£' + str(d.price_total_gbp) if d.price_total_gbp is not None else 'Unknown'}",
            f"- Near-final price: {'£' + str(d.basket_price_gbp) if d.basket_price_gbp is not None else 'Unknown'}",
            f"- Near-final price: {'£' + str(d.basket_price_gbp) if d.basket_price_gbp is not None else 'Unknown'}",
            f"- Scenario price: {'£' + str(d.scenario_price_gbp) if d.scenario_price_gbp is not None else 'Unknown'}",
            f"- Signal: {d.deal_signal or 'None'}",
            f"- Change: {d.price_delta_gbp if d.price_delta_gbp is not None else 'n/a'} GBP | {d.price_delta_pct if d.price_delta_pct is not None else 'n/a'}",
            f"- Bucket: {d.booking_bucket}",
            f"- Board: {d.board_basis or 'Unknown'}",
            f"- Flags: {d.warning_flags or 'None'}",
            f"- URL: {d.source_url}",
            "",
        ]
    rec_path.write_text("\n".join(rec), encoding="utf-8")

    top_lines = ["# Ranked holiday deals", f"", f"Booking threshold: £{threshold:,.0f}", ""]
    for i, d in enumerate(top, 1):
        top_lines += [
            f"## {i}. {d.hotel_name or d.destination or 'Unnamed option'}",
            f"- Action now: {d.action_now}",
            f"- Recommendation: {d.recommendation}",
            f"- Adjusted buy-now score: {d.adjusted_buy_now_score}",
            f"- Core score: {d.score}",
            f"- Signal: {d.deal_signal}",
            f"- Headline price: {'£' + str(d.price_total_gbp) if d.price_total_gbp is not None else 'Unknown'}",
            f"- Near-final price: {'£' + str(d.basket_price_gbp) if d.basket_price_gbp is not None else 'Unknown'}",
            f"- Scenario price: {'£' + str(d.scenario_price_gbp) if d.scenario_price_gbp is not None else 'Unknown'}",
            f"- Delta: {d.price_delta_gbp if d.price_delta_gbp is not None else 'n/a'} GBP | {d.price_delta_pct if d.price_delta_pct is not None else 'n/a'}",
            f"- Bucket: {d.booking_bucket}",
            f"- Board: {d.board_basis or 'Unknown'}",
            f"- Warning flags: {d.warning_flags or 'None'}",
            f"- URL: {d.source_url}",
            "",
        ]
    top_path.write_text("\n".join(top_lines), encoding="utf-8")

    best_payload = _best_today_payload(deals, cfg)
    best_json_path.write_text(json.dumps(best_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    best = best_payload.get("best")
    best_md = ["# Best deal today", ""]
    if best:
        best_md += [
            f"Generated: {best_payload['generated_at']}",
            f"",
            f"## {best.get('hotel_name') or best.get('destination') or 'Unnamed option'}",
            f"- Action now: {best.get('action_now')}",
            f"- Recommendation: {best.get('recommendation')}",
            f"- Scenario price: {'£' + str(best.get('scenario_price_gbp')) if best.get('scenario_price_gbp') is not None else 'Unknown'}",
            f"- Headline price: {'£' + str(best.get('price_total_gbp')) if best.get('price_total_gbp') is not None else 'Unknown'}",
            f"- Near-final price: {'£' + str(best.get('basket_price_gbp')) if best.get('basket_price_gbp') is not None else 'Unknown'}",
            f"- Signal: {best.get('deal_signal') or 'None'}",
            f"- Destination: {best.get('destination') or 'Unknown'}",
            f"- Board basis: {best.get('board_basis') or 'Unknown'}",
            f"- Warning flags: {best.get('warning_flags') or 'None'}",
            f"- URL: {best.get('source_url')}",
            "",
            f"Below threshold count: {best_payload['below_threshold_count']}",
            f"Drop alert count: {best_payload['drop_alert_count']}",
        ]
    else:
        best_md += ["No deals available yet."]
    best_md_path.write_text("\n".join(best_md), encoding="utf-8")

    provider_plan = build_provider_plan(cfg)
    elite_lines = [
        "# Elite search summary",
        "",
        f"Profile: {package_profile_text(cfg)}",
        f"Brief: {cfg.get('overview_prompt', '').strip()}",
        f"Buy line: £{threshold:,.0f}",
        "",
        "## Provider hunt order",
        "",
    ]
    for row in provider_plan:
        elite_lines.append(f"- {row['rank']}. {row['site']} ({row['intensity']}) — {row['reason']}")
    elite_lines += ["", "## Best now", ""]
    for d in top[:6]:
        elite_lines.append(f"- **{d.hotel_name or d.destination or 'Unnamed option'}** — {d.fit_label or 'Candidate'} — {'£' + str(effective_price(d)) if effective_price(d) is not None else 'Unknown'} — {d.shortlist_reason or d.recommendation or 'Worth a look'}")
    elite_lines += ["", "## Worth a look", ""]
    for d in worth[:6]:
        elite_lines.append(f"- **{d.hotel_name or d.destination or 'Unnamed option'}** — {d.fit_label or 'Candidate'} — {'£' + str(effective_price(d)) if effective_price(d) is not None else 'Unknown'} — {d.near_miss_reason or d.shortlist_reason or d.recommendation or 'Worth a look'}")
    elite_path.write_text("\n".join(elite_lines), encoding="utf-8")


def load_results() -> List[Deal]:
    p = RESULTS / "deals.json"
    return [Deal(**row) for row in json.loads(p.read_text(encoding="utf-8"))] if p.exists() else []



def run_demo_scan(mode: str = "balanced") -> List[Deal]:
    sample = [
        Deal(
            source_site="TUI Holidays", source_url="https://www.tui.co.uk/holidays/holidays-from-newcastle-airport", deal_type="package",
            destination="Burgas", country="Bulgaria", departure_date="2026-06-25", nights=7,
            hotel_name="Sunny Beach Suites", board_basis="Breakfast Included", price_total_gbp=2310, review_score=7.9,
            pool=True, beach_minutes=8, bags_included=True, transfers_included=False, free_child_place=False,
            family_room_type="Family Suite", child_pricing_note="Child discount applies", infant_cost_note="Infant taxes only",
            free_cancellation=True, notes="Budget beach option", extraction_confidence=0.77,
            ai_summary="Lowest price with acceptable fit", basket_price_gbp=2385, pricing_completeness="near-final", checkout_stage="room options", checkout_step_count=2, true_price_confidence=0.84, scan_batch="demo", scan_mode=mode,
            verified=False, extractor_used="site_specific.tui", provider_rank=1,
        ),
        Deal(
            source_site="Jet2 Holidays", source_url="https://www.jet2holidays.com/airports/newcastle-airport", deal_type="package",
            destination="Antalya", country="Turkey", departure_date="2026-06-24", nights=7,
            hotel_name="Lara Beach Resort", board_basis="All Inclusive", price_total_gbp=2895, review_score=8.7,
            pool=True, beach_minutes=4, bags_included=True, transfers_included=True, free_child_place=True,
            family_room_type="Family Room", child_pricing_note="Free child place", infant_cost_note="Infant taxes only",
            free_cancellation=False, near_booking_stage="room options", notes="Strong beach and heat fit", extraction_confidence=0.82,
            ai_summary="Very strong value family package", basket_price_gbp=2955, pricing_completeness="near-final", checkout_stage="basket", checkout_step_count=3, true_price_confidence=0.93, scan_batch="demo", scan_mode=mode,
            verified=False, extractor_used="site_specific.jet2", provider_rank=2,
        ),
        Deal(
            source_site="TravelSupermarket Packages", source_url="https://www.travelsupermarket.com/en-gb/holidays/", deal_type="package",
            destination="Paphos", country="Cyprus", departure_date="2026-06-26", nights=7,
            hotel_name="Coral Sands Resort", board_basis="Breakfast Included", price_total_gbp=2760, review_score=8.3,
            pool=True, beach_minutes=9, bags_included=False, transfers_included=False, free_child_place=False,
            family_room_type="1 Bedroom Apartment", child_pricing_note="Child pricing shown in basket", infant_cost_note="Small infant fee",
            free_cancellation=True, notes="Beach close", extraction_confidence=0.71,
            ai_summary="Good compromise option", basket_price_gbp=2810, pricing_completeness="near-final", checkout_stage="price-breakdown", checkout_step_count=2, true_price_confidence=0.81, scan_batch="demo", scan_mode=mode,
            verified=False, extractor_used="site_specific.travelsupermarket", provider_rank=3,
        ),
        Deal(
            source_site="loveholidays", source_url="https://www.loveholidays.com/holidays/newcastle-airport-holidays.html", deal_type="package",
            destination="Rhodes", country="Greece", departure_date="2026-07-03", nights=10,
            hotel_name="Blue Bay Apartments", board_basis="Breakfast Included", price_total_gbp=3015, review_score=8.5,
            pool=True, beach_minutes=6, bags_included=False, transfers_included=False, free_child_place=False,
            family_room_type="Apartment", child_pricing_note="Free child place on selected departures", infant_cost_note="Infant supplement applies",
            free_cancellation=True, notes="Good balance of price and quality", extraction_confidence=0.74,
            ai_summary="Good balance of price and quality", basket_price_gbp=3090, pricing_completeness="near-final", checkout_stage="room selection", checkout_step_count=2, true_price_confidence=0.79, scan_batch="demo", scan_mode=mode,
            verified=False, extractor_used="site_specific.loveholidays", provider_rank=4,
        ),
        Deal(
            source_site="On the Beach", source_url="https://www.onthebeach.co.uk/", deal_type="package",
            destination="Palma", country="Spain", departure_date="2026-06-28", nights=7,
            hotel_name="Playa Palma Family Hotel", board_basis="Half Board", price_total_gbp=3199, review_score=8.8,
            pool=True, beach_minutes=5, bags_included=True, transfers_included=False, free_child_place=True,
            family_room_type="Family Room", child_pricing_note="Free child place", infant_cost_note="Infant included",
            free_cancellation=False, notes="Beachfront Spain option", extraction_confidence=0.79,
            ai_summary="Best Spain family fit", scan_batch="demo", scan_mode=mode,
            verified=False, extractor_used="site_specific.onthebeach", provider_rank=5,
        ),
        Deal(
            source_site="TravelSupermarket Packages", source_url="https://www.travelsupermarket.com/en-gb/holidays/", deal_type="package",
            destination="Hurghada", country="Egypt", departure_date="2026-06-27", nights=10,
            hotel_name="Golden Coast Resort", board_basis="All Inclusive", price_total_gbp=3475, review_score=8.2,
            pool=True, beach_minutes=2, bags_included=True, transfers_included=True, free_child_place=False,
            family_room_type="Family Room", child_pricing_note="Child pricing needs verification", infant_cost_note="Infant included",
            free_cancellation=False, notes="Very hot option", extraction_confidence=0.70,
            ai_summary="Egypt option", scan_batch="demo", scan_mode=mode,
            verified=False, extractor_used="site_specific.travelsupermarket", provider_rank=6,
        ),
        Deal(
            source_site="TUI Holidays", source_url="https://www.tui.co.uk/holidays/holidays-from-newcastle-airport", deal_type="package",
            destination="Antalya", country="Turkey", departure_date="2026-06-24", nights=7,
            hotel_name="Lara Beach Resort", board_basis="All Inclusive", price_total_gbp=3049, review_score=8.7,
            pool=True, beach_minutes=4, bags_included=True, transfers_included=True, free_child_place=False,
            family_room_type="Family Room", child_pricing_note="Child pricing shown later", infant_cost_note="Infant taxes only",
            free_cancellation=False, near_booking_stage="detail", notes="Same resort on a second provider at a higher live price", extraction_confidence=0.80,
            ai_summary="Second provider reference price", basket_price_gbp=3095, pricing_completeness="near-final", checkout_stage="basket", checkout_step_count=3, true_price_confidence=0.90, scan_batch="demo", scan_mode=mode,
            verified=False, extractor_used="site_specific.tui", provider_rank=7,
        ),
        Deal(
            source_site="On the Beach", source_url="https://www.onthebeach.co.uk/", deal_type="package",
            destination="Paphos", country="Cyprus", departure_date="2026-06-26", nights=7,
            hotel_name="Coral Sands Resort", board_basis="Breakfast Included", price_total_gbp=2715, review_score=8.3,
            pool=True, beach_minutes=11, bags_included=False, transfers_included=False, free_child_place=False,
            family_room_type="1 Bedroom Apartment", child_pricing_note="Child pricing shown in basket", infant_cost_note="Small infant fee",
            free_cancellation=True, notes="Near miss on beach walk but a touch cheaper on another source", extraction_confidence=0.76,
            ai_summary="Useful near miss", basket_price_gbp=2765, pricing_completeness="near-final", checkout_stage="price-breakdown", checkout_step_count=2, true_price_confidence=0.82, scan_batch="demo", scan_mode=mode,
            verified=False, extractor_used="site_specific.onthebeach", provider_rank=8,
        ),
        Deal(
            source_site="loveholidays", source_url="https://www.loveholidays.com/holidays/newcastle-airport-holidays.html", deal_type="package",
            destination="Rhodes", country="Greece", departure_date="2026-07-03", nights=10,
            hotel_name="Blue Bay Apartments", board_basis="Breakfast Included", price_total_gbp=2965, review_score=8.5,
            pool=True, beach_minutes=14, bags_included=False, transfers_included=False, free_child_place=False,
            family_room_type="Apartment", child_pricing_note="Free child place on selected departures", infant_cost_note="Infant supplement applies",
            free_cancellation=True, notes="A little further from the beach but under the line", extraction_confidence=0.75,
            ai_summary="Near miss worth a look", basket_price_gbp=3035, pricing_completeness="near-final", checkout_stage="room selection", checkout_step_count=2, true_price_confidence=0.80, scan_batch="demo", scan_mode=mode,
            verified=False, extractor_used="site_specific.loveholidays", provider_rank=9,
        ),
    ]
    cfg = load_config()
    prev = load_previous_history()
    sample = dedupe_deals(sample)
    score_deals(sample, cfg["weights"], mode, cfg)
    apply_price_deltas(sample, prev, cfg)
    apply_strategy_overlay(sample, cfg)
    sample.sort(key=lambda x: (x.adjusted_buy_now_score or 0, x.buy_now_score or 0), reverse=True)
    write_outputs(sample, cfg)
    save_history_snapshot(sample)
    return sample


PROVIDER_TRUST = {
    "Jet2 Holidays": 0.96,
    "TUI Holidays": 0.95,
    "On the Beach": 0.92,
    "loveholidays": 0.91,
    "TravelSupermarket Packages": 0.88,
    "Expedia Packages": 0.86,
    "Skyscanner": 0.80,
    "KAYAK": 0.79,
    "Momondo": 0.78,
    "Google Flights": 0.75,
}

PROVIDER_SUPPORT = {
    "Jet2 Holidays": {"near_checkout": 0.96, "family_discount": 0.95, "package_depth": 0.98, "type": "package"},
    "TUI Holidays": {"near_checkout": 0.94, "family_discount": 0.92, "package_depth": 0.97, "type": "package"},
    "On the Beach": {"near_checkout": 0.90, "family_discount": 0.90, "package_depth": 0.93, "type": "package"},
    "loveholidays": {"near_checkout": 0.89, "family_discount": 0.91, "package_depth": 0.92, "type": "package"},
    "TravelSupermarket Packages": {"near_checkout": 0.84, "family_discount": 0.82, "package_depth": 0.80, "type": "package"},
    "Expedia Packages": {"near_checkout": 0.83, "family_discount": 0.78, "package_depth": 0.79, "type": "package"},
    "Skyscanner": {"near_checkout": 0.44, "family_discount": 0.26, "package_depth": 0.30, "type": "flight"},
    "KAYAK": {"near_checkout": 0.42, "family_discount": 0.24, "package_depth": 0.28, "type": "flight"},
    "Momondo": {"near_checkout": 0.42, "family_discount": 0.22, "package_depth": 0.28, "type": "flight"},
    "Google Flights": {"near_checkout": 0.36, "family_discount": 0.18, "package_depth": 0.22, "type": "flight"},
}


def option_id_for_deal(d: Deal) -> str:
    hotel = re.sub(r"[^a-z0-9]+", "", (d.hotel_name or "").lower())
    dest = re.sub(r"[^a-z0-9]+", "", (d.destination or "").lower())
    dep = (d.departure_date or "")[:10]
    nights = d.nights or ""
    return "|".join([hotel, dest, dep, str(nights)])


def enrich_option_market_view(deals: List[Deal]) -> None:
    groups: Dict[str, List[Deal]] = {}
    for d in deals:
        d.option_id = option_id_for_deal(d)
        groups.setdefault(d.option_id, []).append(d)
    for _, items in groups.items():
        priced = [x for x in items if effective_price(x) is not None]
        best = min(priced, key=lambda x: effective_price(x)) if priced else None
        source_count = len({x.source_site for x in items if x.source_site})
        for d in items:
            d.option_group_size = len(items)
            d.source_count = source_count
            d.provider_trust_score = PROVIDER_TRUST.get(d.source_site, 0.72)
            if best is not None:
                d.best_group_price_gbp = effective_price(best)
                d.best_group_source = best.source_site
                if effective_price(d) is not None and d.best_group_price_gbp is not None:
                    d.group_price_gap_gbp = round((effective_price(d) or 0) - d.best_group_price_gbp, 2)
                else:
                    d.group_price_gap_gbp = None


def classify_recommendations(deals: List[Deal], cfg: Dict[str, Any]) -> None:
    threshold = float(cfg.get("strategy", {}).get("buy_line_gbp", 3000) or 3000)
    prefs = cfg.get("search_preferences", {}) or {}
    for d in deals:
        if not d.fit_label:
            fit_label, fit_gap_score, near_reason = _fit_gap(
                d,
                cfg,
                threshold,
                float(prefs.get("min_temp_c", 26) or 26),
                int(prefs.get("target_beach_minutes", 10) or 10),
            )
            d.fit_label = fit_label
            d.fit_gap_score = fit_gap_score
            d.near_miss_reason = near_reason
    ranked = sorted(
        deals,
        key=lambda d: (
            {"Exact fit": 0, "Near miss worth a look": 1, "Stretch option": 2, "Out of range": 3}.get(d.fit_label or "", 3),
            -(d.adjusted_buy_now_score or d.buy_now_score or 0),
            effective_price(d) if effective_price(d) is not None else 9e9,
            -(d.true_price_confidence or 0),
        ),
    )
    exact_seen = near_seen = stretch_seen = 0
    for idx, d in enumerate(ranked, start=1):
        d.recommendation_rank = idx
        price = effective_price(d)
        best_source = d.best_group_source and d.best_group_source != d.source_site and d.group_price_gap_gbp and d.group_price_gap_gbp > 0
        checkout_strength = (d.true_price_confidence or 0) >= 0.84 and (d.pricing_completeness or "") in {"near-final", "basket", "checkout"}
        if d.fit_label == "Exact fit":
            exact_seen += 1
            d.recommendation_bucket = "best-now" if exact_seen <= 3 else "exact-fit"
        elif d.fit_label == "Near miss worth a look":
            near_seen += 1
            d.recommendation_bucket = "worth-a-look" if near_seen <= 4 else "near-miss"
        elif d.fit_label == "Stretch option":
            stretch_seen += 1
            d.recommendation_bucket = "stretch"
        else:
            d.recommendation_bucket = "out"
        reasons = []
        if price is not None and price <= threshold:
            reasons.append("inside your price line")
        elif price is not None:
            reasons.append(f"£{int(price-threshold):,} above your line")
        if checkout_strength:
            reasons.append("near-final basket signal")
        if d.free_child_place:
            reasons.append("free child place cue")
        if d.family_room_type:
            reasons.append(d.family_room_type)
        if d.beach_minutes is not None:
            reasons.append(f"{d.beach_minutes} min to beach")
        if d.estimated_temp_c is not None:
            reasons.append(f"about {int(d.estimated_temp_c)}°C")
        if best_source:
            reasons.append(f"cheaper at {d.best_group_source}")
        elif d.source_count > 1 and not d.group_price_gap_gbp:
            reasons.append("best seen price across sources")
        d.shortlist_reason = ", ".join(reasons[:5]) if reasons else (d.recommendation or "Worth a look")


def _rank_with_intelligence(d: Deal, cfg: Dict[str, Any], sort_by: str) -> tuple:
    fit_rank = {"Exact fit": 0, "Near miss worth a look": 1, "Stretch option": 2, "Out of range": 3}.get(d.fit_label or "", 3)
    ref_price = effective_price(d) if effective_price(d) is not None else 9e9
    confidence = d.true_price_confidence or 0
    source_bonus = -(d.source_count or 0)
    provider_bonus = -(d.provider_trust_score or 0)
    gap = d.group_price_gap_gbp if d.group_price_gap_gbp is not None else 0
    if sort_by == "price_low":
        return (fit_rank, ref_price, gap, -confidence, source_bonus)
    if sort_by == "beach":
        return (fit_rank, d.beach_minutes if d.beach_minutes is not None else 999, ref_price, -confidence)
    if sort_by == "hottest":
        return (fit_rank, -(d.estimated_temp_c or -999), ref_price, -confidence)
    if sort_by == "family_saver":
        return (fit_rank, 0 if d.free_child_place else 1, 0 if d.family_room_type else 1, ref_price, -confidence)
    if sort_by == "drop_alert":
        return (fit_rank, 0 if d.deal_signal == "DROP ALERT" else 1, ref_price, -confidence)
    if sort_by == "best_true_price":
        return (fit_rank, ref_price, -confidence, provider_bonus, source_bonus)
    if sort_by == "worth_looking_at":
        worth_rank = {"worth-a-look": 0, "near-miss": 1, "stretch": 2, "best-now": 3, "exact-fit": 4}.get(d.recommendation_bucket or "", 9)
        return (worth_rank, fit_rank, ref_price, -confidence, gap)
    intelligence = (
        (d.adjusted_buy_now_score or 0)
        + (d.buy_now_score or 0) * 0.45
        + confidence * 18
        + (d.provider_trust_score or 0) * 6
        + min(10, max(0, (d.source_count or 0) - 1) * 2)
        - max(0, gap) * 0.02
    )
    return (fit_rank, -intelligence, ref_price, -confidence, provider_bonus, source_bonus)


def query_deals(
    deals: List[Deal],
    cfg: Optional[Dict[str, Any]] = None,
    query: str = "",
    max_price: Optional[float] = None,
    min_temp: Optional[float] = None,
    beach_max_minutes: Optional[int] = None,
    require_pool: bool = False,
    breakfast_or_better: bool = False,
    alerts_only: bool = False,
    free_child_only: bool = False,
    family_room_only: bool = False,
    source_site: str = "",
    sort_by: str = "best",
) -> List[Deal]:
    cfg = cfg or load_config()
    query = _safe_text(query)
    enrich_option_market_view(deals)
    out: List[Deal] = []
    for d in deals:
        hay = " ".join([
            _safe_text(d.hotel_name), _safe_text(d.destination), _safe_text(d.country),
            _safe_text(d.board_basis), _safe_text(d.source_site), _safe_text(d.notes), _safe_text(d.recommendation), _safe_text(d.family_room_type), _safe_text(d.child_pricing_note), _safe_text(d.infant_cost_note), _safe_text(d.pros), _safe_text(d.cons), _safe_text(d.fit_summary), _safe_text(d.likely_price_direction), _safe_text(d.likely_price_reason), _safe_text(d.shortlist_reason), _safe_text(d.recommendation_bucket)
        ])
        if query and query not in hay:
            continue
        fit_label, fit_gap_score, near_reason = _fit_gap(d, cfg, max_price, min_temp, beach_max_minutes)
        d.fit_label = fit_label
        d.fit_gap_score = fit_gap_score
        d.near_miss_reason = near_reason
        if fit_label == "Out of range":
            continue
        if require_pool and d.pool is not True:
            continue
        board = _safe_text(d.board_basis)
        if breakfast_or_better and not any(x in board for x in ["breakfast", "bed and breakfast", "half board", "full board", "all inclusive"]):
            continue
        if alerts_only and d.deal_signal != "DROP ALERT":
            continue
        if free_child_only and d.free_child_place is not True:
            continue
        if family_room_only and not _safe_text(d.family_room_type):
            continue
        if source_site and _safe_text(source_site) not in _safe_text(d.source_site):
            continue
        out.append(d)
    classify_recommendations(out, cfg)
    return sorted(out, key=lambda d: _rank_with_intelligence(d, cfg, sort_by))


def build_provider_plan(cfg: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    cfg = cfg or load_config()
    prefs = cfg.get("search_preferences", {}) or {}
    scales = cfg.get("preference_scales", {}) or {}
    orchestration = cfg.get("orchestration", {}) or {}
    package_limit = int(orchestration.get("package_site_limit", 6) or 6)
    flight_limit = int(orchestration.get("flight_site_limit", 4) or 4)
    strictness = float(scales.get("strictness", 72) or 72) / 100.0
    family_bias = 1.0 if prefs.get("prefer_free_child_places") else 0.4
    family_bias += 0.35 if prefs.get("prefer_family_room") else 0.0
    near_checkout_bias = 1.0 if orchestration.get("prefer_near_checkout", True) else 0.4
    rows: List[Dict[str, Any]] = []
    for site in cfg.get("sites", []):
        name = site.get("name", "")
        support = PROVIDER_SUPPORT.get(name, {})
        trust = PROVIDER_TRUST.get(name, 0.72)
        near_checkout = float(support.get("near_checkout", 0.4) or 0.4)
        family_discount = float(support.get("family_discount", 0.3) or 0.3)
        package_depth = float(support.get("package_depth", 0.3) or 0.3)
        type_bonus = 0.16 if site.get("type") == "package" else 0.02
        score = (
            trust * 0.35
            + near_checkout * 0.30 * near_checkout_bias
            + package_depth * 0.18
            + family_discount * 0.17 * family_bias
            + type_bonus
            - strictness * (0.02 if site.get("type") == "flight" else 0.0)
        )
        reasons = []
        if site.get("type") == "package":
            reasons.append("better near-final basket potential")
        if near_checkout >= 0.9:
            reasons.append("deep provider journey")
        if family_discount >= 0.9:
            reasons.append("strong family discount cues")
        elif family_discount >= 0.75:
            reasons.append("useful family-room and child-place signals")
        if not reasons:
            reasons.append("broad market coverage")
        rows.append({
            "site": name,
            "type": site.get("type", "package"),
            "priority_score": round(score, 4),
            "reason": "; ".join(reasons),
            "intensity": "deep" if near_checkout >= 0.88 else ("medium" if near_checkout >= 0.6 else "broad"),
        })
    packages = sorted([r for r in rows if r["type"] == "package"], key=lambda r: r["priority_score"], reverse=True)[:package_limit]
    flights = sorted([r for r in rows if r["type"] == "flight"], key=lambda r: r["priority_score"], reverse=True)[:flight_limit]
    ordered = packages + flights
    for idx, row in enumerate(ordered, start=1):
        row["rank"] = idx
    return ordered


def generate_urls_preview(max_flight_queries: int = 12) -> List[Dict[str, str]]:
    cfg = load_config()
    queries = build_queries(cfg)
    provider_plan = build_provider_plan(cfg)
    ordered_site_names = [r["site"] for r in provider_plan]
    site_lookup = {s.get("name"): s for s in cfg["sites"]}
    sites = [site_lookup[n] for n in ordered_site_names if n in site_lookup]
    out: List[Dict[str, str]] = []
    flight_count = 0
    for site in sites:
        if site["type"] == "package":
            out.append({"site": site["name"], "url": site["url_template"], "type": "package", "destination": "", "country": "", "date": "", "nights": None})
            continue
        for q in queries:
            if flight_count >= max_flight_queries:
                break
            out.append({
                "site": site["name"],
                "url": site["url_template"].format(**q),
                "type": "flight",
                "destination": q["destination_city"],
                "date": q["depart_iso"],
                "country": q.get("destination_country", ""),
                "nights": q.get("nights"),
            })
            flight_count += 1
    return out


def write_outputs(deals: List[Deal], cfg: Optional[Dict[str, Any]] = None) -> None:
    cfg = cfg or load_config()
    enrich_option_market_view(deals)
    classify_recommendations(deals, cfg)
    fields = list(Deal(source_site="", source_url="", deal_type="").to_dict().keys())
    csv_path = RESULTS / "deals.csv"
    json_path = RESULTS / "deals.json"
    alerts_path = RESULTS / "price_drop_alerts.csv"
    rec_path = RESULTS / "autopilot_recommendations.md"
    top_path = RESULTS / "top_deals.md"
    best_json_path = RESULTS / "best_today.json"
    best_md_path = RESULTS / "best_today.md"
    shortlist_path = RESULTS / "worth_looking_at.md"
    elite_path = RESULTS / "elite_search_summary.md"

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for d in deals:
            w.writerow(d.to_dict())

    json_path.write_text(json.dumps([d.to_dict() for d in deals], indent=2, ensure_ascii=False), encoding="utf-8")

    with alerts_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["hotel_name","destination","departure_date","nights","price_total_gbp","basket_price_gbp","price_reference_gbp","pricing_completeness","checkout_stage","checkout_step_count","true_price_confidence","scenario_price_gbp","price_delta_gbp","price_delta_pct","deal_signal","buy_now_score","adjusted_buy_now_score","action_now","source_url"])
        for d in deals:
            if d.deal_signal == "DROP ALERT":
                w.writerow([d.hotel_name,d.destination,d.departure_date,d.nights,d.price_total_gbp,d.basket_price_gbp,d.price_reference_gbp,d.pricing_completeness,d.checkout_stage,d.checkout_step_count,d.true_price_confidence,d.scenario_price_gbp,d.price_delta_gbp,d.price_delta_pct,d.deal_signal,d.buy_now_score,d.adjusted_buy_now_score,d.action_now,d.source_url])

    top = sorted(deals, key=lambda x: _rank_with_intelligence(x, cfg, "best"))[:12]
    worth = sorted([d for d in deals if d.recommendation_bucket in {"worth-a-look", "near-miss", "stretch"}], key=lambda x: _rank_with_intelligence(x, cfg, "worth_looking_at"))[:12]
    threshold = float(cfg.get("strategy", {}).get("buy_line_gbp", 3000))

    rec = ["# Autopilot recommendations", "", f"Buy line: £{threshold:,.0f}", ""]
    for i, d in enumerate(top[:8], 1):
        rec += [
            f"## {i}. {d.hotel_name or d.destination or 'Unnamed option'}",
            f"- Bucket: {d.recommendation_bucket or 'candidate'}",
            f"- Why surfaced: {d.shortlist_reason or d.recommendation or 'Worth a look'}",
            f"- Ranked price: {'£' + str(effective_price(d)) if effective_price(d) is not None else 'Unknown'}",
            f"- Headline price: {'£' + str(d.price_total_gbp) if d.price_total_gbp is not None else 'Unknown'}",
            f"- Near-final price: {'£' + str(d.basket_price_gbp) if d.basket_price_gbp is not None else 'Unknown'}",
            f"- Confidence: {round((d.true_price_confidence or 0)*100)}%",
            f"- Best source for this option: {d.best_group_source or d.source_site or 'Unknown'}",
            f"- Source count seen: {d.source_count or 1}",
            f"- Group gap vs best seen: {'£' + str(d.group_price_gap_gbp) if d.group_price_gap_gbp is not None else 'Unknown'}",
            f"- Action now: {d.action_now or 'Watch'}",
            f"- URL: {d.source_url}",
            "",
        ]
    rec_path.write_text("\n".join(rec), encoding="utf-8")

    top_lines = ["# Ranked holiday deals", "", f"Booking threshold: £{threshold:,.0f}", ""]
    for i, d in enumerate(top, 1):
        top_lines += [
            f"## {i}. {d.hotel_name or d.destination or 'Unnamed option'}",
            f"- Fit: {d.fit_label or 'Candidate'}",
            f"- Recommendation bucket: {d.recommendation_bucket or 'candidate'}",
            f"- Why surfaced: {d.shortlist_reason or d.recommendation or 'Worth a look'}",
            f"- Ranked price: {'£' + str(effective_price(d)) if effective_price(d) is not None else 'Unknown'}",
            f"- Headline price: {'£' + str(d.price_total_gbp) if d.price_total_gbp is not None else 'Unknown'}",
            f"- Near-final price: {'£' + str(d.basket_price_gbp) if d.basket_price_gbp is not None else 'Unknown'}",
            f"- Confidence: {round((d.true_price_confidence or 0)*100)}%",
            f"- Best seen source for same option: {d.best_group_source or d.source_site or 'Unknown'}",
            f"- Source count seen: {d.source_count or 1}",
            f"- Gap vs best seen: {'£' + str(d.group_price_gap_gbp) if d.group_price_gap_gbp is not None else 'Unknown'}",
            f"- Warning flags: {d.warning_flags or 'None'}",
            f"- URL: {d.source_url}",
            "",
        ]
    top_path.write_text("\n".join(top_lines), encoding="utf-8")

    shortlist = ["# Near misses worth a look", ""]
    if worth:
        for i, d in enumerate(worth, 1):
            shortlist += [
                f"## {i}. {d.hotel_name or d.destination or 'Unnamed option'}",
                f"- Fit: {d.fit_label or 'Candidate'}",
                f"- Why still worth a look: {d.shortlist_reason or d.near_miss_reason or d.recommendation or 'Close to your brief'}",
                f"- Ranked price: {'£' + str(effective_price(d)) if effective_price(d) is not None else 'Unknown'}",
                f"- Near-final confidence: {round((d.true_price_confidence or 0)*100)}%",
                f"- URL: {d.source_url}",
                "",
            ]
    else:
        shortlist += ["No near misses surfaced this run."]
    shortlist_path.write_text("\n".join(shortlist), encoding="utf-8")

    best_payload = _best_today_payload(deals, cfg)
    best_json_path.write_text(json.dumps(best_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    best = best_payload.get("best")
    best_md = ["# Best deal today", ""]
    if best:
        best_md += [
            f"Generated: {best_payload['generated_at']}",
            "",
            f"## {best.get('hotel_name') or best.get('destination') or 'Unnamed option'}",
            f"- Recommendation bucket: {best.get('recommendation_bucket') or 'candidate'}",
            f"- Why surfaced: {best.get('shortlist_reason') or best.get('recommendation') or 'Worth a look'}",
            f"- Ranked price: {'£' + str(best.get('price_reference_gbp')) if best.get('price_reference_gbp') is not None else 'Unknown'}",
            f"- Headline price: {'£' + str(best.get('price_total_gbp')) if best.get('price_total_gbp') is not None else 'Unknown'}",
            f"- Near-final price: {'£' + str(best.get('basket_price_gbp')) if best.get('basket_price_gbp') is not None else 'Unknown'}",
            f"- Best seen source for same option: {best.get('best_group_source') or best.get('source_site') or 'Unknown'}",
            f"- Source count seen: {best.get('source_count') or 1}",
            f"- Gap vs best seen: {'£' + str(best.get('group_price_gap_gbp')) if best.get('group_price_gap_gbp') is not None else 'Unknown'}",
            f"- Confidence: {round((best.get('true_price_confidence') or 0)*100)}%",
            f"- URL: {best.get('source_url')}",
            "",
            f"Below threshold count: {best_payload['below_threshold_count']}",
            f"Drop alert count: {best_payload['drop_alert_count']}",
        ]
    else:
        best_md += ["No deals available yet."]
    best_md_path.write_text("\n".join(best_md), encoding="utf-8")

    provider_plan = build_provider_plan(cfg)
    elite_lines = [
        "# Elite search summary",
        "",
        f"Profile: {package_profile_text(cfg)}",
        f"Brief: {cfg.get('overview_prompt', '').strip()}",
        f"Buy line: £{threshold:,.0f}",
        "",
        "## Provider hunt order",
        "",
    ]
    for row in provider_plan:
        elite_lines.append(f"- {row['rank']}. {row['site']} ({row['intensity']}) — {row['reason']}")
    elite_lines += ["", "## Best now", ""]
    for d in top[:6]:
        elite_lines.append(f"- **{d.hotel_name or d.destination or 'Unnamed option'}** — {d.fit_label or 'Candidate'} — {'£' + str(effective_price(d)) if effective_price(d) is not None else 'Unknown'} — {d.shortlist_reason or d.recommendation or 'Worth a look'}")
    elite_lines += ["", "## Worth a look", ""]
    for d in worth[:6]:
        elite_lines.append(f"- **{d.hotel_name or d.destination or 'Unnamed option'}** — {d.fit_label or 'Candidate'} — {'£' + str(effective_price(d)) if effective_price(d) is not None else 'Unknown'} — {d.near_miss_reason or d.shortlist_reason or d.recommendation or 'Worth a look'}")
    elite_path.write_text("\n".join(elite_lines), encoding="utf-8")


def load_results() -> List[Deal]:
    p = RESULTS / "deals.json"
    rows = [Deal(**row) for row in json.loads(p.read_text(encoding="utf-8"))] if p.exists() else []
    if rows:
        cfg = load_config()
        enrich_option_market_view(rows)
        classify_recommendations(rows, cfg)
    return rows


def dedupe_deals(deals: List[Deal]) -> List[Deal]:
    chosen: Dict[str, Deal] = {}
    for d in deals:
        d.duplicate_key = "|".join([duplicate_key(d), re.sub(r"[^a-z0-9]+", "", (d.source_site or "").lower())])
        old = chosen.get(d.duplicate_key)
        if old is None or (d.extraction_confidence or 0) > (old.extraction_confidence or 0):
            chosen[d.duplicate_key] = d
    return list(chosen.values())


# ---- Elite v11 overrides ----

def _alloc_budgets(total: int, rows: List[Dict[str, Any]]) -> Dict[str, int]:
    if not rows:
        return {}
    total = max(total, len(rows))
    scores = [max(0.01, float(r.get("priority_score", 0.1))) for r in rows]
    base = {r["site"]: 1 for r in rows}
    remaining = total - len(rows)
    score_total = sum(scores) or 1.0
    for r, score in zip(rows, scores):
        base[r["site"]] += int(round((score / score_total) * remaining))
    while sum(base.values()) > total:
        biggest = max(base, key=base.get)
        if base[biggest] > 1:
            base[biggest] -= 1
        else:
            break
    while sum(base.values()) < total:
        biggest = max(rows, key=lambda r: r.get("priority_score", 0))["site"]
        base[biggest] += 1
    return base


def pricing_truth_label(d: Deal) -> str:
    stage = (d.checkout_stage or '').lower()
    comp = (d.pricing_completeness or '').lower()
    conf = d.true_price_confidence or 0
    if stage in {'pre-payment', 'payment-summary'} or comp == 'pre-payment' or conf >= 0.96:
        return 'Pre-payment price'
    if stage in {'basket', 'price-breakdown'} or 'basket' in comp or conf >= 0.88:
        return 'Basket-like price'
    if stage in {'room-selection', 'room selection'} or conf >= 0.72:
        return 'Room-stage price'
    if d.price_total_gbp is not None:
        return 'Headline price'
    return 'Unknown price'


def build_provider_plan(cfg: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    cfg = cfg or load_config()
    prefs = cfg.get("search_preferences", {}) or {}
    scales = cfg.get("preference_scales", {}) or {}
    orchestration = cfg.get("orchestration", {}) or {}
    package_limit = int(orchestration.get("package_site_limit", 6) or 6)
    flight_limit = int(orchestration.get("flight_site_limit", 4) or 4)
    strictness = float(scales.get("strictness", 72) or 72) / 100.0
    family_bias = 1.0 if prefs.get("prefer_free_child_places") else 0.4
    family_bias += 0.35 if prefs.get("prefer_family_room") else 0.0
    near_checkout_bias = 1.0 if orchestration.get("prefer_near_checkout", True) else 0.4
    rows: List[Dict[str, Any]] = []
    for site in cfg.get("sites", []):
        name = site.get("name", "")
        support = PROVIDER_SUPPORT.get(name, {})
        trust = PROVIDER_TRUST.get(name, 0.72)
        near_checkout = float(support.get("near_checkout", 0.4) or 0.4)
        family_discount = float(support.get("family_discount", 0.3) or 0.3)
        package_depth = float(support.get("package_depth", 0.3) or 0.3)
        type_bonus = 0.16 if site.get("type") == "package" else 0.02
        score = (
            trust * 0.34
            + near_checkout * 0.31 * near_checkout_bias
            + package_depth * 0.18
            + family_discount * 0.15 * family_bias
            + type_bonus
            - strictness * (0.03 if site.get("type") == "flight" else 0.0)
        )
        reasons = []
        if site.get("type") == "package":
            reasons.append("better near-final basket potential")
        if near_checkout >= 0.93:
            reasons.append("can often reach pre-payment style summaries")
        elif near_checkout >= 0.86:
            reasons.append("deep provider journey")
        if family_discount >= 0.9:
            reasons.append("strong family discount cues")
        elif family_discount >= 0.75:
            reasons.append("useful family-room and child-place signals")
        if not reasons:
            reasons.append("broad market coverage")
        rows.append({
            "site": name,
            "type": site.get("type", "package"),
            "priority_score": round(score, 4),
            "reason": "; ".join(reasons),
            "intensity": "deep" if near_checkout >= 0.88 else ("medium" if near_checkout >= 0.6 else "broad"),
            "near_checkout": near_checkout,
        })
    packages = sorted([r for r in rows if r["type"] == "package"], key=lambda r: r["priority_score"], reverse=True)[:package_limit]
    flights = sorted([r for r in rows if r["type"] == "flight"], key=lambda r: r["priority_score"], reverse=True)[:flight_limit]
    package_budget = _alloc_budgets(int(orchestration.get("package_query_cap", 96) or 96), packages)
    flight_budget = _alloc_budgets(int(orchestration.get("flight_query_cap", 40) or 40), flights)
    ordered = packages + flights
    for idx, row in enumerate(ordered, start=1):
        row["rank"] = idx
        row["search_budget"] = package_budget.get(row["site"], flight_budget.get(row["site"], 1))
        row["priority_band"] = "primary" if idx <= 3 else ("secondary" if idx <= 6 else "support")
    return ordered


def generate_urls_preview(max_flight_queries: int = 40) -> List[Dict[str, str]]:
    cfg = load_config()
    queries = build_queries(cfg)
    provider_plan = build_provider_plan(cfg)
    ordered_site_names = [r["site"] for r in provider_plan]
    site_lookup = {s.get("name"): s for s in cfg["sites"]}
    sites = [site_lookup[n] for n in ordered_site_names if n in site_lookup]
    plan_lookup = {r["site"]: r for r in provider_plan}
    package_rows: Dict[str, List[Dict[str, Any]]] = {}
    flight_rows: Dict[str, List[Dict[str, Any]]] = {}
    effective_flight_cap = min(max_flight_queries, int((cfg.get("orchestration", {}) or {}).get("flight_query_cap", max_flight_queries) or max_flight_queries))
    for site in sites:
        plan = plan_lookup.get(site["name"], {})
        budget = int(plan.get("search_budget", 1) or 1)
        bucket: List[Dict[str, Any]] = []
        if site["type"] == "package":
            for q in queries[:budget]:
                bucket.append({
                    "site": site["name"],
                    "url": site["url_template"],
                    "type": "package",
                    "destination": q["destination_city"],
                    "country": q.get("destination_country", ""),
                    "date": q.get("depart_iso", ""),
                    "return_date": q.get("return_iso", ""),
                    "nights": q.get("nights"),
                    "window_label": q.get("window_label", ""),
                    "search_route_id": f"{site['name']}::{q.get('destination_city','')}::{q.get('depart_iso','')}::{q.get('nights','')}",
                    "provider_priority_band": plan.get("priority_band", "support"),
                    "search_budget": budget,
                    **q,
                })
            package_rows[site["name"]] = bucket
        else:
            for q in queries[:min(budget, effective_flight_cap)]:
                bucket.append({
                    "site": site["name"],
                    "url": site["url_template"].format(**q),
                    "type": "flight",
                    "destination": q["destination_city"],
                    "date": q["depart_iso"],
                    "country": q.get("destination_country", ""),
                    "nights": q.get("nights"),
                    "window_label": q.get("window_label", ""),
                    "search_route_id": f"{site['name']}::{q.get('destination_city','')}::{q.get('depart_iso','')}::{q.get('nights','')}",
                    "provider_priority_band": plan.get("priority_band", "support"),
                    "search_budget": budget,
                    **q,
                })
            flight_rows[site["name"]] = bucket
    out: List[Dict[str, Any]] = []
    provider_order = [s["name"] for s in sites if s["type"] == "package"]
    while any(package_rows.get(name) for name in provider_order):
        for name in provider_order:
            if package_rows.get(name):
                out.append(package_rows[name].pop(0))
    flight_count = 0
    flight_order = [s["name"] for s in sites if s["type"] == "flight"]
    while flight_count < effective_flight_cap and any(flight_rows.get(name) for name in flight_order):
        for name in flight_order:
            if flight_count >= effective_flight_cap:
                break
            if flight_rows.get(name):
                out.append(flight_rows[name].pop(0))
                flight_count += 1
    max_rows = int((cfg.get("orchestration", {}) or {}).get("max_live_rows", 180) or 180)
    return out[:max_rows]


def classify_recommendations(deals: List[Deal], cfg: Dict[str, Any]) -> None:
    threshold = float(cfg.get("strategy", {}).get("buy_line_gbp", 3000) or 3000)
    prefs = cfg.get("search_preferences", {}) or {}
    provider_lookup = {row["site"]: row for row in build_provider_plan(cfg)}
    for d in deals:
        d.pricing_truth_label = pricing_truth_label(d)
        if d.source_site:
            d.provider_priority_band = provider_lookup.get(d.source_site, {}).get("priority_band", d.provider_priority_band or "support")
        if not d.fit_label:
            fit_label, fit_gap_score, near_reason = _fit_gap(
                d,
                cfg,
                threshold,
                float(prefs.get("min_temp_c", 26) or 26),
                int(prefs.get("target_beach_minutes", 10) or 10),
            )
            d.fit_label = fit_label
            d.fit_gap_score = fit_gap_score
            d.near_miss_reason = near_reason
    ranked = sorted(
        deals,
        key=lambda d: (
            {"Exact fit": 0, "Near miss worth a look": 1, "Stretch option": 2, "Out of range": 3}.get(d.fit_label or "", 3),
            -(d.adjusted_buy_now_score or d.buy_now_score or 0),
            effective_price(d) if effective_price(d) is not None else 9e9,
            -(d.true_price_confidence or 0),
        ),
    )
    exact_seen = near_seen = stretch_seen = 0
    for idx, d in enumerate(ranked, start=1):
        d.recommendation_rank = idx
        price = effective_price(d)
        best_source = d.best_group_source and d.best_group_source != d.source_site and d.group_price_gap_gbp and d.group_price_gap_gbp > 0
        checkout_strength = (d.true_price_confidence or 0) >= 0.84 and (d.pricing_completeness or "") in {"pre-payment", "near-final", "basket", "checkout"}
        if d.fit_label == "Exact fit":
            exact_seen += 1
            d.recommendation_bucket = "best-now" if exact_seen <= 3 else "exact-fit"
        elif d.fit_label == "Near miss worth a look":
            near_seen += 1
            d.recommendation_bucket = "worth-a-look" if near_seen <= 4 else "near-miss"
        elif d.fit_label == "Stretch option":
            stretch_seen += 1
            d.recommendation_bucket = "stretch"
        else:
            d.recommendation_bucket = "out"
        reasons = []
        if price is not None and price <= threshold:
            reasons.append("inside your price line")
        elif price is not None:
            reasons.append(f"GBP {int(price-threshold):,} above your line")
        if checkout_strength:
            reasons.append(d.pricing_truth_label.lower())
        if d.free_child_place:
            reasons.append("free child place cue")
        if d.family_room_type:
            reasons.append(d.family_room_type)
        if d.beach_minutes is not None:
            reasons.append(f"{d.beach_minutes} min to beach")
        if d.estimated_temp_c is not None:
            reasons.append(f"about {int(d.estimated_temp_c)}C")
        if best_source:
            reasons.append(f"cheaper at {d.best_group_source}")
        elif d.source_count > 1 and not d.group_price_gap_gbp:
            reasons.append("best seen price across sources")
        d.shortlist_reason = ", ".join(reasons[:5]) if reasons else (d.recommendation or "Worth a look")


def build_provider_tuning_report(deals: List[Deal], cfg: Dict[str, Any]) -> Dict[str, Any]:
    top_sites = (cfg.get('provider_tuning', {}) or {}).get('top_sites', [])
    rows = []
    for site in top_sites:
        site_rows = [d for d in deals if (d.source_site or '') == site]
        if not site_rows:
            rows.append({
                'site': site, 'rows': 0, 'best_stage': 'none', 'avg_confidence': 0.0,
                'prepayment_count': 0, 'basket_count': 0, 'room_count': 0, 'partial_count': 0,
                'evidence_count': 0, 'notes': 'No rows captured yet'
            })
            continue
        stages = [((d.checkout_stage or d.provider_state or 'landing').lower()) for d in site_rows]
        priority = {'pre-payment':4,'basket':3,'room-selection':2,'rooms':2,'hotel':1,'landing':0}
        best_stage = max(stages, key=lambda s: priority.get(s,0))
        prepay = sum(1 for d in site_rows if pricing_truth_label(d) == 'Pre-payment price')
        basket = sum(1 for d in site_rows if pricing_truth_label(d) == 'Basket-like price')
        room = sum(1 for d in site_rows if pricing_truth_label(d) == 'Room-stage price')
        partial = sum(1 for d in site_rows if (d.automation_status or '') == 'partial')
        evidence = sum(1 for d in site_rows if d.html_capture_file or d.screenshot_file or d.raw_text_file)
        avg_conf = round(sum((d.true_price_confidence or 0) for d in site_rows) / max(1,len(site_rows)), 3)
        notes=[]
        if prepay == 0: notes.append('needs deeper pre-payment reach')
        if partial > max(2, len(site_rows)//3): notes.append('many routes still partial')
        if evidence < len(site_rows): notes.append('some captures missing evidence')
        if not notes: notes.append('provider journey looks healthy')
        rows.append({
            'site': site, 'rows': len(site_rows), 'best_stage': best_stage, 'avg_confidence': avg_conf,
            'prepayment_count': prepay, 'basket_count': basket, 'room_count': room, 'partial_count': partial,
            'evidence_count': evidence, 'notes': '; '.join(notes)
        })
    return {'generated_at': datetime.utcnow().isoformat() + 'Z', 'providers': rows}


def write_provider_tuning_outputs(deals: List[Deal], cfg: Dict[str, Any]) -> None:
    report = build_provider_tuning_report(deals, cfg)
    tuning_json = RESULTS / 'provider_tuning_report.json'
    tuning_md = RESULTS / 'provider_tuning_report.md'
    queue_md = RESULTS / 'assisted_review_queue.md'
    truth_md = RESULTS / 'truth_ranked_shortlist.md'
    tuning_json.write_text(json.dumps(report, indent=2), encoding='utf-8')
    lines = ['# Provider tuning report','']
    for row in report['providers']:
        lines += [f"## {row['site']}", '', f"- Captured rows: {row['rows']}", f"- Best stage reached: {row['best_stage']}", f"- Average truth confidence: {row['avg_confidence']}", f"- Pre-payment rows: {row['prepayment_count']}", f"- Basket-like rows: {row['basket_count']}", f"- Room-stage rows: {row['room_count']}", f"- Partial rows: {row['partial_count']}", f"- Evidence count: {row['evidence_count']}", f"- Notes: {row['notes']}", '']
    tuning_md.write_text('\n'.join(lines), encoding='utf-8')

    review_rows = [d for d in deals if (d.automation_status or '') == 'partial' or (d.true_price_confidence or 0) < float((cfg.get('provider_tuning', {}) or {}).get('minimum_truth_confidence', 0.84) or 0.84)]
    review_rows = sorted(review_rows, key=lambda d: (-(d.adjusted_buy_now_score or d.buy_now_score or 0), effective_price(d) if effective_price(d) is not None else 9e9))[:20]
    q = ['# Assisted review queue', '', 'These are promising options where the engine got close enough to be interesting, but not far enough to fully trust without another provider-specific pass.', '']
    for d in review_rows:
        q += [f"- **{d.source_site}** — {d.hotel_name or d.destination or 'Unnamed option'} — {('£' + format(effective_price(d),',.0f')) if effective_price(d) is not None else 'Unknown price'} — state: {d.checkout_stage or d.provider_state or 'landing'} — confidence: {round(d.true_price_confidence or 0, 2)} — evidence: {d.evidence_note or '-'}"]
    if len(q) == 4:
        q.append('- No assisted-review candidates right now.')
    queue_md.write_text('\n'.join(q), encoding='utf-8')

    ranked = sorted(deals, key=lambda d: (-(d.true_price_confidence or 0), effective_price(d) if effective_price(d) is not None else 9e9, -(d.adjusted_buy_now_score or d.buy_now_score or 0)))[:25]
    t = ['# Truth-ranked shortlist', '', 'This view prefers stronger basket/pre-payment evidence over tempting but shallow headline prices.', '']
    for d in ranked:
        t += [f"- **{d.hotel_name or d.destination or 'Unnamed option'}** ({d.source_site}) — {pricing_truth_label(d)} — {('£' + format(effective_price(d),',.0f')) if effective_price(d) is not None else 'Unknown price'} — confidence {round(d.true_price_confidence or 0, 2)} — {d.shortlist_reason or d.recommendation or 'Worth a look'}"]
    truth_md.write_text('\n'.join(t), encoding='utf-8')


_write_outputs_base_v11 = write_outputs

def write_outputs(deals: List[Deal], cfg: Optional[Dict[str, Any]] = None) -> None:
    cfg = cfg or load_config()
    enrich_option_market_view(deals)
    classify_recommendations(deals, cfg)
    fields = list(Deal(source_site="", source_url="", deal_type="").to_dict().keys())
    csv_path = RESULTS / "deals.csv"
    json_path = RESULTS / "deals.json"
    alerts_path = RESULTS / "price_drop_alerts.csv"
    rec_path = RESULTS / "autopilot_recommendations.md"
    top_path = RESULTS / "top_deals.md"
    best_json_path = RESULTS / "best_today.json"
    best_md_path = RESULTS / "best_today.md"
    shortlist_path = RESULTS / "worth_looking_at.md"
    elite_path = RESULTS / "elite_search_summary.md"
    prepay_path = RESULTS / "prepayment_candidates.md"
    manifest_path = RESULTS / "search_manifest.json"

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for d in deals:
            w.writerow(d.to_dict())
    json_path.write_text(json.dumps([d.to_dict() for d in deals], indent=2, ensure_ascii=False), encoding="utf-8")
    with alerts_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["hotel_name","destination","departure_date","nights","price_total_gbp","basket_price_gbp","price_reference_gbp","pricing_completeness","checkout_stage","checkout_step_count","true_price_confidence","scenario_price_gbp","price_delta_gbp","price_delta_pct","deal_signal","buy_now_score","adjusted_buy_now_score","action_now","source_url"])
        for d in deals:
            if d.deal_signal == "DROP ALERT":
                w.writerow([d.hotel_name,d.destination,d.departure_date,d.nights,d.price_total_gbp,d.basket_price_gbp,d.price_reference_gbp,d.pricing_completeness,d.checkout_stage,d.checkout_step_count,d.true_price_confidence,d.scenario_price_gbp,d.price_delta_gbp,d.price_delta_pct,d.deal_signal,d.buy_now_score,d.adjusted_buy_now_score,d.action_now,d.source_url])

    top = sorted(deals, key=lambda x: _rank_with_intelligence(x, cfg, "best"))[:12]
    worth = sorted([d for d in deals if d.recommendation_bucket in {"worth-a-look", "near-miss", "stretch"}], key=lambda x: _rank_with_intelligence(x, cfg, "worth_looking_at"))[:12]
    threshold = float(cfg.get("strategy", {}).get("buy_line_gbp", 3000))
    rec = ["# Autopilot recommendations", "", f"Buy line: GBP {threshold:,.0f}", ""]
    for i, d in enumerate(top[:8], 1):
        rec += [
            f"## {i}. {d.hotel_name or d.destination or 'Unnamed option'}",
            f"- Bucket: {d.recommendation_bucket or 'candidate'}",
            f"- Why surfaced: {d.shortlist_reason or d.recommendation or 'Worth a look'}",
            f"- Truth label: {d.pricing_truth_label or pricing_truth_label(d)}",
            f"- Ranked price: {'GBP ' + str(effective_price(d)) if effective_price(d) is not None else 'Unknown'}",
            f"- Headline price: {'GBP ' + str(d.price_total_gbp) if d.price_total_gbp is not None else 'Unknown'}",
            f"- Near-final price: {'GBP ' + str(d.basket_price_gbp) if d.basket_price_gbp is not None else 'Unknown'}",
            f"- Confidence: {round((d.true_price_confidence or 0)*100)}%",
            f"- Best source for this option: {d.best_group_source or d.source_site or 'Unknown'}",
            f"- Source count seen: {d.source_count or 1}",
            f"- Group gap vs best seen: {'GBP ' + str(d.group_price_gap_gbp) if d.group_price_gap_gbp is not None else 'Unknown'}",
            f"- Action now: {d.action_now or 'Watch'}",
            f"- URL: {d.source_url}",
            "",
        ]
    rec_path.write_text("\n".join(rec), encoding="utf-8")

    top_lines = ["# Ranked holiday deals", "", f"Booking threshold: GBP {threshold:,.0f}", ""]
    for i, d in enumerate(top, 1):
        top_lines += [
            f"## {i}. {d.hotel_name or d.destination or 'Unnamed option'}",
            f"- Fit: {d.fit_label or 'Candidate'}",
            f"- Recommendation bucket: {d.recommendation_bucket or 'candidate'}",
            f"- Why surfaced: {d.shortlist_reason or d.recommendation or 'Worth a look'}",
            f"- Truth label: {d.pricing_truth_label or pricing_truth_label(d)}",
            f"- Ranked price: {'GBP ' + str(effective_price(d)) if effective_price(d) is not None else 'Unknown'}",
            f"- Headline price: {'GBP ' + str(d.price_total_gbp) if d.price_total_gbp is not None else 'Unknown'}",
            f"- Near-final price: {'GBP ' + str(d.basket_price_gbp) if d.basket_price_gbp is not None else 'Unknown'}",
            f"- Confidence: {round((d.true_price_confidence or 0)*100)}%",
            f"- Best seen source for same option: {d.best_group_source or d.source_site or 'Unknown'}",
            f"- Source count seen: {d.source_count or 1}",
            f"- Gap vs best seen: {'GBP ' + str(d.group_price_gap_gbp) if d.group_price_gap_gbp is not None else 'Unknown'}",
            f"- Search route: {d.search_route_id or '-'}",
            f"- Warning flags: {d.warning_flags or 'None'}",
            f"- URL: {d.source_url}",
            "",
        ]
    top_path.write_text("\n".join(top_lines), encoding="utf-8")

    shortlist = ["# Near misses worth a look", ""]
    if worth:
        for i, d in enumerate(worth, 1):
            shortlist += [
                f"## {i}. {d.hotel_name or d.destination or 'Unnamed option'}",
                f"- Fit: {d.fit_label or 'Candidate'}",
                f"- Why still worth a look: {d.shortlist_reason or d.near_miss_reason or d.recommendation or 'Close to your brief'}",
                f"- Ranked price: {'GBP ' + str(effective_price(d)) if effective_price(d) is not None else 'Unknown'}",
                f"- Truth label: {d.pricing_truth_label or pricing_truth_label(d)}",
                f"- Near-final confidence: {round((d.true_price_confidence or 0)*100)}%",
                f"- URL: {d.source_url}",
                "",
            ]
    else:
        shortlist += ["No near misses surfaced this run."]
    shortlist_path.write_text("\n".join(shortlist), encoding="utf-8")

    prepay = sorted([d for d in deals if (d.pricing_truth_label or "") in {"Pre-payment price", "Basket-like price"}], key=lambda x: _rank_with_intelligence(x, cfg, "best_true_price"))[:20]
    prepay_lines = ["# Strongest true-price candidates", ""]
    if prepay:
        for i, d in enumerate(prepay, 1):
            prepay_lines += [
                f"## {i}. {d.hotel_name or d.destination or 'Unnamed option'}",
                f"- Truth label: {d.pricing_truth_label or pricing_truth_label(d)}",
                f"- Ranked price: {'GBP ' + str(effective_price(d)) if effective_price(d) is not None else 'Unknown'}",
                f"- Headline price: {'GBP ' + str(d.price_total_gbp) if d.price_total_gbp is not None else 'Unknown'}",
                f"- Checkout stage: {d.checkout_stage or 'landing'}",
                f"- Confidence: {round((d.true_price_confidence or 0)*100)}%",
                f"- Search route: {d.search_route_id or '-'}",
                f"- URL: {d.source_url}",
                "",
            ]
    else:
        prepay_lines.append("No basket-like or pre-payment candidates captured this run.")
    prepay_path.write_text("\n".join(prepay_lines), encoding="utf-8")

    best_payload = _best_today_payload(deals, cfg)
    best_json_path.write_text(json.dumps(best_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    best = best_payload.get("best")
    best_md = ["# Best deal today", ""]
    if best:
        best_md += [
            f"Generated: {best_payload['generated_at']}",
            "",
            f"## {best.get('hotel_name') or best.get('destination') or 'Unnamed option'}",
            f"- Recommendation bucket: {best.get('recommendation_bucket') or 'candidate'}",
            f"- Why surfaced: {best.get('shortlist_reason') or best.get('recommendation') or 'Worth a look'}",
            f"- Truth label: {best.get('pricing_truth_label') or 'Unknown price'}",
            f"- Ranked price: {'GBP ' + str(best.get('price_reference_gbp')) if best.get('price_reference_gbp') is not None else 'Unknown'}",
            f"- Headline price: {'GBP ' + str(best.get('price_total_gbp')) if best.get('price_total_gbp') is not None else 'Unknown'}",
            f"- Near-final price: {'GBP ' + str(best.get('basket_price_gbp')) if best.get('basket_price_gbp') is not None else 'Unknown'}",
            f"- Best seen source for same option: {best.get('best_group_source') or best.get('source_site') or 'Unknown'}",
            f"- Source count seen: {best.get('source_count') or 1}",
            f"- Gap vs best seen: {'GBP ' + str(best.get('group_price_gap_gbp')) if best.get('group_price_gap_gbp') is not None else 'Unknown'}",
            f"- Confidence: {round((best.get('true_price_confidence') or 0)*100)}%",
            f"- URL: {best.get('source_url')}",
            "",
            f"Below threshold count: {best_payload['below_threshold_count']}",
            f"Drop alert count: {best_payload['drop_alert_count']}",
        ]
    else:
        best_md += ["No deals available yet."]
    best_md_path.write_text("\n".join(best_md), encoding="utf-8")

    provider_plan = build_provider_plan(cfg)
    manifest = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "search_plan": build_search_plan(cfg),
        "provider_plan": provider_plan,
        "rows_planned": len(generate_urls_preview(int((cfg.get("orchestration", {}) or {}).get("flight_query_cap", 40) or 40))),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    elite_lines = [
        "# Elite search summary",
        "",
        f"Profile: {package_profile_text(cfg)}",
        f"Brief: {cfg.get('overview_prompt', '').strip()}",
        f"Buy line: GBP {threshold:,.0f}",
        f"Target live search volume: {int((cfg.get('orchestration', {}) or {}).get('target_search_volume', 180) or 180)} journeys",
        "",
        "## Provider hunt order",
        "",
    ]
    for row in provider_plan:
        elite_lines.append(f"- {row['rank']}. {row['site']} ({row['intensity']}) — budget {row.get('search_budget',1)} — {row['reason']}")
    prepay_hits = len([d for d in deals if (d.pricing_truth_label or "") == "Pre-payment price"])
    elite_lines += ["", f"Pre-payment style hits: {prepay_hits}", "", "## Best now", ""]
    for d in top[:6]:
        elite_lines.append(f"- **{d.hotel_name or d.destination or 'Unnamed option'}** — {d.fit_label or 'Candidate'} — {'GBP ' + str(effective_price(d)) if effective_price(d) is not None else 'Unknown'} — {d.shortlist_reason or d.recommendation or 'Worth a look'}")
    elite_lines += ["", "## Worth a look", ""]
    for d in worth[:6]:
        elite_lines.append(f"- **{d.hotel_name or d.destination or 'Unnamed option'}** — {d.fit_label or 'Candidate'} — {'GBP ' + str(effective_price(d)) if effective_price(d) is not None else 'Unknown'} — {d.near_miss_reason or d.shortlist_reason or d.recommendation or 'Worth a look'}")
    elite_path.write_text("\n".join(elite_lines), encoding="utf-8")


def write_outputs(deals: List[Deal], cfg: Optional[Dict[str, Any]] = None) -> None:
    cfg = cfg or load_config()
    # Use the latest enriched writer, then add basket audit outputs.
    _base_write_outputs = globals().get("_write_outputs_base_v11")
    if _base_write_outputs is not None:
        _base_write_outputs(deals, cfg)
    else:
        # fallback to previous definition in file via alias inserted below at import-time
        pass
    audit_json_path = RESULTS / "basket_audit.json"
    audit_md_path = RESULTS / "basket_audit.md"
    top_n = int(((cfg.get("provider_execution") or {}).get("save_evidence_for_top", 30)) or 30)
    ranked = sorted(deals, key=lambda d: (-(d.adjusted_buy_now_score or d.buy_now_score or 0), effective_price(d) if effective_price(d) is not None else 9e9, -(d.true_price_confidence or 0)))[:top_n]
    audit_rows = []
    lines = ["# Basket audit", "", f"Generated: {datetime.now().isoformat(timespec='seconds')}", ""]
    for idx, d in enumerate(ranked, start=1):
        row = {
            "rank": idx,
            "hotel_name": d.hotel_name,
            "source_site": d.source_site,
            "source_url": d.source_url,
            "destination": d.destination,
            "departure_date": d.departure_date,
            "nights": d.nights,
            "headline_price_gbp": d.headline_price_gbp,
            "basket_price_gbp": d.basket_price_gbp,
            "price_reference_gbp": d.price_reference_gbp,
            "deposit_price_gbp": d.deposit_price_gbp,
            "due_now_gbp": d.due_now_gbp,
            "pricing_truth_label": d.pricing_truth_label,
            "pricing_completeness": d.pricing_completeness,
            "checkout_stage": d.checkout_stage,
            "checkout_step_count": d.checkout_step_count,
            "provider_state": d.provider_state,
            "automation_status": d.automation_status,
            "true_price_confidence": d.true_price_confidence,
            "board_basis": d.board_basis,
            "family_room_type": d.family_room_type,
            "free_child_place": d.free_child_place,
            "child_pricing_note": d.child_pricing_note,
            "infant_cost_note": d.infant_cost_note,
            "bags_included": d.bags_included,
            "baggage_summary": d.baggage_summary,
            "transfers_included": d.transfers_included,
            "transfer_summary": d.transfer_summary,
            "basket_inclusions": d.basket_inclusions,
            "rooms_requested": d.rooms_requested,
            "rooms_matched": d.rooms_matched,
            "raw_text_file": d.raw_text_file,
            "html_capture_file": d.html_capture_file,
            "screenshot_file": d.screenshot_file,
            "basket_json_file": d.basket_json_file,
            "session_id": d.session_id,
            "evidence_note": d.evidence_note,
        }
        audit_rows.append(row)
        lines += [
            f"## {idx}. {d.hotel_name or d.destination or 'Unnamed option'}",
            f"- Provider: {d.source_site}",
            f"- Ranked price: {'GBP ' + str(d.price_reference_gbp) if d.price_reference_gbp is not None else 'Unknown'}",
            f"- Deposit: {'GBP ' + str(d.deposit_price_gbp) if d.deposit_price_gbp is not None else 'Unknown'}",
            f"- Due now: {'GBP ' + str(d.due_now_gbp) if d.due_now_gbp is not None else 'Unknown'}",
            f"- Stage: {d.checkout_stage or 'landing'} | State: {d.provider_state or '-'} | Status: {d.automation_status or '-'}",
            f"- Confidence: {round((d.true_price_confidence or 0)*100)}%",
            f"- Family cues: {', '.join([x for x in [d.family_room_type, d.child_pricing_note, d.infant_cost_note] if x]) or 'None'}",
            f"- Inclusions: {d.basket_inclusions or 'Unknown'}",
            f"- Evidence: screenshot={d.screenshot_file or '-'}, html={d.html_capture_file or '-'}, basket_json={d.basket_json_file or '-'}",
            f"- URL: {d.source_url}",
            "",
        ]
    audit_json_path.write_text(json.dumps(audit_rows, indent=2, ensure_ascii=False), encoding='utf-8')
    audit_md_path.write_text("\n".join(lines), encoding='utf-8')
    write_provider_tuning_outputs(deals, cfg)



# ---- Final elite output layer v15 ----
from collections import Counter, defaultdict

def build_provider_scorecard(deals: List[Deal]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Deal]] = defaultdict(list)
    for d in deals:
        grouped[d.source_site or 'Unknown'].append(d)
    rows: List[Dict[str, Any]] = []
    for site, items in grouped.items():
        truth_hits = sum(1 for d in items if (d.pricing_truth_label or '') in {'Basket-like price','Pre-payment price'})
        avg_conf = sum((d.true_price_confidence or 0) for d in items) / max(1, len(items))
        avg_stage = sum((d.checkout_step_count or 0) for d in items) / max(1, len(items))
        best_price = min((effective_price(d) for d in items if effective_price(d) is not None), default=None)
        rows.append({
            'provider': site,
            'results': len(items),
            'truth_hits': truth_hits,
            'truth_hit_rate': round(truth_hits / max(1, len(items)), 3),
            'avg_confidence': round(avg_conf, 3),
            'avg_step_depth': round(avg_stage, 2),
            'best_price_gbp': best_price,
        })
    rows.sort(key=lambda r: (-r['truth_hits'], -r['avg_confidence'], r['best_price_gbp'] if r['best_price_gbp'] is not None else 9e9))
    return rows


def build_evidence_index(deals: List[Deal]) -> Dict[str, Any]:
    items = []
    for d in sorted(deals, key=lambda x: (-(x.true_price_confidence or 0), effective_price(x) if effective_price(x) is not None else 9e9))[:100]:
        items.append({
            'hotel_name': d.hotel_name,
            'destination': d.destination,
            'provider': d.source_site,
            'ranked_price_gbp': effective_price(d),
            'truth_label': d.pricing_truth_label or pricing_truth_label(d),
            'confidence': d.true_price_confidence,
            'checkout_stage': d.checkout_stage,
            'html_capture_file': d.html_capture_file,
            'screenshot_file': d.screenshot_file,
            'basket_json_file': d.basket_json_file,
            'session_id': d.session_id,
            'source_url': d.source_url,
        })
    return {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'count': len(items),
        'items': items,
    }


def write_final_operator_outputs(deals: List[Deal], cfg: Dict[str, Any]) -> None:
    provider_rows = build_provider_scorecard(deals)
    evidence_index = build_evidence_index(deals)
    provider_json = RESULTS / 'provider_scorecard.json'
    provider_md = RESULTS / 'provider_scorecard.md'
    evidence_json = RESULTS / 'evidence_index.json'
    market_md = RESULTS / 'market_overview.md'
    provider_json.write_text(json.dumps(provider_rows, indent=2, ensure_ascii=False), encoding='utf-8')
    evidence_json.write_text(json.dumps(evidence_index, indent=2, ensure_ascii=False), encoding='utf-8')
    lines = ['# Provider scorecard', '', 'How each source performed in this run.', '']
    for row in provider_rows:
        lines += [f"- **{row['provider']}** — results {row['results']} · truth hits {row['truth_hits']} · avg confidence {int(row['avg_confidence']*100)}% · avg step depth {row['avg_step_depth']} · best price {('GBP ' + format(row['best_price_gbp'], ',.0f')) if row['best_price_gbp'] is not None else 'Unknown'}"]
    provider_md.write_text('\n'.join(lines), encoding='utf-8')
    ranked = sorted(query_deals(deals, cfg=cfg, sort_by='best'), key=lambda d: _rank_with_intelligence(d, cfg, 'best'))[:15]
    near = [d for d in query_deals(deals, cfg=cfg, sort_by='best') if d.fit_label in {'Near miss worth a look','Stretch option'}][:12]
    market = ['# Market overview', '', f"Generated: {datetime.now().isoformat(timespec='seconds')}", f"Brief: {cfg.get('overview_prompt','').strip()}", '', '## Best surfaced options', '']
    for i, d in enumerate(ranked, 1):
        market += [f"### {i}. {d.hotel_name or d.destination or 'Unnamed option'}", f"- Provider: {d.source_site}", f"- Ranked price: {('GBP ' + format(effective_price(d), ',.0f')) if effective_price(d) is not None else 'Unknown'}", f"- Why this is here: {d.shortlist_reason or d.recommendation or d.fit_summary or 'Strong option'}", f"- Truth signal: {d.pricing_truth_label or pricing_truth_label(d)} at {round((d.true_price_confidence or 0)*100)}% confidence", f"- Beach / heat / family: {d.beach_minutes if d.beach_minutes is not None else '?'} min beach · {d.estimated_temp_c if d.estimated_temp_c is not None else '?'}°C · {d.family_room_type or d.child_pricing_note or 'standard family fit'}", '']
    market += ['## Near misses worth a look', '']
    if near:
        for d in near:
            market.append(f"- **{d.hotel_name or d.destination or 'Unnamed option'}** ({d.source_site}) — {('GBP ' + format(effective_price(d), ',.0f')) if effective_price(d) is not None else 'Unknown'} — {d.near_miss_reason or d.shortlist_reason or 'Close to brief'}")
    else:
        market.append('- None surfaced in this run.')
    market_md.write_text('\n'.join(market), encoding='utf-8')

_prev_write_outputs_v15 = write_outputs

def write_outputs(deals: List[Deal], cfg: Optional[Dict[str, Any]] = None) -> None:
    cfg = cfg or load_config()
    _prev_write_outputs_v15(deals, cfg)
    write_final_operator_outputs(deals, cfg)


# ---- Final intelligence and operator pack layer v16 ----
import hashlib
from statistics import median


def _history_files(limit: int = 16):
    files = sorted(HISTORY.glob('deals_*.json'))
    return files[-limit:]


def deal_fingerprint(d: Deal) -> str:
    hotel = re.sub(r'[^a-z0-9]+', '', (d.hotel_name or d.destination or '').lower())[:48]
    dest = re.sub(r'[^a-z0-9]+', '', (d.destination or d.country or '').lower())[:32]
    board = re.sub(r'[^a-z0-9]+', '', (d.board_basis or '').lower())[:24]
    dep = (d.departure_date or '')[:7]
    nights = str(d.nights or '')
    rooms = str(d.rooms_matched or d.rooms_requested or '')
    return '|'.join([hotel, dest, dep, nights, board, rooms])


def _load_history_rows(limit: int = 16) -> list[Deal]:
    rows: list[Deal] = []
    for p in _history_files(limit):
        try:
            payload = json.loads(p.read_text(encoding='utf-8'))
            for row in payload:
                try:
                    rows.append(Deal(**row))
                except Exception:
                    pass
        except Exception:
            pass
    return rows


def build_historical_index(limit: int = 16) -> dict[str, Any]:
    history = _load_history_rows(limit)
    by_fp: dict[str, list[Deal]] = defaultdict(list)
    provider_stats: dict[str, dict[str, float]] = defaultdict(lambda: {'count':0.0,'truth_hits':0.0,'avg_conf_sum':0.0,'delta_sum':0.0,'delta_n':0.0,'family_hits':0.0})
    for d in history:
        fp = deal_fingerprint(d)
        by_fp[fp].append(d)
        site = d.source_site or 'Unknown'
        st = provider_stats[site]
        st['count'] += 1
        st['truth_hits'] += 1 if (getattr(d,'pricing_truth_label','') in {'Basket-like price','Pre-payment price'} or (getattr(d,'true_price_confidence',0) or 0) >= 0.88) else 0
        st['avg_conf_sum'] += float(getattr(d,'true_price_confidence',0) or 0)
        if getattr(d,'price_delta_gbp',None) is not None:
            st['delta_sum'] += float(d.price_delta_gbp or 0)
            st['delta_n'] += 1
        st['family_hits'] += 1 if (getattr(d,'free_child_place',False) or getattr(d,'family_room_type','')) else 0
    return {'by_fingerprint': by_fp, 'provider_stats': provider_stats, 'history_count': len(history)}


def _cheap_for_a_reason_warning(d: Deal) -> str:
    flags = []
    if (d.review_score or 0) and (d.review_score or 0) < 7.0:
        flags.append('lower review score')
    if d.pool is False:
        flags.append('no pool signal')
    if (d.beach_minutes or 0) and (d.beach_minutes or 0) > 12:
        flags.append('further from beach')
    if d.bags_included is False:
        flags.append('bags not clearly included')
    if d.transfers_included is False:
        flags.append('transfers not clearly included')
    if (d.true_price_confidence or 0) < 0.68:
        flags.append('weak price confidence')
    return ', '.join(flags[:4])


def apply_history_and_provider_intelligence(deals: list[Deal], cfg: dict[str, Any]) -> None:
    idx = build_historical_index(limit=16)
    by_fp = idx['by_fingerprint']
    provider_stats = idx['provider_stats']
    for d in deals:
        fp = deal_fingerprint(d)
        history_rows = by_fp.get(fp, [])
        prices = [effective_price(x) for x in history_rows if effective_price(x) is not None]
        current = effective_price(d)
        if prices and current is not None:
            lo = min(prices); hi = max(prices); med = median(prices)
            trend_band = 'historical low' if current <= lo else ('historically cheap' if current <= med * 0.97 else ('historically expensive' if current >= med * 1.04 else 'within usual range'))
            confidence = min(1.0, 0.35 + min(0.55, len(prices) * 0.05))
            if trend_band == 'historical low':
                d.likely_price_direction = 'buy now'
                d.likely_price_reason = f'At or below the lowest like-for-like price seen across {len(prices)} prior points.'
            elif trend_band == 'historically cheap':
                d.likely_price_direction = 'strong now'
                d.likely_price_reason = f'Below the median like-for-like price across {len(prices)} prior points.'
            elif trend_band == 'historically expensive':
                d.likely_price_direction = 'watch'
                d.likely_price_reason = f'Above the median like-for-like price; may improve if dates or provider depth change.'
            else:
                d.likely_price_direction = d.likely_price_direction or 'watch'
                d.likely_price_reason = d.likely_price_reason or f'Within the usual like-for-like range from {len(prices)} prior points.'
            d.notes = (d.notes + ' | ' if d.notes else '') + f'Trend: {trend_band}.'
        elif current is not None:
            d.likely_price_direction = d.likely_price_direction or 'watch'
            d.likely_price_reason = d.likely_price_reason or 'Needs more like-for-like history before calling the trend with confidence.'
        site = d.source_site or 'Unknown'
        st = provider_stats.get(site)
        base = float(PROVIDER_TRUST.get(site, 0.72) or 0.72)
        if st and st['count']:
            truth_rate = st['truth_hits'] / st['count']
            avg_conf = st['avg_conf_sum'] / st['count']
            family_rate = st['family_hits'] / st['count']
            evidence_boost = (truth_rate - 0.5) * 0.18 + (avg_conf - 0.5) * 0.14 + family_rate * 0.05
            d.provider_trust_score = max(0.4, min(0.99, round(base + evidence_boost, 3)))
        else:
            d.provider_trust_score = base
        warning = _cheap_for_a_reason_warning(d)
        if warning:
            existing = [x.strip() for x in (d.warning_flags or '').split('|') if x.strip()]
            for item in warning.split(', '):
                if item and item not in existing:
                    existing.append(item)
            d.warning_flags = ' | '.join(existing[:6])


def build_hotel_clusters(deals: list[Deal]) -> list[dict[str, Any]]:
    clusters: dict[str, list[Deal]] = defaultdict(list)
    for d in deals:
        key = re.sub(r'[^a-z0-9]+', '', (d.hotel_name or d.destination or '').lower())[:60] + '|' + re.sub(r'[^a-z0-9]+', '', (d.destination or '').lower())[:30]
        clusters[key].append(d)
    rows = []
    for key, items in clusters.items():
        if not items:
            continue
        prices = [effective_price(x) for x in items if effective_price(x) is not None]
        best = min(items, key=lambda x: effective_price(x) if effective_price(x) is not None else 9e9)
        providers = sorted({x.source_site for x in items if x.source_site})
        boards = sorted({x.board_basis for x in items if x.board_basis})
        rows.append({
            'cluster_key': key,
            'hotel_name': best.hotel_name or best.destination or 'Unnamed option',
            'destination': best.destination,
            'options': len(items),
            'providers': providers,
            'boards': boards,
            'best_price_gbp': min(prices) if prices else None,
            'worst_price_gbp': max(prices) if prices else None,
            'best_provider': best.source_site,
            'best_fit': best.fit_label or best.recommendation_bucket,
        })
    rows.sort(key=lambda r: (r['best_price_gbp'] if r['best_price_gbp'] is not None else 9e9, -r['options']))
    return rows


def build_assisted_resume_manifest(deals: list[Deal]) -> list[dict[str, Any]]:
    candidates = []
    for d in deals:
        if effective_price(d) is None:
            continue
        needs_help = (d.automation_status in {'blocked','partial','needs_help'} or (d.true_price_confidence or 0) < 0.82 or (d.checkout_stage or '') not in {'basket','pre-payment','price-breakdown','room-selection'})
        if not needs_help:
            continue
        score = (d.adjusted_buy_now_score or d.buy_now_score or 0) + (8 if d.fit_label == 'Exact fit' else 0) + (5 if d.fit_label == 'Near miss worth a look' else 0)
        candidates.append({
            'hotel_name': d.hotel_name or d.destination or 'Unnamed option',
            'provider': d.source_site,
            'destination': d.destination,
            'ranked_price_gbp': effective_price(d),
            'fit_label': d.fit_label or d.recommendation_bucket,
            'checkout_stage': d.checkout_stage or d.provider_state or 'landing',
            'confidence': round(float(d.true_price_confidence or 0), 3),
            'resume_hint': f"Resume on {d.source_site}: reopen saved session, continue from {d.checkout_stage or d.provider_state or 'latest visible state'}, then confirm room, baggage, transfers, and final payable total.",
            'source_url': d.source_url,
            'session_id': d.session_id,
            'score': round(score, 2),
        })
    candidates.sort(key=lambda r: (-r['score'], r['ranked_price_gbp']))
    return candidates[:20]


def write_operator_pack(deals: list[Deal], cfg: dict[str, Any]) -> None:
    ranked = query_deals(deals, cfg=cfg, sort_by='best')
    clusters = build_hotel_clusters(ranked)
    assisted = build_assisted_resume_manifest(ranked)
    history_idx = build_historical_index(limit=16)
    cluster_json = RESULTS / 'hotel_clusters.json'
    cluster_md = RESULTS / 'hotel_clusters.md'
    assisted_json = RESULTS / 'assisted_resume_manifest.json'
    assisted_md = RESULTS / 'assisted_resume_manifest.md'
    history_md = RESULTS / 'historical_pricing_report.md'
    briefing_md = RESULTS / 'operator_briefing_pack.md'
    briefing_html = RESULTS / 'operator_briefing_pack.html'
    cluster_json.write_text(json.dumps(clusters, indent=2, ensure_ascii=False), encoding='utf-8')
    assisted_json.write_text(json.dumps(assisted, indent=2, ensure_ascii=False), encoding='utf-8')

    lines = ['# Hotel clusters', '', 'Same-hotel and same-destination options grouped across providers and variants.', '']
    for row in clusters[:30]:
        spread = None
        if row['best_price_gbp'] is not None and row['worst_price_gbp'] is not None:
            spread = row['worst_price_gbp'] - row['best_price_gbp']
        lines += [f"## {row['hotel_name']}", f"- Destination: {row['destination']}", f"- Providers: {', '.join(row['providers']) or 'Unknown'}", f"- Board variants: {', '.join(row['boards']) or 'Unknown'}", f"- Best seen: {('GBP ' + format(row['best_price_gbp'], ',.0f')) if row['best_price_gbp'] is not None else 'Unknown'} via {row['best_provider'] or 'Unknown'}", f"- Spread: {('GBP ' + format(spread, ',.0f')) if spread is not None else 'Unknown'}", f"- Surfaced as: {row['best_fit'] or 'candidate'}", '']
    cluster_md.write_text('\n'.join(lines), encoding='utf-8')

    lines = ['# Assisted resume manifest', '', 'Promising routes that likely merit human-assisted continuation when a provider gets sticky.', '']
    if assisted:
        for row in assisted:
            lines += [f"## {row['hotel_name']}", f"- Provider: {row['provider']}", f"- Destination: {row['destination']}", f"- Ranked price: {('GBP ' + format(row['ranked_price_gbp'], ',.0f')) if row['ranked_price_gbp'] is not None else 'Unknown'}", f"- Fit: {row['fit_label']}", f"- Stage reached: {row['checkout_stage']}", f"- Confidence: {int(row['confidence']*100)}%", f"- Resume hint: {row['resume_hint']}", f"- URL: {row['source_url']}", '']
    else:
        lines.append('No assisted continuation candidates this run.')
    assisted_md.write_text('\n'.join(lines), encoding='utf-8')

    provider_rows = build_provider_scorecard(ranked)
    lines = ['# Historical pricing report', '', f"History points loaded: {history_idx['history_count']}", '', '## Provider evidence-weighted trust', '']
    for row in provider_rows:
        lines.append(f"- **{row['provider']}** — results {row['results']} · truth hits {row['truth_hits']} · avg confidence {int(row['avg_confidence']*100)}% · best price {('GBP ' + format(row['best_price_gbp'], ',.0f')) if row['best_price_gbp'] is not None else 'Unknown'}")
    lines += ['', '## Strongest current like-for-like value calls', '']
    for d in ranked[:20]:
        lines.append(f"- **{d.hotel_name or d.destination or 'Unnamed option'}** ({d.source_site}) — {('GBP ' + format(effective_price(d), ',.0f')) if effective_price(d) is not None else 'Unknown'} — {d.likely_price_direction or 'watch'} — {d.likely_price_reason or 'No trend reason yet.'}")
    history_md.write_text('\n'.join(lines), encoding='utf-8')

    top = ranked[:12]
    worth = [d for d in ranked if (d.recommendation_bucket or '') in {'worth-a-look','near-miss','stretch'}][:10]
    md = ['# Operator briefing pack', '', f"Generated: {datetime.now().isoformat(timespec='seconds')}", f"Brief: {cfg.get('overview_prompt','').strip()}", f"Profile: {package_profile_text(cfg)}", '']
    md += ['## Best options now', '']
    for i, d in enumerate(top, 1):
        md += [f"### {i}. {d.hotel_name or d.destination or 'Unnamed option'}", f"- Provider: {d.source_site}", f"- Ranked price: {('GBP ' + format(effective_price(d), ',.0f')) if effective_price(d) is not None else 'Unknown'}", f"- Fit: {d.fit_label or d.recommendation_bucket or 'Candidate'}", f"- Why it surfaced: {d.shortlist_reason or d.recommendation or d.fit_summary or 'Strong option'}", f"- Trend call: {(d.likely_price_direction or 'watch').title()} — {d.likely_price_reason or 'Needs more history.'}", f"- Caveats: {d.warning_flags or 'None visible'}", f"- Evidence: {d.evidence_note or 'See evidence index'}", f"- URL: {d.source_url}", '']
    md += ['## Worth a look', '']
    for d in worth:
        md.append(f"- **{d.hotel_name or d.destination or 'Unnamed option'}** — {('GBP ' + format(effective_price(d), ',.0f')) if effective_price(d) is not None else 'Unknown'} — {d.near_miss_reason or d.shortlist_reason or 'Close to brief'}")
    briefing_md.write_text('\n'.join(md), encoding='utf-8')
    html = ['<!doctype html><html><head><meta charset="utf-8"><title>AI Holiday Hunter briefing pack</title><style>body{font-family:Inter,Arial,sans-serif;background:#0f172a;color:#e2e8f0;padding:28px;line-height:1.45}h1,h2,h3{color:#f8fafc}section{background:#111827;border:1px solid #334155;border-radius:16px;padding:18px;margin:0 0 16px}a{color:#86efac}.muted{color:#94a3b8}.pill{display:inline-block;padding:4px 10px;border:1px solid #334155;border-radius:999px;margin-right:8px;color:#bbf7d0}</style></head><body>']
    html.append(f"<h1>AI Holiday Hunter briefing pack</h1><p class='muted'>Generated {datetime.now().isoformat(timespec='seconds')} · {cfg.get('overview_prompt','').strip()}</p>")
    for d in top:
        html.append('<section>')
        html.append(f"<div><span class='pill'>{d.fit_label or d.recommendation_bucket or 'Candidate'}</span><span class='pill'>{d.pricing_truth_label or pricing_truth_label(d)}</span><span class='pill'>{d.source_site}</span></div>")
        html.append(f"<h2>{d.hotel_name or d.destination or 'Unnamed option'}</h2>")
        html.append(f"<p><strong>Ranked price:</strong> {('GBP ' + format(effective_price(d), ',.0f')) if effective_price(d) is not None else 'Unknown'}<br><strong>Why surfaced:</strong> {d.shortlist_reason or d.recommendation or d.fit_summary or 'Strong option'}<br><strong>Trend call:</strong> {(d.likely_price_direction or 'watch').title()} — {d.likely_price_reason or 'Needs more history.'}<br><strong>Caveats:</strong> {d.warning_flags or 'None visible'}<br><strong>URL:</strong> <a href='{d.source_url}'>{d.source_url}</a></p>")
        html.append('</section>')
    html.append('</body></html>')
    briefing_html.write_text(''.join(html), encoding='utf-8')


_prev_query_deals_v16 = query_deals

def query_deals(
    deals: List[Deal],
    cfg: Optional[Dict[str, Any]] = None,
    query: str = "",
    max_price: Optional[float] = None,
    min_temp: Optional[float] = None,
    beach_max_minutes: Optional[int] = None,
    require_pool: bool = False,
    breakfast_or_better: bool = False,
    alerts_only: bool = False,
    free_child_only: bool = False,
    family_room_only: bool = False,
    source_site: str = "",
    sort_by: str = "best",
) -> List[Deal]:
    cfg = cfg or load_config()
    try:
        apply_history_and_provider_intelligence(deals, cfg)
    except Exception:
        pass
    rows = _prev_query_deals_v16(deals, cfg, query, max_price, min_temp, beach_max_minutes, require_pool, breakfast_or_better, alerts_only, free_child_only, family_room_only, source_site, sort_by)
    # strengthen same-option grouping and sort stability
    for d in rows:
        if not d.option_id:
            d.option_id = deal_fingerprint(d)
    return rows


_prev_write_outputs_v16 = write_outputs

def write_outputs(deals: List[Deal], cfg: Optional[Dict[str, Any]] = None) -> None:
    cfg = cfg or load_config()
    try:
        apply_history_and_provider_intelligence(deals, cfg)
    except Exception:
        pass
    _prev_write_outputs_v16(deals, cfg)
    write_operator_pack(deals, cfg)
