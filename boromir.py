"""
Boromir v2 — Daily streaming intelligence brief for MovieWeb.

Fetches FlixPatrol rankings, scores momentum, enriches with TMDB,
calls Claude to select 6 picks and write headlines, posts to Slack.

Required env vars:
    FLIXPATROL_API_KEY
    TMDB_API_KEY
    ANTHROPIC_API_KEY
    SLACK_WEBHOOK_URL
    GOOGLE_SERVICE_ACCOUNT_JSON
    PERF_SHEET_ID
"""

import json
import logging
import os
import re
import time
from collections import defaultdict
from datetime import date, datetime, timedelta

import anthropic
import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

FLIXPATROL_BASE = "https://api.flixpatrol.com/v2"
TMDB_BASE       = "https://api.themoviedb.org/3"
US_COUNTRY      = "cnt_iMUHNbZvnNHK5YdhgwtOoP4u"

GLOBAL_RANK_FLOOR    = 120
NEW_ENTRY_RANK_FLOOR = 100
TITLES_TO_ENRICH     = 30
PICKS_TARGET         = 6
SUPPRESS_DAYS        = 2

PLATFORM_IDS = {
    "cmp_IA6TdMqwf6kuyQvxo9bJ4nKX": "Netflix",
    "cmp_6UhCvnTeRkgZUtcNGslX9bJL": "HBO Max",
    "cmp_qypvowjqFhEIpCc0HlQ6VoYk": "Amazon Prime",
    "cmp_riMmDaNhomIc4J2dWGQPKbkZ": "Paramount+",
    "cmp_VvmYc7OphiUds0Hgjbz5MESn": "Apple TV",
    "cmp_9iwHIMYOCvD6zprSPoHgTJau": "Hulu",
    "cmp_oGtsgdpOrjIu3XzTEnWPt87Y": "Disney+",
    "cmp_bpBPGTvopBHPVtIKhR2CF68W": "Tubi",
    "cmp_qyGnowjqFhEIpCc0HlQ6VoYk": "Pluto",
}

TIER_MAP = {
    "Netflix": 1, "Amazon Prime": 1, "Disney+": 1, "HBO Max": 1,
    "Tubi": 2, "Pluto": 2,
    "Apple TV": 3, "Paramount+": 3, "Hulu": 3,
}
TIER_WEIGHTS = {1: 3.0, 2: 2.5, 3: 2.0, 4: 1.0, 5: 0.5}

NICHE_NEWS_CUTOFF       = date(2025, 11, 3)
SESSIONS_HEAVY          = 25_000
SESSIONS_FAILURE        = 500
HEADLINE_TIER1_MIN      = 3
HEADLINE_WEIGHTED_MIN   = 3.0
PUBLISHED_SUPPRESS_DAYS = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
)
log = logging.getLogger("boromir")


# ─────────────────────────────────────────────────────────────────────────────
# HTTP
# ─────────────────────────────────────────────────────────────────────────────

def _build_session(auth=None):
    session = requests.Session()
    if auth:
        session.auth = auth
    retry = Retry(
        total=4, backoff_factor=3,
        status_forcelist={500, 502, 503, 504},
        allowed_methods={"GET"}, raise_on_status=False,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _qs(params):
    return "&".join(f"{k}={v}" for k, v in params.items())


def fp_get(session, endpoint, params=None, max_pages=20):
    url   = f"{FLIXPATROL_BASE}/{endpoint}"
    qs    = _qs(params or {})
    url   = f"{url}?{qs}" if qs else url
    items, page = [], 0

    while url and page < max_pages:
        last_exc = None
        for attempt in range(4):
            try:
                resp = session.get(url, timeout=120)
                last_exc = None
                break
            except requests.exceptions.Timeout as e:
                last_exc = e
                time.sleep(15 * (attempt + 1))
        if last_exc:
            raise RuntimeError(f"/{endpoint} timed out after 4 attempts")

        if resp.status_code == 401:
            raise PermissionError("FlixPatrol: invalid API key")
        if resp.status_code == 429:
            log.warning("Rate limited on /%s — waiting 60s", endpoint)
            time.sleep(60)
            resp = session.get(url, timeout=120)
        if resp.status_code == 404:
            log.warning("404 on /%s", endpoint)
            return []
        resp.raise_for_status()

        body = resp.json()
        items.extend(body.get("data", []))
        page += 1
        url = body.get("links", {}).get("next")

    log.info("  /%s  %d items", endpoint, len(items))
    return items


# ─────────────────────────────────────────────────────────────────────────────
# FLIXPATROL
# ─────────────────────────────────────────────────────────────────────────────

def _inner(field):
    if isinstance(field, dict):
        return field.get("data") or {}
    return {}


def fetch_flixpatrol(target_date):
    session = _build_session(auth=(os.environ["FLIXPATROL_API_KEY"], ""))
    log.info("Fetching FlixPatrol for %s", target_date)

    # Rankings
    rankings = {}
    for item in fp_get(session, "rankings", {
        "date[type][eq]": 1,
        "date[from][eq]": target_date,
        "date[to][eq]":   target_date,
        "audience[eq]":   1,
    }):
        d     = item.get("data", item)
        inner = _inner(d.get("movie", {}))
        tid   = inner.get("id")
        if not tid:
            continue
        rankings[tid] = {
            "title":        inner.get("title"),
            "imdb_id":      inner.get("imdbId"),
            "tmdb_id":      inner.get("tmdbId"),
            "global_rank":  d.get("ranking"),
            "rank_last":    d.get("rankingLast"),
            "value_change": d.get("valueChange"),
            "days_streak":  d.get("days") or 0,
            "days_total":   d.get("daysTotal") or 0,
        }

    # Top 10s
    SVOD_STANDARD = {k: v for k, v in PLATFORM_IDS.items()
                     if v in ("Netflix", "HBO Max", "Amazon Prime", "Paramount+", "Apple TV", "Hulu")}
    SVOD_OVERALL  = {k: v for k, v in PLATFORM_IDS.items() if v in ("Disney+", "Tubi")}
    PLUTO_ID      = "cmp_qyGnowjqFhEIpCc0HlQ6VoYk"

    top10s = {}

    def _process(items, type_map, platform_map):
        for item in items:
            d     = item.get("data", item)
            inner = _inner(d.get("movie", {}))
            tid   = inner.get("id")
            if not tid:
                continue
            ct = type_map.get(d.get("type"))
            if not ct:
                continue
            platform = platform_map.get(_inner(d.get("company", {})).get("id", ""))
            if not platform:
                continue
            if tid not in top10s:
                top10s[tid] = {
                    "title":   inner.get("title"),
                    "imdb_id": inner.get("imdbId"),
                    "tmdb_id": inner.get("tmdbId"),
                    "top10":   {},
                }
            top10s[tid]["top10"].setdefault(platform, {})[ct] = {
                "ranking":      d.get("ranking"),
                "ranking_last": d.get("rankingLast"),
                "days_total":   d.get("daysTotal"),
            }

    _process(fp_get(session, "top10s", {
        "date[type][eq]": 1, "date[from][eq]": target_date, "date[to][eq]": target_date,
        "company[in]": ",".join(SVOD_STANDARD), "type[in]": "2,3", "country[in]": US_COUNTRY,
    }), {2: "movies", 3: "tvshows"}, SVOD_STANDARD)

    _process(fp_get(session, "top10s", {
        "date[type][eq]": 1, "date[from][eq]": target_date, "date[to][eq]": target_date,
        "company[in]": ",".join(SVOD_OVERALL), "type[in]": "1", "country[in]": US_COUNTRY,
    }), {1: "overall"}, SVOD_OVERALL)

    _process(fp_get(session, "top10s", {
        "date[type][eq]": 1, "date[from][eq]": target_date, "date[to][eq]": target_date,
        "company[in]": PLUTO_ID, "type[in]": "2",
    }), {2: "movies"}, {PLUTO_ID: "Pluto"})

    # Merge
    titles = []
    for tid in set(rankings) | set(top10s):
        r        = rankings.get(tid, {})
        t        = top10s.get(tid, {})
        top10    = t.get("top10", {})
        all_cts  = {ct for types in top10.values() for ct in types}
        if "tvshows" in all_cts:
            media_type = "tv"
        elif "movies" in all_cts:
            media_type = "movie"
        else:
            media_type = "unknown"

        titles.append({
            "flixpatrol_id": tid,
            "title":         r.get("title") or t.get("title") or tid,
            "imdb_id":       r.get("imdb_id") or t.get("imdb_id"),
            "tmdb_id":       r.get("tmdb_id") or t.get("tmdb_id"),
            "media_type":    media_type,
            "global_rank":   r.get("global_rank"),
            "rank_last":     r.get("rank_last"),
            "value_change":  r.get("value_change"),
            "days_streak":   r.get("days_streak", 0),
            "days_total":    r.get("days_total", 0),
            "top10":         top10,
        })

    log.info("FlixPatrol: %d titles merged", len(titles))
    return titles


# ─────────────────────────────────────────────────────────────────────────────
# MOMENTUM SCORING  (Scout logic)
# ─────────────────────────────────────────────────────────────────────────────

def _parse_vc(vc):
    if not vc or str(vc).strip() in ("–", "-", ""):
        return None
    try:
        cleaned = (
            str(vc)
            .replace("\xa0%", "").replace(" %", "").replace("%", "")
            .replace("–", "-").replace("−", "-")
            .strip()
        )
        return float(cleaned)
    except ValueError:
        return None


def score_momentum(title):
    global_rank = title.get("global_rank")
    if not global_rank or global_rank > GLOBAL_RANK_FLOOR:
        return 0.0

    score       = 0.0
    vc          = _parse_vc(title.get("value_change"))
    days_streak = title.get("days_streak", 0)

    if vc is not None:
        if   vc > 100:  score += 3
        elif vc > 50:   score += 2
        elif vc > 10:   score += 1
        elif vc < -50:  score -= 2
        elif vc < -10:  score -= 1

    for platform, types in title.get("top10", {}).items():
        tier   = TIER_MAP.get(platform, 5)
        weight = TIER_WEIGHTS.get(tier, 0.5)

        for ct, data in types.items():
            rank = data.get("ranking")
            if rank is None:
                continue

            rank_score = 3 if rank <= 2 else 2 if rank <= 5 else 1 if rank <= 10 else 0
            if rank_score == 0 and tier >= 3 and global_rank > 50:
                continue

            rank_last = data.get("ranking_last") or 0
            if not rank_last:
                direction = "new"
            elif rank < rank_last:
                direction = "rising"
            elif rank > rank_last:
                direction = "falling"
            else:
                direction = "flat"

            new_entry = 0
            if days_streak <= 3 and global_rank <= NEW_ENTRY_RANK_FLOOR:
                if tier == 1:
                    new_entry = 3 if rank <= 5 else 1
                elif tier == 3:
                    new_entry = 2 if rank <= 5 else 1

            fall_penalty = -2 if direction == "falling" and vc and vc < -10 else 0
            days_on      = data.get("days_total") or 0
            flat_penalty = -1 if (direction == "flat" and days_on >= 10
                                  and (vc is None or abs(vc) <= 10)) else 0

            score += (rank_score + new_entry + fall_penalty + flat_penalty) * weight

    return round(score, 2)


# ─────────────────────────────────────────────────────────────────────────────
# TMDB
# ─────────────────────────────────────────────────────────────────────────────

def fetch_tmdb(session, title):
    tid  = title.get("tmdb_id")
    mt   = title.get("media_type", "unknown")
    name = title.get("title", "")
    raw  = None

    if tid and mt in ("movie", "tv"):
        try:
            r = session.get(
                f"{TMDB_BASE}/{mt}/{tid}",
                params={"api_key": os.environ["TMDB_API_KEY"], "append_to_response": "credits"},
                timeout=15,
            )
            if r.status_code == 200:
                raw = r.json()
                raw["_mt"] = mt
        except Exception as e:
            log.warning("TMDB direct fetch failed for %s: %s", name, e)

    if not raw:
        search = name.split(" | ")[0].strip()
        try:
            r = session.get(
                f"{TMDB_BASE}/search/multi",
                params={"api_key": os.environ["TMDB_API_KEY"], "query": search},
                timeout=15,
            )
            if r.status_code == 200:
                results = r.json().get("results", [])
                if results:
                    raw = results[0]
                    raw["_mt"] = raw.get("media_type", "unknown")
        except Exception as e:
            log.warning("TMDB search failed for %s: %s", name, e)

    if not raw:
        return {}

    mt       = raw.get("_mt", "unknown")
    credits  = raw.get("credits", {})
    cast     = [p["name"] for p in credits.get("cast", [])[:8]]
    directors = [p["name"] for p in credits.get("crew", []) if p.get("job") == "Director"]
    creators  = [p.get("name", "") for p in raw.get("created_by", [])]
    genres   = [g["name"] for g in raw.get("genres", [])]
    release  = raw.get("release_date") or raw.get("first_air_date") or ""
    origins  = raw.get("origin_country") or [
        c["iso_3166_1"] for c in raw.get("production_countries", [])
    ]
    collection = (raw.get("belongs_to_collection") or {}).get("name") if mt == "movie" else None

    return {
        "media_type":     mt,
        "genres":         genres,
        "cast":           cast,
        "directors":      directors,
        "creators":       creators,
        "collection":     collection,
        "release_year":   int(release[:4]) if len(release) >= 4 else None,
        "origin_country": origins,
        "overview":       (raw.get("overview") or "")[:300],
        "vote_average":   raw.get("vote_average"),
        "networks":       [n["name"] for n in raw.get("networks", [])],
        "tagline":        raw.get("tagline"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# GOOGLE SHEETS
# ─────────────────────────────────────────────────────────────────────────────

def _sheets_service():
    sa_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    creds   = Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _read_tab(svc, sheet_id, tab):
    result = svc.spreadsheets().values().get(
        spreadsheetId=sheet_id, range=tab,
    ).execute()
    rows = result.get("values", [])
    if len(rows) < 2:
        return []
    headers = rows[0]
    return [
        dict(zip(headers, row + [""] * max(0, len(headers) - len(row))))
        for row in rows[1:]
    ]


def _append_rows(svc, sheet_id, tab, rows):
    svc.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range=f"{tab}!A:Z",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()


def _ensure_suggestions_tab(svc, sheet_id):
    meta = svc.spreadsheets().get(spreadsheetId=sheet_id).execute()
    tabs = [s["properties"]["title"] for s in meta.get("sheets", [])]
    if "Suggestions" not in tabs:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": "Suggestions"}}}]},
        ).execute()
        _append_rows(svc, sheet_id, "Suggestions", [[
            "date_suggested", "title", "flixpatrol_id",
            "media_type", "hook_type", "hook_value",
        ]])
        log.info("Created Suggestions tab")


def _parse_date(s):
    if not s:
        return None
    s = s.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y",
                "%d/%m/%Y", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None


_STOP_WORDS = {
    "the","a","an","and","or","but","in","on","at","to","for","of","with","is","are","was",
    "were","has","have","had","its","it","this","that","from","by","as","we","us","our",
    "they","them","their","he","she","his","her","you","your","who","what","where","when",
    "why","how","everything","something","nothing","anything","everyone","know","about",
    "here","there","way","get","got","did","just","only","also","already","finally","still",
    "even","never","every","more","less","next","back","up","out","now","new","best","most",
    "first","last","big","top","old","young","real","true","full","long","short","dark",
    "good","great","perfect","original","classic","iconic","epic","huge","wild","latest",
    "biggest","worst","better","worse","hard","strong","right","wrong","free","live","lost",
    "years","later","time","days","months","weeks","year","season","episode","series",
    "sequel","prequel","reboot","movie","film","show","tv","watch","review","trailer",
    "release","streaming","score","rating","run","box","office","cast","crew","role","star",
    "stars","actor","actress","director","writer","producer","character","story","plot",
    "netflix","amazon","disney","hbo","apple","hulu","paramount","prime","max","tubi",
    "peacock","pluto",
}
_NAME_RE = re.compile(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\b")


def load_perf_data(svc):
    sheet_id = os.environ["PERF_SHEET_ID"]
    rows     = _read_tab(svc, sheet_id, "DB")
    log.info("Loaded %d performance rows from DB tab", len(rows))

    if rows:
        log.info("DB first PubDate value: %r", rows[0].get("PubDate", "NOT FOUND"))

    tag_perf        = defaultdict(lambda: {
        "tier1_count": 0, "tier1_heavy": 0, "sessions_t1": 0,
        "weighted_score": 0.0, "failure_count": 0, "sample_titles": [],
    })
    headline_talent = defaultdict(lambda: {
        "tier1_count": 0, "weighted_score": 0.0,
        "heavy_count": 0, "is_signal": False,
    })
    published_suppress = set()
    today = date.today()

    for row in rows:
        ct       = row.get("ContentType", "").strip()
        net_cat  = row.get("NetCat", "").strip()
        pub_date = _parse_date(row.get("PubDate", ""))
        if not pub_date:
            continue

        if pub_date >= NICHE_NEWS_CUTOFF and ct == "Niche News":
            tier, weight = 1, 1.0
        elif pub_date < NICHE_NEWS_CUTOFF and ct == "Mini Feature" and net_cat in ("TV News", "Movie News"):
            tier, weight = 1, 1.0
        elif ct in ("Mini Feature", "Feature"):
            tier, weight = 2, 0.5
        else:
            tier, weight = 3, 0.2

        try:
            sessions = int(str(row.get("ActSess", 0)).replace(",", "").strip())
        except Exception:
            sessions = 0

        is_heavy   = sessions >= SESSIONS_HEAVY
        is_failure = 0 < sessions < SESSIONS_FAILURE
        title_text = row.get("ArticleTitle", "").strip()
        smo_text   = row.get("smoarticletitle", "").strip()

        tags = set()
        pri  = row.get("PriTag", "").strip()
        if pri:
            tags.add(pri.lower())
        for t in row.get("Tags", "").split("|"):
            t = t.strip()
            if t:
                tags.add(t.lower())

        for tag in tags:
            d = tag_perf[tag]
            d["weighted_score"] += weight
            if is_failure:
                d["failure_count"] += 1
            if tier == 1:
                d["tier1_count"] += 1
                d["sessions_t1"]  += sessions
                if is_heavy:
                    d["tier1_heavy"] += 1
                    if len(d["sample_titles"]) < 3:
                        d["sample_titles"].append(title_text)

            if ct in ("Niche News", "Mini Feature") and (today - pub_date).days <= PUBLISHED_SUPPRESS_DAYS:
                published_suppress.add(tag)

        for text in (title_text, smo_text):
            seen = set()
            for match in _NAME_RE.findall(text):
                if any(w.lower() in _STOP_WORDS for w in match.split()):
                    continue
                norm = match.strip().lower()
                if norm in seen:
                    continue
                seen.add(norm)
                d = headline_talent[norm]
                d["weighted_score"] += weight
                if tier == 1:
                    d["tier1_count"] += 1
                    if is_heavy:
                        d["heavy_count"] += 1

    for d in headline_talent.values():
        d["is_signal"] = (
            d["tier1_count"] >= HEADLINE_TIER1_MIN
            or d["weighted_score"] >= HEADLINE_WEIGHTED_MIN
        )

    log.info(
        "Perf: %d tags | %d talent signals | %d recently published (suppressed)",
        len(tag_perf),
        sum(1 for d in headline_talent.values() if d["is_signal"]),
        len(published_suppress),
    )
    return dict(tag_perf), dict(headline_talent), published_suppress


def load_suggestions(svc):
    sheet_id = os.environ["PERF_SHEET_ID"]
    _ensure_suggestions_tab(svc, sheet_id)
    try:
        rows = _read_tab(svc, sheet_id, "Suggestions")
    except Exception as e:
        log.warning("Could not load suggestions: %s", e)
        return {}
    cutoff = date.today() - timedelta(days=SUPPRESS_DAYS)
    recent = {}
    for row in rows:
        d   = _parse_date(row.get("date_suggested", ""))
        fid = row.get("flixpatrol_id", "")
        if d and d >= cutoff and fid:
            recent[fid] = row.get("title", fid)
    log.info("Recent suggestions (last %d days): %d titles", SUPPRESS_DAYS, len(recent))
    return recent


def log_suggestions(svc, picks, today_str):
    sheet_id = os.environ["PERF_SHEET_ID"]
    rows = [
        [
            today_str,
            p["title"],
            p["flixpatrol_id"],
            p.get("media_type", ""),
            p.get("hook_type", ""),
            p.get("hook_value", ""),
        ]
        for p in picks
    ]
    _append_rows(svc, sheet_id, "Suggestions", rows)
    log.info("Logged %d picks to Suggestions tab", len(rows))


# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE
# ─────────────────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are an editorial assistant for MovieWeb, a mainstream movie and TV entertainment site.

Your task: review today's trending streaming titles and select exactly 6 for our daily Slack brief.

SELECTION RULES:
- Hook priority: talent > franchise/collection > nostalgia (10+ years old) > foreign language > streaming event > chart position alone
- Hard exclude: Reality TV, Talk shows, Game shows, Soap operas — check TMDB genres carefully
- At least 2 of the 6 picks must be TV shows
- Skip titles in recent_suggestions unless their momentum_score is exceptional (above 12)
- Prefer titles where mw_performance shows proven history (tier1_heavy > 0 or strong talent_signals)
- A "heavy hitter" means a past article on this title/talent drove 25,000+ sessions on MovieWeb

HEADLINE RULES:
- site_headline: under 75 characters. Standalone. Frame the story around the hook — do not name the streaming title directly in the headline.
- smo_title: 85–105 characters, AP Title Case. Add one concrete detail (chart position, years since release, RT score, franchise entry).
- No vague superlatives. No question headlines.
- Geographic framing is strong: "In America" or "Biggest Movie in America Right Now" when US top 3 on a major platform.

Good headline examples by hook type:
- talent:    "The Better Call Saul Star's Most Brutal Role Is Taking Over Netflix"
- nostalgia: "The 1999 Action Classic That's Quietly Dominating Amazon Prime Right Now"
- foreign:   "The Korean Thriller Beating Every English-Language Show on Netflix"
- franchise: "The MonsterVerse Entry With the Highest RT Score Is Back on Top of HBO Max"
- chart:     "The Biggest Movie in America Right Now Isn't What You'd Expect"

Respond with valid JSON only. No markdown, no explanation outside the JSON."""


def ask_claude(payload, recent_suggestions):
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    recent_str = (
        json.dumps(recent_suggestions, ensure_ascii=False)
        if recent_suggestions else "none"
    )

    user_msg = f"""Today's trending titles, scored by momentum and platform strength:

{json.dumps(payload, ensure_ascii=False, indent=2)}

Titles suggested in the last {SUPPRESS_DAYS} days — skip unless exceptional:
{recent_str}

Select exactly {PICKS_TARGET} picks. Return this JSON:
{{
  "picks": [
    {{
      "flixpatrol_id": "...",
      "title": "...",
      "hook_type": "talent|franchise|nostalgia|foreign_language|streaming_event|chart",
      "hook_value": "the specific person, franchise, year, country, or platform+rank",
      "site_headline": "...",
      "smo_title": "...",
      "angle": "one sentence editorial angle for the article body"
    }}
  ]
}}"""

    log.info("Calling Claude (claude-sonnet-4-6)...")
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
    )

    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

    # Extract outermost JSON object in case Claude adds surrounding text
    start = raw.find("{")
    end   = raw.rfind("}")
    if start != -1 and end > start:
        raw = raw[start:end + 1]

    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        log.error("Claude returned invalid JSON: %s\n%s", e, raw[:500])
        raise


# ─────────────────────────────────────────────────────────────────────────────
# SLACK
# ─────────────────────────────────────────────────────────────────────────────

def build_message(pick, index, total):
    name      = pick["title"]
    mt        = pick.get("media_type", "")
    hook_type = pick.get("hook_type", "")
    hook_val  = pick.get("hook_value", "")
    headline  = pick.get("site_headline", "")
    smo       = pick.get("smo_title", "")
    angle     = pick.get("angle", "")
    type_lbl  = "TV" if mt == "tv" else "Movie" if mt == "movie" else ""

    # Best platform — lowest tier + lowest rank number
    best_platform = ""
    best_ranking  = ""
    best_key      = (99, 99)
    for platform, types in pick.get("_top10", {}).items():
        tier = TIER_MAP.get(platform, 5)
        if tier > 3:
            continue
        for ct, data in types.items():
            rank = data.get("ranking") or 99
            if (tier, rank) < best_key:
                best_key      = (tier, rank)
                rank_last     = data.get("ranking_last")
                if not rank_last:
                    movement  = "new entry"
                elif rank < rank_last:
                    movement  = f"up from #{rank_last}"
                elif rank > rank_last:
                    movement  = f"down from #{rank_last}"
                else:
                    movement  = "unchanged"
                best_platform = platform
                best_ranking  = f"#{rank} ({movement})"

    if not best_platform:
        best_platform = "streaming"
        best_ranking  = "trending"

    # Trend notes
    trend_parts = []
    vc_num = _parse_vc(pick.get("_value_change"))
    if vc_num is not None:
        direction = "Up" if vc_num > 0 else "Down"
        trend_parts.append(f"{direction} {abs(vc_num):.0f}% viewer activity")
    elif "new entry" in best_ranking:
        trend_parts.append("New entry")
    streak = pick.get("_days_streak", 0)
    if streak and streak > 1:
        trend_parts.append(f"{streak}-day streak")
    global_rank = pick.get("_global_rank")
    if global_rank:
        trend_parts.append(f"Global rank {global_rank}")
    trend_str = " | ".join(trend_parts) if trend_parts else "—"

    title_line = f"*{index}/{total}: {name}*" + (f" ({type_lbl})" if type_lbl else "")

    out = [
        title_line,
        f"Platform: {best_platform}",
        f"Ranking: {best_ranking}",
        f"Trend notes: {trend_str}",
        "",
        f"Suggested hook: {hook_type} — {hook_val}",
        "",
        f"Headline: {headline}",
    ]
    if smo and smo != headline:
        out.append(f"SMO: {smo}")
    if angle:
        out += ["", f"_{angle}_"]
    out += ["", "—" * 52]

    return {"text": "\n".join(out), "unfurl_links": False, "unfurl_media": False}


def post_slack(payload):
    resp = requests.post(os.environ["SLACK_WEBHOOK_URL"], json=payload, timeout=15)
    ok   = resp.status_code == 200 and resp.text == "ok"
    if not ok:
        log.warning("Slack returned %d: %s", resp.status_code, resp.text[:100])
    return ok


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def run():
    today     = date.today()
    yesterday = (today - timedelta(days=1)).isoformat()

    log.info("=" * 52)
    log.info("  BOROMIR v2  |  %s", today.isoformat())
    log.info("=" * 52)

    # 1. FlixPatrol
    all_titles = fetch_flixpatrol(yesterday)

    # 2. Score momentum, take top N
    for t in all_titles:
        t["momentum_score"] = score_momentum(t)
    all_titles.sort(key=lambda x: x["momentum_score"], reverse=True)
    candidates = [t for t in all_titles if t["momentum_score"] > 0][:TITLES_TO_ENRICH]
    log.info("Scored %d titles, enriching top %d", len(all_titles), len(candidates))

    # 3. TMDB enrichment
    http = _build_session()
    for t in candidates:
        tmdb = fetch_tmdb(http, t)
        t["tmdb"] = tmdb
        if tmdb.get("media_type") and tmdb["media_type"] != "unknown":
            t["media_type"] = tmdb["media_type"]
        time.sleep(0.2)
    log.info("TMDB enrichment done")

    # 4. MW performance data + suggestion history
    svc                                          = _sheets_service()
    tag_perf, headline_talent, published_suppress = load_perf_data(svc)
    recent_suggestions                            = load_suggestions(svc)

    # 5. Build Claude payload — compact, one entry per candidate
    def mw_signals(t):
        sig        = {}
        norm_title = t["title"].strip().lower()
        ts         = tag_perf.get(norm_title, {})
        if ts.get("tier1_heavy", 0) > 0:
            sig["title_heavy_hitters"] = ts["tier1_heavy"]
        if ts.get("tier1_count", 0) > 0:
            sig["title_tier1_articles"] = ts["tier1_count"]

        talent_hits = []
        all_talent  = (
            t.get("tmdb", {}).get("cast", [])[:5]
            + t.get("tmdb", {}).get("directors", [])
            + t.get("tmdb", {}).get("creators", [])
        )
        for person in all_talent:
            ht = headline_talent.get(person.strip().lower(), {})
            if ht.get("is_signal"):
                talent_hits.append({
                    "name":   person,
                    "tier1":  ht["tier1_count"],
                    "heavy":  ht["heavy_count"],
                })
        if talent_hits:
            sig["talent_signals"] = talent_hits

        tags_to_check = {norm_title} | {
            p.strip().lower()
            for p in t.get("tmdb", {}).get("cast", [])[:3]
            + t.get("tmdb", {}).get("directors", [])
        }
        if tags_to_check & published_suppress:
            sig["recently_published"] = True

        return sig

    payload = []
    for t in candidates:
        tmdb = t.get("tmdb", {})
        payload.append({
            "flixpatrol_id":  t["flixpatrol_id"],
            "title":          t["title"],
            "media_type":     t.get("media_type", "unknown"),
            "momentum_score": t["momentum_score"],
            "global_rank":    t.get("global_rank"),
            "value_change":   t.get("value_change"),
            "days_streak":    t.get("days_streak", 0),
            "platforms": [
                {
                    "platform":     p,
                    "tier":         TIER_MAP.get(p, 5),
                    "content_type": ct,
                    "rank":         data.get("ranking"),
                    "rank_last":    data.get("ranking_last"),
                }
                for p, types in t.get("top10", {}).items()
                for ct, data in types.items()
            ],
            "tmdb": {
                "genres":         tmdb.get("genres", []),
                "cast":           tmdb.get("cast", [])[:6],
                "directors":      tmdb.get("directors", []),
                "creators":       tmdb.get("creators", []),
                "collection":     tmdb.get("collection"),
                "release_year":   tmdb.get("release_year"),
                "origin_country": tmdb.get("origin_country", []),
                "vote_average":   tmdb.get("vote_average"),
                "overview":       tmdb.get("overview", "")[:200],
            },
            "mw_performance": mw_signals(t),
        })

    # 6. Claude picks + writes headlines
    result = ask_claude(payload, recent_suggestions)
    picks  = result.get("picks", [])
    log.info("Claude selected %d picks", len(picks))

    # Merge top10 and trend stats back into picks for Slack formatting
    title_map = {t["flixpatrol_id"]: t for t in candidates}
    for pick in picks:
        source               = title_map.get(pick["flixpatrol_id"], {})
        pick["_top10"]       = source.get("top10", {})
        pick["_global_rank"] = source.get("global_rank")
        pick["_value_change"] = source.get("value_change")
        pick["_days_streak"] = source.get("days_streak", 0)
        if not pick.get("media_type"):
            pick["media_type"] = source.get("media_type", "")

    # 7. Post to Slack
    post_slack({
        "text": f"*Boromir's Picks — {today.strftime('%A, %B %-d')}*  _{len(picks)} titles_"
    })
    time.sleep(1)

    posted = []
    for i, pick in enumerate(picks, 1):
        msg = build_message(pick, i, len(picks))
        if post_slack(msg):
            log.info("  posted %d/%d: %s", i, len(picks), pick["title"])
            posted.append(pick)
        else:
            log.error("  FAILED %d/%d: %s", i, len(picks), pick["title"])
        time.sleep(1)

    log.info("─" * 52)
    log.info("Posted %d/%d picks", len(posted), len(picks))

    # 8. Log to Suggestions tab
    if posted:
        log_suggestions(svc, posted, today.isoformat())


if __name__ == "__main__":
    run()
