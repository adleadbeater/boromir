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

Optional (enables feedback loop):
    SLACK_BOT_TOKEN   — xoxb-... Bot User OAuth Token (reactions:read, channels/groups history)
    SLACK_CHANNEL     — channel name or ID Boromir posts to (e.g. agent-boromir)
"""

import json
import logging
import os
import re
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

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
PLATFORM_CAP         = 2   # max picks from the same platform; enforced in code post-Claude
SUPPRESS_DAYS        = 2
USAGE_TAB            = "Usage"
TOPTAGS_TAB          = "TopTitles"
TOP_TAGS_COUNT       = 15
TOP_TAGS_LOOKBACK_DAYS = 365
TRUE_CRIME_DOC_CAP   = 1   # max true-crime/documentary TV picks; enforced in code post-Claude

# PriTag/Tags in the DB tab mix specific titles with platform names, genres, and
# talent names — this blocklist excludes the non-title values so compute_top_tags
# ranks actual movies/shows, not genre buzzwords. Not exhaustive; extend as new
# non-title values show up in the ranked output.
NON_TITLE_TAGS = {
    "action", "thriller", "horror", "drama", "comedy", "sci-fi", "science fiction",
    "western", "crime", "fantasy", "romance", "documentary", "reality", "animation",
    "mystery", "war", "musical", "biography", "adventure", "true crime", "superhero",
    "netflix", "hulu", "disney+", "disney plus", "hbo max", "max", "prime video",
    "amazon prime", "paramount+", "paramount plus", "apple tv", "apple tv+",
    "tubi", "peacock", "pluto",
    "hot on streaming", "coming/leaving streaming", "coming soon", "leaving soon",
    "streaming", "box office", "trailer", "news",
}

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
DECLINING_VC_THRESHOLD  = -15   # exclude titles dropping more than 15% viewer activity

_PROMO_RE = re.compile(
    r'\b(special look|official look|first look|sneak peek|trailer|teaser|featurette|extended cut preview'
    r'|special edition|anniversary special|\d+ years and beyond'
    r'|a special edition of 20\/20|episode of 20\/20)\b',
    re.IGNORECASE,
)

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

        # days_total comes from top10s per-platform data, not the rankings endpoint
        max_days = max(
            (data.get("days_total") or 0)
            for types in top10.values()
            for data in types.values()
        ) if top10 else 0

        titles.append({
            "flixpatrol_id": tid,
            "title":         r.get("title") or t.get("title") or tid,
            "imdb_id":       r.get("imdb_id") or t.get("imdb_id"),
            "tmdb_id":       r.get("tmdb_id") or t.get("tmdb_id"),
            "media_type":    media_type,
            "global_rank":   r.get("global_rank"),
            "rank_last":     r.get("rank_last"),
            "value_change":  r.get("value_change"),
            "days_total":    max_days,
            "top10":         top10,
        })

    log.info("FlixPatrol: %d titles merged", len(titles))
    return titles


# ─────────────────────────────────────────────────────────────────────────────
# FLIXPATROL — US TOP 10
# ─────────────────────────────────────────────────────────────────────────────

def derive_us_top10(all_titles):
    """
    Build a consolidated US Top 10 from already-fetched platform top10s data.
    The top10s calls already used country[in]=US_COUNTRY, so every entry in
    title["top10"] reflects a US chart position.  We pick each title's best
    platform slot (lowest rank on highest tier), then sort movies and TV
    separately.
    """
    movies, tv = [], []

    for t in all_titles:
        if not t.get("top10"):
            continue
        mt = t.get("media_type", "unknown")
        if mt not in ("movie", "tv"):
            continue

        best_rank      = None
        best_rank_last = None
        best_plat      = ""
        best_key       = (99, 99)
        best_days      = 0

        for p, types in t["top10"].items():
            tier = TIER_MAP.get(p, 5)
            for ct, data in types.items():
                rank = data.get("ranking") or 99
                if (tier, rank) < best_key:
                    best_key       = (tier, rank)
                    best_rank      = data.get("ranking")
                    best_rank_last = data.get("ranking_last")
                    best_plat      = p
                    best_days      = data.get("days_total") or 0

        if best_rank is None:
            continue

        entry = {
            "flixpatrol_id": t["flixpatrol_id"],
            "title":         t["title"],
            "platform":      best_plat,
            "plat_rank":     best_rank,
            "plat_rank_last":best_rank_last,
            "days_total":    t.get("days_total") or best_days,
            "_tier":         best_key[0],
        }
        if mt == "movie":
            movies.append(entry)
        else:
            tv.append(entry)

    # Sort: tier-1 platforms first, then by rank within tier
    movies.sort(key=lambda x: (x["_tier"], x["plat_rank"]))
    tv.sort(key=lambda x: (x["_tier"], x["plat_rank"]))
    log.info("US Top 10 derived: %d movies, %d TV in pool",
             len(movies), len(tv))
    return movies[:10], tv[:10]


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
    # Only score titles with US platform data
    if not title.get("top10"):
        return 0.0

    score = 0.0
    vc    = _parse_vc(title.get("value_change"))

    # 1. Value change — keeps signalling large audience shifts
    if vc is not None:
        if   vc > 100:  score += 3.0
        elif vc > 50:   score += 2.0
        elif vc > 10:   score += 1.0
        elif vc < -50:  score -= 2.0
        elif vc < -10:  score -= 1.0

    # 2. Platform rank movement — this is the primary driver
    max_days = 0
    for platform, types in title.get("top10", {}).items():
        tier = TIER_MAP.get(platform, 5)
        tw   = TIER_WEIGHTS.get(tier, 0.5)

        for ct, data in types.items():
            rank      = data.get("ranking")
            rank_last = data.get("ranking_last")
            days      = data.get("days_total") or 0
            max_days  = max(max_days, days)

            if rank is None or rank > 10:
                continue

            if not rank_last:
                # New entry: biggest story
                score += tw * 2.0
            elif rank < rank_last:
                # Rising: score per spot gained, weighted by platform
                score += (rank_last - rank) * tw * 0.3
            elif rank > rank_last:
                # Dropping: penalty per spot lost
                score -= (rank - rank_last) * 0.2
            else:
                # Stable: small credit, but heavily penalised by staleness below
                score += tw * 0.25

    # 3. Staleness penalty — been at same spot for too long, less of a story
    if max_days > 5:
        score -= (max_days - 5) * 0.2

    return round(max(score, 0.0), 2)


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


def _tv_content_bucket(tmdb):
    """Classify a TV title as narrative (scripted drama/comedy/thriller) or
    true_crime_doc, from TMDB genres + overview. Used to enforce TV diversity —
    Reality/Talk/Game shows are already hard-excluded upstream in the prompt."""
    genres   = [g.lower() for g in (tmdb or {}).get("genres", [])]
    overview = ((tmdb or {}).get("overview") or "").lower()
    if "documentary" in genres:
        return "true_crime_doc"
    if "crime" in genres and "true crime" in overview:
        return "true_crime_doc"
    return "narrative"


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


def _overwrite_tab(svc, sheet_id, tab, all_rows):
    """Ensure tab exists, clear its contents, and write all_rows (header + data) fresh.
    Used for snapshot tabs (like TopTags) that represent current state rather than
    an accumulating log — unlike _append_rows, this replaces everything each call."""
    meta = svc.spreadsheets().get(spreadsheetId=sheet_id).execute()
    tabs = [s["properties"]["title"] for s in meta.get("sheets", [])]
    if tab not in tabs:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": tab}}}]},
        ).execute()
        log.info("Created %s tab", tab)
    else:
        svc.spreadsheets().values().clear(
            spreadsheetId=sheet_id, range=tab, body={},
        ).execute()
    svc.spreadsheets().values().update(
        spreadsheetId=sheet_id, range=f"{tab}!A1",
        valueInputOption="RAW", body={"values": all_rows},
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
            "media_type", "hook_type", "hook_value", "slack_ts", "slack_channel",
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

    tag_perf        = defaultdict(lambda: {
        "tier1_count": 0, "tier1_heavy": 0, "sessions_t1": 0,
        "weighted_score": 0.0, "failure_count": 0, "sample_titles": [],
    })
    headline_talent = defaultdict(lambda: {
        "tier1_count": 0, "weighted_score": 0.0,
        "heavy_count": 0, "sessions_t1": 0, "is_signal": False,
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
                    d["sessions_t1"] += sessions
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
            p.get("_slack_ts", ""),
            p.get("_slack_channel", ""),
        ]
        for p in picks
    ]
    _append_rows(svc, sheet_id, "Suggestions", rows)
    log.info("Logged %d picks to Suggestions tab", len(rows))


# ─────────────────────────────────────────────────────────────────────────────
# REPEAT OPPORTUNITY — monthly tag scan + daily flag
# ─────────────────────────────────────────────────────────────────────────────

def _row_title(row):
    """Best-effort resolve which specific movie/show a DB row is about.
    PriTag is usually the subject, but sometimes holds the platform or a
    person's name instead (e.g. PriTag="Prime Video" with the actual title
    buried in Tags) — in that case fall back to the first non-generic Tags
    entry. Not perfect: a row whose PriTag is a person's name (e.g. "Taylor
    Sheridan") with no clearer Tags entry will still resolve to that name
    rather than a title — there's no dedicated title column in this sheet
    to disambiguate further."""
    pri = row.get("PriTag", "").strip()
    if pri and pri.lower() not in NON_TITLE_TAGS:
        return pri
    for t in row.get("Tags", "").split("|"):
        t = t.strip()
        if t and t.lower() not in NON_TITLE_TAGS:
            return t
    return ""


def compute_top_tags(svc, top_n=TOP_TAGS_COUNT, lookback_days=TOP_TAGS_LOOKBACK_DAYS):
    """Monthly job: rank specific movie/show titles by MW performance over the
    trailing year, and record each title's single best-performing headline
    (exact text) for reuse when it resurfaces. Overwrites the TopTitles tab —
    a current snapshot, not an accumulating log, since only the latest ranking
    is useful for matching."""
    sheet_id = os.environ["PERF_SHEET_ID"]
    rows     = _read_tab(svc, sheet_id, "DB")
    cutoff   = date.today() - timedelta(days=lookback_days)

    title_stats = defaultdict(lambda: {
        "weighted_score": 0.0, "tier1_count": 0, "tier1_heavy": 0,
        "best_sessions": -1, "best_headline": "", "sample_titles": [],
    })
    niche_count = 0

    for row in rows:
        if row.get("ContentType", "").strip() != "Niche News":
            continue
        pub_date = _parse_date(row.get("PubDate", ""))
        if not pub_date or pub_date < cutoff:
            continue
        niche_count += 1

        title = _row_title(row)
        if not title:
            continue

        try:
            sessions = int(str(row.get("ActSess", 0)).replace(",", "").strip())
        except Exception:
            sessions = 0
        is_heavy    = sessions >= SESSIONS_HEAVY
        article_txt = row.get("ArticleTitle", "").strip()

        d = title_stats[title.lower()]
        d["weighted_score"] += 1.0
        d["tier1_count"]    += 1
        if is_heavy:
            d["tier1_heavy"] += 1
            if len(d["sample_titles"]) < 3:
                d["sample_titles"].append(article_txt)
        if sessions > d["best_sessions"]:
            d["best_sessions"] = sessions
            d["best_headline"] = article_txt

    ranked = sorted(
        title_stats.items(),
        key=lambda kv: (kv[1]["tier1_heavy"], kv[1]["weighted_score"]),
        reverse=True,
    )[:top_n]

    computed_date = date.today().isoformat()
    table_rows = [
        [computed_date, title, round(d["weighted_score"], 2), d["tier1_count"],
         d["tier1_heavy"], " | ".join(d["sample_titles"]), d["best_headline"]]
        for title, d in ranked
    ]

    _overwrite_tab(svc, sheet_id, TOPTAGS_TAB, [
        ["computed_date", "title", "weighted_score", "tier1_count",
         "tier1_heavy", "sample_titles", "best_headline"],
        *table_rows,
    ])
    log.info(
        "TopTitles: ranked %d titles from %d Niche News rows (past %d days)",
        len(ranked), niche_count, lookback_days,
    )
    return ranked


def load_top_tags(svc):
    """Read the TopTitles tab (written monthly by compute_top_tags) into a lookup
    dict keyed by lowercased title. Returns {} if the tab doesn't exist yet or is
    empty — fails open, meaning the daily repeat-opportunity check simply finds
    nothing until the first monthly run."""
    sheet_id = os.environ["PERF_SHEET_ID"]
    try:
        rows = _read_tab(svc, sheet_id, TOPTAGS_TAB)
    except Exception as e:
        log.warning("Could not read %s tab: %s", TOPTAGS_TAB, e)
        return {}
    return {
        r["title"]: {"best_headline": r.get("best_headline", "")}
        for r in rows if r.get("title")
    }


def check_repeat_opportunities(all_titles, candidates, top_tags):
    """Flag titles anywhere in today's US Top 10 (not just the 6-pick candidate
    pool) whose title, cast, director, or franchise collection matches a
    historically top-performing MW title (from compute_top_tags).

    Cast/director/collection matching only works for titles already TMDB-enriched
    (the top TITLES_TO_ENRICH momentum-scored candidates) — titles outside that
    set are matched on their raw title string only.
    """
    if not top_tags:
        return []
    candidate_tmdb = {t["flixpatrol_id"]: t.get("tmdb", {}) for t in candidates}
    hits, seen = [], set()

    for t in all_titles:
        if not t.get("top10"):
            continue
        norm_title = t["title"].strip().lower()
        if norm_title in seen:
            continue

        match_tag = norm_title if norm_title in top_tags else None
        if not match_tag:
            tmdb      = candidate_tmdb.get(t["flixpatrol_id"], {})
            check_set = {p.strip().lower() for p in (
                tmdb.get("cast", [])[:5] + tmdb.get("directors", []) + tmdb.get("creators", [])
            )}
            if tmdb.get("collection"):
                check_set.add(tmdb["collection"].strip().lower())
            overlap = check_set & top_tags.keys()
            if overlap:
                match_tag = next(iter(overlap))

        if not match_tag:
            continue

        best_plat, best_rank, best_key = "", None, (99, 99)
        for p, types in t["top10"].items():
            tier = TIER_MAP.get(p, 5)
            for ct, data in types.items():
                r = data.get("ranking") or 99
                if (tier, r) < best_key:
                    best_key, best_plat, best_rank = (tier, r), p, data.get("ranking")

        seen.add(norm_title)
        hits.append({
            "title":         t["title"],
            "tag":           match_tag,
            "platform":      best_plat,
            "rank":          best_rank,
            "best_headline": top_tags[match_tag].get("best_headline", ""),
        })

    return hits


def build_repeat_opportunity_message(hits, today):
    """Format repeat-opportunity flags as their own Slack message, clearly
    separate from Boromir's 6 daily picks."""
    if not hits:
        return None
    date_str = today.strftime("%A, %B %-d")
    lines = [f"*Repeat Opportunity — {date_str}*  _separate from Boromir's 6 picks_", ""]
    for h in hits:
        plat_str = f"{h['platform']} #{h['rank']}" if h["platform"] else "—"
        lines.append(f"*{h['title']}*  ({plat_str})")
        lines.append(f"Matched tag: {h['tag']}")
        if h["best_headline"]:
            lines.append(f"Reuse framing: “{h['best_headline']}”")
        lines.append("")
    return {"text": "\n".join(lines).strip(), "unfurl_links": False, "unfurl_media": False}


# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE USAGE TRACKING
# ─────────────────────────────────────────────────────────────────────────────

# In-memory buffer for Claude usage records during a single run.
# Flushed once at end of run() in a single Sheets API call — never one-per-call.
_USAGE_LOG: list = []


def ensure_usage_tab(svc):
    """Create the Usage tab with a header row if it doesn't exist yet.
    Returns the tab name so callers don't need to repeat it."""
    sheet_id = os.environ["PERF_SHEET_ID"]
    tabs = {s["properties"]["title"]
            for s in svc.spreadsheets().get(spreadsheetId=sheet_id).execute()["sheets"]}
    if USAGE_TAB not in tabs:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": USAGE_TAB}}}]},
        ).execute()
        svc.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=f"{USAGE_TAB}!A1",
            valueInputOption="RAW",
            body={"values": [["timestamp", "call_label", "input_tokens", "output_tokens",
                               "cache_created", "cache_read", "model"]]},
        ).execute()
        log.info("Created %s tab", USAGE_TAB)
    return USAGE_TAB


def flush_usage_to_sheet(svc):
    """Batch-append this run's buffered Claude usage records to the Usage tab.
    Safe to call with an empty buffer (no-op). Fails open — a Sheets error
    logs a warning but never stops the pipeline."""
    if not _USAGE_LOG:
        return
    try:
        tab = ensure_usage_tab(svc)
        sheet_id = os.environ["PERF_SHEET_ID"]
        rows = [
            [r["timestamp"], r["call_label"], r["input_tokens"], r["output_tokens"],
             r["cache_created"], r["cache_read"], r["model"]]
            for r in _USAGE_LOG
        ]
        svc.spreadsheets().values().append(
            spreadsheetId=sheet_id,
            range=f"{tab}!A:G",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": rows},
        ).execute()
        log.info("Flushed %d Claude usage record(s) to %s tab", len(rows), tab)
        _USAGE_LOG.clear()
    except Exception as e:
        # Never let usage-tracking failures block a real run.
        log.warning("Could not flush Claude usage to sheet: %s", e)


# ─────────────────────────────────────────────────────────────────────────────
# SLACK FEEDBACK LOOP
# ─────────────────────────────────────────────────────────────────────────────

def _ensure_feedback_tab(svc, sheet_id):
    meta = svc.spreadsheets().get(spreadsheetId=sheet_id).execute()
    tabs = [s["properties"]["title"] for s in meta.get("sheets", [])]
    if "Feedback" not in tabs:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": "Feedback"}}}]},
        ).execute()
        _append_rows(svc, sheet_id, "Feedback", [[
            "date", "title", "flixpatrol_id", "hook_type",
            "thumbs_up", "thumbs_down", "other_reactions", "thread_notes",
        ]])
        log.info("Created Feedback tab")


def collect_feedback(svc, bot_token):
    """Read 👍/👎 reactions and thread replies from yesterday's picks.
    Requires SLACK_BOT_TOKEN — skipped silently if not set.
    Returns list of feedback dicts passed to Claude as context.
    """
    if not bot_token:
        log.info("No SLACK_BOT_TOKEN — skipping feedback collection")
        return []

    sheet_id = os.environ["PERF_SHEET_ID"]
    try:
        rows = _read_tab(svc, sheet_id, "Suggestions")
    except Exception as e:
        log.warning("Could not read Suggestions for feedback: %s", e)
        return []

    yesterday       = (date.today() - timedelta(days=1)).isoformat()
    yesterday_picks = [
        r for r in rows
        if r.get("date_suggested", "").startswith(yesterday) and r.get("slack_ts")
    ]

    if not yesterday_picks:
        log.info("No Slack timestamps found for yesterday — skipping feedback")
        return []

    headers  = {"Authorization": f"Bearer {bot_token}"}
    feedback = []

    for pick in yesterday_picks:
        ts      = pick["slack_ts"]
        channel = pick.get("slack_channel") or os.environ.get("SLACK_CHANNEL", "")

        result = {
            "title":           pick.get("title", ""),
            "flixpatrol_id":   pick.get("flixpatrol_id", ""),
            "hook_type":       pick.get("hook_type", ""),
            "thumbs_up":       0,
            "thumbs_down":     0,
            "other_reactions": [],
            "thread_notes":    [],
        }

        # Reactions
        try:
            r    = requests.get(
                "https://slack.com/api/reactions.get",
                headers=headers,
                params={"channel": channel, "timestamp": ts},
                timeout=10,
            )
            data = r.json()
            if data.get("ok"):
                for rxn in data.get("message", {}).get("reactions", []):
                    name, count = rxn["name"], rxn["count"]
                    if name in ("+1", "thumbsup"):
                        result["thumbs_up"] = count
                    elif name in ("-1", "thumbsdown"):
                        result["thumbs_down"] = count
                    else:
                        result["other_reactions"].append(f":{name}: ×{count}")
            else:
                log.warning("reactions.get failed (%s): %s", pick.get("title"), data.get("error"))
        except Exception as e:
            log.warning("Reactions fetch error for %s: %s", pick.get("title"), e)

        # Thread replies (skip index 0 — that's the original post)
        try:
            r    = requests.get(
                "https://slack.com/api/conversations.replies",
                headers=headers,
                params={"channel": channel, "ts": ts},
                timeout=10,
            )
            data = r.json()
            if data.get("ok"):
                for msg in data.get("messages", [])[1:]:
                    text = msg.get("text", "").strip()
                    if text:
                        result["thread_notes"].append(text)
        except Exception as e:
            log.warning("Thread fetch error for %s: %s", pick.get("title"), e)

        feedback.append(result)
        log.info(
            "Feedback — %s: 👍%d 👎%d, %d thread note(s)",
            result["title"], result["thumbs_up"], result["thumbs_down"],
            len(result["thread_notes"]),
        )

    # Persist to Google Sheets
    _ensure_feedback_tab(svc, sheet_id)
    rows = [[
        yesterday,
        f["title"], f["flixpatrol_id"], f["hook_type"],
        f["thumbs_up"], f["thumbs_down"],
        ", ".join(f["other_reactions"]),
        " | ".join(f["thread_notes"])[:500],
    ] for f in feedback]
    if rows:
        _append_rows(svc, sheet_id, "Feedback", rows)
        log.info("Logged feedback for %d picks to Feedback tab", len(rows))

    return feedback


# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE
# ─────────────────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are an editorial assistant for MovieWeb, a mainstream movie and TV entertainment site.

Your task: review today's trending streaming titles and select exactly 6 for our daily Slack brief.

SELECTION RULES:
- Hook priority: talent > franchise/collection > nostalgia (10+ years old) > foreign language > streaming event > chart position alone
- Hard exclude: Reality TV, Talk shows, Game shows, Soap operas — check TMDB genres carefully
- Hard exclude: titles where declining is true (viewer activity dropping more than 15%) — skip these entirely
- Strongly prefer new entries (days_total 1–3) over titles that have been in the top 10 for 7+ days with only minor rank movement — freshness is almost always the stronger story
- A franchise or major studio title debuting on any tier-1 platform is a bigger editorial story than a week-old title moving one spot, regardless of which platform it is on — do not overweight Netflix just because it has more data
- When a title's platform is Pluto or Tubi, frame it as a free streaming story: "available free on Pluto" or "the free streaming hit" — that availability context is part of the hook
- At least 2 of the 6 picks must be TV shows, and at least 2 of the TV picks must be scripted narrative series (drama, comedy, thriller) — not true crime, documentary, or reality
- Maximum 1 True Crime or Documentary TV pick total (content_bucket: true_crime_doc) — never more than one non-narrative TV pick
- Maximum 1 pick per platform — spread across Netflix, HBO Max, Amazon Prime, Disney+, etc.
- All picks must have US platform data (platforms will never be empty)
- Skip titles in recent_suggestions unless their momentum_score is exceptional (above 12)
- Prefer titles where mw_performance.talent_signals exists (proven audience for this talent on MovieWeb)
- When hook_type is talent AND the talent appears in mw_performance.talent_signals, set hook_value to: "[Name] | [articles] articles | [over_25k] over 25k sessions | avg [avg_sa] S/A". If the talent has no entry in talent_signals, just use their name — no stats annotation.
- When no talent signal exists, fall back to franchise/nostalgia/chart hooks using the available data

HEADLINE RULES:
- site_headline: under 75 characters. Standalone. Frame the story around the hook — do not name the streaming title directly in the headline.
- No vague superlatives. No question headlines. Never use the word "surge" — use "jump", "climb", "spike", or "rise" instead.
- Never mention FlixPatrol scores, percentages from the data, article counts, session numbers, or any internal metrics in headlines or titles — ever.
- Seldom reference these in angles. When you do, translate them into reader-facing terms (e.g. "audiences are responding" not "130% FlixPatrol score").
- Never describe a title as a "[platform] movie" or "[platform] show" (e.g. "Netflix movie", "HBO show") — this implies it is a platform original. Say "on Netflix", "streaming on HBO Max", "available on Amazon Prime" instead.
- Geographic framing is strong: "In America" or "Biggest Movie in America Right Now" when US top 3 on a major platform.

Good headline examples by hook type:
- talent:    "The Better Call Saul Star's Most Brutal Role Is Taking Over Netflix"
- nostalgia: "The 1999 Action Classic That's Quietly Dominating Amazon Prime Right Now"
- foreign:   "The Korean Thriller Beating Every English-Language Show on Netflix"
- franchise: "The MonsterVerse Entry With the Highest RT Score Is Back on Top of HBO Max"
- chart:     "The Biggest Movie in America Right Now Isn't What You'd Expect"

Respond with valid JSON only. No markdown, no explanation outside the JSON."""


def ask_claude(payload, recent_suggestions, feedback=None):
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    recent_str = (
        json.dumps(recent_suggestions, ensure_ascii=False)
        if recent_suggestions else "none"
    )

    feedback_str = ""
    if feedback:
        lines = []
        for f in feedback:
            line = f"- {f['title']} ({f['hook_type']}): 👍{f['thumbs_up']} 👎{f['thumbs_down']}"
            if f["thread_notes"]:
                notes = "; ".join(f["thread_notes"][:3])
                line += f" | Editor notes: {notes}"
            lines.append(line)
        feedback_str = (
            "\n\nEditorial feedback on yesterday's picks "
            "(use to calibrate hook type and platform preferences today):\n"
            + "\n".join(lines) + "\n"
        )

    user_msg = f"""Today's trending titles, scored by momentum and platform strength:

{json.dumps(payload, ensure_ascii=False, indent=2)}

Titles suggested in the last {SUPPRESS_DAYS} days — skip unless exceptional:
{recent_str}
{feedback_str}
Select exactly {PICKS_TARGET + 3} picks ranked by editorial priority — we will enforce platform caps after and keep the best {PICKS_TARGET}. Return this JSON:
{{
  "picks": [
    {{
      "flixpatrol_id": "...",
      "title": "...",
      "hook_type": "talent|franchise|nostalgia|foreign_language|streaming_event|chart",
      "hook_value": "the specific person, franchise, year, country, or platform+rank",
      "site_headline": "...",
      "angle": "one sentence editorial angle for the article body"
    }}
  ]
}}"""

    log.info("Calling Claude (claude-sonnet-4-6)...")
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        system=[{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": user_msg}],
    )
    u = response.usage
    log.info(
        "claude_usage  input=%s  output=%s  cache_read=%s  cache_write=%s",
        u.input_tokens,
        u.output_tokens,
        getattr(u, "cache_read_input_tokens", 0),
        getattr(u, "cache_creation_input_tokens", 0),
    )
    _USAGE_LOG.append({
        "timestamp":     datetime.now(timezone.utc).isoformat(),
        "call_label":    "picks",
        "input_tokens":  u.input_tokens,
        "output_tokens": u.output_tokens,
        "cache_created": getattr(u, "cache_creation_input_tokens", 0),
        "cache_read":    getattr(u, "cache_read_input_tokens", 0),
        "model":         "claude-sonnet-4-6",
    })

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

    global_rank = pick.get("_global_rank")

    # Trend notes — rank movement only, no FlixPatrol percentages
    trend_parts = []

    if "new entry" in best_ranking:
        trend_parts.append(f"New entry at {best_ranking.split(' ')[0]} on {best_platform}")
    elif "up from" in best_ranking:
        old = best_ranking.split("up from ")[-1].rstrip(")")
        cur = best_ranking.split(" ")[0]
        try:
            spots = int(old.lstrip("#")) - int(cur.lstrip("#"))
            trend_parts.append(f"Up {spots} spot{'s' if spots != 1 else ''} on {best_platform} (from {old})")
        except ValueError:
            trend_parts.append(best_ranking)
    elif "down from" in best_ranking:
        old = best_ranking.split("down from ")[-1].rstrip(")")
        cur = best_ranking.split(" ")[0]
        try:
            spots = int(cur.lstrip("#")) - int(old.lstrip("#"))
            trend_parts.append(f"Down {spots} spot{'s' if spots != 1 else ''} on {best_platform} (from {old})")
        except ValueError:
            trend_parts.append(best_ranking)
    else:
        trend_parts.append(f"Holding {best_ranking.split(' ')[0]} on {best_platform}")

    # Days in top 10
    days_total = pick.get("_days_total", 0)
    if days_total > 0:
        trend_parts.append(f"Day {days_total} in top 10")

    if global_rank:
        trend_parts.append(f"#{global_rank} globally")

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
    if angle:
        out += ["", f"_{angle}_"]
    out += ["", "—" * 52]

    return {"text": "\n".join(out), "unfurl_links": False, "unfurl_media": False}


def enforce_platform_cap(picks, cap=PLATFORM_CAP):
    """Drop picks that exceed per-platform cap; keep the highest-momentum ones first.
    Claude is asked for PICKS_TARGET+3 so we have headroom to still hit PICKS_TARGET.
    """
    counts   = defaultdict(int)
    filtered = []
    for pick in picks:
        # Find best platform (same logic as build_message)
        best_plat = ""
        best_key  = (99, 99)
        for p, types in pick.get("_top10", {}).items():
            tier = TIER_MAP.get(p, 5)
            for ct, data in types.items():
                r = data.get("ranking") or 99
                if (tier, r) < best_key:
                    best_key  = (tier, r)
                    best_plat = p
        if counts[best_plat] < cap:
            counts[best_plat] += 1
            filtered.append(pick)
        else:
            log.info("Platform cap (%d): dropped %s (%s)", cap, pick["title"], best_plat)
    result = filtered[:PICKS_TARGET]
    log.info("Platform cap applied: %d → %d picks", len(picks), len(result))
    return result


def enforce_tv_diversity(picks, cap=TRUE_CRIME_DOC_CAP):
    """Cap true-crime/documentary TV picks at `cap`; keep earlier-ranked picks
    first (Claude ranks by editorial priority). Runs before enforce_platform_cap
    so a dropped doc pick doesn't consume a platform slot."""
    doc_count = 0
    filtered  = []
    for pick in picks:
        if pick.get("media_type") == "tv" and _tv_content_bucket(pick.get("_tmdb", {})) == "true_crime_doc":
            if doc_count >= cap:
                log.info("TV diversity cap (%d): dropped %s (true crime/doc)", cap, pick["title"])
                continue
            doc_count += 1
        filtered.append(pick)
    return filtered


def build_table_message(movies, tv, today):
    """Format consolidated US Top 10 Movies + TV as a monospace table in Slack."""
    date_str = today.strftime("%A, %B %-d")

    W_TITLE = 32
    W_PLAT  = 17
    W_MV    = 6

    header = f"{'#':<3}{'Title':<{W_TITLE}}{'Platform':<{W_PLAT}}{'Mv':<{W_MV}}Days"
    rule   = "─" * (3 + W_TITLE + W_PLAT + W_MV + 4)

    def _row(i, entry):
        name      = entry["title"]
        plat      = entry.get("platform", "")
        rank      = entry["plat_rank"]
        rank_last = entry.get("plat_rank_last")
        days      = entry.get("days_total", 0)

        if not rank_last:
            mv = "new"
        elif rank < rank_last:
            mv = f"↑{rank_last - rank}"
        elif rank > rank_last:
            mv = f"↓{rank - rank_last}"
        else:
            mv = "—"

        if len(name) > W_TITLE - 1:
            name = name[:W_TITLE - 2] + "…"

        plat_str = f"{plat} #{rank}" if plat else f"#{rank}"
        days_str = str(days) if days > 0 else ""

        return f"{i:<3}{name:<{W_TITLE}}{plat_str:<{W_PLAT}}{mv:<{W_MV}}{days_str}"

    movie_rows = [_row(i + 1, e) for i, e in enumerate(movies)]
    tv_rows    = [_row(i + 1, e) for i, e in enumerate(tv)]

    body = "\n".join([
        f"Top 10 in America — {date_str}",
        "",
        "MOVIES",
        header,
        rule,
        *movie_rows,
        "",
        "TV SHOWS",
        header,
        rule,
        *tv_rows,
    ])
    return {"text": f"```\n{body}\n```", "unfurl_links": False, "unfurl_media": False}


def post_slack(payload):
    """Post a message to Slack.

    If SLACK_BOT_TOKEN + SLACK_CHANNEL are set, uses chat.postMessage (returns ts + channel
    so reactions can be read back later).  Falls back to the webhook otherwise.
    Returns a dict {"ts": ..., "channel": ...} on success, None on failure.
    """
    bot_token = os.environ.get("SLACK_BOT_TOKEN")
    channel   = os.environ.get("SLACK_CHANNEL")

    if bot_token and channel:
        resp = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {bot_token}"},
            json={**payload, "channel": channel},
            timeout=15,
        )
        data = resp.json()
        if data.get("ok"):
            return {"ts": data.get("ts"), "channel": data.get("channel")}
        log.warning("Slack API error: %s", data.get("error", "unknown"))
        return None

    # Webhook fallback (no ts available)
    resp = requests.post(os.environ["SLACK_WEBHOOK_URL"], json=payload, timeout=15)
    ok   = resp.status_code == 200 and resp.text == "ok"
    if not ok:
        log.warning("Slack webhook returned %d: %s", resp.status_code, resp.text[:100])
    return {"ts": None, "channel": None} if ok else None


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
    candidates = [
        t for t in all_titles
        if t["momentum_score"] > 0
        and not _PROMO_RE.search(t["title"])
    ][:TITLES_TO_ENRICH]
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

    # 4. MW performance data + suggestion history + editorial feedback
    svc                                          = _sheets_service()
    tag_perf, headline_talent, published_suppress = load_perf_data(svc)
    recent_suggestions                            = load_suggestions(svc)
    feedback                                      = collect_feedback(svc, os.environ.get("SLACK_BOT_TOKEN"))

    # 5. Build Claude payload — compact, one entry per candidate
    def mw_signals(t):
        sig        = {}
        norm_title = t["title"].strip().lower()
        ts         = tag_perf.get(norm_title, {})
        if ts.get("tier1_count", 0) > 0:
            sig["title_articles"]  = ts["tier1_count"]
            sig["title_over_25k"]  = ts.get("tier1_heavy", 0)
            avg = int(ts["sessions_t1"] / ts["tier1_count"]) if ts["tier1_count"] else 0
            sig["title_avg_sa"]    = avg

        talent_hits = []
        all_talent  = (
            t.get("tmdb", {}).get("cast", [])[:5]
            + t.get("tmdb", {}).get("directors", [])
            + t.get("tmdb", {}).get("creators", [])
        )
        for person in all_talent:
            ht = headline_talent.get(person.strip().lower(), {})
            if ht.get("is_signal"):
                articles = ht["tier1_count"]
                avg_sa   = int(ht["sessions_t1"] / articles) if articles else 0
                talent_hits.append({
                    "name":      person,
                    "articles":  articles,
                    "over_25k":  ht["heavy_count"],
                    "avg_sa":    avg_sa,
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
            "days_total":     t.get("days_total", 0),
            "declining":      (_parse_vc(t.get("value_change")) or 0) < DECLINING_VC_THRESHOLD,
            "content_bucket": _tv_content_bucket(tmdb) if t.get("media_type") == "tv" else None,
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
    result = ask_claude(payload, recent_suggestions, feedback)
    picks  = result.get("picks", [])
    log.info("Claude selected %d picks", len(picks))

    # Merge top10 and trend stats back into picks for Slack formatting
    title_map = {t["flixpatrol_id"]: t for t in candidates}
    for pick in picks:
        source               = title_map.get(pick["flixpatrol_id"], {})
        pick["_top10"]        = source.get("top10", {})
        pick["_global_rank"]  = source.get("global_rank")
        pick["_rank_last"]    = source.get("rank_last")
        pick["_value_change"] = source.get("value_change")
        pick["_days_total"]   = source.get("days_total", 0)
        pick["_tmdb"]         = source.get("tmdb", {})
        if not pick.get("media_type"):
            pick["media_type"] = source.get("media_type", "")

    # Enforce TV diversity, then platform cap, in code (Claude asked for
    # PICKS_TARGET+3 to give headroom for both filters)
    picks = enforce_tv_diversity(picks)
    picks = enforce_platform_cap(picks)

    # 7. Post to Slack
    post_slack({
        "text": f"*Boromir's Picks — {today.strftime('%A, %B %-d')}*  _{len(picks)} titles_"
    })
    time.sleep(1)

    posted = []
    for i, pick in enumerate(picks, 1):
        msg    = build_message(pick, i, len(picks))
        result = post_slack(msg)
        if result:
            pick["_slack_ts"]      = result.get("ts") or ""
            pick["_slack_channel"] = result.get("channel") or ""
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

    # 9. US Top 10 reference tables (derived from already-fetched platform data)
    us_movies, us_tv = derive_us_top10(all_titles)
    if us_movies or us_tv:
        time.sleep(1)
        table_msg = build_table_message(us_movies, us_tv, today)
        if post_slack(table_msg) is not None:
            log.info("Posted US Top 10 table (%d movies, %d TV)", len(us_movies), len(us_tv))
        else:
            log.error("Failed to post US Top 10 table")
    else:
        log.warning("No US Top 10 data — skipping table")

    # 10. Repeat-opportunity check (monthly tag list, checked daily against
    #     every title in today's US Top 10 — not just the 6-pick candidates)
    top_tags    = load_top_tags(svc)
    repeat_hits = check_repeat_opportunities(all_titles, candidates, top_tags)
    if repeat_hits:
        time.sleep(1)
        repeat_msg = build_repeat_opportunity_message(repeat_hits, today)
        if post_slack(repeat_msg) is not None:
            log.info("Posted %d repeat-opportunity flag(s)", len(repeat_hits))
        else:
            log.error("Failed to post repeat-opportunity message")
    else:
        log.info("No repeat-opportunity matches today")

    # 11. Flush Claude API usage to the Usage sheet tab
    flush_usage_to_sheet(svc)


def run_monthly_tags():
    """Monthly job: recompute the TopTags tab from the trailing year of Niche
    News performance. Sheets-only — no FlixPatrol/TMDB/Anthropic/Slack calls."""
    today = date.today()
    log.info("=" * 52)
    log.info("  BOROMIR — Monthly Top Tags scan  |  %s", today.isoformat())
    log.info("=" * 52)
    svc = _sheets_service()
    compute_top_tags(svc)


if __name__ == "__main__":
    import sys
    if "--monthly-tags" in sys.argv:
        run_monthly_tags()
    else:
        run()
