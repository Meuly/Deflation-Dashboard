import feedparser
from datetime import datetime, timezone, timedelta


def _parse_time(entry):
    # feedparser may provide: published_parsed or updated_parsed
    t = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
    if not t:
        return None
    return datetime(*t[:6], tzinfo=timezone.utc)


def fetch_recent_feed_items(feed_url: str, hours: int = 48, max_items: int = 20):
    feed = feedparser.parse(feed_url)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    items = []
    for entry in feed.entries[:max_items]:
        dt = _parse_time(entry)
        if dt and dt < cutoff:
            continue
        title = getattr(entry, "title", "") or ""
        link = getattr(entry, "link", "") or ""
        summary = getattr(entry, "summary", "") or ""
        items.append({"time": dt, "title": title, "link": link, "summary": summary})
    return items


def score_policy_items(items):
    # Very simple keyword scoring
    dovish = [
        "financial stability", "liquidity", "facility", "backstop", "support",
        "market functioning", "guarantee", "temporary measure", "provide liquidity",
        "standing repo", "swap line"
    ]
    hawkish = [
        "restrictive", "higher for longer", "inflation remains", "tightening",
        "raise rates", "rate increase", "reduce balance sheet", "quantitative tightening",
        "inflation is too high"
    ]

    score = 0
    hits = []

    for it in items:
        text = f"{it['title']} {it['summary']}".lower()

        d = sum(1 for k in dovish if k in text)
        h = sum(1 for k in hawkish if k in text)

        score += (d - h)
        if d or h:
            hits.append({"title": it["title"], "link": it["link"], "dovish": d, "hawkish": h})

    return score, hits


def policy_actions_indicator(boc_items, fed_items):
    boc_score, boc_hits = score_policy_items(boc_items)
    fed_score, fed_hits = score_policy_items(fed_items)

    # Combine
    total_score = boc_score + fed_score
    any_news = len(boc_items) + len(fed_items) > 0

    if not any_news:
        combined = "YELLOW"
    else:
        if total_score >= 2:
            combined = "GREEN"
        elif total_score <= -2:
            combined = "RED"
        else:
            combined = "YELLOW"

    return {
        "combined": combined,
        "boc_score": boc_score,
        "fed_score": fed_score,
        "total_score": total_score,
        "boc_hits": boc_hits[:5],
        "fed_hits": fed_hits[:5],
        "note": "Keyword-scored policy tone from last 48 hours of official RSS feeds."
    }
