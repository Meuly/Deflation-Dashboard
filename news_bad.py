import feedparser
from datetime import datetime, timezone, timedelta


def _parse_time(entry):
    t = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
    if not t:
        return None
    return datetime(*t[:6], tzinfo=timezone.utc)


def fetch_recent_news(feed_urls, hours: int = 48, max_items: int = 25):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    items = []

    for url in feed_urls:
        feed = feedparser.parse(url)
        for entry in feed.entries[:max_items]:
            dt = _parse_time(entry)
            if dt and dt < cutoff:
                continue
            title = getattr(entry, "title", "") or ""
            link = getattr(entry, "link", "") or ""
            summary = getattr(entry, "summary", "") or ""
            items.append({"time": dt, "title": title, "link": link, "summary": summary, "source": url})

    return items


def detect_bad_news(items):
    bad_terms = [
        "bank", "insolv", "default", "credit event", "liquidity",
        "layoff", "job cuts", "recession", "downgrade", "guidance cut",
        "missed expectations", "delinquen", "foreclosure", "bankrupt",
        "run on", "stress", "bailout"
    ]

    hits = []
    for it in items:
        text = f"{it['title']} {it['summary']}".lower()
        score = sum(1 for k in bad_terms if k in text)
        if score >= 2:  # threshold reduces noise
            hits.append({"title": it["title"], "link": it["link"], "score": score})

    return hits[:6]  # keep email tight
