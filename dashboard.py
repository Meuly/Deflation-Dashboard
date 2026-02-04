from datetime import datetime
import os

from data_sources import fred_series_csv, yahoo_adj_close
from indicators import credit_stress_us_can
from emailer import send_email


def fmt_status(s: str) -> str:
    return {"RED": "🔴", "YELLOW": "🟡", "GREEN": "🟢"}.get(s, "🟡")


def build_email(now_et: str, results: dict) -> tuple[str, str]:
    # Links (stable)
    links = {
        "us_hy_oas_fred": "https://fred.stlouisfed.org/series/BAMLH0A0HYM2",
        "us_hy_oas_chart": "https://fred.stlouisfed.org/graph/?g=OUJ",
        "ca_xhy": "https://finance.yahoo.com/quote/XHY.TO",
    }

    # Indicator statuses (only #1 is real for now)
    credit = results["credit_stress"]
    s1 = credit["combined"]

    # Placeholders until wired
    placeholders = {
        "policy_actions": "YELLOW",
        "asset_correlations": "YELLOW",
        "real_yields": "YELLOW",
        "bad_news_reaction": "YELLOW",
        "high_beta": "YELLOW",
    }

    statuses = [
        ("1. Credit Stress (US+CA)", s1),
        ("2. Policy Actions (BoC+Fed)", placeholders["policy_actions"]),
        ("3. Asset Correlations", placeholders["asset_correlations"]),
        ("4. Real Yields (US+CA)", placeholders["real_yields"]),
        ("5. Bad News Reaction", placeholders["bad_news_reaction"]),
        ("6. High-Beta Leadership", placeholders["high_beta"]),
    ]

    green_count = sum(1 for _, s in statuses if s == "GREEN")

    # Stand-down logic (wired later across all indicators; for now only credit can trigger it)
    stand_down = "NOT ACTIVE"
    stand_down_reason = ""
    if s1 == "RED":
        stand_down = "ACTIVE"
        stand_down_reason = "Credit stress indicator is RED (conservative early protection)."

    # Supportive commentary (non-directive)
    commentary_lines = []
    if s1 == "GREEN":
        commentary_lines.append("Credit conditions are improving on both the U.S. (spreads) and Canada (HY proxy).")
        commentary_lines.append("If other indicators follow, this becomes a sturdier risk-on backdrop.")
    elif s1 == "RED":
        commentary_lines.append("Credit stress is elevated (at least one of U.S. spreads or Canada HY proxy is deteriorating).")
        commentary_lines.append("This is the most common failure-point for early risk-on attempts.")
    else:
        commentary_lines.append("Credit conditions are mixed/unclear (no clean trend yet).")
        commentary_lines.append("This is typically a ‘watch closely’ zone rather than a signal zone.")

    subject = f"Deflation Dashboard (CAN+US) — {now_et}"

    body = []
    body.append("DEFLATION → RISK-ON DASHBOARD (CAN + US)")
    body.append(f"Timestamp: {now_et}")
    body.append("")
    for name, s in statuses:
        body.append(f"{name}: {fmt_status(s)}")

    body.append("")
    body.append(f"GREEN COUNT: {green_count} / 6")
    body.append(f"STAND-DOWN: {stand_down}")
    if stand_down_reason:
        body.append(f"Reason: {stand_down_reason}")

    body.append("")
    body.append("Links & Charts")
    body.append(f"- US HY OAS (FRED): {links['us_hy_oas_fred']}")
    body.append(f"- US HY OAS chart: {links['us_hy_oas_chart']}")
    body.append(f"- Canada HY proxy (XHY.TO): {links['ca_xhy']}")

    body.append("")
    body.append("Context & Interpretation (Non-Directive)")
    body.extend([f"- {x}" for x in commentary_lines])

    return subject, "\n".join(body)


def main():
    # Timestamp label (ET)
    now_et = datetime.now().strftime("%Y-%m-%d %H:%M ET")

    # Fetch data
    us_hy_oas = fred_series_csv("BAMLH0A0HYM2")  # ICE BofA US HY OAS
    ca_hy = yahoo_adj_close("XHY.TO", period="6mo")

    credit = credit_stress_us_can(us_hy_oas, ca_hy)

    results = {
        "credit_stress": credit,
    }

    subject, body = build_email(now_et, results)

    send_email(
        subject=subject,
        body=body,
        username=os.environ["EMAIL_USERNAME"],
        password=os.environ["EMAIL_PASSWORD"],
        sender=os.environ["EMAIL_FROM"],
        recipient=os.environ["EMAIL_TO"],
    )


if __name__ == "__main__":
    main()
