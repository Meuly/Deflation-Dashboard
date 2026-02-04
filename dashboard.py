from datetime import datetime
import os

from data_sources import fred_series_csv, yahoo_adj_close, boc_series_csv
from indicators import credit_stress_us_can, real_yields_us_can, high_beta_leadership
from emailer import send_email


def fmt_status(s: str) -> str:
    return {"RED": "🔴", "YELLOW": "🟡", "GREEN": "🟢"}.get(s, "🟡")


def build_email(now_et: str, results: dict) -> tuple[str, str]:
    # Links (stable)
    links = {
        "us_hy_oas_fred": "https://fred.stlouisfed.org/series/BAMLH0A0HYM2",
        "us_hy_oas_chart": "https://fred.stlouisfed.org/graph/?g=OUJ",
        "ca_xhy": "https://finance.yahoo.com/quote/XHY.TO",
        "us_real_10y_fred": "https://fred.stlouisfed.org/series/DFII10",
        "us_real_10y_chart": "https://fred.stlouisfed.org/series/DFII10",
        "ca_10y_yield_info": "https://www.bankofcanada.ca/rates/interest-rates/canadian-bonds/",
        "btc": "https://finance.yahoo.com/quote/BTC-USD",
        "spy": "https://finance.yahoo.com/quote/SPY",
        "qqq": "https://finance.yahoo.com/quote/QQQ",
        "dia": "https://finance.yahoo.com/quote/DIA",
        "iwm": "https://finance.yahoo.com/quote/IWM",
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
        ("4. Real Yields (US+CA)", results["real_yields"]["combined"]),
        ("5. Bad News Reaction", placeholders["bad_news_reaction"]),
        ("6. High-Beta Leadership", results["high_beta"]["combined"]),
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

    # Real yields commentary (safe, non-directive)
    ry = results.get("real_yields", {})
    ry_s = ry.get("combined", "YELLOW")

    if ry_s == "GREEN":
        commentary_lines.append("Real yields are easing, indicating looser financial conditions.")
    elif ry_s == "RED":
        commentary_lines.append("Real yields are tightening, indicating more restrictive conditions.")
    else:
        commentary_lines.append("Real yield conditions remain mixed or unclear.")
          
    # High-beta leadership commentary (safe, non-directive)
    hb = results.get("high_beta") or {}
    hb_s = hb.get("combined", "YELLOW")

    if hb_s == "GREEN":
        commentary_lines.append(
            "High-beta assets are leading on relative strength, consistent with liquidity returning."
        )
    elif hb_s == "RED":
        commentary_lines.append(
            "High-beta assets are lagging, consistent with risk appetite remaining weak."
        )
    else:
        commentary_lines.append(
            "High-beta leadership is mixed; liquidity signals are not yet decisive."
        )  
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
    body.append(f"- US 10Y Real Yield (FRED DFII10): {links['us_real_10y_fred']}")
    body.append(f"- Canada 10Y yield info (BoC): {links['ca_10y_yield_info']}")
    body.append(f"- BTC-USD: {links['btc']}")
    body.append(f"- SPY: {links['spy']}")
    body.append(f"- QQQ: {links['qqq']}")
    body.append(f"- DIA: {links['dia']}")
    body.append(f"- IWM: {links['iwm']}")

    return subject, "\n".join(body)


def main():
    # Timestamp label (ET)
    now_et = datetime.now().strftime("%Y-%m-%d %H:%M ET")

    # Fetch data
    us_hy_oas = fred_series_csv("BAMLH0A0HYM2")  # ICE BofA US HY OAS
    ca_hy = yahoo_adj_close("XHY.TO", period="6mo")

    credit = credit_stress_us_can(us_hy_oas, ca_hy)
    us_real_10y = fred_series_csv("DFII10")  # US 10Y TIPS real yield
    # Canada 10Y nominal yield proxy via BoC CSV (we'll use a stable CSV endpoint)
    ca_10y_nominal = boc_series_csv(
    "https://www.bankofcanada.ca/valet/observations/BD.CDN.10YR.DQ.YLD/csv?recent=200"
)

    real_yields = real_yields_us_can(us_real_10y, ca_10y_nominal)
    # --- High beta leadership data ---
    btc = yahoo_adj_close("BTC-USD", period="6mo")
    spy = yahoo_adj_close("SPY", period="6mo")
    qqq = yahoo_adj_close("QQQ", period="6mo")
    dia = yahoo_adj_close("DIA", period="6mo")
    iwm = yahoo_adj_close("IWM", period="6mo")

      
    results = {
        "credit_stress": credit,
        "real_yields": real_yields,
        "high_beta": high_beta,
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
