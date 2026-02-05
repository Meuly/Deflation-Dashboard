from datetime import datetime
import os

from data_sources import fred_series_csv, yahoo_adj_close, boc_series_csv
from indicators import credit_stress_us_can, real_yields_us_can, high_beta_leadership, asset_correlations, bad_news_reaction
from emailer import send_email
from news_policy import fetch_recent_feed_items, policy_actions_indicator
from news_bad import fetch_recent_news, detect_bad_news
from state_manager import load_state, save_state, add_run, compute_persistence_flags, last_n_summary

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
        "xic": "https://finance.yahoo.com/quote/XIC.TO",
        "hyg": "https://finance.yahoo.com/quote/HYG",
        "xre": "https://finance.yahoo.com/quote/XRE.TO",
        "vnq": "https://finance.yahoo.com/quote/VNQ",
        "boc_press_rss": "https://www.bankofcanada.ca/rss/press-releases/",
        "fed_press_rss": "https://www.federalreserve.gov/feeds/press_all.xml",
        "cbc_business_rss": "https://www.cbc.ca/cmlink/rss-business",
        "mw_rss": "https://www.marketwatch.com/rss/topstories",
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
        ("2. Policy Actions (BoC+Fed)", results["policy_actions"]["combined"]),
        ("3. Asset Correlations", results["asset_correlations"]["combined"]),
        ("4. Real Yields (US+CA)", results["real_yields"]["combined"]),
        ("5. Bad News Reaction", results["bad_news_reaction"]["combined"]),
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

    ac = results.get("asset_correlations") or {}
    ac_s = ac.get("combined", "YELLOW")
    if ac_s == "GREEN":
        commentary_lines.append("Cross-asset correlations are lower, suggesting forced selling pressure is easing.")
    elif ac_s == "RED":
        commentary_lines.append("Cross-asset correlations are elevated, consistent with mechanical risk-off behavior.")
    else:
        commentary_lines.append("Cross-asset correlations are mixed; forced selling signals are not definitive.")

    # Policy actions commentary (safe, non-directive)
    pol = results.get("policy_actions") or {}
    pol_s = pol.get("combined", "YELLOW")

    if pol_s == "GREEN":
        commentary_lines.append("Policy tone in the last 48 hours leans supportive to liquidity/financial stability.")
    elif pol_s == "RED":
        commentary_lines.append("Policy tone in the last 48 hours leans restrictive, prioritizing inflation containment.")
    else:
        commentary_lines.append("Policy tone remains neutral or mixed based on recent official updates.")
        
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

    br = results.get("bad_news_reaction") or {}
    hits = br.get("bad_hits") or []
    if hits:
        body.append("")
        body.append("Bad-news items detected (last 48h)")
        for h in hits[:4]:
            body.append(f"- {h.get('title','')} | {h.get('link','')}")
            
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
    body.append(f"- XIC.TO (TSX proxy): {links['xic']}")
    body.append(f"- HYG (US HY proxy): {links['hyg']}")
    body.append(f"- XRE.TO (Canada REITs): {links['xre']}")
    body.append(f"- VNQ (US REITs): {links['vnq']}")
    body.append(f"- BoC Press Releases (RSS): {links['boc_press_rss']}")
    body.append(f"- Fed Press Releases (RSS): {links['fed_press_rss']}")
    body.append(f"- CBC Business (RSS): {links['cbc_business_rss']}")
    body.append(f"- MarketWatch Top Stories (RSS): {links['mw_rss']}")

    meta = results.get("meta") or {}
    body.append("")
    body.append("Conclusion (Non-Directive)")
    body.append(f"- Greens: {meta.get('green_count', 'NA')} / 6")
    body.append(
        f"- Risk window opening (≥4 greens for 10 runs): "
        f"{'YES' if meta.get('risk_window_opening') else 'NO'}"
    )
    body.append(
        f"- Stand-down active: "
        f"{'YES' if meta.get('stand_down_active') else 'NO'}"
    )
    body.append(f"- Stand-down trigger: {meta.get('stand_down_reason', 'NA')}")

    hb = meta.get("history_bar", "")
    if hb:
        body.append(f"- Recent runs (12): {hb}   (G=≥4 greens, Y=3 greens, R=≤2 greens)")
        
    return subject, "\n".join(body)


def main():
    import pandas as pd

    # Defaults so the dashboard never crashes if a fetch fails
    spy = pd.Series(dtype=float)
    btc = pd.Series(dtype=float)
    xic = pd.Series(dtype=float)

    import pandas as pd

    # Defaults so we never crash if a fetch fails
    spy = pd.Series(dtype=float)
    btc = pd.Series(dtype=float)

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
    
    # --- Bad news reaction (guaranteed-defined variables) ---
    bad_hits = bad_hits if "bad_hits" in locals() else []

    try:
        bad_reaction = bad_news_reaction(xic=xic, spy=spy, bad_hits=bad_hits)
    except Exception as e:
        errors.append(f"Bad news reaction calc failed: {type(e).__name__}: {e}")
        bad_reaction = {
            "combined": "YELLOW",
            "reason": "bad_news_reaction_failed",
            "bad_hits": bad_hits,
        }
    
    # --- Asset correlation data ---
    xic = yahoo_adj_close("XIC.TO", period="6mo")
    hyg = yahoo_adj_close("HYG", period="6mo")
    xre = yahoo_adj_close("XRE.TO", period="6mo")
    vnq = yahoo_adj_close("VNQ", period="6mo")

    # reuse existing series where possible:
    # btc, spy already fetched for high_beta leadership
    corr = asset_correlations(xic=xic, spy=spy, hyg=hyg, xre=xre, vnq=vnq, btc=btc, lookback=10)
   
    # --- High beta leadership data (fail-soft) ---
    
    errors = []
    bad_hits = []
    
    try:
        btc = yahoo_adj_close("BTC-USD", period="6mo")
    except Exception as e:
        errors.append(f"BTC-USD fetch failed: {type(e).__name__}: {e}")

    try:
        spy = yahoo_adj_close("SPY", period="6mo")
    except Exception as e:
        errors.append(f"SPY fetch failed: {type(e).__name__}: {e}")

    try:
        qqq = yahoo_adj_close("QQQ", period="6mo")
        dia = yahoo_adj_close("DIA", period="6mo")
        iwm = yahoo_adj_close("IWM", period="6mo")
        high_beta = high_beta_leadership(btc, spy, qqq, dia, iwm)
    except Exception as e:
        errors.append(f"High-beta calc failed: {type(e).__name__}: {e}")
        high_beta = {"combined": "YELLOW", "reason": "high_beta_failed"}

    # --- Asset correlation data (fail-soft) ---
    try:
        xic = yahoo_adj_close("XIC.TO", period="6mo")
    except Exception as e:
        errors.append(f"XIC.TO fetch failed: {type(e).__name__}: {e}")

    # --- Bad news reaction (RSS + market response) ---
    try:
        news_feeds = [
            "https://www.bankofcanada.ca/rss/press-releases/",
            "https://www.federalreserve.gov/feeds/press_all.xml",
            "https://www.cbc.ca/cmlink/rss-business",
            "https://www.marketwatch.com/rss/topstories",
        ]
        news_items = fetch_recent_news(news_feeds, hours=48)
        bad_hits = detect_bad_news(news_items)
    except Exception as e:
        errors.append(f"Bad news RSS failed: {type(e).__name__}: {e}")
        bad_hits = []

    try:
        bad_reaction = bad_news_reaction(xic=xic, spy=spy, bad_hits=bad_hits)
    except Exception as e:
        errors.append(f"Bad news reaction calc failed: {type(e).__name__}: {e}")
        bad_reaction = {"combined": "YELLOW", "reason": "bad_news_reaction_failed", "bad_hits": bad_hits}
        
    try:
        bad_reaction = bad_news_reaction(
            xic=xic,
            spy=spy,
            bad_hits=bad_hits
        )
    except Exception as e:
        errors.append(f"Bad news reaction calc failed: {type(e).__name__}: {e}")
        bad_reaction = {
            "combined": "YELLOW",
            "reason": "bad_news_reaction_failed",
            "bad_hits": bad_hits
        }
        hyg = yahoo_adj_close("HYG", period="6mo")
        xre = yahoo_adj_close("XRE.TO", period="6mo")
        vnq = yahoo_adj_close("VNQ", period="6mo")

        corr = asset_correlations(xic=xic, spy=spy, hyg=hyg, xre=xre, vnq=vnq, btc=btc, lookback=10)
    except Exception as e:
        errors.append(f"Asset correlation calc failed: {type(e).__name__}: {e}")
        corr = {"combined": "YELLOW", "reason": "asset_corr_failed"}

        # --- Policy actions (BoC + Fed) via RSS (fail-soft) ---
    try:
        boc_feed = "https://www.bankofcanada.ca/rss/press-releases/"
        fed_feed = "https://www.federalreserve.gov/feeds/press_all.xml"

        boc_items = fetch_recent_feed_items(boc_feed, hours=48)
        fed_items = fetch_recent_feed_items(fed_feed, hours=48)

        policy = policy_actions_indicator(boc_items, fed_items)
    except Exception as e:
        errors.append(f"Policy RSS failed: {type(e).__name__}: {e}")
        policy = {"combined": "YELLOW", "reason": "policy_failed"}

        # --- Bad news reaction (RSS + market response) ---
    try:
        news_feeds = [
            "https://www.bankofcanada.ca/rss/press-releases/",
            "https://www.federalreserve.gov/feeds/press_all.xml",
            "https://www.cbc.ca/cmlink/rss-business",
            "https://www.marketwatch.com/rss/topstories",
        ]
        news_items = fetch_recent_news(news_feeds, hours=48)
        bad_hits = detect_bad_news(news_items)
    except Exception as e:
        errors.append(f"Bad news RSS failed: {type(e).__name__}: {e}")
        bad_hits = []
        
    results = {
        "credit_stress": credit,
        "real_yields": real_yields,
        "high_beta": high_beta,
        "asset_correlations": corr,
        "policy_actions": policy,
        "bad_news_reaction": bad_reaction,
    }

    status_map = {
        "credit_stress": credit["combined"],
        "policy_actions": policy["combined"],
        "asset_correlations": corr["combined"],
        "real_yields": real_yields["combined"],
        "bad_news_reaction": bad_reaction["combined"],
        "high_beta": high_beta["combined"],
    }

    green_count = sum(1 for v in status_map.values() if v == "GREEN")

    state = load_state()
    state = add_run(state, green_count=green_count, statuses=status_map)
    risk_window_opening, stand_down_persist = compute_persistence_flags(state)
    save_state(state)
    history_bar = last_n_summary(state, n=12)
    results.setdefault("meta", {})
    results["meta"]["history_bar"] = history_bar

    override_reasons = []
    if status_map["credit_stress"] == "RED":
        override_reasons.append("credit_stress=RED")
    if status_map["asset_correlations"] == "RED":
        override_reasons.append("asset_correlations=RED")
    if status_map["policy_actions"] == "RED":
        override_reasons.append("policy_actions=RED")

    stand_down_override = len(override_reasons) > 0
    stand_down_active = stand_down_override or stand_down_persist

    results["meta"] = {
        "green_count": green_count,
        "risk_window_opening": risk_window_opening,
        "stand_down_active": stand_down_active,
        "stand_down_reason": (
            "override: " + ", ".join(override_reasons)
            if stand_down_override
            else ("persistence (≤2 greens for 5 runs)" if stand_down_persist else "none")
        ),
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
