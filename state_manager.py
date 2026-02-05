import json
from datetime import datetime, timezone
from pathlib import Path


STATE_PATH = Path("state/history.json")


def load_state():
    if not STATE_PATH.exists():
        return {"runs": []}
    return json.loads(STATE_PATH.read_text())


def save_state(state):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2))


def add_run(state, green_count: int, statuses: dict):
    # statuses: dict like {"credit_stress":"GREEN", ...}
    state["runs"].append({
        "ts": datetime.now(timezone.utc).isoformat(),
        "green_count": green_count,
        "statuses": statuses
    })
    # Keep last 60 runs max
    state["runs"] = state["runs"][-60:]
    return state


def last_n_runs(state, n: int):
    return state.get("runs", [])[-n:]


def compute_persistence_flags(state):
    runs = state.get("runs", [])

    def last_n_all(cond, n):
        recent = runs[-n:]
        if len(recent) < n:
            return False
        return all(cond(r) for r in recent)

    risk_window_opening = last_n_all(lambda r: r["green_count"] >= 4, 10)
    stand_down_persist = last_n_all(lambda r: r["green_count"] <= 2, 5)

    return risk_window_opening, stand_down_persist

def last_n_summary(state, n: int = 12):
    runs = state.get("runs", [])[-n:]
    if not runs:
        return ""

    # Map green_count to a simple character
    def gc_char(gc):
        if gc >= 4:
            return "G"
        if gc <= 2:
            return "R"
        return "Y"

    return "".join(gc_char(r.get("green_count", 0)) for r in runs)
