import streamlit as st
import json
import time
import numpy as np
import pandas as pd
from datetime import datetime

# ── Toggle this to False to use the real Pub/Sub model ──────────────────────
MOCK_MODE = True

if not MOCK_MODE:
    from google.cloud import pubsub_v1
    from config import pubsub_topics, gcp_project_id

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(page_title="Market Prediction Dashboard", layout="wide")

# ── Custom CSS to match the mock UI style ────────────────────────────────────
st.markdown("""
<style>
    /* Global font */
    html, body, [class*="css"] {
        font-family: 'Courier New', monospace;
    }

    /* Hide default Streamlit header padding */
    .block-container { padding-top: 1.5rem; padding-bottom: 1rem; }

    /* MOCK badge */
    .mock-badge {
        display: inline-block;
        background: #fef9c3;
        color: #854d0e;
        border: 0.5px solid #fde047;
        border-radius: 12px;
        font-size: 11px;
        padding: 2px 10px;
        font-family: sans-serif;
        vertical-align: middle;
        margin-left: 8px;
    }
    .live-badge {
        display: inline-block;
        background: #dcfce7;
        color: #15803d;
        border: 0.5px solid #86efac;
        border-radius: 12px;
        font-size: 11px;
        padding: 2px 10px;
        font-family: sans-serif;
        vertical-align: middle;
        margin-left: 8px;
    }

    /* Ticker cards */
    .ticker-grid {
        display: flex;
        gap: 10px;
        flex-wrap: wrap;
        margin-bottom: 1rem;
    }
    .ticker-card {
        background: white;
        border: 0.5px solid #e2e8f0;
        border-radius: 8px;
        padding: 10px 14px;
        min-width: 110px;
        cursor: pointer;
    }
    .ticker-card.active { border: 1.5px solid #3b82f6; }
    .ticker-sym   { font-size: 12px; font-weight: 600; color: #111; font-family: monospace; }
    .ticker-price { font-size: 20px; font-weight: 500; color: #111; margin: 2px 0; }
    .ticker-chg   { font-size: 12px; }
    .chg-up  { color: #16a34a; }
    .chg-dn  { color: #dc2626; }
    .chg-nt  { color: #6b7280; }

    /* Signal card */
    .signal-up   { font-size: 28px; font-weight: 500; color: #16a34a; }
    .signal-down { font-size: 28px; font-weight: 500; color: #dc2626; }
    .signal-nt   { font-size: 28px; font-weight: 500; color: #6b7280; }

    /* Section label */
    .sec-label {
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: #6b7280;
        font-family: sans-serif;
        margin-bottom: 6px;
    }

    /* History badge */
    .badge-up { background:#dcfce7; color:#15803d; padding:2px 8px; border-radius:10px; font-size:11px; }
    .badge-dn { background:#fee2e2; color:#b91c1c; padding:2px 8px; border-radius:10px; font-size:11px; }
    .badge-nt { background:#f1f5f9; color:#475569; padding:2px 8px; border-radius:10px; font-size:11px; }
</style>
""", unsafe_allow_html=True)

# ── Session state ─────────────────────────────────────────────────────────────
TICKERS = ["SPY", "QQQ", "GLD", "DXY", "CL"]
BASE_PRICES = {"SPY": 536.40, "QQQ": 458.20, "GLD": 231.70, "DXY": 104.30, "CL": 78.90}

for key, default in [
    ("histories", {t: [] for t in TICKERS}),
    ("prices",    {t: BASE_PRICES[t] for t in TICKERS}),
    ("ticker",    "SPY"),
    ("threshold", 0.50),
]:
    if key not in st.session_state:
        st.session_state[key] = default

# ── Data functions ────────────────────────────────────────────────────────────
def pull_mock_prediction(ticker: str, threshold: float) -> dict:
    probs = np.random.dirichlet(np.ones(4)).tolist()
    labels = ["up1", "up2", "down1", "down2"]
    pred_labels = [l for l, p in zip(labels, probs) if p >= threshold]
    return {
        "timestamp": datetime.now().strftime("%H:%M:%S"),
        "ticker": ticker,
        "probabilities": probs,
        "pred_labels": pred_labels,
        "prob_threshold": threshold,
    }

if not MOCK_MODE:
    subscriber = pubsub_v1.SubscriberClient()

    def pull_latest_prediction(ticker: str, threshold: float) -> dict | None:
        sub_path = pubsub_topics['prediction'].replace('topics', 'subscriptions') + '-sub'
        response = subscriber.pull(request={"subscription": sub_path, "max_messages": 1})
        for msg in response.received_messages:
            subscriber.acknowledge(request={"subscription": sub_path, "ack_ids": [msg.ack_id]})
            data = json.loads(msg.message.data.decode("utf-8"))
            data.setdefault("prob_threshold", threshold)
            return data
        return None

def get_prediction(ticker, threshold):
    if MOCK_MODE:
        return pull_mock_prediction(ticker, threshold)
    return pull_latest_prediction(ticker, threshold)

def signal_info(labels):
    if not labels:
        return "No signal", "nt"
    is_up = any(l.startswith("up") for l in labels)
    is_dn = any(l.startswith("down") for l in labels)
    if is_up and not is_dn:
        return " + ".join(labels), "up"
    if is_dn and not is_up:
        return " + ".join(labels), "dn"
    return "Conflicted", "nt"

def update_prices():
    for t in TICKERS:
        drift = (np.random.random() - 0.48) * 0.15
        st.session_state.prices[t] = max(1.0, st.session_state.prices[t] + drift)

# ── Header ────────────────────────────────────────────────────────────────────
head_l, head_r = st.columns([3, 1])
with head_l:
    badge = '<span class="mock-badge">mock data</span>' if MOCK_MODE else '<span class="live-badge">&#9679; live</span>'
    st.markdown(
        f'<h3 style="font-family:sans-serif;font-weight:500;margin:0">Market Prediction Dashboard {badge}</h3>'
        f'<p style="font-size:12px;color:#6b7280;font-family:sans-serif;margin-top:2px">biGRU model output — directional signal per instrument</p>',
        unsafe_allow_html=True,
    )

# ── Controls ──────────────────────────────────────────────────────────────────
ctrl1, ctrl2, ctrl3 = st.columns([2, 2, 3])
with ctrl1:
    st.session_state.ticker = st.selectbox(
        "Instrument",
        TICKERS,
        index=TICKERS.index(st.session_state.ticker),
        format_func=lambda t: {
            "SPY": "SPY — S&P 500 ETF", "QQQ": "QQQ — Nasdaq ETF",
            "GLD": "GLD — Gold ETF",    "DXY": "DXY — US Dollar Index", "CL": "CL — Crude Oil"
        }[t],
    )
with ctrl2:
    refresh = st.selectbox("Refresh interval", [5, 10, 30], index=1, format_func=lambda x: f"{x}s")
with ctrl3:
    st.session_state.threshold = st.slider(
        "Probability threshold", min_value=0.30, max_value=0.80,
        value=st.session_state.threshold, step=0.05, format="%.2f"
    )

st.divider()

# ── Ticker cards ──────────────────────────────────────────────────────────────
update_prices()
cards_html = '<div class="ticker-grid">'
for t in TICKERS:
    p = st.session_state.prices[t]
    chg = (p - BASE_PRICES[t]) / BASE_PRICES[t] * 100
    cls = "chg-up" if chg > 0 else "chg-dn" if chg < 0 else "chg-nt"
    sign = "+" if chg > 0 else ""
    active = "active" if t == st.session_state.ticker else ""
    cards_html += f"""
    <div class="ticker-card {active}">
        <div class="ticker-sym">{t}</div>
        <div class="ticker-price">{p:.2f}</div>
        <div class="ticker-chg {cls}">{sign}{chg:.2f}%</div>
    </div>"""
cards_html += "</div>"
st.markdown(cards_html, unsafe_allow_html=True)

# ── Fetch prediction ──────────────────────────────────────────────────────────
pred = get_prediction(st.session_state.ticker, st.session_state.threshold)
if pred:
    st.session_state.histories[st.session_state.ticker].append(pred)
    hist = st.session_state.histories[st.session_state.ticker]
    if len(hist) > 50:
        st.session_state.histories[st.session_state.ticker] = hist[-50:]

# ── Main two-column layout ────────────────────────────────────────────────────
col1, col2 = st.columns(2)
hist = st.session_state.histories[st.session_state.ticker]

with col1:
    st.markdown('<div class="sec-label">Latest signal — ' + st.session_state.ticker + '</div>', unsafe_allow_html=True)
    if hist:
        last = hist[-1]
        sig_text, sig_cls = signal_info(last["pred_labels"])
        css_cls = {"up": "signal-up", "dn": "signal-down", "nt": "signal-nt"}[sig_cls]
        st.markdown(f'<div class="{css_cls}">{sig_text}</div>', unsafe_allow_html=True)
        st.markdown(
            f'<p style="font-size:12px;color:#6b7280;font-family:sans-serif;margin-top:4px">'
            f'@ {last["timestamp"]} &nbsp;•&nbsp; threshold {last["prob_threshold"]:.2f}</p>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown('<div class="signal-nt">Waiting...</div>', unsafe_allow_html=True)

with col2:
    st.markdown('<div class="sec-label">Probability breakdown</div>', unsafe_allow_html=True)
    if hist:
        last = hist[-1]
        labels = ["up1", "up2", "down1", "down2"]
        bar_colors = ["#1D9E75", "#5DCAA5", "#D85A30", "#F09595"]
        prob_df = pd.DataFrame({
            "label": labels,
            "probability": [round(p, 4) for p in last["probabilities"]],
            "color": bar_colors,
        })
        st.bar_chart(
            prob_df.set_index("label")["probability"],
            color="#1D9E75",
            height=180,
        )

st.divider()

# ── History table ─────────────────────────────────────────────────────────────
st.markdown('<div class="sec-label">Prediction history — ' + st.session_state.ticker + '</div>', unsafe_allow_html=True)

if hist:
    rows = list(reversed(hist[-10:]))
    table_rows = ""
    for r in rows:
        sig_text, sig_cls = signal_info(r["pred_labels"])
        badge_cls = {"up": "badge-up", "dn": "badge-dn", "nt": "badge-nt"}[sig_cls]
        p = r["probabilities"]
        table_rows += f"""
        <tr style="border-bottom:0.5px solid #e2e8f0">
            <td style="padding:6px 8px;font-size:12px;font-family:monospace;color:#374151">{r['timestamp']}</td>
            <td style="padding:6px 8px"><span class="{badge_cls}">{sig_text}</span></td>
            <td style="padding:6px 8px;font-size:12px;font-family:monospace;color:#374151">{p[0]:.3f}</td>
            <td style="padding:6px 8px;font-size:12px;font-family:monospace;color:#374151">{p[1]:.3f}</td>
            <td style="padding:6px 8px;font-size:12px;font-family:monospace;color:#374151">{p[2]:.3f}</td>
            <td style="padding:6px 8px;font-size:12px;font-family:monospace;color:#374151">{p[3]:.3f}</td>
        </tr>"""

    st.markdown(f"""
    <table style="width:100%;border-collapse:collapse">
      <thead>
        <tr style="border-bottom:0.5px solid #cbd5e1">
          <th style="padding:6px 8px;text-align:left;font-size:11px;color:#6b7280;font-family:sans-serif;font-weight:500">Timestamp</th>
          <th style="padding:6px 8px;text-align:left;font-size:11px;color:#6b7280;font-family:sans-serif;font-weight:500">Signal</th>
          <th style="padding:6px 8px;text-align:left;font-size:11px;color:#6b7280;font-family:sans-serif;font-weight:500">up1</th>
          <th style="padding:6px 8px;text-align:left;font-size:11px;color:#6b7280;font-family:sans-serif;font-weight:500">up2</th>
          <th style="padding:6px 8px;text-align:left;font-size:11px;color:#6b7280;font-family:sans-serif;font-weight:500">down1</th>
          <th style="padding:6px 8px;text-align:left;font-size:11px;color:#6b7280;font-family:sans-serif;font-weight:500">down2</th>
        </tr>
      </thead>
      <tbody>{table_rows}</tbody>
    </table>
    """, unsafe_allow_html=True)
else:
    st.markdown('<p style="font-size:13px;color:#6b7280;font-family:sans-serif">No predictions yet.</p>', unsafe_allow_html=True)

# ── Auto-refresh ──────────────────────────────────────────────────────────────
time.sleep(refresh)
st.rerun()