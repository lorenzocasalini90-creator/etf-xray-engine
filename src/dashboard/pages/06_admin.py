import streamlit as st
from src.dashboard.components.analytics import get_stats
from src.dashboard.components.ga4 import inject_ga4
inject_ga4()
from collections import Counter
from datetime import datetime

st.set_page_config(page_title="Admin", page_icon="🔒")
st.title("🔒 Admin")

# Password protection
password = st.text_input("Password", type="password")
if password != "xray2026":
    st.warning("Inserisci la password per accedere.")
    st.stop()

# Show stats
stats = get_stats()
st.success(f"Visite totali: {stats['total']}")

if stats['visits']:
    # Visits per page
    page_counts = Counter(v['page'] for v in stats['visits'])
    st.subheader("Visite per pagina")
    for page, count in page_counts.most_common():
        st.write(f"**{page}**: {count}")

    # Last 20 visits
    st.subheader("Ultime 20 visite")
    recent = stats['visits'][-20:][::-1]
    for v in recent:
        ts = v.get('ts', 'N/A')
        try:
            dt = datetime.fromisoformat(ts)
            ts_display = dt.strftime("%d/%m %H:%M")
        except Exception:
            ts_display = ts
        st.write(f"{ts_display} — {v['page']}")

    # Visits today
    today = datetime.now().date()
    today_visits = [v for v in stats['visits'] if v.get('ts', '').startswith(today.isoformat())]
    st.metric("Visite oggi", len(today_visits))
else:
    st.info("Nessuna visita registrata. I dati si resettano ad ogni redeploy.")
