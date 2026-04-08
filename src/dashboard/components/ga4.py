"""Google Analytics 4 tracking for Streamlit pages."""

import streamlit.components.v1 as components

GA4_ID = "G-E6YX8DRY3Z"


def inject_ga4():
    """Inject Google Analytics 4 tracking code using components.html (JS executes in iframe)."""
    ga_code = f"""
    <script async src="https://www.googletagmanager.com/gtag/js?id={GA4_ID}"></script>
    <script>
        window.dataLayer = window.dataLayer || [];
        function gtag(){{dataLayer.push(arguments);}}
        gtag('js', new Date());
        gtag('config', '{GA4_ID}');
    </script>
    """
    components.html(ga_code, height=0, width=0)
