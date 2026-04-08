"""Google Analytics 4 tracking for Streamlit pages."""

import streamlit.components.v1 as components

GA4_ID = "G-E6YX8DRY3Z"


def inject_ga4():
    """Inject GA4 tracking into parent document head, escaping iframe sandbox."""
    components.html(f"""
    <script>
        // Escape the iframe and inject into parent document
        if (!window.parent.document.querySelector('script[src*="googletagmanager"]')) {{
            var script = document.createElement('script');
            script.async = true;
            script.src = 'https://www.googletagmanager.com/gtag/js?id={GA4_ID}';
            window.parent.document.head.appendChild(script);

            script.onload = function() {{
                window.parent.dataLayer = window.parent.dataLayer || [];
                function gtag(){{window.parent.dataLayer.push(arguments);}}
                gtag('js', new Date());
                gtag('config', '{GA4_ID}');
            }};
        }}
    </script>
    """, height=0, width=0)
