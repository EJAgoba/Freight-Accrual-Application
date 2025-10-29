# theme.py
from constants import (
   PRIMARY, PRIMARY_HOVER, ACCENT, DANGER, TEXT, TEXT_MUTED, BORDER, SURFACE, SURFACE_ALT, RING,
   D_TEXT, D_TEXT_MUTED, D_BORDER, D_SURFACE, D_SURFACE_ALT, D_RING
)
def theme_css(mode: str = "light") -> str:
   """Custom CSS with strong dark-mode contrast for uploader text."""
   light_vars = f"""
     --text:{TEXT}; --text-muted:{TEXT_MUTED}; --border:{BORDER};
     --surface:{SURFACE}; --surface-alt:{SURFACE_ALT};
     --primary:{PRIMARY}; --primary-hover:{PRIMARY_HOVER};
     --accent:{ACCENT}; --danger:{DANGER}; --ring:{RING};
     --uploader-bg:#ffffff; --uploader-border:{BORDER}; --uploader-fg:#111111;
     --heading:#0b1220; --field-label:#111827;
   """
   dark_vars = f"""
     --text:{D_TEXT}; --text-muted:{D_TEXT_MUTED}; --border:{D_BORDER};
     --surface:{D_SURFACE}; --surface-alt:{D_SURFACE_ALT};
     --primary:{PRIMARY}; --primary-hover:{PRIMARY_HOVER};
     --accent:{ACCENT}; --danger:{DANGER}; --ring:{D_RING};
     --uploader-bg:#0d1726; --uploader-border:#223047; --uploader-fg:#000000;
     --heading:#eaf1ff; --field-label:#f3f4f6;
   """
   vars_block = light_vars if mode == "light" else dark_vars
   return f"""
<style>
 :root {{
   {vars_block}
 }}
 main .block-container {{
   padding-top: 2.5rem !important;
   padding-bottom: 1.5rem;
 }}
 html, body, [class^="stApp"] {{
   color: var(--text);
   background: var(--surface-alt);
   font-family: "Inter", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
 }}
 header[data-testid="stHeader"] {{
   background: transparent !important;
   border: none !important;
   box-shadow: none !important;
 }}
 /* --- Header (hero) --- */
 .page-top-spacer {{ height: 10px; }}
 .app-header {{
   background: linear-gradient(135deg, color-mix(in oklab, var(--primary) 85%, #0b1020) 0%,
                                        color-mix(in oklab, var(--primary-hover) 85%, #0b1020) 100%);
   border-radius: 16px;
   padding: 1.75rem 2rem;
   margin-bottom: 1.25rem;
   box-shadow: 0 10px 30px rgba(2, 6, 23, 0.18);
   color: white;
   position: relative;
   overflow: hidden;
 }}
 .app-header::after {{
   content: "";
   position: absolute; inset: 0;
   background:
     radial-gradient(900px 300px at -10% -30%, rgba(255,255,255,.14) 0%, transparent 70%),
     radial-gradient(700px 250px at 120% -40%, rgba(255,255,255,.10) 0%, transparent 75%);
   mix-blend-mode: overlay;
   pointer-events: none;
 }}
 .app-title {{
   font-weight: 800;
   font-size: 1.55rem;
   margin-bottom: 0.25rem;
   letter-spacing: 0.4px;
   color: var(--heading);
 }}
 .app-subtitle {{
   font-weight: 500;
   opacity: .96;
   color: rgba(255,255,255,.92);
   font-size: .98rem;
   max-width: 980px;
 }}
 h1, h2, h3, h4, h5, h6 {{ color: var(--heading); }}
 /* --- Buttons --- */
 .stButton>button {{
   background: var(--primary) !important;
   color: #ffffff !important;
   border: none !important;
   border-radius: 10px !important;
   padding: 0.6rem 1rem !important;
   font-weight: 600; transition: all 0.15s ease;
 }}
 .stButton>button:hover {{
   background: var(--primary-hover) !important;
   transform: translateY(-1px);
   box-shadow: 0 6px 14px rgba(0,0,0,.18);
 }}
 .stDownloadButton>button {{
   background: var(--accent) !important; color: #062016 !important;
   border: none !important; border-radius: 10px !important;
   padding: 0.6rem 1rem !important; font-weight: 700;
 }}
 .stDownloadButton>button:hover {{ filter: brightness(0.96); transform: translateY(-1px); }}
 /* --- Field Labels --- */
 [data-testid="stWidgetLabel"], [data-testid="stWidgetLabel"] * {{
   color: var(--field-label) !important;
   opacity: 1 !important;
 }}
 .stRadio, .stRadio label, .stRadio div[role="radiogroup"] *, .stRadio p {{
   color: var(--field-label) !important;
 }}
 /* --- Inputs / Selects --- */
 .stTextInput > div > div > input,
 .stTextArea textarea,
 .stSelectbox > div > div {{
   background: var(--surface);
   border: 1px solid var(--border);
   border-radius: 10px;
   font-size: 0.95rem;
   color: var(--text);
 }}
 .stTextInput > div > div > input::placeholder,
 .stTextArea textarea::placeholder {{ color: var(--text-muted); }}
 .stTextInput > div > div > input:focus,
 .stTextArea textarea:focus,
 .stSelectbox > div > div:focus-within {{
   outline: none;
   box-shadow: 0 0 0 3px var(--ring);
   border-color: transparent;
 }}
 /* --- File Uploader --- */
 [data-testid="stFileUploader"] > div {{
   background: var(--uploader-bg) !important;
   border: 1px dashed var(--uploader-border) !important;
   border-radius: 12px !important;
 }}
 /* Force ALL uploader text and sublabels to strong black in dark mode */
 [data-testid="stFileUploader"] *:not(svg) {{
   color: var(--uploader-fg) !important;
   font-weight: 600 !important;
 }}
 [data-testid="stFileUploader"] small,
 [data-testid="stFileUploader"] span {{
   color: var(--uploader-fg) !important;
   font-weight: 600 !important;
 }}
 [data-testid="stFileUploader"] svg {{ fill: var(--uploader-fg) !important; }}
 .stAlert > div {{
   border-radius: 10px;
   border: 1px solid var(--border);
   background: color-mix(in oklab, var(--surface) 78%, var(--accent) 22%);
 }}
</style>
"""
