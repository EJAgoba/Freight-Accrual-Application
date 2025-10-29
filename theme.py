# theme.py
from constants import (
   PRIMARY, PRIMARY_HOVER, ACCENT, DANGER, TEXT, TEXT_MUTED, BORDER, SURFACE, SURFACE_ALT, RING,
   D_TEXT, D_TEXT_MUTED, D_BORDER, D_SURFACE, D_SURFACE_ALT, D_RING
)
def theme_css(mode: str = "light") -> str:
   """Polished theme with strong dark-mode readability; no HTML injection."""
   light_vars = f"""
     --text:{TEXT}; --text-muted:{TEXT_MUTED}; --border:{BORDER};
     --surface:{SURFACE}; --surface-alt:{SURFACE_ALT};
     --primary:{PRIMARY}; --primary-hover:{PRIMARY_HOVER};
     --accent:{ACCENT}; --danger:{DANGER}; --ring:{RING};
     --uploader-bg:#ffffff; --uploader-border:{BORDER}; --uploader-fg:#111111;
     --heading:#0b1220; --field-label:#111827;
     --icon-btn-bg:#eef2ff; --icon-btn-bg-hover:#e0e7ff; --icon-btn-icon:#1f2937;
   """
   dark_vars = f"""
     --text:{D_TEXT}; --text-muted:{D_TEXT_MUTED}; --border:{D_BORDER};
     --surface:{D_SURFACE}; --surface-alt:{D_SURFACE_ALT};
     --primary:{PRIMARY}; --primary-hover:{PRIMARY_HOVER};
     --accent:{ACCENT}; --danger:{DANGER}; --ring:{D_RING};
     --uploader-bg:#1b2636; --uploader-border:#223047; --uploader-fg:#000000;  /* strong black text */
     --heading:#eaf1ff; --field-label:#f3f4f6;
     --icon-btn-bg:#273449; --icon-btn-bg-hover:#334155; --icon-btn-icon:#e5e7eb;
   """
   vars_block = light_vars if mode == "light" else dark_vars
   return f"""
<style>
 :root {{ {vars_block} }}
 /* Layout */
 main .block-container {{ padding-top: 2.6rem !important; padding-bottom: 1.5rem; }}
 html, body, [class^="stApp"] {{
   color: var(--text); background: var(--surface-alt);
   font-family: "Inter", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
 }}
 header[data-testid="stHeader"] {{ background: transparent !important; border: none !important; box-shadow: none !important; }}
 /* Hero header styles (HTML rendered in app.py) */
 .app-header {{
   background: linear-gradient(145deg,
       color-mix(in oklab, var(--primary) 90%, #0b1020) 0%,
       color-mix(in oklab, var(--primary-hover) 85%, #0b1020) 100%);
   border-radius: 16px;
   padding: 2.0rem 2rem 2.3rem;
   margin-bottom: 1.25rem;
   box-shadow: 0 10px 30px rgba(2, 6, 23, 0.18);
   position: relative;
   text-align: center;
   overflow: hidden;
 }}
 .app-header::after {{
   content: "";
   position: absolute; inset: 0;
   background:
     radial-gradient(900px 300px at -10% -30%, rgba(255,255,255,.14) 0%, transparent 70%),
     radial-gradient(700px 250px at 120% -40%, rgba(255,255,255,.10) 0%, transparent 75%);
   mix-blend-mode: overlay; pointer-events: none;
 }}
 .app-logo {{
   height: 60px; width: auto; display: block; margin: 0 auto 10px auto;
   filter: drop-shadow(0 3px 8px rgba(0,0,0,0.25));
 }}
 .app-title {{ font-weight: 800; font-size: 1.55rem; margin-bottom: 0.25rem; letter-spacing: .4px; color: var(--heading); }}
 .app-subtitle {{ font-weight: 500; opacity: .96; color: rgba(255,255,255,.92); font-size: .98rem; max-width: 980px; margin: 0 auto; }}
 .page-top-spacer {{ height: 10px; }}
 /* Section headings */
 h1, h2, h3, h4, h5, h6 {{ color: var(--heading); }}
 /* Buttons */
 .stButton>button {{
   background: var(--primary) !important; color: #fff !important; border: none !important;
   border-radius: 10px !important; padding: .6rem 1rem !important; font-weight: 600;
   transition: all .15s ease;
 }}
 .stButton>button:hover {{ background: var(--primary-hover) !important; transform: translateY(-1px); box-shadow: 0 6px 14px rgba(0,0,0,.18); }}
 .stDownloadButton>button {{
   background: var(--accent) !important; color: #062016 !important; border: none !important;
   border-radius: 10px !important; padding: .6rem 1rem !important; font-weight: 700;
 }}
 .stDownloadButton>button:hover {{ filter: brightness(.96); transform: translateY(-1px); }}
 /* Field labels */
 [data-testid="stWidgetLabel"], [data-testid="stWidgetLabel"] * {{ color: var(--field-label) !important; opacity: 1 !important; }}
 .stRadio, .stRadio label, .stRadio div[role="radiogroup"] *, .stRadio p {{ color: var(--field-label) !important; }}
 /* Inputs / selects */
 .stTextInput > div > div > input,
 .stTextArea textarea,
 .stSelectbox > div > div {{
   background: var(--surface); border: 1px solid var(--border); border-radius: 10px;
   font-size: .95rem; color: var(--text);
 }}
 .stTextInput > div > div > input::placeholder, .stTextArea textarea::placeholder {{ color: var(--text-muted); }}
 .stTextInput > div > div > input:focus, .stTextArea textarea:focus, .stSelectbox > div > div:focus-within {{
   outline: none; box-shadow: 0 0 0 3px var(--ring); border-color: transparent;
 }}
 /* File uploader: dark-friendly with strong black text */
 [data-testid="stFileUploader"] > div {{
   background: var(--uploader-bg) !important; border: 1px dashed var(--uploader-border) !important; border-radius: 12px !important;
 }}
 [data-testid="stFileUploader"] *:not(svg),
 [data-testid="stFileUploader"] small, [data-testid="stFileUploader"] span {{
   color: var(--uploader-fg) !important; font-weight: 600 !important;
 }}
 [data-testid="stFileUploader"] svg {{ fill: var(--uploader-fg) !important; }}
 /* Icon buttons (sidebar collapse, fullscreen) */
 header [data-testid="baseButton-header"],
 button[kind="header"],
 [data-testid="collapsedControl"] > div > button,
 button[title="View fullscreen"],
 button[aria-label="View fullscreen"] {{
   background: var(--icon-btn-bg) !important;
   border: 1px solid color-mix(in oklab, var(--icon-btn-bg) 70%, #000 30%) !important;
   border-radius: 10px !important; box-shadow: 0 2px 8px rgba(0,0,0,.15);
   transition: background .15s ease, transform .15s ease;
 }}
 header [data-testid="baseButton-header"]:hover,
 button[kind="header"]:hover,
 [data-testid="collapsedControl"] > div > button:hover,
 button[title="View fullscreen"]:hover,
 button[aria-label="View fullscreen"]:hover {{
   background: var(--icon-btn-bg-hover) !important; transform: translateY(-1px);
 }}
 header [data-testid="baseButton-header"] svg,
 button[kind="header"] svg,
 [data-testid="collapsedControl"] > div > button svg,
 button[title="View fullscreen"] svg,
 button[aria-label="View fullscreen"] svg {{
   fill: var(--icon-btn-icon) !important; stroke: var(--icon-btn-icon) !important;
 }}
 /* Alerts */
 .stAlert > div {{ border-radius: 10px; border: 1px solid var(--border); background: color-mix(in oklab, var(--surface) 78%, var(--accent) 22%); }}
</style>
"""
