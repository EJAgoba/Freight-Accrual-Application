# theme.py

from constants import (

    PRIMARY, PRIMARY_HOVER, ACCENT, DANGER, TEXT, TEXT_MUTED, BORDER, SURFACE, SURFACE_ALT, RING

)

def theme_css() -> str:

    """Light theme only. Clean header, good contrast, no dark mode."""

    return f"""
<style>

  :root {{

    --text:{TEXT};

    --text-muted:{TEXT_MUTED};

    --border:{BORDER};

    --surface:{SURFACE};

    --surface-alt:{SURFACE_ALT};

    --primary:{PRIMARY};

    --primary-hover:{PRIMARY_HOVER};

    --accent:{ACCENT};

    --danger:{DANGER};

    --ring:{RING};

    --heading:#0b1220;

    --field-label:#111827;

    --uploader-bg:#ffffff;

    --uploader-border:{BORDER};

    --uploader-fg:#111111;

  }}

  /* Layout */

  main .block-container {{ padding-top: 2.4rem !important; padding-bottom: 1.4rem; }}

  html, body, [class^="stApp"] {{

    color: var(--text);

    background: var(--surface-alt);

    font-family: "Inter", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;

  }}

  header[data-testid="stHeader"] {{ background: transparent !important; border: 0 !important; box-shadow: none !important; }}

  /* Hero header (rendered in app.py) */

  .app-header {{

    background: linear-gradient(145deg, color-mix(in oklab, var(--primary) 90%, #0b1020) 0%,

                                         color-mix(in oklab, var(--primary-hover) 85%, #0b1020) 100%);

    border-radius: 16px;

    padding: 1.8rem 2rem 2rem;

    margin-bottom: 1.25rem;

    box-shadow: 0 10px 30px rgba(2, 6, 23, 0.14);

    text-align: center;

    position: relative;

    overflow: hidden;

  }}

  .app-header::after {{

    content:"";

    position:absolute; inset:0;

    background:

      radial-gradient(900px 300px at -10% -30%, rgba(255,255,255,.14) 0%, transparent 70%),

      radial-gradient(700px 250px at 120% -40%, rgba(255,255,255,.10) 0%, transparent 75%);

    mix-blend-mode: overlay; pointer-events:none;

  }}

  .app-logo {{ height: 56px; width: auto; display:block; margin: 0 auto 10px; filter: drop-shadow(0 3px 8px rgba(0,0,0,.25)); }}

  .app-title {{ font-weight: 800; font-size: 1.55rem; color: #fff; margin-bottom: .25rem; letter-spacing:.3px; }}

  .app-subtitle {{ color: rgba(255,255,255,.95); font-weight: 500; font-size: .98rem; max-width: 980px; margin: 0 auto; }}

  /* Section headings */

  h1, h2, h3, h4, h5, h6 {{ color: var(--heading); }}

  /* Buttons */

  .stButton>button {{

    background: var(--primary) !important; color: #fff !important; border: 0 !important;

    border-radius: 10px !important; padding: .6rem 1rem !important; font-weight: 600;

    transition: all .15s ease;

  }}

  .stButton>button:hover {{ background: var(--primary-hover) !important; transform: translateY(-1px); box-shadow:0 6px 14px rgba(0,0,0,.12); }}

  .stDownloadButton>button {{

    background: var(--accent) !important; color: #062016 !important; border: 0 !important;

    border-radius: 10px !important; padding: .6rem 1rem !important; font-weight: 700;

  }}

  .stDownloadButton>button:hover {{ filter: brightness(.97); transform: translateY(-1px); }}

  /* Field labels */

  [data-testid="stWidgetLabel"], [data-testid="stWidgetLabel"] * {{ color: var(--field-label) !important; opacity:1 !important; }}

  .stRadio, .stRadio label, .stRadio div[role="radiogroup"] *, .stRadio p {{ color: var(--field-label) !important; }}

  /* Inputs / selects */

  .stTextInput > div > div > input,

  .stTextArea textarea,

  .stSelectbox > div > div {{

    background: var(--surface);

    border: 1px solid var(--border);

    border-radius: 10px;

    font-size: .95rem;

    color: var(--text);

  }}

  .stTextInput > div > div > input::placeholder, .stTextArea textarea::placeholder {{ color: var(--text-muted); }}

  .stTextInput > div > div > input:focus, .stTextArea textarea:focus, .stSelectbox > div > div:focus-within {{

    outline:none; box-shadow:0 0 0 3px var(--ring); border-color: transparent;

  }}

  /* File uploader */

  [data-testid="stFileUploader"] > div {{

    background: var(--uploader-bg) !important;

    border: 1px dashed var(--uploader-border) !important;

    border-radius: 12px !important;

  }}

  [data-testid="stFileUploader"] *:not(svg),

  [data-testid="stFileUploader"] small,

  [data-testid="stFileUploader"] span {{ color: var(--uploader-fg) !important; font-weight: 600 !important; }}

  [data-testid="stFileUploader"] svg {{ fill: var(--uploader-fg) !important; }}
</style>

"""
 
