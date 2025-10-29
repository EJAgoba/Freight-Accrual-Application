# theme.py

from constants import (

    PRIMARY, PRIMARY_HOVER, ACCENT, DANGER, TEXT, TEXT_MUTED, BORDER, SURFACE, SURFACE_ALT, RING,

    D_TEXT, D_TEXT_MUTED, D_BORDER, D_SURFACE, D_SURFACE_ALT, D_RING

)

def theme_css(mode: str = "light") -> str:

    """

    Returns CSS for 'light' or 'dark' mode.

    Dark mode fixes:

      - Deeper page background & cards

      - Higher-contrast text/subtitle

      - Dark-styled inputs/selects/uploaders/radios

      - Hero header tuned for dark

    """

    light_vars = f"""

      --text:{TEXT}; --text-muted:{TEXT_MUTED}; --border:{BORDER};

      --surface:{SURFACE}; --surface-alt:{SURFACE_ALT};

      --primary:{PRIMARY}; --primary-hover:{PRIMARY_HOVER};

      --accent:{ACCENT}; --danger:{DANGER}; --ring:{RING};

      --uploader-bg:#ffffff; --uploader-border:{BORDER}; --uploader-fg:{TEXT_MUTED};

      --heading:#0b1220;

    """

    dark_vars = f"""

      --text:{D_TEXT}; --text-muted:{D_TEXT_MUTED}; --border:{D_BORDER};

      --surface:{D_SURFACE}; --surface-alt:{D_SURFACE_ALT};

      --primary:{PRIMARY}; --primary-hover:{PRIMARY_HOVER};

      --accent:{ACCENT}; --danger:{DANGER}; --ring:{D_RING};

      --uploader-bg:#0d1726; --uploader-border:#223047; --uploader-fg:#9fb0c8;

      --heading:#eaf1ff;

    """

    vars_block = light_vars if mode == "light" else dark_vars

    return f"""
<style>

  :root {{

    {vars_block}

  }}

  /* ====== Layout ====== */

  main .block-container {{

    padding-top: 2.5rem !important;  /* fixes header clipping */

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

  /* ====== App Header (hero) ====== */

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

    position: absolute;

    inset: 0;

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

  /* ====== Section headings ====== */

  h1, h2, h3, h4, h5, h6 {{ color: var(--heading); }}

  /* ====== Cards ====== */

  .cintas-card {{

    background: var(--surface);

    border: 1px solid var(--border);

    border-radius: 12px;

    padding: 16px;

    box-shadow: 0 2px 10px rgba(2, 6, 23, 0.18);

  }}

  /* ====== Buttons ====== */

  .stButton>button {{

    background: var(--primary) !important; color: #ffffff !important; border: none !important;

    border-radius: 10px !important; padding: 0.6rem 1rem !important;

    font-weight: 600; transition: all 0.15s ease;

  }}

  .stButton>button:hover {{

    background: var(--primary-hover) !important;

    transform: translateY(-1px);

    box-shadow: 0 6px 14px rgba(0,0,0,.18);

  }}

  /* Download buttons */

  .stDownloadButton>button {{

    background: var(--accent) !important; color: #062016 !important;

    border: none !important; border-radius: 10px !important;

    padding: 0.6rem 1rem !important; font-weight: 700;

  }}

  .stDownloadButton>button:hover {{ filter: brightness(0.96); transform: translateY(-1px); }}

  /* ====== Inputs / Selects / Radios ====== */

  div[role="radiogroup"] > label, .stSelectbox > div > div {{

    border-radius: 999px !important;

  }}

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

  /* ====== File Uploader (dark-friendly) ====== */

  [data-testid="stFileUploader"] > div {{

    background: var(--uploader-bg) !important;

    border: 1px dashed var(--uploader-border) !important;

    color: var(--uploader-fg) !important;

    border-radius: 12px !important;

  }}

  [data-testid="stFileUploader"] svg {{ fill: var(--uploader-fg) !important; }}

  /* ====== Tables ====== */

  .stDataFrame, .stTable {{ border-radius: 12px; overflow: hidden; }}

  .stDataFrame [data-testid="stTable"] td, .stDataFrame [data-testid="stTable"] th {{

    border-color: var(--border) !important;

  }}

  /* ====== Alerts ====== */

  .stAlert > div {{

    border-radius: 10px;

    border: 1px solid var(--border);

    background: color-mix(in oklab, var(--surface) 78%, var(--accent) 22%);

  }}

  /* ====== Scrollbar ====== */

  ::-webkit-scrollbar {{ height: 10px; width: 10px; }}

  ::-webkit-scrollbar-thumb {{

    background: color-mix(in oklab, var(--text) 16%, transparent);

    border-radius: 12px;

  }}
</style>

"""
