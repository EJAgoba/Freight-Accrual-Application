# theme.py

from constants import (

    PRIMARY, PRIMARY_HOVER, ACCENT, DANGER, TEXT, TEXT_MUTED, BORDER, SURFACE, SURFACE_ALT, RING,

    D_TEXT, D_TEXT_MUTED, D_BORDER, D_SURFACE, D_SURFACE_ALT, D_RING

)

def theme_css(mode: str = "light") -> str:

    """

    Returns CSS for 'light' or 'dark' mode. Clean, modern, and fully responsive.

    """

    light_vars = f"""

      --text:{TEXT}; --text-muted:{TEXT_MUTED}; --border:{BORDER};

      --surface:{SURFACE}; --surface-alt:{SURFACE_ALT};

      --primary:{PRIMARY}; --primary-hover:{PRIMARY_HOVER};

      --accent:{ACCENT}; --danger:{DANGER}; --ring:{RING};

    """

    dark_vars = f"""

      --text:{D_TEXT}; --text-muted:{D_TEXT_MUTED}; --border:{D_BORDER};

      --surface:{D_SURFACE}; --surface-alt:{D_SURFACE_ALT};

      --primary:{PRIMARY}; --primary-hover:{PRIMARY_HOVER};

      --accent:{ACCENT}; --danger:{DANGER}; --ring:{D_RING};

    """

    vars_block = light_vars if mode == "light" else dark_vars

    return f"""
<style>

  :root {{

    {vars_block}

  }}

  /* ====== Layout ====== */

  main .block-container {{

    padding-top: 2.5rem !important; /* Prevent header cut-off */

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

  /* ====== App Header ====== */

  .page-top-spacer {{ height: 10px; }}

  .app-header {{

    background: linear-gradient(135deg, var(--primary) 0%, var(--primary-hover) 100%);

    border-radius: 16px;

    padding: 1.75rem 2rem;

    margin-bottom: 1.25rem;

    box-shadow: 0 6px 25px rgba(0, 0, 0, 0.08);

    color: white;

    position: relative;

    overflow: hidden;

  }}

  .app-header::after {{

    content: "";

    position: absolute;

    inset: 0;

    background: radial-gradient(circle at top left, rgba(255,255,255,0.15) 0%, transparent 70%);

    mix-blend-mode: overlay;

  }}

  .app-title {{

    font-weight: 800;

    font-size: 1.6rem;

    margin-bottom: 0.25rem;

    letter-spacing: 0.5px;

  }}

  .app-subtitle {{

    font-weight: 400;

    opacity: 0.92;

    color: rgba(255, 255, 255, 0.9);

    font-size: 0.96rem;

    max-width: 900px;

  }}

  /* ====== Cards ====== */

  .cintas-card {{

    background: var(--surface);

    border: 1px solid var(--border);

    border-radius: 12px;

    padding: 16px;

    box-shadow: 0 2px 4px rgba(0,0,0,0.03);

  }}

  /* ====== Buttons ====== */

  .stButton>button {{

    background: var(--primary) !important; color: white !important; border: none !important;

    border-radius: 10px !important; padding: 0.6rem 1rem !important;

    font-weight: 600; transition: all 0.15s ease;

  }}

  .stButton>button:hover {{

    background: var(--primary-hover) !important;

    transform: translateY(-1px);

    box-shadow: 0 3px 6px rgba(0,0,0,0.08);

  }}

  /* Download buttons */

  .stDownloadButton>button {{

    background: var(--accent) !important; color: #002d1f !important;

    border: none !important; border-radius: 10px !important;

    padding: 0.6rem 1rem !important; font-weight: 600;

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

  }}

  .stTextInput > div > div > input:focus,

  .stTextArea textarea:focus,

  .stSelectbox > div > div:focus-within {{

    outline: none;

    box-shadow: 0 0 0 3px var(--ring);

    border-color: transparent;

  }}

  /* ====== Alerts ====== */

  .stAlert > div {{

    border-radius: 10px;

    border: 1px solid var(--border);

    background: color-mix(in oklab, var(--surface) 85%, var(--accent) 15%);

  }}

  /* ====== Scrollbar ====== */

  ::-webkit-scrollbar {{ height: 10px; width: 10px; }}

  ::-webkit-scrollbar-thumb {{

    background: color-mix(in oklab, var(--text) 15%, transparent);

    border-radius: 12px;

  }}
</style>

"""
 
