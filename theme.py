# theme.py

from constants import (

    PRIMARY, PRIMARY_HOVER, ACCENT, DANGER, TEXT, TEXT_MUTED, BORDER, SURFACE, SURFACE_ALT, RING,

    D_TEXT, D_TEXT_MUTED, D_BORDER, D_SURFACE, D_SURFACE_ALT, D_RING

)

def theme_css(mode: str = "light") -> str:

    """

    Returns CSS for 'light' or 'dark'. Call with st.markdown(theme_css(mode), unsafe_allow_html=True)

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

  /* Layout + Typography */

  .block-container {{ padding-top: 0.75rem; padding-bottom: 1.25rem; }}

  html, body, [class^="stApp"] {{

    color: var(--text);

    background: var(--surface-alt);

  }}

  /* Header */

  .app-header {{

    background: linear-gradient(180deg, color-mix(in oklab, var(--primary) 18%, transparent) 0%, transparent 100%),

                var(--surface);

    border: 1px solid var(--border);

    border-radius: 14px;

    padding: 18px 20px;

    margin-bottom: 1rem;

    box-shadow: 0 1px 0 rgba(0,0,0,.03), 0 6px 20px rgba(2,6,23,.04);

  }}

  .app-title {{ font-weight: 700; font-size: 1.1rem; letter-spacing: .2px; }}

  .app-subtitle {{ opacity:.85; color: var(--text-muted); margin-top: 4px; }}

  /* Cards */

  .cintas-card {{

    background: var(--surface);

    border: 1px solid var(--border);

    border-radius: 12px;

    padding: 14px;

    box-shadow: 0 1px 0 rgba(0,0,0,.03), 0 10px 22px rgba(2,6,23,.04);

  }}

  /* Buttons */

  .stButton>button {{

    background: var(--primary) !important; color: white !important; border: 0 !important;

    border-radius: 10px !important; padding: 0.6rem 0.9rem !important;

    box-shadow: 0 2px 0 rgba(0,0,0,.05);

  }}

  .stButton>button:hover {{ background: var(--primary-hover) !important; transform: translateY(-1px); }}

  /* Download buttons */

  .stDownloadButton>button {{

    background: var(--accent) !important; color: #00150f !important; border: 0 !important;

    border-radius: 10px !important; padding: 0.6rem 0.9rem !important;

  }}

  .stDownloadButton>button:hover {{ filter: brightness(0.96); transform: translateY(-1px); }}

  /* Radios/Selects to pill style */

  div[role="radiogroup"] > label, .stSelectbox > div > div {{

    border-radius: 999px !important;

  }}

  .stRadio > label {{ font-weight: 600; color: var(--text); }}

  /* Inputs */

  .stTextInput > div > div > input,

  .stTextArea textarea,

  .stSelectbox > div > div {{

    background: var(--surface);

    border: 1px solid var(--border);

    border-radius: 10px;

  }}

  .stTextInput > div > div > input:focus,

  .stTextArea textarea:focus,

  .stSelectbox > div > div:focus-within {{

    outline: none;

    box-shadow: 0 0 0 3px var(--ring);

    border-color: transparent;

  }}

  /* File Uploader */

  .uploadedFile {{ color: var(--text-muted) !important; }}

  /* Tables */

  .stDataFrame, .stTable {{ border-radius: 12px; overflow: hidden; }}

  .stDataFrame [data-testid="stTable"] td, .stDataFrame [data-testid="stTable"] th {{

    border-color: var(--border) !important;

  }}

  /* Alerts */

  .stAlert > div {{

    border-radius: 12px; border: 1px solid var(--border);

    background: color-mix(in oklab, var(--surface) 85%, var(--accent) 15%);

  }}

  /* Subtle scrollbar */

  ::-webkit-scrollbar {{ height: 10px; width: 10px; }}

  ::-webkit-scrollbar-thumb {{ background: color-mix(in oklab, var(--text) 15%, transparent); border-radius: 12px; }}
</style>

"""
 
