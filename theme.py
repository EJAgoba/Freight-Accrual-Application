from constants import CINTAS_BLUE, CINTAS_RED, CINTAS_GRAY
THEME_CSS = f"""
<style>
 .block-container {{ padding-top: 1rem; padding-bottom: 1.5rem; }}
 .cintas-header {{
   background: linear-gradient(90deg, {CINTAS_BLUE} 0%, #1f5ed6 100%);
   color: white; padding: 16px 20px; border-radius: 12px; margin-bottom: 1rem;
 }}
 .stButton>button {{ background:{CINTAS_BLUE}; color:white; border:none; }}
 .stDownloadButton>button {{ background:{CINTAS_RED}; color:white; border:none; }}
 .cintas-card {{ background:{CINTAS_GRAY}; padding:14px; border-radius:10px; border:1px solid #e3e8ef; }}
</style>
"""
