import streamlit as st

st.set_page_config(page_title="Netflix", layout="wide")

pg = st.navigation([
    st.Page("netflix-dashboard.py", title="Netflix Anaytics", icon=":material/chat:"), 
    st.Page("netflix-recommendations.py", title="Netflix Recommendations", icon=":material/text_snippet:")
    ])
pg.run()