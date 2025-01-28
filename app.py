import streamlit as st
from streamlit_option_menu import option_menu

st.set_page_config(page_title='PhonePe Pulse', page_icon=None, layout="wide", initial_sidebar_state="expanded", menu_items=None)

# Center the title using HTML and CSS
st.markdown(
    """
    <h1 style="text-align: center; color: white;">PhonePe Details</h1>
    """,
    unsafe_allow_html=True
)

# 2. horizontal menu
main = option_menu(None, ["Insurance", "Transaction"],
    icons=['shield', 'currency-exchange'],  # Replace with your chosen icons
    menu_icon="cast", default_index=0, orientation="horizontal")