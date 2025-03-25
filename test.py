import streamlit as st

# Initialize session state for year selection
if 'selected_year' not in st.session_state:
    st.session_state.selected_year = 2023
if 'selected_quarter' not in st.session_state:
    st.session_state.selected_quarter = "1"

# Define available years
years = list(range(2018, 2025))

# Function to get available quarters based on selected year
def quarter_selector(year):
    return ["1", "2", "3", "4"] if year < 2024 else ["1", "2", "3"]

# Use columns to keep the dropdowns in the same row
col1, col2 = st.columns([1, 1])  # Adjust width as needed

with col1:
    selected_year = st.selectbox("Select Year", years, index=years.index(st.session_state.selected_year))
    st.session_state.selected_year = selected_year  # Update session state

with col2:
    quarters = quarter_selector(selected_year)
    selected_quarter = st.selectbox("Select Quarter", quarters, index=quarters.index(st.session_state.selected_quarter))
    st.session_state.selected_quarter = selected_quarter  # Update session state
