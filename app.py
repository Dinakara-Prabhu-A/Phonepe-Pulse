import streamlit as st
import plotly.express as px
import pandas as pd
import sqlite3
import json
from streamlit_option_menu import option_menu

# Streamlit Page Configuration
st.set_page_config(page_title='PhonePe Pulse', layout="wide")

# Title
st.markdown("<h1 style='text-align: center; color: #7F00FF;'>₹PhonePe Pulse 2018-2024 Analysis</h1>", unsafe_allow_html=True)

# Sidebar for Navigation
menu = option_menu(None, ["Insurance", "Transaction"],
                   icons=['shield', 'currency-exchange'],
                   menu_icon="cast", default_index=0, orientation="horizontal")

# Year & Quarter Selection
years = list(range(2022, 2025)) if menu == "Insurance" else list(range(2018, 2025))
quarter_selector = lambda year: ["2", "3", "4"] if (menu == "Insurance" and year == 2022) else ["1", "2", "3", "4"]

col1, col2 = st.columns(2)
with col1:
    selected_year = st.selectbox("Select Year", years, index=len(years) - 1)
with col2:
    selected_quarter = st.selectbox("Select Quarter", quarter_selector(selected_year), index=0)

# Database Connection Function
def fetch_data(query, db_path):
    try:
        with sqlite3.connect(db_path) as conn:
            return pd.read_sql_query(query, conn)
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame()

# Database Paths
db_paths = {name: f"artifact/{name}_{menu.lower()}.db" for name in ["state", "district", "pincode"]}

# Fetch Summary Data
summary_query = f"""
    SELECT COALESCE(SUM({'policies_purchased' if menu == 'Insurance' else 'all_transactions'}), 0) AS total_count, 
           COALESCE(SUM({'premium_value' if menu == 'Insurance' else 'payment_value'}), 0) AS total_value
    FROM state_{menu.lower()}_data
    WHERE year='{selected_year}' AND quarter='{selected_quarter}'
"""
summary_df = fetch_data(summary_query, db_paths["state"])

total_count, total_value = int(summary_df.iloc[0, 0]), float(summary_df.iloc[0, 1])
avg_value = round(total_value / total_count, 2) if total_count else 0

# Load India GeoJSON Locally
with open("notebook/indian_state.geojson", 'r') as f:
    india_state = json.load(f)

# Fetch Data for Map
map_query = f"""
    SELECT state, SUM({'policies_purchased' if menu == 'Insurance' else 'all_transactions'}) AS total_value
    FROM state_{menu.lower()}_data
    WHERE year='{selected_year}' AND quarter='{selected_quarter}'
    GROUP BY state
"""
map_data = fetch_data(map_query, db_paths["state"])
map_data["state_display"] = map_data["state"].str.replace("-", " ").str.title()

# Create Layout with Map and Metrics
col1, col2 = st.columns([2, 1])

with col1:
    # Create Choropleth Map
    fig = px.choropleth(
        map_data,
        geojson=india_state,
        locations='state',
        featureidkey='properties.ST_NM',
        color='total_value',
        color_continuous_scale=px.colors.diverging.BrBG,
        title=f'Statewise {'Policies Purchased' if menu == 'Insurance' else 'Transactions'} in {selected_year} Q{selected_quarter}',
        template="plotly_dark"
    )
    fig.update_traces(customdata=map_data["state_display"], hovertemplate="<b>%{customdata}</b>: %{z}<extra></extra>")
    fig.update_layout(width=800, height=600, geo=dict(fitbounds="locations", visible=False))
    st.plotly_chart(fig, use_container_width=True)

with col2:
    # Display Metrics with spacing
    st.markdown("###")
    st.metric(f"Total {'Policies Purchased' if menu == 'Insurance' else 'Transactions'}", f"{total_count:,}")
    st.markdown("###")
    st.metric(f"Total {'Premium Value' if menu == 'Insurance' else 'Payment Value'}", f"₹{total_value:,.2f}")
    st.markdown("###")
    st.metric(f"Average {'Premium' if menu == 'Insurance' else 'Transaction'} Value", f"₹{avg_value:,.2f}")

# Fetch Top 10 Data for Bar Charts
bar_queries = {
    "State": f"""
        SELECT state, SUM({'policies_purchased' if menu == 'Insurance' else 'all_transactions'}) AS total_value
        FROM state_{menu.lower()}_data
        WHERE year='{selected_year}' AND quarter='{selected_quarter}'
        GROUP BY state
        ORDER BY total_value DESC LIMIT 10
    """,
    "District": f"""
        SELECT district, SUM({'policies_purchased' if menu == 'Insurance' else 'all_transactions'}) AS total_value
        FROM district_{menu.lower()}_data
        WHERE year='{selected_year}' AND quarter='{selected_quarter}'
        GROUP BY district
        ORDER BY total_value DESC LIMIT 10
    """,
    "Pincode": f"""
        SELECT CAST(pincode AS TEXT) AS pincode, SUM({'policies_purchased' if menu == 'Insurance' else 'all_transactions'}) AS total_value
        FROM pincode_{menu.lower()}_data
        WHERE year='{selected_year}' AND quarter='{selected_quarter}'
        GROUP BY pincode
        ORDER BY total_value DESC LIMIT 10
    """
}

top_data = {key: fetch_data(query, db_paths[key.lower()]) for key, query in bar_queries.items()}

# Create Bar Charts
col1, col2, col3 = st.columns(3)

fig_state = px.bar(top_data["State"], x="state", y="total_value", title="Top 10 States", text_auto=True, template="plotly_dark")
fig_district = px.bar(top_data["District"], x="district", y="total_value", title="Top 10 Districts", text_auto=True, template="plotly_dark")
fig_pincode = px.bar(top_data["Pincode"], x="pincode", y="total_value", title="Top 10 Pincodes", text_auto=True, template="plotly_dark")

col1.plotly_chart(fig_state, use_container_width=True)
col2.plotly_chart(fig_district, use_container_width=True)
col3.plotly_chart(fig_pincode, use_container_width=True)

# Footer
st.markdown("<h4 style='text-align: center; color: white;'>Dashboard for exploring PhonePe Pulse data with interactive visualizations.</h4>", unsafe_allow_html=True)