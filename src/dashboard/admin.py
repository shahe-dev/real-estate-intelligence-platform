# src/dashboard/admin.py

"""
Internal Admin Dashboard
Streamlit-based monitoring and control panel
"""

import streamlit as st
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pandas as pd
import plotly.express as px
from datetime import datetime

from src.utils.db import get_db
from config.settings import settings

st.set_page_config(
    page_title="Dubai Real Estate Intelligence - Admin",
    page_icon="🏢",
    layout="wide"
)

# Initialize database
con = get_db()

# Sidebar navigation
st.sidebar.title("🏢 Dubai RE Intel")
page = st.sidebar.radio("Navigation", [
    "Dashboard",
    "Data Quality",
    "Content Manager",
    "API Monitor",
    "Data Explorer"
])

# === DASHBOARD PAGE ===
if page == "Dashboard":
    st.title("📊 System Dashboard")
    
    # KPIs
    col1, col2, col3, col4 = st.columns(4)
    
    stats = con.execute("""
        SELECT 
            COUNT(*) as total_tx,
            COUNT(DISTINCT area_name_en) as total_areas,
            SUM(CASE WHEN is_luxury THEN 1 ELSE 0 END) as luxury_tx,
            MAX(transaction_year) as latest_year
        FROM transactions_clean
    """).fetchone()
    
    col1.metric("Total Transactions", f"{stats[0]:,}")
    col2.metric("Unique Areas", stats[1])
    col3.metric("Luxury Properties", f"{stats[2]:,}")
    col4.metric("Latest Data", stats[3])
    
    # Charts
    st.subheader("Transaction Volume by Year")
    
    yearly_data = con.execute("""
        SELECT 
            transaction_year,
            COUNT(*) as transactions,
            AVG(actual_worth) as avg_price
        FROM transactions_clean
        WHERE trans_group_en = 'Sales'
        GROUP BY transaction_year
        ORDER BY transaction_year
    """).df()
    
    fig1 = px.bar(yearly_data, x='transaction_year', y='transactions', 
                  title="Sales Transactions by Year")
    st.plotly_chart(fig1, use_container_width=True)
    
    fig2 = px.line(yearly_data, x='transaction_year', y='avg_price',
                   title="Average Sale Price Trend")
    st.plotly_chart(fig2, use_container_width=True)
    
    # Top areas
    st.subheader("Top 10 Areas by Activity")
    top_areas = con.execute("""
        SELECT 
            area_name_en,
            total_transactions,
            avg_price,
            luxury_count
        FROM metrics_area
        ORDER BY total_transactions DESC
        LIMIT 10
    """).df()
    
    st.dataframe(top_areas, use_container_width=True)

# === DATA QUALITY PAGE ===
elif page == "Data Quality":
    st.title("🔍 Data Quality Monitor")
    
    # Version info
    st.subheader("Data Versions")
    versions = con.execute("""
        SELECT * FROM data_versions 
        ORDER BY version_id DESC
    """).df()
    
    st.dataframe(versions)
    
    # Quality metrics
    st.subheader("Quality Scores")
    
    quality_dist = con.execute("""
        SELECT 
            CASE 
                WHEN quality_score = 1.0 THEN 'Excellent'
                WHEN quality_score >= 0.7 THEN 'Good'
                WHEN quality_score >= 0.5 THEN 'Fair'
                ELSE 'Poor'
            END as quality_category,
            COUNT(*) as count
        FROM transactions_current
        GROUP BY quality_category
    """).df()
    
    fig = px.pie(quality_dist, values='count', names='quality_category',
                 title="Data Quality Distribution")
    st.plotly_chart(fig)
    
    # Invalid records
    st.subheader("Invalid Records")
    invalid = con.execute("""
        SELECT 
            transaction_id,
            area_name_en,
            property_type_en,
            actual_worth,
            validation_warnings
        FROM transactions_current
        WHERE is_valid = FALSE
        LIMIT 50
    """).df()
    
    st.dataframe(invalid, use_container_width=True)

# === CONTENT MANAGER ===
elif page == "Content Manager":
    st.title("📝 Content Manager")
    
    # List generated content
    content_dir = settings.CONTENT_OUTPUT_DIR
    
    if content_dir.exists():
        files = list(content_dir.glob("*.md"))
        
        st.subheader(f"Generated Content ({len(files)} files)")
        
        if files:
            selected_file = st.selectbox("Select file to preview:", 
                                         [f.name for f in files])
            
            if selected_file:
                filepath = content_dir / selected_file
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                st.markdown("### Preview:")
                st.markdown(content)
                
                # Download button
                st.download_button(
                    label="Download",
                    data=content,
                    file_name=selected_file,
                    mime="text/markdown"
                )
        else:
            st.info("No content generated yet. Run the content generator.")
    
    # Generate new content
    st.subheader("Generate New Content")
    
    content_type = st.selectbox("Content Type", [
        "Area Guide",
        "Market Report",
        "Property Comparison"
    ])
    
    if content_type == "Area Guide":
        areas = con.execute("""
            SELECT area_name_en FROM metrics_area 
            ORDER BY total_transactions DESC 
            LIMIT 20
        """).df()
        
        selected_area = st.selectbox("Select Area:", areas['area_name_en'])
        
        if st.button("Generate"):
            from src.content.generator import ContentGenerator
            generator = ContentGenerator()
            
            with st.spinner("Generating..."):
                filepath = generator.generate_area_guide(selected_area)
            
            if filepath:
                st.success(f"Generated: {filepath}")
            else:
                st.error("Generation failed")

# === API MONITOR ===
elif page == "API Monitor":
    st.title("🔌 API Monitor")
    
    st.info("API is running at http://localhost:8000")
    
    # Test endpoints
    st.subheader("Quick API Tests")
    
    if st.button("Test /api/areas"):
        import requests
        try:
            response = requests.get("http://localhost:8000/api/areas")
            st.json(response.json())
        except:
            st.error("API not running. Start with: python src/api/main.py")
    
    if st.button("Test /api/stats/overview"):
        import requests
        try:
            response = requests.get("http://localhost:8000/api/stats/overview")
            st.json(response.json())
        except:
            st.error("API not running")

# === DATA EXPLORER ===
elif page == "Data Explorer":
    st.title("🔎 Data Explorer")
    
    st.subheader("Custom SQL Query")
    
    query = st.text_area("Enter SQL Query:", 
                         value="SELECT * FROM metrics_area LIMIT 10",
                         height=150)
    
    if st.button("Execute"):
        try:
            result = con.execute(query).df()
            st.dataframe(result, use_container_width=True)
            
            # Export option
            csv = result.to_csv(index=False)
            st.download_button(
                label="Download as CSV",
                data=csv,
                file_name="query_result.csv",
                mime="text/csv"
            )
        except Exception as e:
            st.error(f"Query error: {e}")