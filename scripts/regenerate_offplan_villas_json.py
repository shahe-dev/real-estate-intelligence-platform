"""
Regenerate 03-off-plan-villas-dubai-data-inputs.json
using transactions_verified (correct fields)
"""
import duckdb
import json
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "database" / "property_monitor.db"
OUTPUT_PATH = Path(__file__).parent.parent / "data" / "generated_content" / "content-expansion" / "03-off-plan-villas-dubai-data-inputs.json"

con = duckdb.connect(str(DB_PATH), read_only=True)
data = {}

# --- META ---
data["meta"] = {
    "generated_at": datetime.now().strftime("%Y-%m-%d"),
    "data_source": "Property Monitor - transactions_verified view",
    "period": "2025 (Jan 1 - Dec 31)",
    "content_spec": "03-off-plan-villas-dubai-report.md",
    "note": "Regenerated with corrected fields: property_type from unit_type_original (Villa only, excludes Townhouse), transaction_type from reg_type_en (Off-Plan filter, not off_plan boolean)"
}

# Base filter used everywhere
BASE = "transaction_type = 'Off-Plan' AND property_type = 'Villa' AND transaction_year = 2025"

# --- 1. OVERVIEW ---
r = con.execute(f"""
    SELECT
        COUNT(*) as txns,
        SUM(actual_worth) as total_value,
        ROUND(AVG(actual_worth)) as avg_price,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY actual_worth)) as median_price,
        ROUND(AVG(procedure_area * 10.764)) as avg_size_sqft
    FROM transactions_verified
    WHERE {BASE}
""").fetchone()

data["offplan_villa_overview"] = {
    "total_transactions": r[0],
    "total_value": int(r[1]),
    "total_value_formatted": f"AED {r[1]/1e9:.2f}B",
    "avg_price": int(r[2]),
    "avg_price_formatted": f"AED {r[2]/1e6:.2f}M",
    "median_price": int(r[3]),
    "median_price_formatted": f"AED {r[3]/1e6:.2f}M",
    "avg_size_sqft": int(r[4])
}
print(f"1. Overview: {r[0]:,} off-plan villas, AED {r[1]/1e9:.2f}B")

# --- 2. TYPE COMPARISON (all off-plan by property type) ---
rows = con.execute("""
    SELECT
        property_type,
        COUNT(*) as txns,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as mkt_share,
        ROUND(AVG(actual_worth)) as avg_price,
        SUM(actual_worth) as total_value
    FROM transactions_verified
    WHERE transaction_type = 'Off-Plan' AND transaction_year = 2025
    GROUP BY property_type
    ORDER BY txns DESC
""").fetchall()

data["offplan_type_comparison"] = []
for r in rows:
    data["offplan_type_comparison"].append({
        "property_type": r[0],
        "transaction_count": r[1],
        "market_share": float(r[2]),
        "avg_price": int(r[3]),
        "avg_price_formatted": f"AED {r[3]/1e6:.2f}M",
        "total_value": int(r[4]),
        "total_value_formatted": f"AED {r[4]/1e9:.2f}B"
    })
print(f"2. Type comparison: {len(rows)} types")

# --- 3. CALCULATED METRICS ---
total_op = con.execute("""
    SELECT COUNT(*), SUM(actual_worth)
    FROM transactions_verified
    WHERE transaction_type = 'Off-Plan' AND transaction_year = 2025
""").fetchone()

villa_op = con.execute(f"""
    SELECT COUNT(*), SUM(actual_worth), AVG(actual_worth), AVG(meter_sale_price * 10.764)
    FROM transactions_verified
    WHERE {BASE}
""").fetchone()

apt_avg = con.execute("""
    SELECT AVG(actual_worth)
    FROM transactions_verified
    WHERE transaction_type = 'Off-Plan' AND property_type = 'Apartment' AND transaction_year = 2025
""").fetchone()[0]

ready_villa_avg = con.execute("""
    SELECT AVG(actual_worth)
    FROM transactions_verified
    WHERE transaction_type = 'Existing' AND property_type = 'Villa' AND transaction_year = 2025
""").fetchone()[0]

data["calculated_metrics"] = {
    "total_offplan_transactions": total_op[0],
    "total_offplan_value": int(total_op[1]),
    "villa_share_of_offplan": round(villa_op[0] * 100.0 / total_op[0], 1),
    "villa_value_share": round(villa_op[1] * 100.0 / total_op[1], 1),
    "villa_vs_apartment_premium": round((villa_op[2] - apt_avg) / apt_avg * 100),
    "offplan_villa_discount": round((ready_villa_avg - villa_op[2]) / ready_villa_avg * 100, 1),
    "avg_price_per_sqft": int(villa_op[3]) if villa_op[3] else 0
}
print(f"3. Calculated metrics: villa share {data['calculated_metrics']['villa_share_of_offplan']}%")

# --- 4. TOP AREAS ---
rows = con.execute(f"""
    SELECT
        area_name_en,
        COUNT(*) as txns,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as mkt_share,
        ROUND(AVG(actual_worth)) as avg_price,
        SUM(actual_worth) as total_value,
        ROUND(AVG(procedure_area * 10.764)) as avg_sqft
    FROM transactions_verified
    WHERE {BASE}
    GROUP BY area_name_en
    ORDER BY txns DESC
    LIMIT 10
""").fetchall()

data["top_areas_offplan_villas"] = []
for i, r in enumerate(rows):
    data["top_areas_offplan_villas"].append({
        "rank": i + 1,
        "area": r[0],
        "transaction_count": r[1],
        "market_share": float(r[2]),
        "avg_price": int(r[3]),
        "avg_price_formatted": f"AED {r[3]/1e6:.2f}M",
        "total_value_formatted": f"AED {r[4]/1e9:.2f}B",
        "avg_size_sqft": int(r[5])
    })
print(f"4. Top areas: #{1} {rows[0][0]} ({rows[0][1]:,})")

# --- 5. TOP DEVELOPERS ---
rows = con.execute(f"""
    SELECT
        master_project_en,
        COUNT(*) as txns,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as mkt_share,
        ROUND(AVG(actual_worth)) as avg_price,
        SUM(actual_worth) as total_value
    FROM transactions_verified
    WHERE {BASE}
      AND master_project_en IS NOT NULL AND master_project_en != ''
    GROUP BY master_project_en
    ORDER BY txns DESC
    LIMIT 10
""").fetchall()

data["top_developers_offplan_villas"] = []
for i, r in enumerate(rows):
    data["top_developers_offplan_villas"].append({
        "rank": i + 1,
        "developer": r[0],
        "transaction_count": r[1],
        "market_share": float(r[2]),
        "avg_price": int(r[3]),
        "avg_price_formatted": f"AED {r[3]/1e6:.2f}M",
        "total_value_formatted": f"AED {r[4]/1e9:.2f}B"
    })
print(f"5. Top developers: #{1} {rows[0][0]} ({rows[0][1]:,})")

# --- 6. TOP PROJECTS ---
rows = con.execute(f"""
    SELECT
        project_name_en,
        master_project_en,
        area_name_en,
        COUNT(*) as txns,
        ROUND(AVG(actual_worth)) as avg_price,
        MIN(actual_worth) as min_price,
        MAX(actual_worth) as max_price
    FROM transactions_verified
    WHERE {BASE}
      AND project_name_en IS NOT NULL AND project_name_en != ''
    GROUP BY project_name_en, master_project_en, area_name_en
    ORDER BY txns DESC
    LIMIT 15
""").fetchall()

data["top_projects_offplan_villas"] = []
for i, r in enumerate(rows):
    data["top_projects_offplan_villas"].append({
        "rank": i + 1,
        "project_name": r[0],
        "developer": r[1],
        "area": r[2],
        "transaction_count": r[3],
        "avg_price": int(r[4]),
        "avg_price_formatted": f"AED {r[4]/1e6:.2f}M",
        "min_price": int(r[5]),
        "max_price": int(r[6]),
        "price_range": f"AED {r[5]/1e6:.2f}M - AED {r[6]/1e6:.2f}M"
    })
print(f"6. Top projects: #{1} {rows[0][0]} ({rows[0][3]:,})")

# --- 7. BEDROOM BREAKDOWN ---
rows = con.execute(f"""
    SELECT
        rooms_en,
        COUNT(*) as txns,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as share,
        ROUND(AVG(actual_worth)) as avg_price,
        ROUND(AVG(procedure_area * 10.764)) as avg_sqft,
        ROUND(AVG(meter_sale_price * 10.764)) as avg_psf
    FROM transactions_verified
    WHERE {BASE}
      AND rooms_en IS NOT NULL AND rooms_en != ''
    GROUP BY rooms_en
    ORDER BY
        CASE rooms_en
            WHEN '1 B/R' THEN 1 WHEN '2 B/R' THEN 2 WHEN '3 B/R' THEN 3
            WHEN '4 B/R' THEN 4 WHEN '5 B/R' THEN 5 WHEN '6 B/R' THEN 6
            WHEN '7 B/R' THEN 7 WHEN '7+ B/R' THEN 8 WHEN '8 B/R' THEN 9
            WHEN '9 B/R' THEN 10 WHEN '10 B/R' THEN 11
            ELSE 99
        END
""").fetchall()

data["offplan_villas_by_bedroom"] = []
for r in rows:
    data["offplan_villas_by_bedroom"].append({
        "bedrooms": r[0],
        "transaction_count": r[1],
        "market_share": float(r[2]),
        "avg_price": int(r[3]),
        "avg_price_formatted": f"AED {r[3]/1e6:.2f}M",
        "avg_size_sqft": int(r[4]),
        "avg_psf": int(r[5]) if r[5] else 0
    })
print(f"7. Bedrooms: {len(rows)} categories")

# --- 8. PRICE SEGMENTS ---
rows = con.execute(f"""
    SELECT
        CASE
            WHEN actual_worth < 2000000 THEN 'Under 2M'
            WHEN actual_worth < 5000000 THEN '2M - 5M'
            WHEN actual_worth < 10000000 THEN '5M - 10M'
            WHEN actual_worth < 20000000 THEN '10M - 20M'
            ELSE '20M+'
        END as segment,
        COUNT(*) as txns,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as share,
        ROUND(AVG(actual_worth)) as avg_price
    FROM transactions_verified
    WHERE {BASE}
    GROUP BY 1
    ORDER BY MIN(actual_worth)
""").fetchall()

data["offplan_villas_price_segments"] = []
for r in rows:
    data["offplan_villas_price_segments"].append({
        "price_segment": r[0],
        "transaction_count": r[1],
        "market_share": float(r[2]),
        "avg_price": int(r[3]),
        "avg_price_formatted": f"AED {r[3]/1e6:.2f}M"
    })
print(f"8. Price segments: {len(rows)} buckets")

# --- 9. MONTHLY ---
rows = con.execute(f"""
    SELECT
        transaction_month,
        COUNT(*) as txns,
        ROUND(AVG(actual_worth)) as avg_price
    FROM transactions_verified
    WHERE {BASE}
    GROUP BY transaction_month
    ORDER BY transaction_month
""").fetchall()

month_names = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
data["monthly_offplan_villas"] = []
for r in rows:
    data["monthly_offplan_villas"].append({
        "month": month_names[r[0]-1],
        "transaction_count": r[1],
        "avg_price": int(r[2]),
        "avg_price_formatted": f"AED {r[2]/1e6:.2f}M"
    })
print(f"9. Monthly: {len(rows)} months")

# --- 10. OFF-PLAN vs READY ---
rows = con.execute("""
    SELECT
        transaction_type,
        COUNT(*) as txns,
        ROUND(AVG(actual_worth)) as avg_price,
        ROUND(AVG(meter_sale_price * 10.764)) as avg_psf
    FROM transactions_verified
    WHERE property_type = 'Villa' AND transaction_year = 2025
    GROUP BY transaction_type
""").fetchall()

op_row = next((r for r in rows if r[0] == 'Off-Plan'), None)
ex_row = next((r for r in rows if r[0] == 'Existing'), None)

data["villa_offplan_vs_ready"] = {
    "offplan": {
        "transaction_type": "Off-Plan",
        "transaction_count": op_row[1],
        "avg_price": int(op_row[2]),
        "avg_price_formatted": f"AED {op_row[2]/1e6:.2f}M",
        "avg_psf": int(op_row[3]) if op_row[3] else 0
    },
    "ready": {
        "transaction_type": "Existing",
        "transaction_count": ex_row[1],
        "avg_price": int(ex_row[2]),
        "avg_price_formatted": f"AED {ex_row[2]/1e6:.2f}M",
        "avg_psf": int(ex_row[3]) if ex_row[3] else 0
    }
}
print(f"10. Off-plan vs Ready: {op_row[1]:,} vs {ex_row[1]:,}")

# --- 11. FAQ DATA ---
top_area = data["top_areas_offplan_villas"][0]
second_area = data["top_areas_offplan_villas"][1]
third_area = data["top_areas_offplan_villas"][2]
top_dev = data["top_developers_offplan_villas"][0]
entry_seg = next((s for s in data["offplan_villas_price_segments"] if s["price_segment"] == "Under 2M"), None)
lux_seg = next((s for s in data["offplan_villas_price_segments"] if s["price_segment"] == "20M+"), None)

dev_areas = con.execute(f"""
    SELECT area_name_en, COUNT(*) as txns
    FROM transactions_verified
    WHERE {BASE} AND master_project_en = ?
    GROUP BY area_name_en ORDER BY txns DESC LIMIT 3
""", [top_dev["developer"]]).fetchall()

bed_3 = next((b for b in data["offplan_villas_by_bedroom"] if b["bedrooms"] == "3 B/R"), None)
bed_5 = next((b for b in data["offplan_villas_by_bedroom"] if b["bedrooms"] == "5 B/R"), None)

discount_pct = round((ex_row[2] - op_row[2]) / ex_row[2] * 100, 1)

data["faq_data"] = {
    "avg_offplan_villa_price": data["offplan_villa_overview"]["avg_price_formatted"],
    "entry_level_avg": entry_seg["avg_price_formatted"] if entry_seg else "N/A",
    "luxury_avg": lux_seg["avg_price_formatted"] if lux_seg else "N/A",
    "top_area": top_area["area"],
    "top_area_transactions": top_area["transaction_count"],
    "top_area_avg_price": top_area["avg_price_formatted"],
    "second_area": second_area["area"],
    "third_area": third_area["area"],
    "offplan_villa_discount": f"{discount_pct}%",
    "offplan_villa_avg": data["villa_offplan_vs_ready"]["offplan"]["avg_price_formatted"],
    "ready_villa_avg": data["villa_offplan_vs_ready"]["ready"]["avg_price_formatted"],
    "top_developer": top_dev["developer"],
    "top_developer_transactions": top_dev["transaction_count"],
    "top_developer_areas": [a[0] for a in dev_areas],
    "avg_villa_size": f"{data['offplan_villa_overview']['avg_size_sqft']:,} sqft",
    "size_3br": f"{bed_3['avg_size_sqft']:,} sqft" if bed_3 else "N/A",
    "size_5br": f"{bed_5['avg_size_sqft']:,} sqft" if bed_5 else "N/A"
}
print(f"11. FAQ data built")

# --- WRITE ---
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
with open(OUTPUT_PATH, 'w') as f:
    json.dump(data, f, indent=2)

print(f"\n{'='*60}")
print(f"Written: {OUTPUT_PATH}")
print(f"Checkpoint: total_transactions = {data['offplan_villa_overview']['total_transactions']:,} (expected: 7,099)")

con.close()
