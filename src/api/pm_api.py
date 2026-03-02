# src/api/pm_api.py

"""
Property Monitor FastAPI Server
Serves real estate analytics from Property Monitor BigQuery data
Uses separate database: data/database/property_monitor.db
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List
import uvicorn
import duckdb

from config.bigquery_settings import bq_settings

app = FastAPI(
    title="Property Monitor Real Estate API",
    description="API for Dubai real estate analytics powered by Property Monitor data",
    version="1.0.0"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_pm_db(read_only=True):
    """Get Property Monitor database connection"""
    return duckdb.connect(str(bq_settings.PM_DB_PATH), read_only=read_only)


# ============================================================
# HEALTH & STATUS ENDPOINTS
# ============================================================

@app.get("/")
def root():
    """API Health Check"""
    return {
        "status": "online",
        "service": "Property Monitor Real Estate API",
        "version": "1.0.0",
        "database": "property_monitor.db"
    }


@app.get("/api/status")
def get_status():
    """Get database and data status"""
    try:
        con = get_pm_db()
        stats = con.execute("""
            SELECT
                COUNT(*) as total_transactions,
                COUNT(DISTINCT area_name_en) as unique_areas,
                MIN(instance_date) as earliest_date,
                MAX(instance_date) as latest_date,
                SUM(CASE WHEN is_luxury THEN 1 ELSE 0 END) as luxury_count
            FROM transactions_clean
        """).fetchone()

        return {
            "status": "healthy",
            "data": {
                "total_transactions": stats[0],
                "unique_areas": stats[1],
                "date_range": f"{stats[2]} to {stats[3]}",
                "luxury_transactions": stats[4]
            }
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


# ============================================================
# MARKET OVERVIEW ENDPOINTS
# ============================================================

@app.get("/api/overview")
def get_market_overview():
    """Get complete market overview"""
    try:
        con = get_pm_db()

        # Overall stats
        stats = con.execute("""
            SELECT
                COUNT(*) as total_transactions,
                COUNT(DISTINCT area_name_en) as unique_areas,
                COUNT(DISTINCT project_name_en) as unique_projects,
                COUNT(DISTINCT developer) as unique_developers,
                MIN(instance_date) as earliest_date,
                MAX(instance_date) as latest_date,
                AVG(actual_worth) as avg_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY actual_worth) as median_price,
                SUM(actual_worth) as total_volume,
                SUM(CASE WHEN is_luxury THEN 1 ELSE 0 END) as luxury_count,
                SUM(CASE WHEN transaction_type = 'Off-Plan' THEN 1 ELSE 0 END) as offplan_count
            FROM transactions_verified
        """).fetchone()

        # Top areas
        top_areas = con.execute("""
            SELECT area_name_en, total_transactions, avg_price
            FROM metrics_area
            ORDER BY total_transactions DESC
            LIMIT 5
        """).df().to_dict('records')

        return {
            "summary": {
                "total_transactions": stats[0],
                "unique_areas": stats[1],
                "unique_projects": stats[2],
                "unique_developers": stats[3],
                "date_range": {"from": str(stats[4]), "to": str(stats[5])},
                "avg_price": stats[6],
                "median_price": stats[7],
                "total_market_volume": stats[8],
                "luxury_transactions": stats[9],
                "offplan_transactions": stats[10],
                "offplan_percentage": round(stats[10] / stats[0] * 100, 1) if stats[0] > 0 else 0
            },
            "top_areas": top_areas
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# AREA ENDPOINTS
# ============================================================

@app.get("/api/areas")
def get_areas(
    min_transactions: int = Query(10, description="Minimum transaction count"),
    sort_by: str = Query("total_transactions", description="Sort by field"),
    limit: int = Query(50, description="Maximum results")
):
    """Get list of all areas with metrics"""
    try:
        con = get_pm_db()
        result = con.execute(f"""
            SELECT
                area_name_en as area,
                total_transactions,
                unique_projects,
                avg_price,
                median_price,
                avg_price_sqm,
                luxury_count,
                offplan_count,
                top_developers
            FROM metrics_area
            WHERE total_transactions >= {min_transactions}
            ORDER BY {sort_by} DESC
            LIMIT {limit}
        """).df()

        return result.to_dict('records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/area/{area_name}")
def get_area_details(area_name: str):
    """Get detailed analytics for a specific area"""
    try:
        con = get_pm_db()

        # Basic metrics
        metrics = con.execute(f"""
            SELECT * FROM metrics_area
            WHERE area_name_en = '{area_name}'
        """).df()

        if metrics.empty:
            raise HTTPException(status_code=404, detail=f"Area '{area_name}' not found")

        # Monthly trends
        trends = con.execute(f"""
            SELECT
                transaction_year,
                transaction_month,
                property_type,
                tx_count,
                avg_price,
                median_price,
                avg_price_sqm,
                offplan_count
            FROM metrics_monthly_trends
            WHERE area_name_en = '{area_name}'
            ORDER BY transaction_year DESC, transaction_month DESC
            LIMIT 24
        """).df()

        # Property types breakdown
        prop_types = con.execute(f"""
            SELECT
                property_type,
                rooms_en,
                tx_count,
                avg_price,
                median_price,
                avg_price_sqm,
                avg_size_sqm
            FROM metrics_property_types
            WHERE area_name_en = '{area_name}'
            ORDER BY tx_count DESC
        """).df()

        # Top projects
        projects = con.execute(f"""
            SELECT
                project_name_en,
                developer,
                tx_count,
                avg_price,
                transaction_type
            FROM metrics_projects
            WHERE area_name_en = '{area_name}'
            ORDER BY tx_count DESC
            LIMIT 10
        """).df()

        # Off-plan vs Ready comparison
        offplan_stats = con.execute(f"""
            SELECT * FROM metrics_offplan_comparison
            WHERE area_name_en = '{area_name}'
        """).df()

        return {
            "area": area_name,
            "metrics": metrics.to_dict('records')[0],
            "monthly_trends": trends.to_dict('records'),
            "property_types": prop_types.to_dict('records'),
            "top_projects": projects.to_dict('records'),
            "offplan_comparison": offplan_stats.to_dict('records')[0] if not offplan_stats.empty else None
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/area/{area_name}/trends")
def get_area_trends(
    area_name: str,
    property_type: Optional[str] = Query(None, description="Filter by property type")
):
    """Get price trends for an area"""
    try:
        con = get_pm_db()

        query = f"""
            SELECT
                transaction_year,
                transaction_month,
                property_type,
                avg_price,
                tx_count,
                pct_change_mom
            FROM metrics_price_changes
            WHERE area_name_en = '{area_name}'
        """

        if property_type:
            query += f" AND property_type = '{property_type}'"

        query += " ORDER BY transaction_year, transaction_month"

        result = con.execute(query).df()

        if result.empty:
            raise HTTPException(status_code=404, detail=f"No trend data for '{area_name}'")

        return result.to_dict('records')
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# COMPARE AREAS
# ============================================================

@app.get("/api/compare")
def compare_areas(
    areas: List[str] = Query(..., description="Areas to compare (max 5)")
):
    """Compare multiple areas side by side"""
    if len(areas) > 5:
        raise HTTPException(status_code=400, detail="Maximum 5 areas for comparison")

    try:
        con = get_pm_db()
        areas_str = "', '".join(areas)

        result = con.execute(f"""
            SELECT
                area_name_en as area,
                total_transactions,
                avg_price,
                median_price,
                avg_price_sqm,
                luxury_count,
                offplan_count,
                unique_projects
            FROM metrics_area
            WHERE area_name_en IN ('{areas_str}')
            ORDER BY total_transactions DESC
        """).df()

        if result.empty:
            raise HTTPException(status_code=404, detail="No data found for specified areas")

        return result.to_dict('records')

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# LUXURY MARKET ENDPOINTS
# ============================================================

@app.get("/api/luxury")
def get_luxury_market(
    year: Optional[int] = Query(None, description="Filter by year"),
    min_transactions: int = Query(5, description="Minimum transactions")
):
    """Get luxury market data (5M+ AED)"""
    try:
        con = get_pm_db()

        query = """
            SELECT
                area_name_en,
                transaction_year,
                transaction_month,
                luxury_tx_count,
                avg_luxury_price,
                median_luxury_price,
                max_luxury_price,
                luxury_projects,
                luxury_developers
            FROM metrics_luxury
            WHERE luxury_tx_count >= ?
        """
        params = [min_transactions]

        if year:
            query += " AND transaction_year = ?"
            params.append(year)

        query += " ORDER BY luxury_tx_count DESC"

        result = con.execute(query, params).df()
        return result.to_dict('records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/luxury/summary")
def get_luxury_summary():
    """Get luxury market summary by area"""
    try:
        con = get_pm_db()

        result = con.execute("""
            SELECT
                area_name_en as area,
                total_luxury_transactions,
                avg_luxury_price,
                highest_sale,
                avg_price_sqm,
                luxury_projects_count,
                luxury_developers_count
            FROM metrics_luxury_summary
            ORDER BY total_luxury_transactions DESC
            LIMIT 20
        """).df()

        return result.to_dict('records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# DEVELOPER ENDPOINTS
# ============================================================

@app.get("/api/developers")
def get_developers(
    min_transactions: int = Query(50, description="Minimum transactions"),
    limit: int = Query(30, description="Maximum results")
):
    """Get top developers by transaction volume"""
    try:
        con = get_pm_db()

        result = con.execute(f"""
            SELECT
                developer,
                total_transactions,
                projects_count,
                areas_active,
                avg_price,
                median_price,
                total_sales_volume,
                luxury_units,
                offplan_sales,
                first_sale_date,
                last_sale_date
            FROM metrics_developers
            WHERE total_transactions >= {min_transactions}
            ORDER BY total_transactions DESC
            LIMIT {limit}
        """).df()

        return result.to_dict('records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/developer/{developer_name}")
def get_developer_details(developer_name: str):
    """Get detailed info for a specific developer"""
    try:
        con = get_pm_db()

        # Developer summary
        developer = con.execute(f"""
            SELECT * FROM metrics_developers
            WHERE developer = '{developer_name}'
        """).df()

        if developer.empty:
            raise HTTPException(status_code=404, detail=f"Developer '{developer_name}' not found")

        # Projects by this developer
        projects = con.execute(f"""
            SELECT
                project_name_en,
                area_name_en,
                tx_count,
                avg_price,
                transaction_type,
                first_sale_date,
                last_sale_date
            FROM metrics_projects
            WHERE developer = '{developer_name}'
            ORDER BY tx_count DESC
            LIMIT 20
        """).df()

        return {
            "developer": developer_name,
            "summary": developer.to_dict('records')[0],
            "projects": projects.to_dict('records')
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# PROJECT ENDPOINTS
# ============================================================

@app.get("/api/projects")
def get_projects(
    area: Optional[str] = Query(None, description="Filter by area"),
    developer: Optional[str] = Query(None, description="Filter by developer"),
    offplan: Optional[bool] = Query(None, description="Filter off-plan only"),
    min_transactions: int = Query(10, description="Minimum transactions"),
    limit: int = Query(50, description="Maximum results")
):
    """Get project listings with filters"""
    try:
        con = get_pm_db()

        query = f"""
            SELECT
                project_name_en as project,
                developer,
                area_name_en as area,
                tx_count as transactions,
                avg_price,
                median_price,
                avg_price_sqm,
                transaction_type as sale_type,
                luxury_units,
                unit_types_available,
                first_sale_date,
                last_sale_date
            FROM metrics_projects
            WHERE tx_count >= {min_transactions}
        """

        if area:
            query += f" AND area_name_en = '{area}'"
        if developer:
            query += f" AND developer = '{developer}'"
        if offplan is not None:
            reg_type = 'Off-Plan' if offplan else 'Existing'
            query += f" AND transaction_type = '{reg_type}'"

        query += f" ORDER BY tx_count DESC LIMIT {limit}"

        result = con.execute(query).df()
        return result.to_dict('records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# OFF-PLAN ENDPOINTS
# ============================================================

@app.get("/api/offplan")
def get_offplan_market():
    """Get off-plan market overview"""
    try:
        con = get_pm_db()

        # Overall off-plan stats
        overall = con.execute("""
            SELECT
                COUNT(*) as total_offplan,
                AVG(actual_worth) as avg_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY actual_worth) as median_price,
                COUNT(DISTINCT area_name_en) as areas,
                COUNT(DISTINCT project_name_en) as projects
            FROM transactions_verified
            WHERE transaction_type = 'Off-Plan'
        """).fetchone()

        # Top off-plan areas
        top_areas = con.execute("""
            SELECT
                area_name_en as area,
                offplan_count,
                ready_count,
                avg_offplan_price,
                avg_ready_price,
                offplan_percentage
            FROM metrics_offplan_comparison
            WHERE offplan_count > 100
            ORDER BY offplan_count DESC
            LIMIT 15
        """).df()

        return {
            "summary": {
                "total_offplan_transactions": overall[0],
                "avg_price": overall[1],
                "median_price": overall[2],
                "active_areas": overall[3],
                "active_projects": overall[4]
            },
            "top_areas": top_areas.to_dict('records')
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# TRENDS & ANALYTICS
# ============================================================

@app.get("/api/trends/monthly")
def get_monthly_trends(
    year: int = Query(2024, description="Year"),
    property_type: Optional[str] = Query(None, description="Property type filter")
):
    """Get market-wide monthly trends"""
    try:
        con = get_pm_db()

        query = f"""
            SELECT
                transaction_month as month,
                SUM(tx_count) as transactions,
                AVG(avg_price) as avg_price,
                AVG(median_price) as median_price,
                SUM(offplan_count) as offplan_transactions
            FROM metrics_monthly_trends
            WHERE transaction_year = {year}
        """

        if property_type:
            query += f" AND property_type = '{property_type}'"

        query += " GROUP BY transaction_month ORDER BY transaction_month"

        result = con.execute(query).df()
        return {"year": year, "trends": result.to_dict('records')}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/trends/yearly")
def get_yearly_comparison():
    """Get year-over-year market comparison"""
    try:
        con = get_pm_db()

        result = con.execute("""
            SELECT
                transaction_year as year,
                SUM(tx_count) as total_transactions,
                AVG(avg_price) as avg_price,
                AVG(avg_price_sqm) as avg_price_sqm
            FROM metrics_monthly_trends
            GROUP BY transaction_year
            ORDER BY transaction_year
        """).df()

        return result.to_dict('records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# RAW DATA ACCESS
# ============================================================

@app.get("/api/transactions")
def get_transactions(
    area: Optional[str] = Query(None, description="Filter by area"),
    min_price: Optional[float] = Query(None, description="Minimum price"),
    max_price: Optional[float] = Query(None, description="Maximum price"),
    property_type: Optional[str] = Query(None, description="Property type"),
    year: Optional[int] = Query(None, description="Transaction year"),
    limit: int = Query(100, description="Maximum results", le=1000)
):
    """Get individual transactions with filters"""
    try:
        con = get_pm_db()

        query = """
            SELECT
                transaction_id,
                instance_date as date,
                area_name_en as area,
                project_name_en as project,
                property_type,
                rooms_en as bedrooms,
                actual_worth as price,
                meter_sale_price as price_sqm,
                procedure_area as size_sqm,
                transaction_type as sale_type,
                developer,
                is_luxury
            FROM transactions_verified
            WHERE 1=1
        """

        if area:
            query += f" AND area_name_en = '{area}'"
        if min_price:
            query += f" AND actual_worth >= {min_price}"
        if max_price:
            query += f" AND actual_worth <= {max_price}"
        if property_type:
            query += f" AND property_type = '{property_type}'"
        if year:
            query += f" AND transaction_year = {year}"

        query += f" ORDER BY instance_date DESC LIMIT {limit}"

        result = con.execute(query).df()
        return result.to_dict('records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# SUPPLY INTELLIGENCE ENDPOINTS (Phase 2)
# ============================================================

import math

def clean_nan_values(obj):
    """Recursively clean NaN values from dictionaries and lists for JSON serialization"""
    if isinstance(obj, dict):
        return {k: clean_nan_values(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nan_values(item) for item in obj]
    elif isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    else:
        return obj

@app.get("/api/supply/overview")
def get_supply_overview():
    """Get market-wide supply-demand overview statistics"""
    try:
        con = get_pm_db()

        # Market balance distribution
        balance_dist = con.execute("""
            SELECT
                market_balance,
                COUNT(*) as areas,
                SUM(supply_offplan_units) as total_offplan_units,
                ROUND(AVG(supply_demand_ratio), 2) as avg_sd_ratio
            FROM metrics_supply_demand_area
            WHERE market_balance != 'Insufficient Data'
            GROUP BY market_balance
            ORDER BY
                CASE market_balance
                    WHEN 'Severely Undersupplied' THEN 1
                    WHEN 'Undersupplied' THEN 2
                    WHEN 'Balanced' THEN 3
                    WHEN 'Slightly Oversupplied' THEN 4
                    WHEN 'Oversupplied' THEN 5
                    WHEN 'Severely Oversupplied' THEN 6
                END
        """).df()

        # Overall stats
        stats = con.execute("""
            SELECT
                COUNT(*) as total_areas,
                SUM(supply_offplan_units) as total_offplan_units,
                SUM(demand_offplan_tx) as total_offplan_tx,
                AVG(supply_demand_ratio) as avg_sd_ratio,
                COUNT(CASE WHEN market_balance IN ('Oversupplied', 'Severely Oversupplied') THEN 1 END) as oversupplied_count,
                COUNT(CASE WHEN market_balance IN ('Undersupplied', 'Severely Undersupplied') THEN 1 END) as undersupplied_count
            FROM metrics_supply_demand_area
        """).fetchone()

        result = {
            "market_balance_distribution": balance_dist.to_dict('records'),
            "total_areas": stats[0],
            "total_offplan_units": stats[1],
            "total_offplan_transactions": stats[2],
            "avg_supply_demand_ratio": round(stats[3], 2) if stats[3] and not math.isnan(stats[3]) else None,
            "oversupplied_areas": stats[4],
            "undersupplied_areas": stats[5]
        }

        # Clean any NaN values
        return clean_nan_values(result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/supply/oversaturated")
def get_oversaturated_areas(
    threshold: float = Query(3.0, description="Supply-demand ratio threshold"),
    limit: int = Query(20, description="Maximum number of results")
):
    """Get list of oversaturated markets"""
    try:
        from src.analytics.supply_intelligence import SupplyIntelligence
        si = SupplyIntelligence()
        result = si.detect_supply_saturation(threshold=threshold, min_supply_units=100)

        response = {
            "threshold": threshold,
            "count": len(result),
            "areas": result.head(limit).to_dict('records')
        }
        return clean_nan_values(response)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/supply/opportunities")
def get_investment_opportunities(limit: int = Query(20, description="Maximum number of results")):
    """Get top investment opportunities ranked by opportunity score"""
    try:
        con = get_pm_db()

        result = con.execute(f"""
            SELECT
                area,
                opportunity_classification,
                market_balance,
                ROUND(opportunity_score, 1) as opportunity_score,
                supply_offplan_units,
                demand_offplan_tx,
                ROUND(supply_demand_ratio, 2) as supply_demand_ratio,
                ROUND(price_yoy_change_pct, 1) as price_yoy_change_pct,
                supply_developers,
                investment_timing
            FROM metrics_market_opportunities
            ORDER BY opportunity_score DESC
            LIMIT {limit}
        """).df()

        response = {
            "count": len(result),
            "opportunities": result.to_dict('records')
        }
        return clean_nan_values(response)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/supply/arbitrage")
def get_arbitrage_opportunities(
    min_price_growth: float = Query(10.0, description="Minimum YoY price growth %"),
    limit: int = Query(15, description="Maximum number of results")
):
    """Find arbitrage opportunities (high growth + limited supply + low competition)"""
    try:
        from src.analytics.supply_intelligence import SupplyIntelligence
        si = SupplyIntelligence()
        result = si.find_arbitrage_opportunities(min_price_growth=min_price_growth)

        response = {
            "count": len(result),
            "opportunities": result.head(limit).to_dict('records')
        }
        return clean_nan_values(response)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/supply/emerging-hotspots")
def get_emerging_hotspots(limit: int = Query(15, description="Maximum number of results")):
    """Identify emerging market hotspots"""
    try:
        from src.analytics.supply_intelligence import SupplyIntelligence
        si = SupplyIntelligence()
        result = si.identify_emerging_hotspots()

        response = {
            "count": len(result),
            "hotspots": result.head(limit).to_dict('records')
        }
        return clean_nan_values(response)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/supply/developers/reliability")
def get_developer_reliability(
    min_completed: int = Query(2, description="Minimum completed projects"),
    limit: int = Query(20, description="Maximum number of results")
):
    """Get developer reliability scores and track records"""
    try:
        from src.analytics.supply_intelligence import SupplyIntelligence
        si = SupplyIntelligence()
        result = si.score_developer_reliability(min_completed_projects=min_completed)

        response = {
            "count": len(result),
            "developers": result.head(limit).to_dict('records')
        }
        return clean_nan_values(response)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/supply/areas-by-balance")
def get_areas_by_market_balance(
    balance: str = Query(..., description="Market balance category (e.g., 'Slightly Oversupplied')")
):
    """
    Get all areas in a specific market balance category with key metrics.
    Enables drill-down from market balance distribution chart.
    """
    try:
        con = get_pm_db()

        areas = con.execute("""
            SELECT
                sd.area,
                sd.supply_demand_ratio,
                sd.supply_offplan_units,
                sd.demand_offplan_tx,
                sd.market_balance,
                opp.opportunity_score,
                opp.investment_timing,
                opp.opportunity_classification,
                opp.price_yoy_change_pct,
                sd.supply_developers
            FROM metrics_supply_demand_area sd
            LEFT JOIN metrics_market_opportunities opp ON sd.area = opp.area
            WHERE sd.market_balance = ?
            ORDER BY
                COALESCE(opp.opportunity_score, 0) DESC,
                sd.supply_demand_ratio ASC
        """, [balance]).df()

        result = {
            "balance_category": balance,
            "total_areas": len(areas),
            "areas": areas.to_dict('records')
        }

        return clean_nan_values(result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/supply/forecast")
def get_delivery_forecast(
    start_quarter: str = Query('Q1 2026', description="Starting quarter (e.g., 'Q1 2026')"),
    quarters: int = Query(8, description="Number of quarters to forecast")
):
    """Get quarterly delivery wave forecast"""
    try:
        from src.analytics.supply_intelligence import SupplyIntelligence
        si = SupplyIntelligence()
        result = si.forecast_delivery_waves(start_quarter=start_quarter, quarters_ahead=quarters)

        response = {
            "start_quarter": start_quarter,
            "quarters_forecasted": len(result),
            "forecast": result.to_dict('records')
        }
        return clean_nan_values(response)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/supply/area/{area_name}")
def get_area_supply_intelligence(area_name: str):
    """Get comprehensive supply-demand intelligence for specific area"""
    try:
        from src.analytics.supply_intelligence import SupplyIntelligence
        si = SupplyIntelligence()
        result = si.get_area_intelligence(area_name)

        if 'error' in result:
            raise HTTPException(status_code=404, detail=result['error'])

        return clean_nan_values(result)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/supply/alerts")
def get_market_alerts():
    """Get current market alerts (oversupply, opportunities, risks)"""
    try:
        from src.analytics.supply_intelligence import SupplyIntelligence
        si = SupplyIntelligence()
        alerts = si.generate_market_alerts()

        # Convert SupplyAlert objects to dicts
        alerts_data = [
            {
                "area": alert.area,
                "type": alert.alert_type,
                "severity": alert.severity,
                "message": alert.message,
                "metrics": alert.metrics
            }
            for alert in alerts
        ]

        # Group by type
        by_type = {}
        for alert in alerts_data:
            alert_type = alert['type']
            if alert_type not in by_type:
                by_type[alert_type] = []
            by_type[alert_type].append(alert)

        # Format response with key names matching frontend expectations
        response = {
            "total_alerts": len(alerts_data),
            "oversupply": by_type.get('OVERSUPPLY', []),
            "opportunities": by_type.get('OPPORTUNITY', []),
            "risks": by_type.get('RISK', []),
            "by_type": by_type,
            "all_alerts": alerts_data
        }
        return clean_nan_values(response)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# CONTENT GENERATION ENDPOINTS
# ============================================================

@app.post("/api/content/generate/area")
def generate_area_content(
    area_name: str = Query(..., description="Area name"),
    year_from: Optional[int] = Query(None, description="Start year"),
    year_to: Optional[int] = Query(None, description="End year"),
    with_verification: bool = Query(False, description="Generate Excel verification file for data science team")
):
    """Generate AI content for an area"""
    try:
        from src.content.pm_generator import PMContentGenerator
        generator = PMContentGenerator()
        result = generator.generate_area_guide(area_name, year_from, year_to, with_verification=with_verification)

        # Handle tuple return when verification is enabled
        verification_filename = None
        if with_verification and isinstance(result, tuple):
            filepath, verification_path = result
            verification_filename = verification_path.name if hasattr(verification_path, 'name') else str(verification_path).split('\\')[-1]
        else:
            filepath = result

        if filepath:
            response = {
                "status": "success",
                "message": f"Generated area guide for {area_name}",
                "filename": filepath.name if hasattr(filepath, 'name') else str(filepath).split('/')[-1]
            }
            if verification_filename:
                response["verification_file"] = verification_filename
            return response
        else:
            raise HTTPException(status_code=400, detail=f"Failed to generate content for {area_name}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/content/generate/batch")
def generate_batch_content():
    """Generate AI content for top 10 areas"""
    try:
        from src.content.pm_generator import generate_pm_content_batch
        generated = generate_pm_content_batch()

        return {
            "status": "success",
            "message": f"Generated {len(generated)} area guides",
            "files": [str(f).split('/')[-1] for f in generated]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/content/generate/market-report")
def generate_market_report(
    year: int = Query(2025, description="Year"),
    period_type: str = Query("monthly", description="Period type: monthly, quarterly, semi_annual, or annual"),
    period_number: int = Query(1, description="Period number (1-12 for monthly, 1-4 for quarterly, 1-2 for semi-annual, 1 for annual)"),
    with_verification: bool = Query(False, description="Generate Excel verification file for data science team")
):
    """
    Generate market report for any period type.

    Period types:
    - monthly: period_number 1-12 (January-December)
    - quarterly: period_number 1-4 (Q1-Q4)
    - semi_annual: period_number 1-2 (H1-H2)
    - annual: period_number 1 (Full Year)

    Set with_verification=true to generate an Excel file with figure verification,
    query logs, and calculation traces for data science team review.
    """
    try:
        from src.content.pm_generator import PMContentGenerator
        generator = PMContentGenerator()
        result = generator.generate_market_report(
            year=year,
            period_type=period_type,
            period_number=period_number,
            with_verification=with_verification
        )

        # Handle tuple return when verification is enabled
        verification_filename = None
        if with_verification and isinstance(result, tuple):
            filepath, verification_path = result
            verification_filename = verification_path.name if hasattr(verification_path, 'name') else str(verification_path).split('\\')[-1]
        else:
            filepath = result

        # Build descriptive message based on period type
        period_labels = {
            'monthly': ['', 'January', 'February', 'March', 'April', 'May', 'June',
                       'July', 'August', 'September', 'October', 'November', 'December'],
            'quarterly': ['', 'Q1', 'Q2', 'Q3', 'Q4'],
            'semi_annual': ['', 'H1', 'H2'],
            'annual': ['', 'Full Year']
        }

        labels = period_labels.get(period_type, period_labels['monthly'])
        period_name = labels[period_number] if period_number < len(labels) else f"Period {period_number}"

        if filepath:
            response = {
                "status": "success",
                "message": f"Generated {period_type} market report for {period_name} {year}",
                "filename": filepath.name if hasattr(filepath, 'name') else str(filepath).split('/')[-1],
                "period_info": {
                    "year": year,
                    "period_type": period_type,
                    "period_number": period_number,
                    "period_name": f"{period_name} {year}"
                }
            }
            if verification_filename:
                response["verification_file"] = verification_filename
            return response
        else:
            raise HTTPException(status_code=400, detail="Failed to generate market report")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/content/generate/developer")
def generate_developer_content(
    developer_name: str = Query(..., description="Developer name"),
    with_verification: bool = Query(False, description="Generate Excel verification file for data science team")
):
    """Generate AI content for a developer"""
    try:
        from src.content.pm_generator import PMContentGenerator
        generator = PMContentGenerator()
        result = generator.generate_developer_report(developer_name, with_verification=with_verification)

        # Handle tuple return when verification is enabled
        verification_filename = None
        if with_verification and isinstance(result, tuple):
            filepath, verification_path = result
            verification_filename = verification_path.name if hasattr(verification_path, 'name') else str(verification_path).split('\\')[-1]
        else:
            filepath = result

        if filepath:
            response = {
                "status": "success",
                "message": f"Generated developer profile for {developer_name}",
                "filename": filepath.name if hasattr(filepath, 'name') else str(filepath).split('/')[-1]
            }
            if verification_filename:
                response["verification_file"] = verification_filename
            return response
        else:
            raise HTTPException(status_code=400, detail=f"Failed to generate content for {developer_name}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/content/generate/offplan")
def generate_offplan_content(
    year: int = Query(2025, description="Year"),
    period_type: str = Query("annual", description="Period type: monthly, quarterly, semi_annual, or annual"),
    period_number: int = Query(1, description="Period number (1-12 for monthly, 1-4 for quarterly, 1-2 for semi-annual, 1 for annual)"),
    with_verification: bool = Query(False, description="Generate Excel verification file for data science team")
):
    """
    Generate off-plan market report for any period type.

    Period types:
    - monthly: period_number 1-12 (January-December)
    - quarterly: period_number 1-4 (Q1-Q4)
    - semi_annual: period_number 1-2 (H1-H2)
    - annual: period_number 1 (Full Year)
    """
    try:
        from src.content.pm_generator import PMContentGenerator
        generator = PMContentGenerator()
        result = generator.generate_offplan_report(
            year=year,
            period_type=period_type,
            period_number=period_number,
            with_verification=with_verification
        )

        # Handle tuple return when verification is enabled
        verification_filename = None
        if with_verification and isinstance(result, tuple):
            filepath, verification_path = result
            verification_filename = verification_path.name if hasattr(verification_path, 'name') else str(verification_path).split('\\')[-1]
        else:
            filepath = result

        # Build descriptive message based on period type
        period_labels = {
            'monthly': ['', 'January', 'February', 'March', 'April', 'May', 'June',
                       'July', 'August', 'September', 'October', 'November', 'December'],
            'quarterly': ['', 'Q1', 'Q2', 'Q3', 'Q4'],
            'semi_annual': ['', 'H1', 'H2'],
            'annual': ['', 'Full Year']
        }

        labels = period_labels.get(period_type, period_labels['annual'])
        period_name = labels[period_number] if period_number < len(labels) else f"Period {period_number}"

        if filepath:
            response = {
                "status": "success",
                "message": f"Generated {period_type} off-plan report for {period_name} {year}",
                "filename": filepath.name if hasattr(filepath, 'name') else str(filepath).split('/')[-1],
                "period_info": {
                    "year": year,
                    "period_type": period_type,
                    "period_number": period_number,
                    "period_name": f"{period_name} {year}"
                }
            }
            if verification_filename:
                response["verification_file"] = verification_filename
            return response
        else:
            raise HTTPException(status_code=400, detail="Failed to generate off-plan report")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/content/generate/luxury-report")
def generate_luxury_report(
    year: int = Query(2025, description="Year"),
    period_type: str = Query("annual", description="Period type: monthly, quarterly, semi_annual, or annual"),
    period_number: int = Query(1, description="Period number"),
    with_verification: bool = Query(False, description="Generate Excel verification file for data science team")
):
    """
    Generate luxury market report (5M+ AED properties).

    This generates a market report focused on the luxury segment.
    """
    try:
        from src.content.pm_generator import PMContentGenerator
        generator = PMContentGenerator()

        # Generate luxury-focused report using the market report generator
        # The luxury data is already filtered by is_luxury in the database
        result = generator.generate_luxury_report(
            year=year,
            period_type=period_type,
            period_number=period_number,
            with_verification=with_verification
        )

        # Handle tuple return when verification is enabled
        verification_filename = None
        if with_verification and isinstance(result, tuple):
            filepath, verification_path = result
            verification_filename = verification_path.name if hasattr(verification_path, 'name') else str(verification_path).split('\\')[-1]
        else:
            filepath = result

        period_labels = {
            'monthly': ['', 'January', 'February', 'March', 'April', 'May', 'June',
                       'July', 'August', 'September', 'October', 'November', 'December'],
            'quarterly': ['', 'Q1', 'Q2', 'Q3', 'Q4'],
            'semi_annual': ['', 'H1', 'H2'],
            'annual': ['', 'Full Year']
        }

        labels = period_labels.get(period_type, period_labels['annual'])
        period_name = labels[period_number] if period_number < len(labels) else f"Period {period_number}"

        if filepath:
            response = {
                "status": "success",
                "message": f"Generated luxury market report for {period_name} {year}",
                "filename": filepath.name if hasattr(filepath, 'name') else str(filepath).split('/')[-1],
                "period_info": {
                    "year": year,
                    "period_type": period_type,
                    "period_number": period_number,
                    "period_name": f"{period_name} {year}"
                }
            }
            if verification_filename:
                response["verification_file"] = verification_filename
            return response
        else:
            raise HTTPException(status_code=400, detail="Failed to generate luxury report")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/content/generate/supply-forecast")
def generate_supply_forecast(
    start_quarter: str = Query('Q1 2026', description="Starting quarter (e.g., 'Q1 2026')"),
    quarters_ahead: int = Query(8, description="Number of quarters to forecast")
):
    """
    Generate supply forecast report using MarketIntelligenceEngine (Phase 2).

    Provides delivery pipeline, oversupply risks, and emerging hotspots.
    """
    try:
        import duckdb
        from src.analytics.market_intelligence.engine import MarketIntelligenceEngine
        from config.settings import settings
        from datetime import datetime

        # Get intelligence
        con = duckdb.connect(str(bq_settings.PM_DB_PATH), read_only=True)
        engine = MarketIntelligenceEngine(con)
        intel = engine.get_supply_forecast_intelligence(start_quarter=start_quarter, quarters_ahead=quarters_ahead)
        con.close()

        # Generate markdown content
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"pm_supply_forecast_{start_quarter.replace(' ', '_')}_{timestamp}.md"
        filepath = settings.CONTENT_OUTPUT_DIR / filename

        # Ensure output directory exists
        filepath.parent.mkdir(parents=True, exist_ok=True)

        # Build content
        content = f"""---
title: "Dubai Real Estate Supply Forecast - {start_quarter} onwards"
date: {datetime.now().strftime('%Y-%m-%d')}
quarters_forecasted: {quarters_ahead}
content_type: supply_forecast
data_source: Property Monitor
---

# Dubai Real Estate Supply Forecast

**Forecast Period:** {start_quarter} onwards ({quarters_ahead} quarters)
**Generated:** {datetime.now().strftime('%B %d, %Y')}
**Data Source:** Property Monitor Exclusive Supply Intelligence

---

{intel.primary_insights}

---

## About This Report

This supply forecast report combines:
- **Project Pipeline Data:** {intel.supporting_data.get('delivery_forecast', [{}])[0].get('projects_delivering', 'N/A') if intel.supporting_data.get('delivery_forecast') else 'N/A'} projects in Q1 delivery wave
- **Supply-Demand Correlation:** Exclusive market balance metrics
- **Market Alerts:** {len(intel.supporting_data.get('alerts', []))} active market alerts

**Competitive Advantage:** This intelligence is unavailable on any competitor platform in Dubai.

---

*Generated by Property Monitor AI Content System*
*© {datetime.now().year} Property Monitor Intelligence*
"""

        # Write file
        filepath.write_text(content, encoding='utf-8')

        return {
            "status": "success",
            "message": f"Generated supply forecast for {start_quarter} onwards",
            "filename": filename,
            "forecast_info": {
                "start_quarter": start_quarter,
                "quarters_ahead": quarters_ahead,
                "delivery_forecasts": len(intel.supporting_data.get('delivery_forecast', [])),
                "oversupply_alerts": len(intel.supporting_data.get('oversupplied_areas', [])),
                "emerging_hotspots": len(intel.supporting_data.get('emerging_hotspots', []))
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/content/generate/project-profile")
def generate_project_profile(
    area_name: str = Query(..., description="Area name for project profile")
):
    """
    Generate project profile intelligence for specific area (Phase 2).

    Provides area context, supply pipeline, and investment outlook.
    """
    try:
        import duckdb
        from src.analytics.market_intelligence.engine import MarketIntelligenceEngine
        from config.settings import settings
        from datetime import datetime

        # Get intelligence
        con = duckdb.connect(str(bq_settings.PM_DB_PATH), read_only=True)
        engine = MarketIntelligenceEngine(con)
        intel = engine.get_project_profile_intelligence(area_name)
        con.close()

        # Generate markdown content
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_area_name = area_name.replace(' ', '_').replace('/', '-')
        filename = f"pm_project_profile_{safe_area_name}_{timestamp}.md"
        filepath = settings.CONTENT_OUTPUT_DIR / filename

        # Ensure output directory exists
        filepath.parent.mkdir(parents=True, exist_ok=True)

        # Get opportunity score for title enhancement
        opp_score = intel.supporting_data.get('opportunity', {}).get('score', 0)
        score_label = "High Potential" if opp_score > 70 else "Moderate Potential" if opp_score > 40 else "Standard"

        # Build content
        content = f"""---
title: "Dubai Real Estate Project Profile - {area_name}"
date: {datetime.now().strftime('%Y-%m-%d')}
area: {area_name}
content_type: project_profile
data_source: Property Monitor
opportunity_score: {opp_score}
---

# {area_name} - Project Profile

**Market Potential:** {score_label}
**Generated:** {datetime.now().strftime('%B %d, %Y')}
**Data Source:** Property Monitor Supply Intelligence

---

{intel.primary_insights}

---

## Data Coverage

This profile combines:
- **Supply Pipeline:** {intel.supporting_data.get('area_metrics', {}).get('supply_offplan_units', 0):,.0f} off-plan units
- **Market Activity:** {intel.supporting_data.get('area_metrics', {}).get('demand_offplan_tx', 0):,.0f} recent transactions
- **Developer Analysis:** {len(intel.supporting_data.get('top_developers', []))} major developers active
- **Delivery Timeline:** {len(intel.supporting_data.get('delivery_timeline', []))} quarters forecasted

**Unique Intelligence:** Supply-demand correlation unavailable on competitor platforms.

---

*Generated by Property Monitor AI Content System*
*© {datetime.now().year} Property Monitor Intelligence*
"""

        # Write file
        filepath.write_text(content, encoding='utf-8')

        return {
            "status": "success",
            "message": f"Generated project profile for {area_name}",
            "filename": filename,
            "area_info": {
                "area_name": area_name,
                "opportunity_score": opp_score,
                "opportunity_classification": intel.supporting_data.get('opportunity', {}).get('classification'),
                "supply_units": intel.supporting_data.get('area_metrics', {}).get('supply_offplan_units', 0),
                "top_developers_count": len(intel.supporting_data.get('top_developers', []))
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/content/sample")
def get_sample_content():
    """Get the most recent generated content"""
    try:
        from config.settings import settings
        content_dir = settings.CONTENT_OUTPUT_DIR

        if not content_dir.exists():
            return {"status": "no_content", "message": "No content generated yet"}

        # Get most recent PM content file
        pm_files = sorted(content_dir.glob("pm_*.md"), key=lambda x: x.stat().st_mtime, reverse=True)

        if not pm_files:
            return {"status": "no_content", "message": "No Property Monitor content generated yet"}

        latest = pm_files[0]
        content = latest.read_text(encoding='utf-8')

        return {
            "status": "success",
            "filename": latest.name,
            "preview": content[:3000] + "..." if len(content) > 3000 else content
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/content/list")
def list_content():
    """List all generated content files"""
    try:
        from config.settings import settings
        content_dir = settings.CONTENT_OUTPUT_DIR

        if not content_dir.exists():
            return {"count": 0, "files": []}

        # Get PM content files
        files = sorted(content_dir.glob("pm_*.md"), key=lambda x: x.stat().st_mtime, reverse=True)

        return {
            "count": len(files),
            "files": [
                {
                    "filename": f.name,
                    "size": f.stat().st_size,
                    "modified": f.stat().st_mtime
                }
                for f in files[:20]
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/content/{filename}")
def get_content(filename: str):
    """Get content of a specific file with visualization data"""
    try:
        from config.settings import settings
        import re
        import json
        import yaml

        filepath = settings.CONTENT_OUTPUT_DIR / filename

        if not filepath.exists():
            raise HTTPException(status_code=404, detail="Content file not found")

        content = filepath.read_text(encoding='utf-8')

        # Parse YAML frontmatter for metadata
        metadata = {}
        if content.startswith('---'):
            parts = content.split('---', 2)
            if len(parts) >= 3:
                try:
                    metadata = yaml.safe_load(parts[1]) or {}
                except:
                    pass

        # Extract Chart.js configs from HTML comments
        chartjs_configs = {}
        pattern = r'<!-- CHARTJS_CONFIG:(\w+):(.+?) -->'
        matches = re.findall(pattern, content, re.DOTALL)
        for chart_name, config_json in matches:
            try:
                chartjs_configs[chart_name] = json.loads(config_json)
            except:
                pass

        return {
            "filename": filename,
            "content": content,
            "metadata": metadata,
            "visualizations": {
                "chartjs_configs": chartjs_configs,
                "has_charts": len(chartjs_configs) > 0
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# VISUALIZATION / CHART ENDPOINTS
# ============================================================

@app.get("/api/charts/trend")
def get_trend_chart(
    year: int = Query(2024, description="Year"),
    metric: str = Query("transaction_count", description="Metric: transaction_count, total_volume, avg_price"),
    segment: Optional[str] = Query(None, description="Segment: luxury, offplan, ready")
):
    """Get transaction trend chart data with Chart.js config"""
    try:
        from src.visualization import VizGenerator
        gen = VizGenerator()
        result = gen.generate_trend_chart(year, metric, segment)
        gen.close()

        return {
            "title": result.title,
            "chart_type": result.chart_type.value,
            "chartjs_config": result.interactive_config,
            "insights": result.insights,
            "static_image_base64": result.static_image
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/charts/top-areas")
def get_top_areas_chart(
    year: int = Query(None, description="Year filter"),
    quarter: int = Query(None, description="Quarter (1-4)"),
    metric: str = Query("transaction_count", description="Metric: transaction_count, total_volume, avg_price"),
    limit: int = Query(10, description="Number of areas"),
    segment: Optional[str] = Query(None, description="Segment: luxury, offplan")
):
    """Get top areas ranking chart with Chart.js config"""
    try:
        from src.visualization import VizGenerator
        gen = VizGenerator()
        result = gen.generate_top_areas_chart(year, quarter, metric, limit, segment)
        gen.close()

        return {
            "title": result.title,
            "chart_type": result.chart_type.value,
            "chartjs_config": result.interactive_config,
            "insights": result.insights,
            "static_image_base64": result.static_image
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/charts/segments")
def get_segment_chart(
    year: int = Query(None, description="Year filter"),
    quarter: int = Query(None, description="Quarter (1-4)"),
    segment_type: str = Query("offplan_ready", description="Type: offplan_ready, luxury_tiers, property_types")
):
    """Get market segment distribution chart with Chart.js config"""
    try:
        from src.visualization import VizGenerator
        gen = VizGenerator()
        result = gen.generate_segment_chart(year, quarter, segment_type)
        gen.close()

        return {
            "title": result.title,
            "chart_type": result.chart_type.value,
            "chartjs_config": result.interactive_config,
            "insights": result.insights,
            "static_image_base64": result.static_image
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/charts/report/{report_type}")
def get_report_charts(
    report_type: str,
    year: int = Query(2024, description="Year"),
    quarter: int = Query(None, description="Quarter (1-4)")
):
    """
    Get all charts for a report type.

    Report types: market, luxury, offplan, area_guide, developer
    """
    try:
        from src.visualization import VizGenerator
        from src.visualization.generator import OutputFormat

        gen = VizGenerator()
        results = gen.generate_report_visualizations(
            report_type=report_type,
            year=year,
            quarter=quarter,
            output_format=OutputFormat.BOTH,
            include_insights=True
        )
        gen.close()

        charts = {}
        for name, result in results.items():
            charts[name] = {
                "title": result.title,
                "chart_type": result.chart_type.value,
                "chartjs_config": result.interactive_config,
                "insights": result.insights,
                "legend_description": result.legend_description,
                "markdown": result.markdown
            }

        return {
            "report_type": report_type,
            "year": year,
            "quarter": quarter,
            "charts": charts
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# SERVER STARTUP
# ============================================================

def check_and_build_metrics():
    """Check if metrics exist, build if missing"""
    try:
        con = get_pm_db()

        try:
            result = con.execute("SELECT COUNT(*) FROM metrics_area").fetchone()
            if result[0] > 0:
                print(f"  Metrics found: {result[0]} areas")
                return True
        except:
            pass

        print("\n  Metrics not found. Building...")
        from src.metrics.pm_calculator import rebuild_pm_metrics
        rebuild_pm_metrics()
        print("  Metrics built!\n")
        return True

    except Exception as e:
        print(f"\n  Error: {e}")
        print("  Run: python src/metrics/pm_calculator.py\n")
        return False


def start_server(host="0.0.0.0", port=8001):
    """Start the Property Monitor API server"""
    print("=" * 60)
    print("PROPERTY MONITOR REAL ESTATE API")
    print("=" * 60)
    print(f"  Database: {bq_settings.PM_DB_PATH}")

    check_and_build_metrics()

    print(f"\n  Starting server...")
    print(f"  Server: http://{host}:{port}")
    print(f"  Docs: http://localhost:{port}/docs")
    print(f"  Status: Ready\n")

    uvicorn.run(
        "src.api.pm_api:app",
        host=host,
        port=port,
        reload=True
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Property Monitor API Server')
    parser.add_argument('--host', default='0.0.0.0', help='Host address')
    parser.add_argument('--port', type=int, default=8001, help='Port number')

    args = parser.parse_args()
    start_server(host=args.host, port=args.port)
