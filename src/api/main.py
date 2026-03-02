# src/api/main.py

"""
FastAPI Server - Run this to start your API
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List
import uvicorn

from src.utils.db import get_db
from config.settings import settings

app = FastAPI(
    title="Dubai Real Estate Intelligence API",
    description="API for Dubai Land Department transaction data and insights",
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

# Database connection
# con = get_db()

def get_con():
    """Get database connection in read-only mode (allows multiple processes)"""
    return get_db(read_only=True)

@app.get("/")
def root():
    """API Health Check"""
    return {
        "status": "online",
        "message": "Dubai Real Estate Intelligence API",
        "version": "1.0.0"
    }

@app.get("/api/areas")
def get_areas(
    min_transactions: int = Query(10, description="Minimum transaction count"),
    luxury_only: bool = Query(False, description="Only luxury areas")
):
    """Get list of areas with metrics"""
    
    query = f"""
        SELECT 
            area_name_en as area,
            total_transactions,
            avg_price,
            median_price,
            avg_price_sqm,
            luxury_count,
            nearby_metros,
            nearby_malls
        FROM metrics_area
        WHERE total_transactions >= {min_transactions}
    """
    
    if luxury_only:
        query += " AND luxury_count > 0"
    
    query += " ORDER BY total_transactions DESC"
    
    try:
        result = get_con().execute(query).df()
        return result.to_dict('records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/area/{area_name}")
def get_area_details(area_name: str):
    """Get detailed metrics for a specific area"""
    
    try:
        # Basic metrics
        metrics = get_con().execute(f"""
            SELECT * FROM metrics_area
            WHERE area_name_en = '{area_name}'
        """).df()
        
        if metrics.empty:
            raise HTTPException(status_code=404, detail=f"Area '{area_name}' not found")
        
        # Recent trends
        trends = get_con().execute(f"""
            SELECT 
                transaction_year,
                transaction_month,
                property_type_en,
                tx_count,
                avg_price,
                avg_price_sqm
            FROM metrics_monthly_trends
            WHERE area_name_en = '{area_name}'
            ORDER BY transaction_year DESC, transaction_month DESC
            LIMIT 12
        """).df()
        
        # Property types
        prop_types = get_con().execute(f"""
            SELECT 
                rooms_en,
                tx_count,
                avg_price,
                avg_price_sqm,
                avg_size_sqm
            FROM metrics_property_types
            WHERE area_name_en = '{area_name}'
            ORDER BY tx_count DESC
        """).df()
        
        return {
            "area": area_name,
            "metrics": metrics.to_dict('records')[0],
            "recent_trends": trends.to_dict('records'),
            "property_types": prop_types.to_dict('records')
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/trends")
def get_market_trends(
    year: int = Query(2024, description="Year"),
    property_type: Optional[str] = Query(None, description="Property type filter")
):
    """Get market-wide trends"""
    
    query = f"""
        SELECT 
            area_name_en,
            transaction_month,
            AVG(avg_price) as avg_price,
            SUM(tx_count) as total_transactions
        FROM metrics_monthly_trends
        WHERE transaction_year = {year}
    """
    
    if property_type:
        query += f" AND property_type_en = '{property_type}'"
    
    query += """
        GROUP BY area_name_en, transaction_month
        ORDER BY transaction_month, total_transactions DESC
    """
    
    try:
        result = get_con().execute(query).df()
        return result.to_dict('records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/luxury")
def get_luxury_market(year: Optional[int] = Query(None)):
    """Get luxury market data (5M+ AED)"""
    
    query = "SELECT * FROM metrics_luxury"
    
    if year:
        query += f" WHERE transaction_year = {year}"
    
    query += " ORDER BY luxury_tx_count DESC"
    
    try:
        result = get_con().execute(query).df()
        return result.to_dict('records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/projects")
def get_projects(
    area: Optional[str] = Query(None),
    off_plan: Optional[bool] = Query(None)
):
    """Get project/developer data"""
    
    query = """
        SELECT 
            project_name_en,
            master_project_en,
            area_name_en,
            reg_type_en,
            tx_count,
            avg_price,
            first_sale_year,
            last_sale_year,
            luxury_units
        FROM metrics_projects
        WHERE 1=1
    """
    
    if area:
        query += f" AND area_name_en = '{area}'"
    
    if off_plan is not None:
        reg_type = 'Off-Plan Properties' if off_plan else 'Existing Properties'
        query += f" AND reg_type_en = '{reg_type}'"
    
    query += " ORDER BY tx_count DESC LIMIT 50"
    
    try:
        result = get_con().execute(query).df()
        return result.to_dict('records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/compare")
def compare_areas(areas: List[str] = Query(..., description="Area names to compare")):
    """Compare multiple areas"""
    
    if len(areas) > 5:
        raise HTTPException(status_code=400, detail="Maximum 5 areas for comparison")
    
    areas_str = "', '".join(areas)
    
    try:
        result = get_con().execute(f"""
            SELECT 
                area_name_en,
                total_transactions,
                avg_price,
                median_price,
                avg_price_sqm,
                luxury_count
            FROM metrics_area
            WHERE area_name_en IN ('{areas_str}')
        """).df()
        
        if result.empty:
            raise HTTPException(status_code=404, detail="No data found for specified areas")
        
        return result.to_dict('records')
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats/overview")
def get_overview_stats():
    """Get overall market statistics"""

    try:
        result = get_con().execute("""
            SELECT
                COUNT(*) as total_transactions,
                COUNT(DISTINCT area_name_en) as total_areas,
                AVG(actual_worth) as overall_avg_price,
                SUM(CASE WHEN is_luxury THEN 1 ELSE 0 END) as luxury_transactions,
                MIN(transaction_year) as data_from_year,
                MAX(transaction_year) as data_to_year
            FROM transactions_clean
            WHERE trans_group_en = 'Sales'
        """).df()

        return result.to_dict('records')[0]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/content/list")
def list_generated_content():
    """List all generated content files"""

    try:
        content_dir = settings.CONTENT_OUTPUT_DIR

        if not content_dir.exists():
            return {"files": []}

        files = []
        for filepath in content_dir.glob("*.md"):
            stat = filepath.stat()
            files.append({
                "filename": filepath.name,
                "size": stat.st_size,
                "created": stat.st_ctime,
                "modified": stat.st_mtime
            })

        # Sort by modification time (newest first)
        files.sort(key=lambda x: x['modified'], reverse=True)

        return {"files": files, "count": len(files)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/content/{filename}")
def get_content_file(filename: str):
    """Get specific generated content file with visualization data"""

    try:
        content_dir = settings.CONTENT_OUTPUT_DIR
        filepath = content_dir / filename

        if not filepath.exists():
            raise HTTPException(status_code=404, detail="Content file not found")

        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        # Parse YAML frontmatter for metadata
        metadata = {}
        if content.startswith('---'):
            parts = content.split('---', 2)
            if len(parts) >= 3:
                try:
                    import yaml
                    metadata = yaml.safe_load(parts[1]) or {}
                except:
                    pass

        # Extract Chart.js configs from HTML comments
        import re
        chartjs_configs = {}
        pattern = r'<!-- CHARTJS_CONFIG:(\w+):(.+?) -->'
        matches = re.findall(pattern, content, re.DOTALL)
        for chart_name, config_json in matches:
            try:
                import json
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

@app.post("/api/content/generate/area")
def generate_area_content(
    area_name: str = Query(..., description="Area name"),
    year_from: Optional[int] = Query(None, description="Start year"),
    year_to: Optional[int] = Query(None, description="End year")
):
    """Generate content for a specific area"""

    try:
        from src.content.generator import ContentGenerator

        generator = ContentGenerator()
        filepath = generator.generate_area_guide(area_name, year_from, year_to)

        if not filepath:
            raise HTTPException(status_code=500, detail="Content generation failed")

        return {
            "status": "success",
            "filename": filepath.name,
            "message": f"Content generated for {area_name}"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/content/sample")
def get_sample_content():
    """Get a sample of generated content to demonstrate capability"""

    try:
        content_dir = settings.CONTENT_OUTPUT_DIR

        if not content_dir.exists():
            return {
                "status": "no_content",
                "message": "No content generated yet. Use the content generator to create content."
            }

        # Get the most recent markdown file
        files = sorted(content_dir.glob("*.md"), key=lambda p: p.stat().st_mtime, reverse=True)

        if not files:
            return {
                "status": "no_content",
                "message": "No content generated yet."
            }

        sample_file = files[0]
        with open(sample_file, 'r', encoding='utf-8') as f:
            content = f.read()

        # Extract just the first 1500 characters as a preview
        preview = content[:1500] + "..." if len(content) > 1500 else content

        return {
            "status": "success",
            "filename": sample_file.name,
            "preview": preview,
            "full_length": len(content)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/content/generate/batch")
def generate_batch_content():
    """Generate area guides for top 10 areas"""

    try:
        from src.content.generator import generate_content_batch

        filepaths = generate_content_batch()

        return {
            "status": "success",
            "count": len(filepaths),
            "message": f"Generated {len(filepaths)} area guides"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/content/generate/market-report")
def generate_market_report(
    year: int = Query(2024, description="Year"),
    month: int = Query(1, description="Month (1-12)")
):
    """Generate monthly market report"""

    try:
        from src.content.generator import ContentGenerator

        generator = ContentGenerator()
        filepath = generator.generate_monthly_market_report(year, month)

        if not filepath:
            raise HTTPException(status_code=500, detail="Market report generation failed")

        return {
            "status": "success",
            "filename": filepath.name,
            "message": f"Market report generated for {month}/{year}"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/content/generate/property-comparison")
def generate_property_comparison(
    room_types: List[str] = Query(..., description="Room types to compare")
):
    """Generate property type comparison"""

    try:
        from src.content.generator import ContentGenerator

        generator = ContentGenerator()
        filepath = generator.generate_property_comparison(room_types)

        if not filepath:
            raise HTTPException(status_code=500, detail="Property comparison generation failed")

        return {
            "status": "success",
            "filename": filepath.name,
            "message": f"Property comparison generated for {', '.join(room_types)}"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/content/generate/luxury-report")
def generate_luxury_report(
    year: int = Query(2024, description="Year")
):
    """Generate luxury market report"""

    try:
        # Get luxury data
        luxury_data = get_con().execute(f"""
            SELECT
                area_name_en,
                luxury_tx_count,
                avg_luxury_price,
                max_luxury_price
            FROM metrics_luxury
            WHERE transaction_year = {year}
            ORDER BY luxury_tx_count DESC
            LIMIT 10
        """).df()

        if luxury_data.empty:
            raise HTTPException(status_code=404, detail=f"No luxury data found for {year}")

        # Format as report content
        report_content = f"""# Dubai Luxury Market Report {year}

## Overview
Analysis of high-end properties (AED 5M+) in Dubai's most prestigious locations.

## Top Luxury Markets

| Area | Transactions | Avg Price | Max Price |
|------|-------------|-----------|-----------|
"""
        for _, row in luxury_data.iterrows():
            report_content += f"| {row['area_name_en']} | {int(row['luxury_tx_count']):,} | AED {int(row['avg_luxury_price']):,} | AED {int(row['max_luxury_price']):,} |\n"

        report_content += f"""

## Key Insights
- Total luxury transactions analyzed: {int(luxury_data['luxury_tx_count'].sum()):,}
- Top luxury area: {luxury_data.iloc[0]['area_name_en']}
- Highest average price: AED {int(luxury_data['avg_luxury_price'].max()):,}

---
**Generated**: {year} Luxury Market Analysis
**Source**: Dubai Land Department Transaction Records
"""

        # Save the report
        content_dir = settings.CONTENT_OUTPUT_DIR
        content_dir.mkdir(parents=True, exist_ok=True)
        filepath = content_dir / f"luxury_report_{year}.md"

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(report_content)

        return {
            "status": "success",
            "filename": filepath.name,
            "message": f"Luxury report generated for {year}"
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def check_and_build_metrics():
    """Check if metrics exist, build them if missing"""
    try:
        con = get_db(read_only=True)

        # Check if metrics_area table exists and has data
        try:
            result = con.execute("SELECT COUNT(*) FROM metrics_area").fetchone()
            if result[0] > 0:
                print(f"[OK] Metrics found: {result[0]} areas")
                return True
        except:
            pass

        # Metrics missing or empty - build them
        print("\n[WARNING] Metrics not found. Building metrics automatically...")
        print("This may take a few minutes...\n")

        from src.metrics.calculator import rebuild_metrics
        rebuild_metrics()

        print("\n[SUCCESS] Metrics built successfully!\n")
        return True

    except Exception as e:
        print(f"\n[ERROR] Error building metrics: {e}")
        print("You may need to run: python src/metrics/calculator.py\n")
        return False

def start_server():
    """Start the API server"""
    print("="*60)
    print("DUBAI REAL ESTATE INTELLIGENCE API")
    print("="*60)

    # Check and build metrics if needed
    check_and_build_metrics()

    print(f"\n[INFO] Starting API Server...")
    print(f"   Server: http://{settings.API_HOST}:{settings.API_PORT}")
    print(f"   Docs: http://localhost:{settings.API_PORT}/docs")
    print(f"   Status: Ready\n")

    uvicorn.run(
        "src.api.main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=True
    )

if __name__ == "__main__":
    start_server()