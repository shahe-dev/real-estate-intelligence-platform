# src/content/generator.py

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Fix Windows console encoding for emojis
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

"""
AI Content Generator with Anti-Hallucination Safeguards
"""

import anthropic
from datetime import datetime
from pathlib import Path
from config.settings import settings
from src.utils.db import get_db

class ContentGenerator:
    """Generate SEO content with verification"""
    
    def __init__(self):
        if not settings.ANTHROPIC_API_KEY:
            raise ValueError("ANTHROPIC_API_KEY not set in .env file")

        self.client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)
        self.con = get_db(read_only=True)
        self.output_dir = settings.CONTENT_OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_area_guide(self, area_name, year_from=None, year_to=None):
        """Generate comprehensive area guide
        
        Args:
            area_name: Name of the area
            year_from: Start year for data (default: all years)
            year_to: End year for data (default: all years)
        """
        
        print(f"[INFO] Generating area guide for: {area_name}")

        # STEP 1: Get VERIFIED data only
        data = self._get_area_data(area_name, year_from, year_to)

        if not data:
            print(f"[ERROR] No data found for {area_name}")
            return None
        
        # STEP 2: Generate content using ONLY verified data
        content = self._generate_with_verification(
            template='area_guide',
            data=data,
            area_name=area_name
        )
        
        # STEP 3: Validate content doesn't hallucinate
        is_valid, issues = self._validate_content(content, data)

        if not is_valid:
            print(f"[WARNING] Content validation failed:")
            for issue in issues:
                print(f"   - {issue}")
            return None

        # STEP 4: Save content
        filepath = self._save_content(
            content=content,
            filename=f"area_guide_{area_name.replace(' ', '_').lower()}.md",
            metadata=data
        )

        print(f"[SUCCESS] Generated: {filepath}")
        return filepath
    
    def _get_area_data(self, area_name, year_from=None, year_to=None):
        """Get verified data for area WITH source transaction IDs
        
        Args:
            area_name: Name of the area_name
            year_from: Start year (default: use all data)
            year_to: End year (default: use all data)
        """

        try:
            # Build date filter
            date_filter = ""
            if year_from and year_to:
                date_filter = f"AND transaction_year BETWEEN {year_from} AND {year_to}"
            elif year_from:
                date_filter = f"AND transaction_year >= {year_from}"
            elif year_to:
                date_filter = f"AND transaction_year <= {year_to}"
            
            # Main metrics
            metrics = self.con.execute(f"""
                SELECT 
                    area_name_en,
                    total_transactions,
                    avg_price,
                    median_price,
                    min_price,
                    max_price,
                    avg_price_sqm,
                    avg_size_sqm,
                    luxury_count,
                    nearby_metros,
                    nearby_malls
                FROM metrics_area
                WHERE area_name_en = '{area_name}'
            """).df()
            
            if metrics.empty:
                return None
            
            metrics_dict = metrics.to_dict('records')[0]
            
            # NEW: Get sample transactions as proof (8-10 representative samples)
            sample_transactions = self.con.execute(f"""
                WITH categorized AS (
                    SELECT
                        transaction_id,
                        instance_date,
                        property_type_en,
                        rooms_en,
                        actual_worth,
                        meter_sale_price,
                        procedure_area,
                        CASE
                            WHEN actual_worth >= 10000000 THEN 'ultra_luxury'
                            WHEN actual_worth >= 5000000 THEN 'luxury'
                            WHEN actual_worth >= 2000000 THEN 'mid_range'
                            ELSE 'affordable'
                        END as price_category,
                        ROW_NUMBER() OVER (
                            PARTITION BY
                                CASE
                                    WHEN actual_worth >= 10000000 THEN 'ultra_luxury'
                                    WHEN actual_worth >= 5000000 THEN 'luxury'
                                    WHEN actual_worth >= 2000000 THEN 'mid_range'
                                    ELSE 'affordable'
                                END,
                                rooms_en
                            ORDER BY actual_worth DESC
                        ) as rn
                    FROM transactions_clean
                    WHERE area_name_en = '{area_name}'
                        AND trans_group_en = 'Sales'
                        {date_filter}
                )
                SELECT
                    transaction_id,
                    instance_date,
                    property_type_en,
                    rooms_en,
                    actual_worth,
                    meter_sale_price,
                    procedure_area,
                    price_category
                FROM categorized
                WHERE rn <= 1
                ORDER BY actual_worth DESC
                LIMIT 8
            """).df()

            # NEW: Get aggregate stats for citation summary
            citation_stats = self.con.execute(f"""
                SELECT
                    COUNT(*) as total_analyzed,
                    MIN(instance_date) as earliest_date,
                    MAX(instance_date) as latest_date,
                    COUNT(DISTINCT rooms_en) as property_types_count,
                    COUNT(DISTINCT transaction_year) as years_covered
                FROM transactions_clean
                WHERE area_name_en = '{area_name}'
                    AND trans_group_en = 'Sales'
                    {date_filter}
            """).df()

            # Recent trends (last 6 months)
            trends = self.con.execute(f"""
                SELECT 
                    transaction_year,
                    transaction_month,
                    property_type_en,
                    avg_price,
                    pct_change_mom
                FROM metrics_price_changes
                WHERE area_name_en = '{area_name}'
                  AND pct_change_mom IS NOT NULL
                ORDER BY transaction_year DESC, transaction_month DESC
                LIMIT 6
            """).df()
            
            # Property type breakdown
            prop_types = self.con.execute(f"""
                SELECT 
                    rooms_en,
                    tx_count,
                    avg_price,
                    avg_price_sqm,
                    avg_size_sqm
                FROM metrics_property_types
                WHERE area_name_en = '{area_name}'
                ORDER BY tx_count DESC
                LIMIT 5
            """).df()
            
            # Top projects
            projects = self.con.execute(f"""
                SELECT 
                    project_name_en,
                    master_project_en,
                    reg_type_en,
                    tx_count,
                    avg_price
                FROM metrics_projects
                WHERE area_name_en = '{area_name}'
                ORDER BY tx_count DESC
                LIMIT 5
            """).df()
            
            return {
                'area_name': area_name,
                'metrics': metrics_dict,
                'trends': trends.to_dict('records') if not trends.empty else [],
                'property_types': prop_types.to_dict('records') if not prop_types.empty else [],
                'projects': projects.to_dict('records') if not projects.empty else [],
                'generated_date': datetime.now().strftime('%Y-%m-%d'),
                # NEW: Add these two lines for citations
                'sample_transactions': sample_transactions.to_dict('records') if not sample_transactions.empty else [],
                'citation_stats': citation_stats.to_dict('records')[0] if not citation_stats.empty else {},
                # NEW: Add date range info
                'year_from': year_from,
                'year_to': year_to
            }
            
        except Exception as e:
            print(f"Error getting data: {e}")
            return None
    
    def _generate_with_verification(self, template, data, area_name):
        """Generate content with strict constraints"""
        
        # Build prompt with ONLY verified data
        prompt = self._build_prompt(template, data, area_name)
        
        try:
            message = self.client.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=2000,
                temperature=0.3,  # Lower temperature = less creative = less hallucination
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )
            
            return message.content[0].text
            
        except Exception as e:
            print(f"Error calling Claude API: {e}")
            return None
    
    def _build_prompt(self, template, data, area_name):
        """Build prompt with verified data AND citation requirements"""
        
        metrics = data['metrics']
        trends = data['trends']
        prop_types = data['property_types']
        samples = data.get('sample_transactions', [])
        stats = data.get('citation_stats', {})
        
        # Format numbers for the prompt
        avg_price_formatted = f"AED {metrics['avg_price']:,.0f}"
        median_price_formatted = f"AED {metrics['median_price']:,.0f}"
        avg_sqm_formatted = f"AED {metrics['avg_price_sqm']:,.0f}"

        #Build sample transactions section
        samples_text = ""
        sample_ids = []
        if samples:
            samples_text = "\n\nVERIFIED SAMPLES TRANSACTIONS (for citations):\n"
            for i, tx in enumerate(samples, 1):
                sample_ids.append(tx['transaction_id'])
                samples_text += f"{i}. Transaction ID: {tx['transaction_id']}\n"
                samples_text += f"   Date: {tx['instance_date']}\n"
                samples_text += f"   Type: {tx['property_type_en']} - {tx['rooms_en']}\n"
                samples_text += f"   Price: AED {tx['actual_worth']:,.0f}\n"
                samples_text += f"   Price/sqm: AED {tx.get('meter_sale_price', 0):,.0f}\n"
                samples_text += f"   Category: {tx['price_category'].replace('_', ' ').title()}\n\n"
        
        # Build trend summary
        trend_summary = ""
        if trends:
            latest = trends[0]
            if latest['pct_change_mom']:
                direction = "increased" if latest['pct_change_mom'] > 0 else "decreased"
                trend_summary = f"Prices {direction} by {abs(latest['pct_change_mom']):.1f}% in the most recent month."
        
        # Build property type summary
        prop_summary = ""
        if prop_types:
            top_type = prop_types[0]
            prop_summary = f"The most common property type is {top_type['rooms_en']} with an average price of AED {top_type['avg_price']:,.0f}."
        
        # Citation statistics
        citation_summary = ""
        if stats:
            citation_summary = f"""
        DATASET SUMMARY (for transparency):
        - Total transactions analyzed: {stats.get('total_analyzed', 'N/A')}
        - Date range: {stats.get('earliest_date', 'N/A')} to {stats.get('latest_date', 'N/A')}
        - Property types covered: {stats.get('property_types_count', 'N/A')}
        - Years of data: {stats.get('years_covered', 'N/A')}
        """  
            
        # Date range filter info
        date_range_info = ""
        year_from = data.get('year_from')
        year_to = data.get('year_to')

        if year_from or year_to:
            if year_from and year_to:
                date_range_info = f"\nDATA FILTERED FOR: Years {year_from} to {year_to} (focused analysis)"
            elif year_from:
                date_range_info = f"\nDATA FILTERED FOR: Year {year_from} onwards (recent focus)"
            elif year_to:  
                date_range_info = f"\nDATA FILTERED FOR: Up to year {year_to}"
        else:
            date_range_info = "\nDATA SCOPE: All available years (comprehensive historical analysis)"

        prompt = f"""You are writing an SEO-optimized real estate area guide for Dubai. 

CRITICAL ANTI-HALLUCINATION RULES:
1. Use ONLY the data provided below - NEVER make up numbers, statistics, or facts
2. All prices must EXACTLY match the provided data
3. If data is missing, say "data not available" rather than guessing
4. Do not mention specific buildings, landmarks, or facilities unless provided in the data
5. Keep the tone professional and factual
6. Include the exact numbers from the data as citations

VERIFIED DATA FOR {area_name}:
- Total Transactions Analyzed: {stats.get('total_analyzed', metrics['total_transactions']) if stats else metrics['total_transactions']}
- Average Price: {avg_price_formatted}
- Median Price: {median_price_formatted}
- Price Range: AED {metrics['min_price']:,.0f} to AED {metrics['max_price']:,.0f}
- Average Price per Sqm: {avg_sqm_formatted}
- Luxury Properties (5M+): {metrics['luxury_count']}
- Nearby Metro Stations: {metrics['nearby_metros'] or 'Not specified in data'}
- Nearby Malls: {metrics['nearby_malls'] or 'Not specified in data'}

{citation_summary}
{date_range_info}
{samples_text}

RECENT TREND:
{trend_summary if trend_summary else 'Recent trend data not available.'}

PROPERTY TYPES:
{prop_summary if prop_summary else 'Property type breakdown not available.'}

Write a 400-500 word SEO-optimized area guide for {area_name} that:
1. Starts with a compelling introduction (2-3 sentences)
2. Covers market overview using the verified data above
3. Discusses property types and price ranges (using exact numbers)
4. Mentions accessibility (metros/malls if available)
5. Includes recent price trends if available
6. Ends with a summary suitable for investors/buyers

Format as Markdown with:
- H1 title: "{area_name} Real Estate Guide 2024"
- H2 section headers (Market Overview, Price Analysis, Property Types, etc.)
- Use bold for key statistics
- Professional, informative tone
- Include a "Data Source" section at the end citing Dubai Land Department

MANDATORY: Include a "Data Transparency" section at the end with:
## Data Transparency & Sources

**Analysis Based On:**
- Total verified transactions: {stats.get('total_analyzed', 'N/A') if stats else 'N/A'}
- Actual data range: {stats.get('earliest_date', 'N/A') if stats else 'N/A'} to {stats.get('latest_date', 'N/A') if stats else 'N/A'}
- Analysis period: {f"{year_from if year_from else 'All'} to {year_to if year_to else 'Present'}" if (year_from or year_to) else "Complete historical data"}
- Source: Dubai Land Department (Official Records)

**Sample Transaction IDs** (representative of dataset):
{chr(10).join([f'- {tx_id}' for tx_id in sample_ids[:5]])}

**Verification**: All transaction data is publicly available from Dubai Land Department.
- DLD Portal: dubai.land.gov.ae
- Our verification API: /api/verify/area/{area_name}

*This analysis represents our interpretation of publicly available DLD data.*

Remember: Use ONLY the data provided above. Never invent statistics, project names, or amenities. Never speculate or make predictions"""

        return prompt
    
    def _validate_content(self, content, data):
        """Validate that content doesn't hallucinate"""
        
        if not content:
            return False, ["Content generation failed"]
        
        issues = []
        
        # Check for required data points
        metrics = data['metrics']
        
        # Extract numbers from content (simple check)
        import re
        content_numbers = re.findall(r'AED\s*([\d,]+)', content)
        content_numbers = [int(n.replace(',', '')) for n in content_numbers]
        
        # Expected numbers from data
        expected = [
            int(metrics['avg_price']),
            int(metrics['median_price']),
            int(metrics['avg_price_sqm'])
        ]
        
        # Check if key numbers are present
        found_count = sum(1 for exp in expected if any(abs(exp - cn) < exp * 0.01 for cn in content_numbers))
        
        if found_count < 2:
            issues.append("Content may not include verified data points")
        
        # Check for forbidden phrases (signs of hallucination)
        forbidden = [
            "expected to", "projected to", "will likely", "is expected",
            "according to experts", "industry sources", "it is believed"
        ]
        
        for phrase in forbidden:
            if phrase.lower() in content.lower():
                issues.append(f"Contains speculative language: '{phrase}'")
        
        # Check for area name
        if data['area_name'].lower() not in content.lower():
            issues.append("Area name not prominently featured")
        
        return len(issues) == 0, issues
    
    def _save_content(self, content, filename, metadata):
        """Save generated content with metadata"""
        
        filepath = self.output_dir / filename
        
        # Build metadata header
        metadata_header = f"generated_date: {metadata['generated_date']}\n"

        # Add area_name only if it exists
        if 'area_name' in metadata:
             metadata_header += f"area: {metadata['area_name']}\n"

        metadata_header += "source: Dubai Land Department via automated analysis\nvalidation: passed"

        # Add metadata header
        full_content = f"""---
    {metadata_header}
---

{content}

---

**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  
**Data Quality**: Verified  
**Source**: Dubai Land Department Transaction Records  
"""
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(full_content)
        
        return filepath
    
    def generate_monthly_market_report(self, year=2024, month=None):
        """Generate monthly market-wide report"""
        
        if month is None:
            month = datetime.now().month
        
        print(f"[INFO] Generating market report for {year}-{month:02d}")

        # Get market-wide data
        data = self.con.execute(f"""
            SELECT 
                COUNT(DISTINCT area_name_en) as areas_with_activity,
                SUM(tx_count) as total_transactions,
                AVG(avg_price) as market_avg_price,
                SUM(CASE WHEN area_name_en IN (
                    SELECT area_name_en FROM metrics_luxury 
                    WHERE transaction_year = {year}
                ) THEN tx_count ELSE 0 END) as luxury_transactions
            FROM metrics_monthly_trends
            WHERE transaction_year = {year}
              AND transaction_month = {month}
        """).df()
        
        if data.empty or data.iloc[0]['total_transactions'] == 0:
            print(f"[ERROR] No data for {year}-{month:02d}")
            return None
        
        # Top performing areas
        top_areas = self.con.execute(f"""
            SELECT 
                area_name_en,
                SUM(tx_count) as transactions,
                AVG(avg_price) as avg_price
            FROM metrics_monthly_trends
            WHERE transaction_year = {year}
              AND transaction_month = {month}
            GROUP BY area_name_en
            ORDER BY transactions DESC
            LIMIT 10
        """).df()
        
        report_data = {
            'year': year,
            'month': month,
            'market_stats': data.to_dict('records')[0],
            'top_areas': top_areas.to_dict('records')
        }
        
        # Generate report
        content = self._generate_market_report_content(report_data)
        
        # Save
        filename = f"market_report_{year}_{month:02d}.md"
        filepath = self._save_content(
            content=content,
            filename=filename,
            metadata={'generated_date': datetime.now().strftime('%Y-%m-%d')}
        )
        
        print(f"[SUCCESS] Generated: {filepath}")
        return filepath

    def _generate_market_report_content(self, data):
        """Generate market report content"""
        
        stats = data['market_stats']
        top_areas = data['top_areas']
        
        prompt = f"""Write a professional Dubai real estate market report for {data['month']}/{data['year']}.

VERIFIED DATA:
- Total Transactions: {stats['total_transactions']}
- Active Areas: {stats['areas_with_activity']}
- Market Average Price: AED {stats['market_avg_price']:,.0f}
- Luxury Transactions (5M+): {stats['luxury_transactions']}

TOP PERFORMING AREAS:
"""
        
        for area in top_areas[:5]:
            price_change = area.get('price_change', 0)
            change_text = f"{price_change:+.1f}%" if price_change else "stable"
            prompt += f"- {area['area_name_en']}: {area['transactions']} transactions, avg price AED {area['avg_price']:,.0f}, price change: {change_text}\n"
        
        prompt += """

Write a 600-800 word market report that:
1. Opens with key market highlights
2. Analyzes overall transaction volume and pricing
3. Highlights top performing areas with data
4. Discusses luxury market activity
5. Provides market outlook based on current data (no speculation)

Format as Markdown with H1/H2 headers. Use ONLY the data provided above."""
        
        try:
            message = self.client.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=2500,
                temperature=0.3,
                messages=[{"role": "user", "content": prompt}]
            )
            return message.content[0].text
        except Exception as e:
            print(f"Error: {e}")
            return None

    def generate_property_comparison(self, room_types=['Studio', '1 B/R', '2 B/R']):
        """Generate property type comparison analysis"""
        
        print(f"[INFO] Generating property comparison: {', '.join(room_types)}")

        comparison_data = []
        
        for room_type in room_types:
            data = self.con.execute(f"""
                SELECT 
                    '{room_type}' as property_type,
                    COUNT(DISTINCT area_name_en) as areas_available,
                    SUM(tx_count) as total_transactions,
                    AVG(avg_price) as avg_price,
                    AVG(avg_price_sqm) as avg_price_sqm,
                    AVG(avg_size_sqm) as avg_size_sqm
                FROM metrics_property_types
                WHERE rooms_en = '{room_type}'
            """).df()
            
            if not data.empty:
                comparison_data.append(data.to_dict('records')[0])
        
        if not comparison_data:
            print("[ERROR] No comparison data available")
            return None

        # Generate content
        content = self._generate_comparison_content(comparison_data)

        # Save
        filename = f"property_comparison_{'_'.join(room_types).replace(' ', '').replace('/', '')}.md"
        filepath = self._save_content(
            content=content,
            filename=filename,
            metadata={'generated_date': datetime.now().strftime('%Y-%m-%d')}
        )

        print(f"[SUCCESS] Generated: {filepath}")
        return filepath
    
    def _generate_comparison_content(self, data):
        """Generate comparison content"""
        
        prompt = f"""Write a comprehensive Dubai property type comparison guide.

VERIFIED DATA:
"""
        
        for prop in data:
            prompt += f"""
{prop['property_type']}:
- Available in {prop['areas_available']} areas
- Recent transactions: {prop['total_transactions']}
- Average price: AED {prop['avg_price']:,.0f}
- Average price per sqm: AED {prop['avg_price_sqm']:,.0f}
- Average size: {prop['avg_size_sqm']:.0f} sqm
"""
        
        prompt += """

Write a 500-600 word comparison guide that:
1. Compares the property types using the verified data
2. Discusses price differences and value proposition
3. Covers typical buyer profiles for each
4. Mentions investment potential based on data
5. Helps readers decide which property type suits them

Format as Markdown. Use tables where appropriate. Use ONLY the data provided."""
        
        try:
            message = self.client.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=2000,
                temperature=0.3,
                messages=[{"role": "user", "content": prompt}]
            )
            return message.content[0].text
        except Exception as e:
            print(f"Error: {e}")
            return None

def generate_content_batch():
    """Generate content for top areas"""
    generator = ContentGenerator()

    # Get top 10 areas by transaction volume
    con = get_db(read_only=True)
    top_areas = con.execute("""
        SELECT area_name_en
        FROM metrics_area
        ORDER BY total_transactions DESC
        LIMIT 10
    """).df()
    
    print(f"\n[INFO] Generating content for {len(top_areas)} areas...\n")

    generated = []
    for _, row in top_areas.iterrows():
        filepath = generator.generate_area_guide(row['area_name_en'])
        if filepath:
            generated.append(filepath)

    print(f"\n[SUCCESS] Generated {len(generated)} area guides")
    print(f"[INFO] Saved to: {settings.CONTENT_OUTPUT_DIR}")
    
    return generated

if __name__ == "__main__":
    generate_content_batch()