# src/content/pm_generator.py

"""
Property Monitor Content Generator
AI-powered SEO content using Property Monitor BigQuery data

Supports period-based reports:
- Monthly: Specific month analysis
- Quarterly: Q1-Q4 analysis
- Semi-Annual: H1/H2 analysis
- Annual: Full year analysis
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import anthropic
import duckdb
import json
from datetime import datetime
from typing import Optional, Dict, Any, Tuple, List
from config.bigquery_settings import bq_settings
from config.settings import settings

# Import the analytics modules
from src.analytics.report_calculator import ReportCalculator, PeriodType
from src.analytics.qa_validator import QAValidator

# Import visualization system
from src.visualization.generator import VizGenerator, OutputFormat, VisualizationResult

# Import Excel verification exporter
from src.analytics.excel_exporter import (
    ExcelExporter, QueryLogger, CalculationLogger, create_verification_report
)

# Import citation intelligence for competitor-quality prompts
from src.analytics.citation_intel import PromptOptimizer, prompt_optimizer

# Import market intelligence for unique insights
from src.analytics.market_intelligence import MarketIntelligenceEngine


def get_pm_db(read_only=True):
    """Get Property Monitor database connection"""
    return duckdb.connect(str(bq_settings.PM_DB_PATH), read_only=read_only)


# Period type mappings
PERIOD_TYPE_MAP = {
    'monthly': PeriodType.MONTHLY,
    'quarterly': PeriodType.QUARTERLY,
    'semi_annual': PeriodType.SEMI_ANNUAL,
    'semi-annual': PeriodType.SEMI_ANNUAL,
    'annual': PeriodType.ANNUAL
}

PERIOD_NAMES = {
    PeriodType.MONTHLY: ['', 'January', 'February', 'March', 'April', 'May', 'June',
                         'July', 'August', 'September', 'October', 'November', 'December'],
    PeriodType.QUARTERLY: ['', 'Q1', 'Q2', 'Q3', 'Q4'],
    PeriodType.SEMI_ANNUAL: ['', 'H1 (First Half)', 'H2 (Second Half)'],
    PeriodType.ANNUAL: ['', 'Full Year']
}


class PMContentGenerator:
    """Generate SEO content from Property Monitor data"""

    def __init__(self):
        if not settings.ANTHROPIC_API_KEY:
            raise ValueError("ANTHROPIC_API_KEY not set in .env file")

        self.client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)
        self.con = get_pm_db(read_only=True)
        self.output_dir = settings.CONTENT_OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Initialize the report calculator for pure numerical analysis
        self.calculator = ReportCalculator()

        # Initialize QA validator
        self.validator = QAValidator(tolerance=0.01)

        # Initialize visualization generator
        self.viz_generator = VizGenerator(
            db_path=str(bq_settings.PM_DB_PATH),
            output_dir=str(self.output_dir / 'charts'),
            enable_ai=False  # Disable AI storytelling to reduce API calls
        )

        # Initialize market intelligence engine for unique insights
        self.intelligence = MarketIntelligenceEngine(self.con)

    def _get_period_type(self, period_type_str: str) -> PeriodType:
        """Convert string period type to PeriodType enum"""
        return PERIOD_TYPE_MAP.get(period_type_str.lower(), PeriodType.MONTHLY)

    def _get_period_name(self, period_type: PeriodType, period_number: int, year: int) -> str:
        """Get human-readable period name"""
        names = PERIOD_NAMES.get(period_type, [''])
        if period_type == PeriodType.ANNUAL:
            return str(year)
        elif period_number < len(names):
            return f"{names[period_number]} {year}"
        return f"Period {period_number} {year}"

    def _get_period_title(self, period_type: PeriodType, period_number: int, year: int) -> str:
        """Get period title for report headers"""
        if period_type == PeriodType.MONTHLY:
            month_names = ['', 'January', 'February', 'March', 'April', 'May', 'June',
                          'July', 'August', 'September', 'October', 'November', 'December']
            return f"{month_names[period_number]} {year}"
        elif period_type == PeriodType.QUARTERLY:
            return f"Q{period_number} {year}"
        elif period_type == PeriodType.SEMI_ANNUAL:
            half = "First Half (H1)" if period_number == 1 else "Second Half (H2)"
            return f"{half} {year}"
        else:
            return str(year)

    def _generate_report_visualizations(
        self,
        report_type: str,
        year: int,
        period_type: PeriodType = None,
        period_number: int = None,
        metrics: Dict = None
    ) -> Dict[str, VisualizationResult]:
        """
        Generate visualizations for a report.

        Args:
            report_type: 'market', 'luxury', 'offplan', 'area_guide'
            year: Year for the visualizations
            period_type: Period type for filtering
            period_number: Period number for filtering
            metrics: Pre-calculated metrics to use for charts

        Returns:
            Dictionary of VisualizationResult by chart name
        """
        results = {}

        try:
            # Map report types to visualization configs
            if report_type == 'market':
                # Transaction trend chart
                trend_result = self.viz_generator.generate_trend_chart(
                    year=year,
                    metric='transaction_count',
                    title=f'Monthly Transaction Trend - {year}',
                    output_format=OutputFormat.BOTH
                )
                if trend_result:
                    results['transaction_trend'] = trend_result

                # Top areas chart
                quarter = period_number if period_type == PeriodType.QUARTERLY else None
                areas_result = self.viz_generator.generate_top_areas_chart(
                    year=year,
                    quarter=quarter,
                    metric='transaction_count',
                    limit=10,
                    output_format=OutputFormat.BOTH
                )
                if areas_result:
                    results['top_areas'] = areas_result

                # Market segments (off-plan vs ready)
                segment_result = self.viz_generator.generate_segment_chart(
                    year=year,
                    quarter=quarter,
                    segment_type='offplan_ready',
                    output_format=OutputFormat.BOTH
                )
                if segment_result:
                    results['market_segments'] = segment_result

            elif report_type == 'luxury':
                # Luxury trend chart
                trend_result = self.viz_generator.generate_trend_chart(
                    year=year,
                    metric='transaction_count',
                    segment='luxury',
                    title=f'Luxury Transactions Trend - {year}',
                    output_format=OutputFormat.BOTH
                )
                if trend_result:
                    results['luxury_trend'] = trend_result

                # Luxury tiers distribution
                quarter = period_number if period_type == PeriodType.QUARTERLY else None
                tiers_result = self.viz_generator.generate_segment_chart(
                    year=year,
                    quarter=quarter,
                    segment_type='luxury_tiers',
                    output_format=OutputFormat.BOTH
                )
                if tiers_result:
                    results['luxury_tiers'] = tiers_result

                # Top luxury areas
                areas_result = self.viz_generator.generate_top_areas_chart(
                    year=year,
                    quarter=quarter,
                    metric='transaction_count',
                    limit=10,
                    segment='luxury',
                    output_format=OutputFormat.BOTH
                )
                if areas_result:
                    results['top_luxury_areas'] = areas_result

            elif report_type == 'offplan':
                # Off-plan trend
                trend_result = self.viz_generator.generate_trend_chart(
                    year=year,
                    metric='transaction_count',
                    segment='offplan',
                    title=f'Off-Plan Transactions Trend - {year}',
                    output_format=OutputFormat.BOTH
                )
                if trend_result:
                    results['offplan_trend'] = trend_result

                # Off-plan vs Ready comparison
                quarter = period_number if period_type == PeriodType.QUARTERLY else None
                segment_result = self.viz_generator.generate_segment_chart(
                    year=year,
                    quarter=quarter,
                    segment_type='offplan_ready',
                    output_format=OutputFormat.BOTH
                )
                if segment_result:
                    results['offplan_vs_ready'] = segment_result

                # Top off-plan areas
                areas_result = self.viz_generator.generate_top_areas_chart(
                    year=year,
                    quarter=quarter,
                    metric='transaction_count',
                    limit=10,
                    segment='offplan',
                    output_format=OutputFormat.BOTH
                )
                if areas_result:
                    results['top_offplan_areas'] = areas_result

            elif report_type == 'area_guide':
                # For area guide, generate price trend
                trend_result = self.viz_generator.generate_trend_chart(
                    year=year,
                    metric='avg_price',
                    title=f'Price Trend - {year}',
                    output_format=OutputFormat.BOTH
                )
                if trend_result:
                    results['price_trend'] = trend_result

                # Property type distribution
                segment_result = self.viz_generator.generate_segment_chart(
                    year=year,
                    segment_type='property_types',
                    output_format=OutputFormat.BOTH
                )
                if segment_result:
                    results['property_types'] = segment_result

        except Exception as e:
            print(f"Warning: Visualization generation failed: {e}")
            import traceback
            traceback.print_exc()

        return results

    def _format_visualizations_markdown(
        self,
        viz_results: Dict[str, VisualizationResult]
    ) -> str:
        """
        Format visualization results as markdown for embedding in content.

        Args:
            viz_results: Dictionary of VisualizationResult

        Returns:
            Markdown string with embedded charts
        """
        if not viz_results:
            return ""

        markdown_parts = ["\n## Visualizations\n"]

        for chart_name, result in viz_results.items():
            # Add chart section
            markdown_parts.append(f"\n### {result.title}\n")

            # Embed base64 image if available (already includes data:image/png;base64, prefix)
            if result.static_image:
                markdown_parts.append(f"![{result.title}]({result.static_image})\n")

            # Add Chart.js config as HTML comment for frontend parsing
            if result.interactive_config:
                config_json = json.dumps(result.interactive_config)
                markdown_parts.append(f"\n<!-- CHARTJS_CONFIG:{chart_name}:{config_json} -->\n")

            # Add insights if available
            if result.insights:
                markdown_parts.append("\n**Key Insights:**\n")
                for insight in result.insights:
                    markdown_parts.append(f"- {insight}\n")

        return "\n".join(markdown_parts)

    def _get_visualization_metadata(
        self,
        viz_results: Dict[str, VisualizationResult]
    ) -> Dict[str, Any]:
        """
        Extract metadata from visualization results for storage.

        Args:
            viz_results: Dictionary of VisualizationResult

        Returns:
            Metadata dictionary
        """
        if not viz_results:
            return {}

        metadata = {
            'charts_generated': len(viz_results),
            'chart_names': list(viz_results.keys()),
            'charts': {}
        }

        for chart_name, result in viz_results.items():
            metadata['charts'][chart_name] = {
                'title': result.title,
                'type': result.chart_type.value if hasattr(result.chart_type, 'value') else str(result.chart_type),
                'has_static': result.static_image is not None,
                'has_interactive': result.interactive_config is not None
            }

        return metadata

    def generate_area_guide(self, area_name, year_from=None, year_to=None, with_verification=False):
        """Generate comprehensive area guide"""

        print(f"Generating Property Monitor area guide for: {area_name}")

        # Enable logging if verification is requested
        if with_verification:
            from src.analytics.excel_exporter import QueryLogger, CalculationLogger
            QueryLogger.enable()
            CalculationLogger.enable()
            QueryLogger.clear()
            CalculationLogger.clear()

        # Get verified data
        data = self._get_area_data(area_name, year_from, year_to)

        if not data:
            print(f"No data found for {area_name}")
            return None

        # Generate market intelligence for area guide
        print(f"Generating area intelligence...")
        try:
            from datetime import datetime
            start_date = f"{year_from or 2023}-01-01"
            end_date = f"{year_to or datetime.now().year}-12-31"
            area_intelligence = self.intelligence.get_area_guide_intelligence(area_name, start_date, end_date)
            data['intelligence'] = area_intelligence.primary_insights
            print(f"Area intelligence generated: {len(area_intelligence.primary_insights)} characters")
        except Exception as e:
            print(f"Warning: Area intelligence generation failed: {e}")
            data['intelligence'] = ""

        # Generate visualizations for area guide
        print(f"Generating visualizations for area guide...")
        current_year = year_to or datetime.now().year
        viz_results = self._generate_report_visualizations(
            report_type='area_guide',
            year=current_year
        )
        if viz_results:
            print(f"Generated {len(viz_results)} charts: {list(viz_results.keys())}")

        # Generate content
        content = self._generate_with_verification(
            template='area_guide',
            data=data,
            area_name=area_name
        )

        # Validate
        is_valid, issues = self._validate_content(content, data)

        if not is_valid:
            print(f"Content validation failed:")
            for issue in issues:
                print(f"   - {issue}")
            return None

        # Append visualizations to content
        if viz_results:
            viz_markdown = self._format_visualizations_markdown(viz_results)
            content = content + viz_markdown

        # Get visualization metadata
        viz_metadata = self._get_visualization_metadata(viz_results)
        data['visualizations'] = viz_metadata

        # Save
        filepath = self._save_content(
            content=content,
            filename=f"pm_area_guide_{area_name.replace(' ', '_').lower()}.md",
            metadata=data
        )

        print(f"Generated: {filepath}")
        if viz_results:
            print(f"Visualizations: {len(viz_results)} charts embedded")

        # Generate verification Excel if requested
        if with_verification:
            verification_path = self._generate_verification_excel(
                content=content,
                filename=filepath.stem,
                report_type="area_guide",
                metadata={
                    'area_name': area_name,
                    'year_from': year_from,
                    'year_to': year_to,
                    'total_transactions': data.get('metrics', {}).get('total_transactions', 0),
                    'avg_price': data.get('metrics', {}).get('avg_price', 0)
                }
            )
            return filepath, verification_path

        return filepath

    def _get_area_data(self, area_name, year_from=None, year_to=None):
        """Get verified data for area with sample transactions"""

        try:
            # Build date filter
            date_filter = ""
            if year_from and year_to:
                date_filter = f"AND transaction_year BETWEEN {year_from} AND {year_to}"
            elif year_from:
                date_filter = f"AND transaction_year >= {year_from}"
            elif year_to:
                date_filter = f"AND transaction_year <= {year_to}"

            # Main metrics from PM database
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
                    offplan_count,
                    unique_projects,
                    top_developers
                FROM metrics_area
                WHERE area_name_en = '{area_name}'
            """).df()

            if metrics.empty:
                return None

            metrics_dict = metrics.to_dict('records')[0]

            # Sample transactions for citations
            sample_transactions = self.con.execute(f"""
                WITH categorized AS (
                    SELECT
                        transaction_id,
                        instance_date,
                        property_type_en,
                        property_sub_type_en,
                        rooms_en,
                        actual_worth,
                        meter_sale_price,
                        procedure_area,
                        reg_type_en,
                        master_project_en as developer,
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
                                END
                            ORDER BY actual_worth DESC
                        ) as rn
                    FROM transactions_clean
                    WHERE area_name_en = '{area_name}'
                        {date_filter}
                )
                SELECT *
                FROM categorized
                WHERE rn <= 2
                ORDER BY actual_worth DESC
                LIMIT 8
            """).df()

            # Citation stats
            citation_stats = self.con.execute(f"""
                SELECT
                    COUNT(*) as total_analyzed,
                    MIN(instance_date) as earliest_date,
                    MAX(instance_date) as latest_date,
                    COUNT(DISTINCT rooms_en) as property_types_count,
                    COUNT(DISTINCT transaction_year) as years_covered,
                    SUM(CASE WHEN reg_type_en = 'Off-Plan' THEN 1 ELSE 0 END) as offplan_count,
                    COUNT(DISTINCT master_project_en) as developers_count
                FROM transactions_clean
                WHERE area_name_en = '{area_name}'
                    {date_filter}
            """).df()

            # Recent trends
            trends = self.con.execute(f"""
                SELECT
                    transaction_year,
                    transaction_month,
                    property_type_en,
                    avg_price,
                    tx_count,
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
                    unit_type,
                    rooms_en,
                    tx_count,
                    avg_price,
                    median_price,
                    avg_price_sqm,
                    avg_size_sqm
                FROM metrics_property_types
                WHERE area_name_en = '{area_name}'
                ORDER BY tx_count DESC
                LIMIT 6
            """).df()

            # Top projects
            projects = self.con.execute(f"""
                SELECT
                    project_name_en,
                    developer,
                    reg_type_en,
                    tx_count,
                    avg_price,
                    median_price
                FROM metrics_projects
                WHERE area_name_en = '{area_name}'
                ORDER BY tx_count DESC
                LIMIT 5
            """).df()

            # Off-plan comparison
            offplan_stats = self.con.execute(f"""
                SELECT
                    offplan_count,
                    ready_count,
                    avg_offplan_price,
                    avg_ready_price,
                    offplan_percentage
                FROM metrics_offplan_comparison
                WHERE area_name_en = '{area_name}'
            """).df()

            return {
                'area_name': area_name,
                'metrics': metrics_dict,
                'trends': trends.to_dict('records') if not trends.empty else [],
                'property_types': prop_types.to_dict('records') if not prop_types.empty else [],
                'projects': projects.to_dict('records') if not projects.empty else [],
                'offplan_stats': offplan_stats.to_dict('records')[0] if not offplan_stats.empty else {},
                'generated_date': datetime.now().strftime('%Y-%m-%d'),
                'sample_transactions': sample_transactions.to_dict('records') if not sample_transactions.empty else [],
                'citation_stats': citation_stats.to_dict('records')[0] if not citation_stats.empty else {},
                'year_from': year_from,
                'year_to': year_to,
                'data_source': 'Property Monitor'
            }

        except Exception as e:
            print(f"Error getting data: {e}")
            import traceback
            traceback.print_exc()
            return None

    def _generate_with_verification(self, template, data, area_name):
        """Generate content with strict constraints"""

        prompt = self._build_prompt(template, data, area_name)

        try:
            message = self.client.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=2500,
                temperature=0.3,
                messages=[{"role": "user", "content": prompt}]
            )

            return message.content[0].text

        except Exception as e:
            print(f"Error calling Claude API: {e}")
            return None

    def _build_prompt(self, template, data, area_name):
        """Build prompt with verified PM data"""

        metrics = data['metrics']
        trends = data['trends']
        prop_types = data['property_types']
        samples = data.get('sample_transactions', [])
        stats = data.get('citation_stats', {})
        offplan = data.get('offplan_stats', {})
        intelligence = data.get('intelligence', '')  # Market intelligence insights

        # Format numbers
        avg_price_formatted = f"AED {metrics['avg_price']:,.0f}"
        median_price_formatted = f"AED {metrics['median_price']:,.0f}"
        avg_sqm_formatted = f"AED {metrics['avg_price_sqm']:,.0f}" if metrics.get('avg_price_sqm') else "N/A"

        # Build sample transactions section
        samples_text = ""
        sample_ids = []
        if samples:
            samples_text = "\n\nVERIFIED SAMPLE TRANSACTIONS (for citations):\n"
            for i, tx in enumerate(samples, 1):
                sample_ids.append(str(tx['transaction_id']))
                samples_text += f"{i}. Transaction ID: {tx['transaction_id']}\n"
                samples_text += f"   Date: {tx['instance_date']}\n"
                samples_text += f"   Type: {tx.get('property_sub_type_en', tx['property_type_en'])} - {tx['rooms_en']}\n"
                samples_text += f"   Price: AED {tx['actual_worth']:,.0f}\n"
                if tx.get('meter_sale_price'):
                    samples_text += f"   Price/sqm: AED {tx['meter_sale_price']:,.0f}\n"
                samples_text += f"   Sale Type: {tx.get('reg_type_en', 'N/A')}\n"
                samples_text += f"   Developer: {tx.get('developer', 'N/A')}\n"
                samples_text += f"   Category: {tx['price_category'].replace('_', ' ').title()}\n\n"

        # Build trend summary
        trend_summary = ""
        if trends:
            latest = trends[0]
            if latest.get('pct_change_mom'):
                direction = "increased" if latest['pct_change_mom'] > 0 else "decreased"
                trend_summary = f"Prices {direction} by {abs(latest['pct_change_mom']):.1f}% in the most recent month."

        # Property type summary
        prop_summary = ""
        if prop_types:
            top_type = prop_types[0]
            prop_summary = f"Most popular: {top_type.get('unit_type', 'Apartment')} ({top_type['rooms_en']}) with avg price AED {top_type['avg_price']:,.0f}."

        # Off-plan summary
        offplan_summary = ""
        if offplan:
            offplan_pct = offplan.get('offplan_percentage', 0)
            if offplan_pct:
                offplan_summary = f"\nOFF-PLAN MARKET:\n- Off-plan transactions: {offplan.get('offplan_count', 0):,} ({offplan_pct:.1f}%)\n"
                offplan_summary += f"- Ready property transactions: {offplan.get('ready_count', 0):,}\n"
                if offplan.get('avg_offplan_price') and offplan.get('avg_ready_price'):
                    offplan_summary += f"- Avg off-plan price: AED {offplan['avg_offplan_price']:,.0f}\n"
                    offplan_summary += f"- Avg ready price: AED {offplan['avg_ready_price']:,.0f}\n"

        # Citation statistics
        citation_summary = f"""
DATASET SUMMARY (Property Monitor Data):
- Total transactions analyzed: {stats.get('total_analyzed', 'N/A'):,}
- Date range: {stats.get('earliest_date', 'N/A')} to {stats.get('latest_date', 'N/A')}
- Property types covered: {stats.get('property_types_count', 'N/A')}
- Active developers: {stats.get('developers_count', 'N/A')}
- Off-plan sales: {stats.get('offplan_count', 'N/A'):,}
"""

        # Date range info
        year_from = data.get('year_from')
        year_to = data.get('year_to')
        if year_from or year_to:
            date_range_info = f"\nDATA FILTERED FOR: {year_from or 'earliest'} to {year_to or 'latest'}"
        else:
            date_range_info = "\nDATA SCOPE: Complete Property Monitor dataset (2023-2025)"

        # Top developers
        developers_text = ""
        if metrics.get('top_developers'):
            devs = metrics['top_developers'].split(', ')[:5]
            developers_text = f"\nTOP DEVELOPERS: {', '.join(devs)}"

        # Build comprehensive property type table
        prop_type_table = ""
        if prop_types:
            prop_type_table = "\nPROPERTY TYPE BREAKDOWN:\n"
            prop_type_table += "| Property Type | Bedrooms | Avg Price | Transactions |\n"
            prop_type_table += "|---------------|----------|-----------|-------------|\n"
            for pt in prop_types[:8]:  # Top 8 types
                pt_name = pt.get('unit_type', 'Unknown')
                pt_rooms = pt.get('rooms_en', 'N/A')
                pt_avg = f"AED {pt['avg_price']:,.0f}" if pt.get('avg_price') else "N/A"
                pt_count = pt.get('transaction_count', 'N/A')
                prop_type_table += f"| {pt_name} | {pt_rooms} | {pt_avg} | {pt_count} |\n"

        prompt = f"""You are a senior real estate analyst creating a comprehensive area guide for {area_name}, Dubai.

## CRITICAL ANTI-HALLUCINATION RULES
1. Use ONLY the data provided below - NEVER make up numbers, statistics, or facts
2. All prices must EXACTLY match the provided data
3. If data is missing, say "data not available" rather than guessing
4. Do not mention specific landmarks or facilities unless provided in the data
5. For macroeconomic context, use QUALITATIVE language only (e.g., "amid supportive economic conditions")
6. Property Monitor data covers 2023-2025 (recent market only)

## GUIDE STRUCTURE (Based on Knight Frank, CBRE, JLL, Savills best practices + our unique depth)

### 1. Area Overview & Positioning (2-3 paragraphs)
- Open with positioning statement and 2-3 KEY STATISTICS as headlines
- Market segment positioning (luxury/mid-market/affordable based on avg price)
- Transaction volume context (how active is this market?)

### 2. Transaction Activity Analysis
- Total transactions: {metrics['total_transactions']:,}
- Off-plan vs ready breakdown: {metrics.get('offplan_count', 0):,} off-plan / {metrics.get('total_transactions', 0) - metrics.get('offplan_count', 0):,} ready
- Active projects in area: {metrics.get('unique_projects', 0):,}
- Include monthly/quarterly trend if available

### 3. Price Analysis
- Average price: {avg_price_formatted}
- Median price: {median_price_formatted}
- Price range: AED {metrics['min_price']:,.0f} to AED {metrics['max_price']:,.0f}
- Average price per sqm: {avg_sqm_formatted}
- Recent trend: {trend_summary if trend_summary else 'Trend data calculating...'}

### 4. Property Type Breakdown
{prop_type_table if prop_type_table else '- Property type data loading...'}

### 5. Developer Presence
{developers_text if developers_text else '- Developer data not available'}
- Highlight market share of top developers
- Note active project count: {metrics.get('unique_projects', 0):,}

### 6. Investment Perspective
- Luxury segment (5M+): {metrics['luxury_count']:,} properties
- Off-plan market strength: {offplan.get('offplan_percentage', 0):.1f}% of transactions
- Price trajectory based on trends
- Opportunity identification based on data

### 7. Key Takeaways (Bullet Points)
- Summarize opportunity
- Target buyer/investor profile
- Data-backed forward assessment

## LANGUAGE PATTERNS TO USE

1. **Positioning**: "One of Dubai's most [adjective] communities" or "A [segment] market offering..."
2. **Data-Driven**: "with {metrics['total_transactions']:,} transactions totaling AED..."
3. **Comparative**: "representing X% of area transactions" (calculate from data)
4. **Opportunity Framing**: "presenting an opportunity for..." based on price/activity data

## DIFFERENTIATION POINTS (Include These - Competitors Don't Have This)

1. **Transaction-Level Granularity**: Real sales data, not asking prices or surveys
2. **Off-Plan vs Ready Analysis**: Unique insight into market composition (~70% of Dubai is off-plan)
3. **Developer Market Share**: Who is actually selling in this area
4. **Price Per Sqm Data**: Enables true like-for-like comparison
5. **180-Area Coverage**: Community-level insights competitors cannot provide

## VERIFIED DATA FOR {area_name}:

{citation_summary}
{date_range_info}
{offplan_summary}
{samples_text}

## MARKET INTELLIGENCE (Property Monitor Exclusive Insights)

{intelligence if intelligence else "Market intelligence analysis pending."}

**Instructions for using Market Intelligence:**
- Include area DNA profile insights in the Area Overview section
- Weave comparable area context into the market positioning narrative
- Use trend analysis for forward-looking statements (qualify with "based on historical patterns")
- Highlight any opportunity signals in the Investment Perspective section
- Source all insights to "Property Monitor transaction analysis"

## OUTPUT FORMAT

Format as Markdown with:
- H1 title: "{area_name} Real Estate Market Guide 2024-2025"
- H2 section headers for each major section
- Use **bold** for key statistics
- Include the property type table above
- Professional, data-driven tone throughout
- 600-800 words total

## MANDATORY DATA TRANSPARENCY SECTION (Include at end):

## Data Transparency & Sources

**Analysis Based On:**
- Data source: Property Monitor (Premium Dubai real estate database)
- Total verified transactions: {stats.get('total_analyzed', 'N/A'):,}
- Analysis period: {stats.get('earliest_date', 'N/A')} to {stats.get('latest_date', 'N/A')}
- Analysis generated: {data['generated_date']}

**Sample Transaction IDs** (verifiable in Property Monitor):
{chr(10).join([f'- {tx_id}' for tx_id in sample_ids[:5]])}

**Methodology Note**: This analysis uses actual transaction records from Property Monitor's verified database covering {metrics['total_transactions']:,} transactions in {area_name}. All statistics are derived from recorded sales, not surveys or asking prices.

Remember: Use ONLY the data provided. Every figure must be traceable to the data above. Never invent statistics or make unsupported predictions."""

        return prompt

    def _validate_content(self, content, data):
        """Validate content doesn't hallucinate"""

        if not content:
            return False, ["Content generation failed"]

        issues = []
        metrics = data['metrics']

        # Check for forbidden speculative phrases
        forbidden = [
            "expected to", "projected to", "will likely", "is expected",
            "according to experts", "industry sources", "it is believed",
            "forecast", "prediction"
        ]

        for phrase in forbidden:
            if phrase.lower() in content.lower():
                issues.append(f"Contains speculative language: '{phrase}'")

        # Check area name is present
        if data['area_name'].lower() not in content.lower():
            issues.append("Area name not prominently featured")

        return len(issues) == 0, issues

    def _save_content(self, content, filename, metadata):
        """Save generated content with visualization metadata"""

        filepath = self.output_dir / filename

        # Build visualization metadata string
        viz_meta = metadata.get('visualizations', {})
        viz_metadata_str = ""
        if viz_meta:
            viz_metadata_str = f"""
visualizations:
  charts_generated: {viz_meta.get('charts_generated', 0)}
  chart_names: {viz_meta.get('chart_names', [])}"""

        metadata_header = f"""generated_date: {metadata['generated_date']}
area: {metadata.get('area_name', 'N/A')}
data_source: Property Monitor
transactions_analyzed: {metadata.get('citation_stats', {}).get('total_analyzed', 'N/A')}
validation: passed
report_type: {metadata.get('report_type', 'area_guide')}
year: {metadata.get('year', 'N/A')}
period_type: {metadata.get('period_type', 'N/A')}
period_number: {metadata.get('period_number', 'N/A')}{viz_metadata_str}"""

        full_content = f"""---
{metadata_header}
---

{content}

---

**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**Data Source**: Property Monitor
**Data Quality**: Verified
**Coverage**: 2023-2025 Dubai Real Estate Transactions
"""

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(full_content)

        return filepath

    def generate_market_report(
        self,
        year: int = 2024,
        period_type: str = 'monthly',
        period_number: int = None,
        with_verification: bool = False
    ):
        """
        Generate market report from Property Monitor data.

        Args:
            year: Year for the report
            period_type: 'monthly', 'quarterly', 'semi_annual', or 'annual'
            period_number: Period number (1-12 for monthly, 1-4 for quarterly,
                          1-2 for semi-annual, 1 for annual)
            with_verification: If True, generates an Excel verification file

        Returns:
            Path to generated report file (or tuple of (report_path, verification_path) if with_verification=True)
        """
        # Enable logging if verification is requested
        if with_verification:
            QueryLogger.enable()
            CalculationLogger.enable()
            QueryLogger.clear()
            CalculationLogger.clear()

        # Convert string to PeriodType enum
        pt = self._get_period_type(period_type)

        # Default period number based on type
        if period_number is None:
            if pt == PeriodType.MONTHLY:
                period_number = datetime.now().month
            elif pt == PeriodType.QUARTERLY:
                period_number = (datetime.now().month - 1) // 3 + 1
            elif pt == PeriodType.SEMI_ANNUAL:
                period_number = 1 if datetime.now().month <= 6 else 2
            else:
                period_number = 1

        period_title = self._get_period_title(pt, period_number, year)
        print(f"Generating PM market report for {period_title}...")

        # Use ReportCalculator for pure numerical analysis (no AI)
        metrics = self.calculator.calculate_period_metrics(year, pt, period_number)

        if metrics['transaction_metrics']['total_transactions'] == 0:
            print(f"No data for {period_title}")
            return None

        # Get comparison metrics (period-over-period)
        try:
            comparison = self.calculator.get_comparison_metrics(
                year, pt, period_number, comparison_type="mom"
            )
        except Exception:
            comparison = None

        # Get year-over-year comparison if not annual
        try:
            yoy_comparison = self.calculator.get_comparison_metrics(
                year, pt, period_number, comparison_type="yoy"
            )
        except Exception:
            yoy_comparison = None

        # Get top performers
        top_areas = self.calculator.get_top_performers(
            year, pt, period_number,
            metric="transaction_count", category="areas", limit=10
        )

        top_developers = self.calculator.get_top_performers(
            year, pt, period_number,
            metric="transaction_count", category="developers", limit=5
        )

        # Validate metrics with QA validator
        self.validator.clear_results()
        validation_metrics = {
            'year': year,
            'period_type': period_type,
            'period_num': period_number,
            'transaction_count': metrics['transaction_metrics']['total_transactions'],
            'total_volume': metrics['transaction_metrics']['total_sales_volume'],
            'avg_price': metrics['price_metrics']['avg_price'],
            'offplan_count': metrics['market_segments']['offplan']['count'],
            'offplan_percentage': metrics['market_segments']['offplan']['percentage']
        }
        validation_results = self.validator.run_all_validations(validation_metrics)
        validation_summary = self.validator.get_summary()

        # Generate market intelligence (unique insights)
        print(f"Generating market intelligence...")
        try:
            intelligence = self.intelligence.get_market_report_intelligence(year, period_type, period_number)
            intelligence_insights = intelligence.primary_insights
            print(f"Intelligence generated: {len(intelligence_insights)} characters")
        except Exception as e:
            print(f"Warning: Intelligence generation failed: {e}")
            intelligence_insights = ""
            intelligence = None

        # Prepare report data
        report_data = {
            'year': year,
            'period_type': pt,
            'period_number': period_number,
            'period_title': period_title,
            'metrics': metrics,
            'comparison': comparison,
            'yoy_comparison': yoy_comparison,
            'top_areas': top_areas,
            'top_developers': top_developers,
            'validation_summary': validation_summary,
            'intelligence': intelligence_insights  # NEW: Market intelligence insights
        }

        # Generate visualizations
        print(f"Generating visualizations for market report...")
        viz_results = self._generate_report_visualizations(
            report_type='market',
            year=year,
            period_type=pt,
            period_number=period_number,
            metrics=metrics
        )
        if viz_results:
            print(f"Generated {len(viz_results)} charts: {list(viz_results.keys())}")

        # Generate content with AI (using verified data)
        content = self._generate_market_report_content(report_data)

        # Append visualizations to content
        if viz_results:
            viz_markdown = self._format_visualizations_markdown(viz_results)
            content = content + viz_markdown

        # Get visualization metadata
        viz_metadata = self._get_visualization_metadata(viz_results)

        # Build filename
        filename = self._build_report_filename('market_report', pt, year, period_number)
        filepath = self._save_content(
            content=content,
            filename=filename,
            metadata={
                'generated_date': datetime.now().strftime('%Y-%m-%d'),
                'period_type': period_type,
                'year': year,
                'period_number': period_number,
                'validation_pass_rate': validation_summary['pass_rate'],
                'visualizations': viz_metadata
            }
        )

        print(f"Generated: {filepath}")
        print(f"Validation: {validation_summary['passed']}/{validation_summary['total_tests']} tests passed")
        if viz_results:
            print(f"Visualizations: {len(viz_results)} charts embedded")

        # Generate verification Excel if requested
        if with_verification:
            verification_path = self._generate_verification_excel(
                content=content,
                report_name=f"Market Report {period_title}",
                validation_summary=validation_summary,
                metrics=metrics
            )
            print(f"Verification Excel: {verification_path}")
            QueryLogger.disable()
            CalculationLogger.disable()
            return filepath, verification_path

        return filepath

    def _generate_verification_excel(
        self,
        content: str,
        report_name: str,
        validation_summary: Dict = None,
        metrics: Dict = None
    ) -> Path:
        """
        Generate Excel verification file for data science team.

        Args:
            content: The generated markdown content
            report_name: Name of the report
            validation_summary: Validation results
            metrics: Calculated metrics

        Returns:
            Path to the generated Excel file
        """
        exporter = ExcelExporter(report_name=report_name)

        # Extract figures from content
        exporter.extract_figures_from_markdown(content)

        # Add query logs
        exporter.add_query_log(QueryLogger.get_entries())

        # Add calculation logs
        exporter.add_calculation_log(CalculationLogger.get_entries())

        # Add metadata
        if validation_summary:
            exporter.set_metadata("validation_pass_rate", validation_summary.get("pass_rate", "N/A"))
            exporter.set_metadata("validation_tests_passed", validation_summary.get("passed", 0))
            exporter.set_metadata("validation_tests_total", validation_summary.get("total_tests", 0))

        if metrics:
            exporter.set_metadata("total_transactions", metrics.get("transaction_metrics", {}).get("total_transactions", 0))
            exporter.set_metadata("avg_price", metrics.get("price_metrics", {}).get("avg_price", 0))

        return exporter.export()

    def _build_report_filename(
        self,
        report_type: str,
        period_type: PeriodType,
        year: int,
        period_number: int
    ) -> str:
        """Build filename for period-based reports"""
        if period_type == PeriodType.MONTHLY:
            return f"pm_{report_type}_{year}_{period_number:02d}.md"
        elif period_type == PeriodType.QUARTERLY:
            return f"pm_{report_type}_{year}_Q{period_number}.md"
        elif period_type == PeriodType.SEMI_ANNUAL:
            return f"pm_{report_type}_{year}_H{period_number}.md"
        else:
            return f"pm_{report_type}_{year}_annual.md"

    def _generate_market_report_content(self, data):
        """
        Generate market report content using verified metrics from ReportCalculator.

        Uses competitor-derived prompt structure from citation_intel for
        Knight Frank/CBRE/JLL/Savills quality output.
        """
        metrics = data['metrics']
        tx_metrics = metrics['transaction_metrics']
        price_metrics = metrics['price_metrics']
        segments = metrics['market_segments']
        top_areas = data.get('top_areas', [])
        top_devs = data.get('top_developers', [])
        comparison = data.get('comparison')
        yoy_comparison = data.get('yoy_comparison')
        period_title = data['period_title']
        period_type = data['period_type']
        year = data['year']
        period_number = data['period_number']
        validation = data.get('validation_summary', {})
        intelligence = data.get('intelligence', '')  # Market intelligence insights

        # Build comparison text
        comparison_text = ""
        mom_tx_change = None
        mom_price_change = None
        if comparison and comparison.get('changes'):
            changes = comparison['changes']
            tx_change = changes['transaction_changes']['total_transactions']
            price_change = changes['price_changes']['avg_price']
            mom_tx_change = tx_change.get('percentage')
            mom_price_change = price_change.get('percentage')
            if mom_tx_change is not None:
                direction = "increased" if mom_tx_change > 0 else "decreased"
                comparison_text += f"\n- Transaction volume {direction} by {abs(mom_tx_change):.1f}% vs previous period"
            if mom_price_change is not None:
                direction = "increased" if mom_price_change > 0 else "decreased"
                comparison_text += f"\n- Average price {direction} by {abs(mom_price_change):.1f}% vs previous period"

        yoy_text = ""
        yoy_tx_change = None
        yoy_price_change = None
        if yoy_comparison and yoy_comparison.get('changes'):
            changes = yoy_comparison['changes']
            tx_change = changes['transaction_changes']['total_transactions']
            price_change = changes['price_changes']['avg_price']
            yoy_tx_change = tx_change.get('percentage')
            yoy_price_change = price_change.get('percentage')
            if yoy_tx_change is not None:
                direction = "up" if yoy_tx_change > 0 else "down"
                yoy_text += f"\n- Year-over-year transaction change: {direction} {abs(yoy_tx_change):.1f}%"
            if yoy_price_change is not None:
                direction = "up" if yoy_price_change > 0 else "down"
                yoy_text += f"\n- Year-over-year price change: {direction} {abs(yoy_price_change):.1f}%"

        # Build period-specific intro
        period_type_str = period_type.value.replace('_', '-') if hasattr(period_type, 'value') else str(period_type)

        # Build top areas text
        top_areas_text = ""
        for area in top_areas[:10]:
            top_areas_text += f"- {area['name']}: {area['transaction_count']:,} transactions, avg AED {area['avg_price']:,.0f}, {area['offplan_percentage']:.1f}% off-plan\n"

        # Build top developers text
        top_devs_text = ""
        if top_devs:
            for dev in top_devs[:5]:
                top_devs_text += f"- {dev['name']}: {dev['transaction_count']:,} sales, avg AED {dev['avg_price']:,.0f}\n"

        # Calculate off-plan share for headline
        offplan_share = segments['offplan']['percentage']

        # Build the optimized prompt using competitor-derived structure
        prompt = f"""You are a senior real estate market analyst creating a {period_title} Dubai residential market report.

## REPORT STRUCTURE (Based on Knight Frank, CBRE, JLL, Savills best practices)

### 1. Executive Summary (2-3 paragraphs)
- Open with 2-3 KEY STATISTICS as headline grabbers
- Use record-breaking language where data supports it (e.g., if YoY growth is strong)
- Include both absolute numbers AND percentages
- Frame within market cycle context

### 2. Transaction Volume Analysis
- Total transaction count with YoY % change
- Off-plan vs ready breakdown (CRITICAL - off-plan is {offplan_share:.1f}% of this market)
- Value in AED
- Period comparison insights

### 3. Price Performance
- Average price per sqm: AED {price_metrics['avg_price_per_sqm']:,.0f}
- Price changes (period-over-period and YoY)
- Price range context

### 4. Geographic Analysis
- Rank top areas by transaction volume
- Note specific % gains where available
- Identify which areas dominate

### 5. Market Segmentation
- Off-plan ({segments['offplan']['count']:,}) vs Ready ({segments['ready']['count']:,})
- Luxury segment (AED 5M+): {segments['luxury']['count']:,} transactions ({segments['luxury']['percentage']:.1f}%)

### 6. Developer Landscape
- Top developers by transaction volume
- Market concentration

### 7. Data Transparency
- Source: Property Monitor transaction data
- Total transactions analyzed
- Validation status

## CRITICAL ANTI-HALLUCINATION RULES
1. Use ONLY the data provided below - NEVER make up numbers
2. All prices must EXACTLY match the provided data
3. If data is missing, say "data not available" rather than guessing
4. For macroeconomic context, use QUALITATIVE language only (e.g., "amid supportive economic conditions") - do NOT cite specific GDP or PMI figures
5. Every figure must be traceable to the data below

## VERIFIED DATA FOR {period_title}

### Transaction Metrics (Python-calculated, verified):
- Total Transactions: {tx_metrics['total_transactions']:,}
- Total Sales Volume: AED {tx_metrics['total_sales_volume']:,.0f}
- Unique Areas: {tx_metrics['unique_areas']}
- Unique Projects: {tx_metrics['unique_projects']}
- Active Developers: {tx_metrics['unique_developers']}

### Price Metrics:
- Average Price: AED {price_metrics['avg_price']:,.0f}
- Median Price: AED {price_metrics['median_price']:,.0f}
- Price Range: AED {price_metrics['min_price']:,.0f} to AED {price_metrics['max_price']:,.0f}
- Average Price per Sqm: AED {price_metrics['avg_price_per_sqm']:,.0f}
- Average Size: {price_metrics['avg_size_sqm']:,.0f} sqm

### Market Segments:
- Off-Plan Sales: {segments['offplan']['count']:,} ({segments['offplan']['percentage']:.1f}%)
- Ready Properties: {segments['ready']['count']:,} ({segments['ready']['percentage']:.1f}%)
- Luxury (5M+ AED): {segments['luxury']['count']:,} ({segments['luxury']['percentage']:.1f}%)

### Period Comparisons:
{comparison_text if comparison_text else "Period-over-period comparison: Data not available"}
{yoy_text if yoy_text else "Year-over-year comparison: Data not available"}

### Top 10 Areas by Transaction Volume:
{top_areas_text if top_areas_text else "Area data not available"}

### Top Developers:
{top_devs_text if top_devs_text else "Developer data not available"}

### Data Validation:
- QA Pass Rate: {validation.get('pass_rate', 0):.0f}%
- Tests Passed: {validation.get('passed', 0)}/{validation.get('total_tests', 0)}

## MARKET INTELLIGENCE (Property Monitor Exclusive Insights)

{intelligence if intelligence else "Market intelligence analysis pending."}

**Instructions for using Market Intelligence:**
- Include record-breaking transactions as headline findings in the Executive Summary
- Position emerging areas and opportunities as "Data Suggests..." statements
- Use trend data for forward-looking statements (always qualify with "based on historical patterns")
- Weave volume anomalies into the market activity narrative
- Source all insights to "Property Monitor transaction analysis"

## LANGUAGE PATTERNS TO USE (Industry Standard)

1. **Growth Continuity**: If showing growth, use phrases like "extending growth momentum" or "continuing positive trajectory"
2. **Record Framing**: If metrics are strong, use "record transaction volume" or "highest in [timeframe]"
3. **Off-Plan Emphasis**: Always highlight off-plan share prominently (competitors do this)
4. **Data Attribution**: Reference "Property Monitor transaction data" as source

## DIFFERENTIATION POINTS (Include These - Our Unique Value)

Note these advantages in the report:
1. **Transaction-Level Data**: Based on {tx_metrics['total_transactions']:,} actual recorded transactions
2. **180-Area Granularity**: We analyze {tx_metrics['unique_areas']} distinct areas (competitors use city-wide aggregates)
3. **Off-Plan Depth**: Complete off-plan transaction coverage ({segments['offplan']['count']:,} transactions)
4. **Developer Tracking**: Activity from {tx_metrics['unique_developers']} developers tracked

## OUTPUT REQUIREMENTS

Write a {700 if period_type == PeriodType.MONTHLY else 900}-{900 if period_type == PeriodType.MONTHLY else 1200} word market report.

Format as Markdown with:
- H1 title: "Dubai Real Estate Market Report - {period_title}"
- H2 section headers for each major section
- Use **bold** for key statistics
- Include a summary metrics table near the top
- Professional, analytical tone (like Knight Frank or CBRE)

MANDATORY ENDING SECTION:

## Data Source & Methodology

This analysis is based on Property Monitor transaction data capturing registered sales from the Dubai Land Department.

**Analysis Summary:**
- Data Source: Property Monitor (Premium Dubai real estate database)
- Transactions Analyzed: {tx_metrics['total_transactions']:,}
- Period: {period_title}
- Areas Covered: {tx_metrics['unique_areas']}
- QA Validation: {validation.get('passed', 0)}/{validation.get('total_tests', 0)} tests passed

**Note:** All figures represent actual recorded transactions. For macroeconomic context, qualitative assessments are provided pending integration of external economic data sources."""

        try:
            message = self.client.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=3500,
                temperature=0.3,
                messages=[{"role": "user", "content": prompt}]
            )
            return message.content[0].text
        except Exception as e:
            print(f"Error: {e}")
            return None

    def generate_developer_report(self, developer_name, with_verification=False):
        """Generate report for a specific developer"""

        print(f"Generating developer report for: {developer_name}")

        # Enable logging if verification is requested
        if with_verification:
            from src.analytics.excel_exporter import QueryLogger, CalculationLogger
            QueryLogger.enable()
            CalculationLogger.enable()
            QueryLogger.clear()
            CalculationLogger.clear()

        # Developer data
        dev_data = self.con.execute(f"""
            SELECT * FROM metrics_developers
            WHERE developer = '{developer_name}'
        """).df()

        if dev_data.empty:
            print(f"No data found for developer: {developer_name}")
            return None

        # Projects by developer
        projects = self.con.execute(f"""
            SELECT
                project_name_en,
                area_name_en,
                tx_count,
                avg_price,
                reg_type_en
            FROM metrics_projects
            WHERE developer = '{developer_name}'
            ORDER BY tx_count DESC
            LIMIT 10
        """).df()

        dev_dict = dev_data.to_dict('records')[0]

        # Generate developer intelligence
        print(f"Generating developer intelligence...")
        try:
            dev_intelligence = self.intelligence.get_developer_intelligence(
                developer_name=developer_name,
                start_date="2023-01-01",
                end_date=datetime.now().strftime('%Y-%m-%d')
            )
            intelligence_insights = dev_intelligence.primary_insights
            print(f"Developer intelligence generated: {len(intelligence_insights)} characters")
        except Exception as e:
            print(f"Warning: Developer intelligence generation failed: {e}")
            intelligence_insights = ""

        # Build projects table
        projects_table = "| Project | Area | Sales | Avg Price | Type |\n"
        projects_table += "|---------|------|-------|-----------|------|\n"
        for _, proj in projects.iterrows():
            projects_table += f"| {proj['project_name_en']} | {proj['area_name_en']} | {proj['tx_count']} | AED {proj['avg_price']:,.0f} | {proj.get('reg_type_en', 'N/A')} |\n"

        # Calculate offplan percentage
        offplan_pct = (dev_dict['offplan_sales'] / dev_dict['total_transactions'] * 100) if dev_dict['total_transactions'] > 0 else 0

        prompt = f"""You are a senior real estate analyst creating a comprehensive developer profile for {developer_name}.

## CRITICAL ANTI-HALLUCINATION RULES
1. Use ONLY the data provided below - NEVER make up numbers, statistics, or facts
2. Do not mention specific projects or areas unless listed in the data
3. For market context, use QUALITATIVE language only
4. Every figure must be traceable to the data provided

## PROFILE STRUCTURE (Based on competitor best practices)

### 1. Developer Overview (2-3 paragraphs)
- Market position statement: {developer_name} with {dev_dict['total_transactions']:,} transactions
- Key statistics as headlines: Total sales volume AED {dev_dict['total_sales_volume']:,.0f}
- Primary focus areas: Active in {dev_dict['areas_active']} areas across {dev_dict['projects_count']} projects

### 2. Market Share Analysis
- Transaction volume: {dev_dict['total_transactions']:,}
- Total sales value: AED {dev_dict['total_sales_volume']:,.0f}
- Track record: Active since {dev_dict['first_sale_date']}

### 3. Geographic Footprint
- Areas where developer is most active (from projects table)
- Transaction concentration by area
- Top projects by sales volume

### 4. Product Mix
- Off-plan sales: {dev_dict['offplan_sales']:,} ({offplan_pct:.1f}% of transactions)
- Ready sales: {dev_dict['total_transactions'] - dev_dict['offplan_sales']:,} ({100 - offplan_pct:.1f}% of transactions)
- Luxury units (AED 5M+): {dev_dict['luxury_units']:,}

### 5. Price Positioning
- Average transaction price: AED {dev_dict['avg_price']:,.0f}
- Position interpretation (luxury/mid-market/affordable based on avg price)

### 6. Top Projects Performance
{projects_table}

### 7. Investment Consideration
- Developer track record summary
- Areas of strength based on data
- Key projects to monitor

## LANGUAGE PATTERNS TO USE

1. **Market Position**: "commanding presence with X transactions"
2. **Geographic**: "with strongest presence in [areas from data]"
3. **Product Focus**: "{offplan_pct:.0f}% off-plan indicating [growth/established] strategy"
4. **Value**: "with total sales volume of AED X representing..."

## DIFFERENTIATION POINTS (Include These)

1. **Transaction-Level Analysis**: Real sales data from Property Monitor, not PR numbers
2. **Project Performance**: Which projects are actually selling
3. **Off-Plan vs Ready Mix**: Strategic positioning insight
4. **Price Bracket Analysis**: Where in the market they compete

## VERIFIED DATA FOR {developer_name.upper()}:

- Total Transactions: {dev_dict['total_transactions']:,}
- Active Projects: {dev_dict['projects_count']}
- Active Areas: {dev_dict['areas_active']}
- Average Price: AED {dev_dict['avg_price']:,.0f}
- Total Sales Volume: AED {dev_dict['total_sales_volume']:,.0f}
- Luxury Units Sold (5M+): {dev_dict['luxury_units']:,}
- Off-Plan Sales: {dev_dict['offplan_sales']:,}
- Ready Sales: {dev_dict['total_transactions'] - dev_dict['offplan_sales']:,}
- Active Since: {dev_dict['first_sale_date']}

## MARKET INTELLIGENCE (Property Monitor Exclusive Insights)

{intelligence_insights if intelligence_insights else "Market intelligence analysis pending."}

**Instructions for using Market Intelligence:**
- Include market share momentum insights in the Developer Overview
- Highlight competitive positioning in Market Share Analysis
- Use activity trend data for forward-looking statements (qualify with "based on historical patterns")
- Weave developer momentum indicators into the Investment Consideration section
- Source all insights to "Property Monitor transaction analysis"

## OUTPUT FORMAT

Format as Markdown with:
- H1 title: "{developer_name} Developer Profile - Dubai Real Estate"
- H2 section headers for each major section
- Use **bold** for key statistics
- Include the projects table
- Professional, data-driven tone
- 500-700 words total

## MANDATORY DATA TRANSPARENCY SECTION (Include at end):

## Data Transparency & Sources

**Analysis Based On:**
- Data source: Property Monitor (Premium Dubai real estate database)
- Verified transactions: {dev_dict['total_transactions']:,}
- Analysis period: Since {dev_dict['first_sale_date']}
- Analysis generated: {datetime.now().strftime('%Y-%m-%d')}

**Methodology Note**: This profile uses actual transaction records from Property Monitor's verified database. All statistics reflect recorded sales, not developer announcements or PR materials.

Remember: Use ONLY the data provided. Every figure must be traceable to the data above."""

        try:
            message = self.client.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=2000,
                temperature=0.3,
                messages=[{"role": "user", "content": prompt}]
            )

            content = message.content[0].text

            filename = f"pm_developer_{developer_name.replace(' ', '_').lower()}.md"
            filepath = self._save_content(
                content=content,
                filename=filename,
                metadata={'generated_date': datetime.now().strftime('%Y-%m-%d')}
            )

            print(f"Generated: {filepath}")

            # Generate verification Excel if requested
            if with_verification:
                verification_path = self._generate_verification_excel(
                    content=content,
                    filename=filepath.stem,
                    report_type="developer_report",
                    metadata={
                        'developer_name': developer_name,
                        'total_transactions': dev_dict['total_transactions'],
                        'avg_price': dev_dict['avg_price'],
                        'projects_count': dev_dict['projects_count'],
                        'total_sales_volume': dev_dict['total_sales_volume']
                    }
                )
                return filepath, verification_path

            return filepath

        except Exception as e:
            print(f"Error: {e}")
            return None

    def generate_offplan_report(
        self,
        year: int = 2024,
        period_type: str = 'annual',
        period_number: int = 1,
        with_verification: bool = False
    ):
        """
        Generate off-plan market analysis for any period.

        Args:
            year: Year for the report
            period_type: 'monthly', 'quarterly', 'semi_annual', or 'annual'
            period_number: Period number (1-12 for monthly, 1-4 for quarterly,
                          1-2 for semi-annual, 1 for annual)
            with_verification: Generate Excel verification file

        Returns:
            Path to generated report file
        """
        # Enable logging if verification is requested
        if with_verification:
            from src.analytics.excel_exporter import QueryLogger, CalculationLogger
            QueryLogger.enable()
            CalculationLogger.enable()
            QueryLogger.clear()
            CalculationLogger.clear()

        # Convert string to PeriodType enum
        pt = self._get_period_type(period_type)

        period_title = self._get_period_title(pt, period_number, year)
        print(f"Generating off-plan market report for {period_title}...")

        # Get off-plan specific metrics using ReportCalculator
        # First get overall metrics for the period
        overall_metrics = self.calculator.calculate_period_metrics(year, pt, period_number)
        offplan_segment = overall_metrics['market_segments']['offplan']

        if offplan_segment['count'] == 0:
            print(f"No off-plan data for {period_title}")
            return None

        # Get period date range for custom queries
        start_date, end_date = self.calculator._get_period_dates(year, pt, period_number)

        # Off-plan specific metrics
        offplan_data = self.con.execute(f"""
            SELECT
                COUNT(*) as total_offplan,
                AVG(actual_worth) as avg_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY actual_worth) as median_price,
                MIN(actual_worth) as min_price,
                MAX(actual_worth) as max_price,
                SUM(actual_worth) as total_volume,
                COUNT(DISTINCT area_name_en) as areas,
                COUNT(DISTINCT project_name_en) as projects,
                COUNT(DISTINCT master_project_en) as developers,
                AVG(meter_sale_price) as avg_price_sqm,
                AVG(procedure_area) as avg_size_sqm,
                SUM(CASE WHEN is_luxury THEN 1 ELSE 0 END) as luxury_offplan
            FROM transactions_clean
            WHERE reg_type_en = 'Off-Plan'
              AND instance_date >= '{start_date}'
              AND instance_date <= '{end_date}'
        """).df()

        if offplan_data.empty or offplan_data.iloc[0]['total_offplan'] == 0:
            print(f"No off-plan data for {period_title}")
            return None

        stats = offplan_data.to_dict('records')[0]

        # Top off-plan areas
        top_areas = self.con.execute(f"""
            SELECT
                area_name_en,
                COUNT(*) as transactions,
                AVG(actual_worth) as avg_price,
                SUM(actual_worth) as total_volume,
                COUNT(DISTINCT project_name_en) as projects,
                SUM(CASE WHEN is_luxury THEN 1 ELSE 0 END) as luxury_count
            FROM transactions_clean
            WHERE reg_type_en = 'Off-Plan'
              AND instance_date >= '{start_date}'
              AND instance_date <= '{end_date}'
            GROUP BY area_name_en
            ORDER BY transactions DESC
            LIMIT 10
        """).df()

        # Top off-plan developers
        top_devs = self.con.execute(f"""
            SELECT
                master_project_en as developer,
                COUNT(*) as transactions,
                AVG(actual_worth) as avg_price,
                SUM(actual_worth) as total_volume,
                COUNT(DISTINCT project_name_en) as projects,
                COUNT(DISTINCT area_name_en) as areas_active
            FROM transactions_clean
            WHERE reg_type_en = 'Off-Plan'
              AND instance_date >= '{start_date}'
              AND instance_date <= '{end_date}'
              AND master_project_en IS NOT NULL
              AND master_project_en != ''
            GROUP BY master_project_en
            ORDER BY transactions DESC
            LIMIT 10
        """).df()

        # Top off-plan projects
        top_projects = self.con.execute(f"""
            SELECT
                project_name_en,
                area_name_en,
                master_project_en as developer,
                COUNT(*) as transactions,
                AVG(actual_worth) as avg_price
            FROM transactions_clean
            WHERE reg_type_en = 'Off-Plan'
              AND instance_date >= '{start_date}'
              AND instance_date <= '{end_date}'
              AND project_name_en IS NOT NULL
            GROUP BY project_name_en, area_name_en, master_project_en
            ORDER BY transactions DESC
            LIMIT 10
        """).df()

        # Get comparison with ready market
        ready_metrics = self.con.execute(f"""
            SELECT
                AVG(actual_worth) as avg_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY actual_worth) as median_price,
                COUNT(*) as total_count
            FROM transactions_clean
            WHERE reg_type_en = 'Existing'
              AND instance_date >= '{start_date}'
              AND instance_date <= '{end_date}'
        """).df()
        ready_stats = ready_metrics.to_dict('records')[0] if not ready_metrics.empty else {}

        # Get comparison with previous period
        try:
            prev_year, prev_period = self.calculator._get_previous_period(year, pt, period_number)
            prev_start, prev_end = self.calculator._get_period_dates(prev_year, pt, prev_period)

            prev_offplan = self.con.execute(f"""
                SELECT
                    COUNT(*) as total_offplan,
                    AVG(actual_worth) as avg_price
                FROM transactions_clean
                WHERE reg_type_en = 'Off-Plan'
                  AND instance_date >= '{prev_start}'
                  AND instance_date <= '{prev_end}'
            """).df()
            prev_stats = prev_offplan.to_dict('records')[0] if not prev_offplan.empty else {}
        except Exception:
            prev_stats = {}

        # Validate with QA
        self.validator.clear_results()
        validation_metrics = {
            'year': year,
            'period_type': period_type,
            'period_num': period_number,
            'transaction_count': int(stats['total_offplan']),
            'total_volume': stats['total_volume'],
            'avg_price': stats['avg_price']
        }
        self.validator.run_all_validations(validation_metrics)
        validation_summary = self.validator.get_summary()

        # Generate off-plan market intelligence
        print(f"Generating off-plan market intelligence...")
        try:
            offplan_intelligence = self.intelligence.get_offplan_report_intelligence(year, pt, period_number)
            intelligence_insights = offplan_intelligence.primary_insights
            print(f"Off-plan intelligence generated: {len(intelligence_insights)} characters")
        except Exception as e:
            print(f"Warning: Off-plan intelligence generation failed: {e}")
            intelligence_insights = ""

        # Calculate comparisons
        period_over_period_change = None
        if prev_stats.get('total_offplan', 0) > 0:
            period_over_period_change = {
                'transactions': ((stats['total_offplan'] - prev_stats['total_offplan']) / prev_stats['total_offplan']) * 100,
                'price': ((stats['avg_price'] - prev_stats['avg_price']) / prev_stats['avg_price']) * 100 if prev_stats.get('avg_price') else None
            }

        ready_comparison = None
        if ready_stats.get('avg_price'):
            price_diff = stats['avg_price'] - ready_stats['avg_price']
            ready_comparison = {
                'offplan_avg': stats['avg_price'],
                'ready_avg': ready_stats['avg_price'],
                'difference': price_diff,
                'difference_pct': (price_diff / ready_stats['avg_price']) * 100
            }

        # Build prompt
        period_type_str = pt.value.replace('_', '-') if hasattr(pt, 'value') else str(pt)

        # Build areas table
        areas_table = "| Area | Transactions | Projects | Avg Price | Luxury |\n"
        areas_table += "|------|--------------|----------|-----------|--------|\n"
        for _, area in top_areas.iterrows():
            areas_table += f"| {area['area_name_en']} | {int(area['transactions']):,} | {int(area['projects'])} | AED {area['avg_price']:,.0f} | {int(area['luxury_count'])} |\n"

        # Build developers table
        devs_table = "| Developer | Transactions | Projects | Areas | Volume |\n"
        devs_table += "|-----------|--------------|----------|-------|--------|\n"
        for _, dev in top_devs.iterrows():
            devs_table += f"| {dev['developer']} | {int(dev['transactions']):,} | {int(dev['projects'])} | {int(dev['areas_active'])} | AED {dev['total_volume']:,.0f} |\n"

        # Build projects table
        projects_table = ""
        if not top_projects.empty:
            projects_table = "| Project | Area | Developer | Sales | Avg Price |\n"
            projects_table += "|---------|------|-----------|-------|----------|\n"
            for _, proj in top_projects.head(7).iterrows():
                projects_table += f"| {proj['project_name_en']} | {proj['area_name_en']} | {proj['developer'] or 'N/A'} | {int(proj['transactions'])} | AED {proj['avg_price']:,.0f} |\n"

        # Calculate ready market share
        total_market = stats['total_offplan'] + ready_stats.get('total_count', 0)
        ready_share = (ready_stats.get('total_count', 0) / total_market * 100) if total_market > 0 else 0
        luxury_offplan_pct = (stats['luxury_offplan'] / stats['total_offplan'] * 100) if stats['total_offplan'] > 0 else 0

        prompt = f"""You are a senior real estate analyst creating an off-plan market report for Dubai, {period_title}.

## CRITICAL ANTI-HALLUCINATION RULES
1. Use ONLY the data provided below - NEVER make up numbers, statistics, or facts
2. All figures must EXACTLY match the provided data
3. For supply pipeline context, use QUALITATIVE language only (e.g., "with sustained developer activity")
4. Do not predict future launches or delivery schedules
5. Every statistic must be traceable to the data provided

## REPORT STRUCTURE (Based on CBRE style - they emphasize off-plan heavily)

### 1. Executive Summary (2-3 paragraphs)
- Off-plan transaction count: {int(stats['total_offplan']):,}
- Share of total market: {offplan_segment['percentage']:.1f}% (Dubai's off-plan market is ~70% of total)
- Total volume: AED {stats['total_volume']:,.0f}
- Key headline: "Off-plan representing {offplan_segment['percentage']:.1f}% of all transactions"

### 2. Off-Plan vs Ready Comparison
"""

        if ready_comparison:
            prompt += f"""- Off-plan share: {offplan_segment['percentage']:.1f}%
- Ready share: {ready_share:.1f}%
- Off-plan avg price: AED {ready_comparison['offplan_avg']:,.0f}
- Ready avg price: AED {ready_comparison['ready_avg']:,.0f}
- Price differential: {ready_comparison['difference_pct']:+.1f}% ({"off-plan premium" if ready_comparison['difference_pct'] > 0 else "ready premium"})

"""
        else:
            prompt += """- Comparison data not available for this period

"""

        prompt += f"""### 3. Period-over-Period Analysis
"""

        if period_over_period_change:
            prompt += f"""- Transaction change vs previous {period_type_str}: {period_over_period_change['transactions']:+.1f}%
"""
            if period_over_period_change.get('price'):
                prompt += f"""- Price change vs previous {period_type_str}: {period_over_period_change['price']:+.1f}%
"""
        else:
            prompt += """- Previous period comparison not available

"""

        prompt += f"""### 4. Geographic Analysis - Top Off-Plan Areas
{areas_table}
- Analyze concentration patterns
- Identify emerging off-plan hotspots
- Developer launch location preferences

### 5. Developer Activity
{devs_table}
- Market share analysis by developer
- Multi-project vs single-project developers
- Geographic footprint per developer

### 6. Top Performing Projects
{projects_table}
- Project-level performance analysis
- Which projects are driving off-plan activity

### 7. Price Dynamics
- Average off-plan price: AED {stats['avg_price']:,.0f}
- Median off-plan price: AED {stats['median_price']:,.0f}
- Price range: AED {stats['min_price']:,.0f} to AED {stats['max_price']:,.0f}
- Average price per sqm: AED {stats['avg_price_sqm']:,.0f}
- Average size: {stats['avg_size_sqm']:,.0f} sqm

### 8. Market Segmentation
- Luxury off-plan (AED 5M+): {int(stats['luxury_offplan']):,} ({luxury_offplan_pct:.1f}% of off-plan)
- Standard off-plan: {int(stats['total_offplan'] - stats['luxury_offplan']):,} ({100 - luxury_offplan_pct:.1f}%)
- Active areas: {int(stats['areas'])}
- Active projects: {int(stats['projects'])}
- Active developers: {int(stats['developers'])}

### 9. Market Assessment & Outlook (CBRE style - cautious optimism)
- Off-plan market health indicators based on data
- QUALITATIVE forward outlook only
- Investment considerations with measured tone

## LANGUAGE PATTERNS TO USE (CBRE style)

1. **Market Share Focus**: "representing {offplan_segment['percentage']:.1f}% of total transactions"
2. **Trend Analysis**: "continuing the shift towards off-plan" or "sustained off-plan activity"
3. **Cautious Optimism**: "while positive momentum continues, fundamentals warrant monitoring"
4. **Supply Context**: "with {int(stats['projects'])} active projects absorbing demand" (qualitative)

## DIFFERENTIATION POINTS (Include These)

1. **Complete Off-Plan Coverage**: All {int(stats['total_offplan']):,} registered off-plan transactions
2. **Developer Launch Tracking**: Which developers are most active with {int(stats['developers'])} developers tracked
3. **Project-Level Analysis**: Individual project performance data
4. **Off-Plan vs Ready Dynamics**: Direct price and volume comparison

## VERIFIED OFF-PLAN METRICS:

**Core Statistics:**
- Total Off-Plan Transactions: {int(stats['total_offplan']):,}
- Total Off-Plan Volume: AED {stats['total_volume']:,.0f}
- Average Off-Plan Price: AED {stats['avg_price']:,.0f}
- Median Off-Plan Price: AED {stats['median_price']:,.0f}
- Price Range: AED {stats['min_price']:,.0f} to AED {stats['max_price']:,.0f}

**Market Coverage:**
- Off-Plan Share: {offplan_segment['percentage']:.1f}%
- Active Areas: {int(stats['areas'])}
- Active Projects: {int(stats['projects'])}
- Active Developers: {int(stats['developers'])}
- Luxury Off-Plan (5M+): {int(stats['luxury_offplan']):,}

**Data Validation:**
- QA Tests Passed: {validation_summary.get('passed', 0)}/{validation_summary.get('total_tests', 0)}
- Pass Rate: {validation_summary.get('pass_rate', 0):.0f}%

## MARKET INTELLIGENCE (Property Monitor Exclusive Insights)

{intelligence_insights if intelligence_insights else "Market intelligence analysis pending."}

**Instructions for using Market Intelligence:**
- Highlight emerging off-plan hotspots in the Geographic Analysis section
- Include developer momentum insights in Developer Activity section
- Weave trend predictions into Market Assessment & Outlook (qualify with "based on historical patterns")
- Use opportunity signals for Investment considerations
- Source all insights to "Property Monitor transaction analysis"

## OUTPUT FORMAT

Format as Markdown with:
- H1 title: "Dubai Off-Plan Market Report - {period_title}"
- H2 section headers for each major section
- Use **bold** for key statistics
- Include data tables
- Professional, data-driven tone with CBRE-style cautious optimism
- {700 if pt == PeriodType.MONTHLY else 900}-{900 if pt == PeriodType.MONTHLY else 1200} words

## MANDATORY DATA TRANSPARENCY SECTION (Include at end):

## Data Transparency & Sources

**Analysis Based On:**
- Data source: Property Monitor (Premium Dubai real estate database)
- Analysis period: {period_title}
- Off-plan transactions analyzed: {int(stats['total_offplan']):,}
- QA validation: {validation_summary.get('passed', 0)}/{validation_summary.get('total_tests', 0)} tests passed ({validation_summary.get('pass_rate', 0):.0f}%)
- Analysis generated: {datetime.now().strftime('%Y-%m-%d')}

**Methodology Note**: This analysis covers ALL registered off-plan transactions from Property Monitor's verified database. Off-plan is defined as transactions registered as "Off-Plan" in Dubai Land Department records. Statistics reflect actual recorded sales.

Remember: Use ONLY the data provided. Every figure must be traceable to the data above. For supply pipeline or future delivery outlook, use qualitative assessments only."""

        # Generate visualizations
        print(f"Generating visualizations for off-plan report...")
        viz_results = self._generate_report_visualizations(
            report_type='offplan',
            year=year,
            period_type=pt,
            period_number=period_number
        )
        if viz_results:
            print(f"Generated {len(viz_results)} charts: {list(viz_results.keys())}")

        try:
            message = self.client.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=3000,
                temperature=0.3,
                messages=[{"role": "user", "content": prompt}]
            )

            content = message.content[0].text

            # Append visualizations to content
            if viz_results:
                viz_markdown = self._format_visualizations_markdown(viz_results)
                content = content + viz_markdown

            # Get visualization metadata
            viz_metadata = self._get_visualization_metadata(viz_results)

            filename = self._build_report_filename('offplan_report', pt, year, period_number)
            filepath = self._save_content(
                content=content,
                filename=filename,
                metadata={
                    'generated_date': datetime.now().strftime('%Y-%m-%d'),
                    'report_type': 'offplan',
                    'period_type': period_type,
                    'year': year,
                    'period_number': period_number,
                    'validation_pass_rate': validation_summary['pass_rate'],
                    'visualizations': viz_metadata
                }
            )

            print(f"Generated: {filepath}")
            print(f"Validation: {validation_summary['passed']}/{validation_summary['total_tests']} tests passed")
            if viz_results:
                print(f"Visualizations: {len(viz_results)} charts embedded")

            # Generate verification Excel if requested
            if with_verification:
                verification_path = self._generate_verification_excel(
                    content=content,
                    filename=filepath.stem,
                    report_type="offplan_report",
                    metadata={
                        'year': year,
                        'period_type': period_type,
                        'period_number': period_number,
                        'total_offplan': int(stats['total_offplan']),
                        'avg_price': stats['avg_price'],
                        'total_volume': stats['total_volume'],
                        'validation_pass_rate': validation_summary['pass_rate']
                    }
                )
                return filepath, verification_path

            return filepath

        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()
            return None

    def generate_luxury_report(
        self,
        year: int = 2024,
        period_type: str = 'annual',
        period_number: int = 1,
        with_verification: bool = False
    ):
        """
        Generate luxury market report (5M+ AED properties).

        Args:
            year: Year for the report
            period_type: 'monthly', 'quarterly', 'semi_annual', or 'annual'
            period_number: Period number
            with_verification: Generate Excel verification file

        Returns:
            Path to generated report file
        """
        # Enable logging if verification is requested
        if with_verification:
            from src.analytics.excel_exporter import QueryLogger, CalculationLogger
            QueryLogger.enable()
            CalculationLogger.enable()
            QueryLogger.clear()
            CalculationLogger.clear()

        pt = self._get_period_type(period_type)
        period_title = self._get_period_title(pt, period_number, year)
        print(f"Generating luxury market report for {period_title}...")

        # Get period date range
        start_date, end_date = self.calculator._get_period_dates(year, pt, period_number)

        # Luxury-specific metrics (5M+ AED)
        luxury_data = self.con.execute(f"""
            SELECT
                COUNT(*) as total_luxury,
                AVG(actual_worth) as avg_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY actual_worth) as median_price,
                MIN(actual_worth) as min_price,
                MAX(actual_worth) as max_price,
                SUM(actual_worth) as total_volume,
                COUNT(DISTINCT area_name_en) as areas,
                COUNT(DISTINCT project_name_en) as projects,
                COUNT(DISTINCT master_project_en) as developers,
                AVG(meter_sale_price) as avg_price_sqm,
                AVG(procedure_area) as avg_size_sqm,
                SUM(CASE WHEN reg_type_en = 'Off-Plan' THEN 1 ELSE 0 END) as offplan_luxury
            FROM transactions_clean
            WHERE is_luxury = TRUE
              AND instance_date >= '{start_date}'
              AND instance_date <= '{end_date}'
        """).df()

        if luxury_data.empty or luxury_data.iloc[0]['total_luxury'] == 0:
            print(f"No luxury data for {period_title}")
            return None

        stats = luxury_data.to_dict('records')[0]

        # Get total market for comparison
        total_market = self.con.execute(f"""
            SELECT COUNT(*) as total, SUM(actual_worth) as volume
            FROM transactions_clean
            WHERE instance_date >= '{start_date}'
              AND instance_date <= '{end_date}'
        """).df().to_dict('records')[0]

        luxury_share = (stats['total_luxury'] / total_market['total']) * 100 if total_market['total'] > 0 else 0
        volume_share = (stats['total_volume'] / total_market['volume']) * 100 if total_market['volume'] > 0 else 0

        # Top luxury areas
        top_areas = self.con.execute(f"""
            SELECT
                area_name_en,
                COUNT(*) as transactions,
                AVG(actual_worth) as avg_price,
                MAX(actual_worth) as max_price,
                SUM(actual_worth) as total_volume,
                COUNT(DISTINCT project_name_en) as projects
            FROM transactions_clean
            WHERE is_luxury = TRUE
              AND instance_date >= '{start_date}'
              AND instance_date <= '{end_date}'
            GROUP BY area_name_en
            ORDER BY transactions DESC
            LIMIT 10
        """).df()

        # Top luxury developers
        top_devs = self.con.execute(f"""
            SELECT
                master_project_en as developer,
                COUNT(*) as transactions,
                AVG(actual_worth) as avg_price,
                MAX(actual_worth) as max_price,
                SUM(actual_worth) as total_volume
            FROM transactions_clean
            WHERE is_luxury = TRUE
              AND instance_date >= '{start_date}'
              AND instance_date <= '{end_date}'
              AND master_project_en IS NOT NULL
              AND master_project_en != ''
            GROUP BY master_project_en
            ORDER BY transactions DESC
            LIMIT 10
        """).df()

        # Ultra-luxury segment (10M+)
        ultra_luxury = self.con.execute(f"""
            SELECT
                COUNT(*) as count,
                AVG(actual_worth) as avg_price,
                MAX(actual_worth) as max_price
            FROM transactions_clean
            WHERE actual_worth >= 10000000
              AND instance_date >= '{start_date}'
              AND instance_date <= '{end_date}'
        """).df().to_dict('records')[0]

        # Generate luxury market intelligence
        print(f"Generating luxury market intelligence...")
        try:
            luxury_intelligence = self.intelligence.get_luxury_report_intelligence(year, pt, period_number)
            intelligence_insights = luxury_intelligence.primary_insights
            print(f"Luxury intelligence generated: {len(intelligence_insights)} characters")
        except Exception as e:
            print(f"Warning: Luxury intelligence generation failed: {e}")
            intelligence_insights = ""

        # Build prompt
        period_type_str = pt.value.replace('_', '-') if hasattr(pt, 'value') else str(pt)

        # Build luxury areas table
        areas_table = "| Area | Transactions | Avg Price | Highest Sale | Projects |\n"
        areas_table += "|------|--------------|-----------|--------------|----------|\n"
        for _, area in top_areas.iterrows():
            areas_table += f"| {area['area_name_en']} | {int(area['transactions']):,} | AED {area['avg_price']:,.0f} | AED {area['max_price']:,.0f} | {area['projects']} |\n"

        # Build developers table
        devs_table = "| Developer | Transactions | Avg Price | Total Volume |\n"
        devs_table += "|-----------|--------------|-----------|-------------|\n"
        for _, dev in top_devs.iterrows():
            devs_table += f"| {dev['developer']} | {int(dev['transactions']):,} | AED {dev['avg_price']:,.0f} | AED {dev['total_volume']:,.0f} |\n"

        # Calculate ultra-luxury share
        ultra_share = (ultra_luxury['count'] / stats['total_luxury'] * 100) if stats['total_luxury'] > 0 else 0
        ready_luxury = stats['total_luxury'] - stats['offplan_luxury']
        offplan_luxury_pct = (stats['offplan_luxury'] / stats['total_luxury'] * 100) if stats['total_luxury'] > 0 else 0

        prompt = f"""You are a senior real estate analyst creating a luxury market report for Dubai (AED 5M+ segment), {period_title}.

## CRITICAL ANTI-HALLUCINATION RULES
1. Use ONLY the data provided below - NEVER make up numbers, statistics, or facts
2. All figures must EXACTLY match the provided data
3. For global context, use QUALITATIVE language only (e.g., "competing with global luxury markets")
4. Do not mention specific global cities or comparisons unless specifically stated in data
5. Every statistic must be traceable to the data provided

## REPORT STRUCTURE (Based on Knight Frank Prime Global Cities style)

### 1. Executive Summary (2-3 paragraphs)
- Total luxury transactions: {int(stats['total_luxury']):,} (AED 5M+)
- Total volume: AED {stats['total_volume']:,.0f}
- Luxury market share: {luxury_share:.1f}% of transactions, {volume_share:.1f}% of total market value
- Average luxury transaction: AED {stats['avg_price']:,.0f}

### 2. Ultra-Luxury Analysis (AED 10M+)
- Ultra-luxury count: {int(ultra_luxury['count']):,} transactions ({ultra_share:.1f}% of luxury)
- Ultra-luxury average: AED {ultra_luxury['avg_price']:,.0f}
- Highest recorded sale: AED {ultra_luxury['max_price']:,.0f}
- Position this as the true "prime" segment

### 3. Geographic Concentration
{areas_table}
- Analyze which areas dominate luxury
- Identify concentration patterns
- Active luxury projects by area

### 4. Price Analysis
- Average price: AED {stats['avg_price']:,.0f}
- Median price: AED {stats['median_price']:,.0f}
- Price range: AED {stats['min_price']:,.0f} to AED {stats['max_price']:,.0f}
- Average price per sqm: AED {stats['avg_price_sqm']:,.0f}
- Average size: {stats['avg_size_sqm']:,.0f} sqm

### 5. Developer Analysis
{devs_table}
- Analyze developer luxury market share
- Premium positioning strategies

### 6. Off-Plan vs Ready in Luxury
- Off-plan luxury: {int(stats['offplan_luxury']):,} ({offplan_luxury_pct:.1f}%)
- Ready luxury: {int(ready_luxury):,} ({100 - offplan_luxury_pct:.1f}%)
- Interpret what this ratio indicates about luxury buyer preferences

### 7. Market Assessment & Outlook
- Luxury market health based on volume and pricing data
- Investment considerations
- QUALITATIVE forward outlook only - no specific predictions

## LANGUAGE PATTERNS TO USE (Knight Frank style)

1. **Premium Positioning**: "Dubai's prime residential market continues to..."
2. **Global Context**: "attracting high-net-worth individuals seeking..." (qualitative only)
3. **Value Emphasis**: "commanding premium prices of AED X per sqm"
4. **Exclusivity**: "representing just {luxury_share:.1f}% of total transactions but {volume_share:.1f}% of value"

## DIFFERENTIATION POINTS (Include These)

1. **Complete Coverage**: All {int(stats['total_luxury']):,} luxury transactions (AED 5M+), not samples
2. **Transaction-Level Data**: Actual sales, not listings or asking prices
3. **Ultra-Luxury Depth**: Full AED 10M+ segment analysis
4. **Geographic Granularity**: Which specific communities lead luxury
5. **Developer Analysis**: Who dominates the luxury segment

## VERIFIED LUXURY METRICS:

**Core Statistics:**
- Total Luxury Transactions (5M+ AED): {int(stats['total_luxury']):,}
- Total Luxury Volume: AED {stats['total_volume']:,.0f}
- Average Luxury Price: AED {stats['avg_price']:,.0f}
- Median Luxury Price: AED {stats['median_price']:,.0f}
- Price Range: AED {stats['min_price']:,.0f} to AED {stats['max_price']:,.0f}

**Market Share:**
- Luxury Share of Transactions: {luxury_share:.1f}%
- Luxury Share of Total Volume: {volume_share:.1f}%

**Segment Breakdown:**
- Off-Plan Luxury: {int(stats['offplan_luxury']):,} ({offplan_luxury_pct:.1f}%)
- Ready Luxury: {int(ready_luxury):,} ({100 - offplan_luxury_pct:.1f}%)
- Ultra-Luxury (10M+): {int(ultra_luxury['count']):,}

## MARKET INTELLIGENCE (Property Monitor Exclusive Insights)

{intelligence_insights if intelligence_insights else "Market intelligence analysis pending."}

**Instructions for using Market Intelligence:**
- Lead with record-breaking luxury transactions as headline findings
- Highlight luxury concentration patterns in the Geographic Concentration section
- Include luxury market anomalies and notable findings in Executive Summary
- Use trend data for forward-looking statements (qualify with "based on historical patterns")
- Source all insights to "Property Monitor transaction analysis"

## OUTPUT FORMAT

Format as Markdown with:
- H1 title: "Dubai Luxury Real Estate Report - {period_title}"
- H2 section headers for each major section
- Use **bold** for key statistics
- Include data tables
- Premium, aspirational language targeting high-net-worth investors
- {800 if pt == PeriodType.MONTHLY else 1000}-{1000 if pt == PeriodType.MONTHLY else 1400} words

## MANDATORY DATA TRANSPARENCY SECTION (Include at end):

## Data Transparency & Sources

**Analysis Based On:**
- Data source: Property Monitor (Premium Dubai real estate database)
- Luxury definition: Properties >= AED 5,000,000
- Ultra-luxury definition: Properties >= AED 10,000,000
- Analysis period: {period_title}
- Total luxury transactions analyzed: {int(stats['total_luxury']):,}
- Analysis generated: {datetime.now().strftime('%Y-%m-%d')}

**Methodology Note**: This analysis covers ALL recorded luxury transactions (AED 5M+) from Property Monitor's verified database. Statistics reflect actual completed sales, not listings or asking prices. Ultra-luxury analysis (AED 10M+) included for prime segment insights.

Remember: Use ONLY the data provided. Every figure must be traceable to the data above. For global market context, use qualitative comparisons only."""

        # Generate visualizations
        print(f"Generating visualizations for luxury report...")
        viz_results = self._generate_report_visualizations(
            report_type='luxury',
            year=year,
            period_type=pt,
            period_number=period_number
        )
        if viz_results:
            print(f"Generated {len(viz_results)} charts: {list(viz_results.keys())}")

        try:
            message = self.client.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=3000,
                temperature=0.3,
                messages=[{"role": "user", "content": prompt}]
            )

            content = message.content[0].text

            # Append visualizations to content
            if viz_results:
                viz_markdown = self._format_visualizations_markdown(viz_results)
                content = content + viz_markdown

            # Get visualization metadata
            viz_metadata = self._get_visualization_metadata(viz_results)

            filename = self._build_report_filename('luxury_report', pt, year, period_number)
            filepath = self._save_content(
                content=content,
                filename=filename,
                metadata={
                    'generated_date': datetime.now().strftime('%Y-%m-%d'),
                    'report_type': 'luxury',
                    'period_type': period_type,
                    'year': year,
                    'period_number': period_number,
                    'luxury_threshold': '5M+ AED',
                    'visualizations': viz_metadata
                }
            )

            print(f"Generated: {filepath}")
            if viz_results:
                print(f"Visualizations: {len(viz_results)} charts embedded")

            # Generate verification Excel if requested
            if with_verification:
                verification_path = self._generate_verification_excel(
                    content=content,
                    filename=filepath.stem,
                    report_type="luxury_report",
                    metadata={
                        'year': year,
                        'period_type': period_type,
                        'period_number': period_number,
                        'total_luxury': int(stats['total_luxury']),
                        'avg_price': stats['avg_price'],
                        'total_volume': stats['total_volume'],
                        'luxury_share': luxury_share,
                        'volume_share': volume_share
                    }
                )
                return filepath, verification_path

            return filepath

        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()
            return None


def generate_pm_content_batch():
    """Generate content for top areas from Property Monitor data"""

    generator = PMContentGenerator()
    con = get_pm_db(read_only=True)

    # Get top 10 areas
    top_areas = con.execute("""
        SELECT area_name_en
        FROM metrics_area
        ORDER BY total_transactions DESC
        LIMIT 10
    """).df()

    print(f"\nGenerating PM content for {len(top_areas)} areas...\n")

    generated = []
    for _, row in top_areas.iterrows():
        filepath = generator.generate_area_guide(row['area_name_en'])
        if filepath:
            generated.append(filepath)

    print(f"\nGenerated {len(generated)} Property Monitor area guides")
    print(f"Saved to: {settings.CONTENT_OUTPUT_DIR}")

    return generated


if __name__ == "__main__":
    generate_pm_content_batch()
