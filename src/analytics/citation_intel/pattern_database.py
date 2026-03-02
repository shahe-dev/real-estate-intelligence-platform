# src/analytics/citation_intel/pattern_database.py
"""
Pattern Database for Competitor Report Analysis

Stores structural patterns, language patterns, and data presentation formats
extracted from Knight Frank, CBRE, JLL, and Savills reports.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime


@dataclass
class ReportPattern:
    """Pattern extracted from a competitor report"""
    source: str  # Knight Frank, CBRE, JLL, Savills
    pattern_type: str  # structure, language, metric, visualization
    pattern_name: str
    description: str
    examples: List[str] = field(default_factory=list)
    frequency: str = "common"  # common, occasional, rare


@dataclass
class MetricPresentation:
    """How competitors present specific metrics"""
    metric_name: str
    presentation_formats: List[str]
    context_provided: List[str]
    comparisons_used: List[str]


class PatternDatabase:
    """
    Database of patterns extracted from competitor reports.

    Use this to understand:
    1. How top-tier reports are structured
    2. What language patterns they use
    3. How they present data and metrics
    4. What visualizations they include
    """

    def __init__(self):
        self.patterns: List[ReportPattern] = []
        self.metric_presentations: List[MetricPresentation] = []
        self.competitor_gaps: Dict[str, List[str]] = {}
        self._load_extracted_patterns()

    def _load_extracted_patterns(self):
        """Load patterns extracted from competitor analysis"""

        # ============================================================
        # REPORT STRUCTURE PATTERNS
        # ============================================================

        self.patterns.extend([
            ReportPattern(
                source="Knight Frank",
                pattern_type="structure",
                pattern_name="Executive Summary Opening",
                description="Reports open with 2-3 key statistics as headline grabbers",
                examples=[
                    "Record 56,854 home sales in Q3 2025, up 17% year-over-year",
                    "Q3 transaction value: AED 117bn (US$31.8bn)",
                    "Average values rose 2.5% during Q3"
                ],
                frequency="common"
            ),
            ReportPattern(
                source="CBRE",
                pattern_type="structure",
                pattern_name="Key Stats Dashboard",
                description="Open with 5-6 key metrics displayed prominently with arrows showing direction",
                examples=[
                    "▲ 5.0% YoY forecast increase in GDP in 2025",
                    "▲ 53.8 UAE PMI reading in September 2024",
                    "▲ 19.4% Increase in average residential rents in Dubai",
                    "▲ 6.6% YoY YTD to August 2024 increase in UAE's RevPAR"
                ],
                frequency="common"
            ),
            ReportPattern(
                source="CBRE",
                pattern_type="structure",
                pattern_name="Multi-Sector Coverage",
                description="Comprehensive coverage across multiple real estate sectors",
                examples=[
                    "Macroeconomic Overview → Offices → Residential → Hospitality → Retail → Industrial",
                    "Each sector gets dedicated analysis with Dubai and Abu Dhabi breakdown"
                ],
                frequency="common"
            ),
            ReportPattern(
                source="CBRE",
                pattern_type="structure",
                pattern_name="Dual-City Analysis",
                description="Side-by-side Dubai vs Abu Dhabi comparison for each sector",
                examples=[
                    "Dubai Offices section followed by Abu Dhabi Offices section",
                    "Dubai Residential vs Abu Dhabi Residential with comparable metrics"
                ],
                frequency="common"
            ),
            ReportPattern(
                source="Knight Frank",
                pattern_type="structure",
                pattern_name="Market Segment Breakdown",
                description="Separate sections for apartments vs villas vs ultra-luxury",
                examples=[
                    "Apartment Sector: 2.3% quarterly increase, 9.6% YoY growth",
                    "Villa Market: 3.6% quarterly growth, 12% YoY increase",
                    "Ultra-Luxury Segment (US$10M+): 103 homes sold in Q3"
                ],
                frequency="common"
            ),
            ReportPattern(
                source="Knight Frank",
                pattern_type="structure",
                pattern_name="Geographic Performance Ranking",
                description="Rank areas/communities by performance metrics",
                examples=[
                    "Top performers: Meydan City (+22% quarterly), Palm Jumeirah (+31% annually)",
                    "La Mer led with 33.8% quarterly and 54.7% annual gains"
                ],
                frequency="common"
            ),
            ReportPattern(
                source="JLL",
                pattern_type="structure",
                pattern_name="YoY Comparison Format",
                description="Every metric includes year-over-year comparison",
                examples=[
                    "Sales transactions increased 32% year-on-year",
                    "Rental market recorded 15.7% annual growth"
                ],
                frequency="common"
            ),
            ReportPattern(
                source="Savills",
                pattern_type="structure",
                pattern_name="Historical Trend Context",
                description="Provide multi-year context for current figures",
                examples=[
                    "Rose tenfold from 469 transactions (2020) to 4,670 (2024)",
                    "Off-plan share: 69% in 2024 vs just 14% in 2020"
                ],
                frequency="common"
            ),
            ReportPattern(
                source="All",
                pattern_type="structure",
                pattern_name="Supply-Demand Analysis Section",
                description="Dedicated section analyzing supply pipeline vs demand",
                examples=[
                    "Housing completions achieved only 46% on-time delivery",
                    "Projected 331,000 homes completion between 2026-2030"
                ],
                frequency="common"
            ),
            ReportPattern(
                source="Knight Frank",
                pattern_type="structure",
                pattern_name="Price Band Segmentation",
                description="Analyze market by price brackets to identify trends",
                examples=[
                    "Below AED 1M: 14% listing reduction with 10% sales growth",
                    "AED 1-25M: faster sales than stock replenishment",
                    "Above AED 25M: stock rising faster than deal activity"
                ],
                frequency="occasional"
            ),
        ])

        # ============================================================
        # LANGUAGE PATTERNS
        # ============================================================

        self.patterns.extend([
            ReportPattern(
                source="Knight Frank",
                pattern_type="language",
                pattern_name="Growth Continuity Emphasis",
                description="Emphasize sustained growth trajectory with specific timeframes",
                examples=[
                    "extending an unbroken run of quarterly growth that began in late 2020",
                    "Five consecutive years of uninterrupted quarterly growth",
                    "for the second consecutive year"
                ],
                frequency="common"
            ),
            ReportPattern(
                source="All",
                pattern_type="language",
                pattern_name="Record-Breaking Framing",
                description="Frame achievements as records or milestones",
                examples=[
                    "Record 56,854 home sales",
                    "highest January-March period on record",
                    "fastest pace on record"
                ],
                frequency="common"
            ),
            ReportPattern(
                source="Knight Frank",
                pattern_type="language",
                pattern_name="Global Context Comparison",
                description="Compare Dubai to global markets for perspective",
                examples=[
                    "almost equalling US$10M+ home sales in London and New York combined",
                    "held the world's top position for $10 million+ home sales"
                ],
                frequency="common"
            ),
            ReportPattern(
                source="All",
                pattern_type="language",
                pattern_name="Expert Attribution",
                description="Quote named experts with titles for credibility",
                examples=[
                    '"Dubai\'s prime residential market continues to attract..." — Andrew Cummings, Savills Head of Residential',
                    'Faisal Durrani, Partner and Head of Research MENA',
                    'Taimur Khan, Head of Research MEA at JLL'
                ],
                frequency="common"
            ),
            ReportPattern(
                source="Savills",
                pattern_type="language",
                pattern_name="Investor-Focused Terminology",
                description="Use language that speaks to investor concerns",
                examples=[
                    "high-net-worth individuals seeking space, privacy and superior lifestyle quality",
                    "investor confidence",
                    "end-users, rather than speculative purchasers"
                ],
                frequency="common"
            ),
            ReportPattern(
                source="JLL",
                pattern_type="language",
                pattern_name="Economic Contextualization",
                description="Connect real estate to broader economic indicators",
                examples=[
                    "stabilizing inflation and a robust labour market",
                    "GDP growth has been amongst the strongest",
                    "supported by diversification into non-oil sectors"
                ],
                frequency="common"
            ),
            ReportPattern(
                source="CBRE",
                pattern_type="language",
                pattern_name="Macro-First Framing",
                description="Always contextualize real estate within broader economic indicators",
                examples=[
                    "The UAE continues to benefit from solid underlying macro fundamentals",
                    "Against the backdrop of a very positive non-oil economy",
                    "supported by diversification into non-oil sectors"
                ],
                frequency="common"
            ),
            ReportPattern(
                source="CBRE",
                pattern_type="language",
                pattern_name="Supply-Demand Tension",
                description="Frame market dynamics in terms of supply-demand imbalance",
                examples=[
                    "Given the current lack of available supply",
                    "supply fails to keep pace with demand",
                    "limited new office deliveries recorded year to date"
                ],
                frequency="common"
            ),
            ReportPattern(
                source="CBRE",
                pattern_type="language",
                pattern_name="Cautious Optimism",
                description="Balance positive trends with measured concern",
                examples=[
                    "slight cause for concern from a fundamental perspective",
                    "which would suggest greater speculation in the market",
                    "normalisation of growth rates away from recent highs"
                ],
                frequency="common"
            ),
            ReportPattern(
                source="CBRE",
                pattern_type="language",
                pattern_name="Pull Quote Emphasis",
                description="Large stylized quotes that summarize key market thesis",
                examples=[
                    '"The UAE remains the key beneficiary amid regional challenges, with capital flows rising from affected countries"',
                ],
                frequency="common"
            ),
            ReportPattern(
                source="CBRE",
                pattern_type="language",
                pattern_name="Off-Plan vs Ready Segmentation",
                description="Consistently separate off-plan and ready market analysis",
                examples=[
                    "off-plan transactions up over 50% on the same period last year",
                    "70% of all residential transactions in Dubai are now off-plan",
                    "ready sales rose impressively by around 45%"
                ],
                frequency="common"
            ),
        ])

        # ============================================================
        # METRIC PRESENTATION PATTERNS
        # ============================================================

        self.metric_presentations.extend([
            MetricPresentation(
                metric_name="Transaction Volume",
                presentation_formats=[
                    "Total count with YoY percentage change",
                    "Value in both AED and USD",
                    "Breakdown by segment (off-plan vs ready)"
                ],
                context_provided=[
                    "Historical comparison (vs previous year, vs peak)",
                    "Market share by segment",
                    "Pace comparison (fastest on record, etc.)"
                ],
                comparisons_used=[
                    "Year-over-year percentage",
                    "Quarter-over-quarter percentage",
                    "Comparison to historical peak"
                ]
            ),
            MetricPresentation(
                metric_name="Price Performance",
                presentation_formats=[
                    "Average price per square foot (AED and USD)",
                    "Percentage change (quarterly, annually)",
                    "Price since cycle trough (e.g., since Q1 2020)"
                ],
                context_provided=[
                    "Comparison to previous market peak",
                    "Segment breakdown (apartments vs villas)",
                    "Top/bottom performing areas"
                ],
                comparisons_used=[
                    "Current vs 2014 peak",
                    "Current vs Q1 2020 (cycle trough)",
                    "Quarterly and annual growth rates"
                ]
            ),
            MetricPresentation(
                metric_name="Ultra-Luxury Segment",
                presentation_formats=[
                    "Count of transactions above threshold (US$10M+)",
                    "Total value of segment",
                    "Average deal value"
                ],
                context_provided=[
                    "Global ranking (vs London, New York)",
                    "Highest single transaction",
                    "YoY growth rate"
                ],
                comparisons_used=[
                    "Global city comparisons",
                    "Historical growth (10x since 2020)",
                    "Share of total market"
                ]
            ),
            MetricPresentation(
                metric_name="Rental Performance",
                presentation_formats=[
                    "Annual growth percentage",
                    "Breakdown by property type",
                    "Comparison to sales growth"
                ],
                context_provided=[
                    "Supply factors affecting rents",
                    "Demand drivers",
                    "Stabilization signals"
                ],
                comparisons_used=[
                    "Apartments vs villas growth",
                    "Year-over-year change",
                    "Historical trajectory"
                ]
            ),
            MetricPresentation(
                metric_name="Off-Plan vs Ready Transactions",
                presentation_formats=[
                    "Transaction count with YoY % change for each segment",
                    "Value breakdown (AED billions) by segment",
                    "Share of total transactions (e.g., 70% off-plan)"
                ],
                context_provided=[
                    "Market speculation indicators",
                    "Payment plan impact on buyer ability",
                    "Listing availability by segment"
                ],
                comparisons_used=[
                    "YTD period vs same period prior year",
                    "Off-plan growth rate vs ready growth rate",
                    "Segment value vs segment count"
                ]
            ),
            MetricPresentation(
                metric_name="Price Per Square Unit",
                presentation_formats=[
                    "AED per sqft (residential sales)",
                    "AED per sqm (office/industrial)",
                    "Breakdown by property type and grade"
                ],
                context_provided=[
                    "Top-performing communities by rate",
                    "Grade breakdown (Prime, Grade A, B, C)",
                    "Luxury vs mainstream comparison"
                ],
                comparisons_used=[
                    "12-month, 6-month, 3-month % change",
                    "Apartments vs Villas",
                    "Dubai vs Abu Dhabi"
                ]
            ),
            MetricPresentation(
                metric_name="Supply Pipeline",
                presentation_formats=[
                    "Number of units expected by year",
                    "Breakdown by major project/location",
                    "Completion rate vs announced"
                ],
                context_provided=[
                    "Impact on future price/rent trajectory",
                    "Developer delivery track record",
                    "Supply-demand balance projection"
                ],
                comparisons_used=[
                    "Current year vs upcoming years",
                    "Historical delivery rates",
                    "Announced vs actual completions"
                ]
            ),
        ])

        # ============================================================
        # COMPETITOR GAPS (What they DON'T cover that we can)
        # ============================================================

        self.competitor_gaps = {
            "granularity": [
                "Area-level analysis (180 areas) vs city-wide aggregates",
                "Weekly/monthly velocity vs quarterly snapshots",
                "Project-level performance vs area summaries",
                "Developer market share tracking"
            ],
            "data_freshness": [
                "Daily transaction data vs quarterly reports",
                "Real-time market signals",
                "Early trend detection before quarterly reports"
            ],
            "transaction_level": [
                "Actual transaction prices vs asking/survey data",
                "488K+ real sales records",
                "Sample transaction citations for verification"
            ],
            "mid_market_focus": [
                "AED 1-5M segment analysis (93% of market)",
                "First-time buyer trends",
                "Affordable luxury positioning"
            ],
            "predictive_insights": [
                "Emerging hotspot detection",
                "Price trajectory modeling",
                "Seasonality patterns by area",
                "Market cycle positioning"
            ],
            "opportunity_identification": [
                "Undervalued area detection",
                "Price arbitrage between areas",
                "Off-plan vs ready price dynamics",
                "Developer momentum scoring"
            ]
        }

    def get_structure_patterns(self) -> List[ReportPattern]:
        """Get all structure-related patterns"""
        return [p for p in self.patterns if p.pattern_type == "structure"]

    def get_language_patterns(self) -> List[ReportPattern]:
        """Get all language-related patterns"""
        return [p for p in self.patterns if p.pattern_type == "language"]

    def get_patterns_by_source(self, source: str) -> List[ReportPattern]:
        """Get patterns from a specific competitor"""
        return [p for p in self.patterns if p.source == source or p.source == "All"]

    def get_metric_presentation(self, metric_name: str) -> Optional[MetricPresentation]:
        """Get presentation format for a specific metric"""
        for mp in self.metric_presentations:
            if mp.metric_name.lower() == metric_name.lower():
                return mp
        return None

    def get_differentiation_opportunities(self) -> Dict[str, List[str]]:
        """Get list of things we can do that competitors don't"""
        return self.competitor_gaps

    def generate_prompt_guidelines(self) -> str:
        """Generate guidelines for content prompts based on patterns"""
        guidelines = """
# Content Generation Guidelines (Based on Competitor Analysis)

## DATA SOURCING REQUIREMENTS (CRITICAL)

### What We CAN Source Internally (Property Monitor Database):
- Transaction counts, volumes, values
- Price per sqft by area/property type
- Off-plan vs ready breakdown
- Area-level performance metrics
- Developer market share
- Historical transaction trends (2023-2026)
- 488K+ actual transaction records

### External Data Sources Used by Competitors:
Competitors cite these sources - we should NOT include specific figures
from these sources unless we have API access:

1. **Economic Indicators** (Oxford Economics, Macrobond):
   - GDP growth rates, forecasts
   - PMI readings
   - Inflation rates
   - Non-oil sector contribution

2. **Property Indices** (REIDIN):
   - Official price indices
   - Rental indices

3. **Hospitality Metrics** (STR Global):
   - RevPAR, ADR, occupancy rates
   - Visitor numbers

4. **Official Government Data**:
   - Dubai Land Department (we have via Property Monitor)
   - Department of Culture & Tourism (visitor stats)
   - Abu Dhabi data (Quanta)

### SOURCING RULES:
- ONLY cite specific numbers we can back up from our database
- For macro context, use qualitative language: "strong economic growth"
  instead of "5.0% GDP growth" unless we have the source
- Always note "Source: Property Monitor transaction data" for our figures
- Flag content that would benefit from external data integration

## Report Structure (Match Competitor Quality)

1. **Open with Impact**: Lead with 2-3 key statistics as headline grabbers
   - Use record-breaking language when applicable
   - Include both absolute numbers and percentages
   - SOURCE: Our transaction database only

2. **Segment Analysis**: Break down by:
   - Property type (apartments vs villas)
   - Price brackets (luxury, mid-market, affordable)
   - Registration type (off-plan vs ready)
   - SOURCE: Our transaction database

3. **Geographic Performance**: Rank areas by:
   - Price growth (quarterly and annual)
   - Transaction volume
   - Value concentration
   - SOURCE: Our transaction database

4. **Supply-Demand Context**:
   - Focus on demand metrics we can measure (transaction velocity)
   - Be cautious with supply projections (need external source)
   - Note: "Supply data requires external sourcing"

## Language Patterns (Industry Standard)

1. **Growth Continuity**: Emphasize sustained trends
   - "extending an unbroken run of growth"
   - "consecutive quarters/years"

2. **Record Framing**: Position achievements as milestones
   - "record transaction volume"
   - "highest on record"
   - "fastest pace"

3. **Macro Context** (BE CAREFUL):
   - Use qualitative: "supportive economic environment"
   - Avoid specific GDP/PMI numbers without source access
   - OK to say: "amid strong non-oil sector growth"

4. **Expert Attribution**: Include named expert quotes
   - Title and organization
   - Insight or forecast

## Metric Presentation (Best Practices)

1. **Transaction Volume** (WE HAVE THIS):
   - Count + YoY %
   - Value in AED
   - Segment breakdown
   - Area-level detail

2. **Price Performance** (WE HAVE THIS):
   - Per sqft average
   - Quarterly + annual change
   - By area and property type

3. **Luxury Segment** (WE HAVE THIS):
   - Count above threshold (AED 5M+, 10M+)
   - Top transactions
   - Area concentration

4. **Rental Performance** (LIMITED):
   - We have some rental data
   - Note gaps where needed

## DIFFERENTIATION (Our Unique Value)

Add these elements that competitors DON'T provide:

1. **Granular Area Analysis**: 180-area breakdown vs city-wide
2. **Emerging Hotspots**: Areas with accelerating growth before media coverage
3. **Opportunity Detection**: Undervalued areas, price arbitrage
4. **Forward-Looking Signals**: Trend predictions based on our data patterns
5. **Developer Analysis**: Market share shifts, momentum scoring
6. **Mid-Market Focus**: The 93% below AED 5M that competitors ignore
7. **Transaction-Level Citations**: Actual deal references for verification

## EXTERNAL DATA GAPS (Potential API Integrations Needed)

To fully match competitor depth, consider adding:
1. **Economic Data API**: World Bank, IMF, or Oxford Economics
2. **PMI Data**: S&P Global UAE PMI
3. **Tourism Stats**: Dubai Economy & Tourism API
4. **Supply Pipeline**: Developer announcements aggregator
5. **Rental Index**: RERA rental index API
"""
        return guidelines


# Create singleton instance
pattern_db = PatternDatabase()


if __name__ == "__main__":
    # Print summary of patterns
    db = PatternDatabase()

    print("=" * 60)
    print("COMPETITOR PATTERN DATABASE")
    print("=" * 60)

    print(f"\nTotal patterns extracted: {len(db.patterns)}")
    print(f"Metric presentations: {len(db.metric_presentations)}")
    print(f"Differentiation categories: {len(db.competitor_gaps)}")

    print("\n" + "-" * 40)
    print("STRUCTURE PATTERNS:")
    for p in db.get_structure_patterns():
        print(f"  - {p.pattern_name} ({p.source})")

    print("\n" + "-" * 40)
    print("LANGUAGE PATTERNS:")
    for p in db.get_language_patterns():
        print(f"  - {p.pattern_name} ({p.source})")

    print("\n" + "-" * 40)
    print("DIFFERENTIATION OPPORTUNITIES:")
    for category, items in db.get_differentiation_opportunities().items():
        print(f"\n  {category.upper()}:")
        for item in items:
            print(f"    - {item}")

    print("\n" + "=" * 60)
    print("PROMPT GUIDELINES:")
    print("=" * 60)
    print(db.generate_prompt_guidelines())
