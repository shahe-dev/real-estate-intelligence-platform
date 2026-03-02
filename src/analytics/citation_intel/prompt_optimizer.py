# src/analytics/citation_intel/prompt_optimizer.py
"""
Prompt Optimizer for Content Generation

Uses patterns extracted from competitor analysis to generate optimized prompts
that match industry-standard quality while highlighting our unique advantages.
"""

from typing import Dict, List, Optional
from dataclasses import dataclass
from .pattern_database import PatternDatabase, pattern_db


@dataclass
class OptimizedPrompt:
    """An optimized prompt with metadata"""
    content_type: str
    prompt_text: str
    structure_guidance: str
    language_guidance: str
    differentiation_points: List[str]
    data_requirements: List[str]


class PromptOptimizer:
    """
    Generates optimized prompts for content generation based on competitor patterns.

    This class bridges the gap between raw data analysis and compelling content
    by providing structure, language, and differentiation guidance.
    """

    def __init__(self, pattern_database: Optional[PatternDatabase] = None):
        self.db = pattern_database or pattern_db

    def get_market_report_prompt(self, report_context: Dict) -> OptimizedPrompt:
        """
        Generate optimized prompt for market reports.

        Args:
            report_context: Dict with keys like 'period', 'year', 'data_summary'
        """
        period = report_context.get('period', 'Q4')
        year = report_context.get('year', 2025)

        prompt_text = f"""
You are a senior real estate market analyst creating a {period} {year} Dubai residential market report.

## REPORT STRUCTURE (Based on Knight Frank, CBRE, JLL, Savills best practices)

### 1. Executive Summary (2-3 paragraphs)
- Open with 2-3 KEY STATISTICS as headline grabbers
- Use record-breaking language where data supports it
- Include both absolute numbers AND percentages
- Frame within market cycle context

### 2. Transaction Volume Analysis
- Total transaction count with YoY % change
- Off-plan vs ready breakdown (CRITICAL - this is ~70% of Dubai market)
- Value in AED (include billions format for large sums)
- Monthly/quarterly trend visualization

### 3. Price Performance
- Average price per sqft by property type
- Quarterly and annual % changes
- Top performing areas with specific gains
- Price bracket analysis (luxury vs mid-market)

### 4. Geographic Analysis
- Rank top 10 areas by transaction volume
- Rank top 10 areas by price growth
- Identify emerging areas with accelerating momentum
- Community-level insights (this is OUR ADVANTAGE - competitors don't have this granularity)

### 5. Market Segmentation
- Apartments vs Villas performance
- Luxury segment (AED 5M+) analysis
- Ultra-luxury (AED 10M+) if significant activity
- Off-plan market dynamics

### 6. Supply-Demand Context
- Transaction velocity trends
- Listing dynamics where available
- NOTE: Supply pipeline data requires external sourcing - use qualitative language

### 7. Outlook & Opportunities
- Forward-looking statements based on trend data
- Emerging hotspot identification (OUR UNIQUE VALUE)
- Investment opportunity highlights

## LANGUAGE PATTERNS TO USE

1. **Growth Continuity**: "extending an unbroken run of quarterly growth", "for the Nth consecutive quarter"
2. **Record Framing**: "record transaction volume", "highest since [date]", "fastest pace on record"
3. **Macro Context** (Use qualitative): "amid supportive economic conditions", "benefiting from strong non-oil sector growth"
4. **Data Attribution**: Always cite "Source: Property Monitor transaction data" for our figures
5. **Cautious Optimism**: Balance positive trends with measured language

## DIFFERENTIATION POINTS (Include These - Competitors Don't Have)

1. **180-Area Granularity**: We analyze at community level, not just city-wide
2. **488K+ Transaction Records**: Real transaction data, not surveys
3. **Daily Data Freshness**: We see trends before quarterly reports
4. **Emerging Hotspot Detection**: Areas accelerating before media coverage
5. **Mid-Market Focus**: The 93% below AED 5M that competitors under-report

## CRITICAL DATA SOURCING RULES

- ONLY cite specific numbers from the data provided below
- For economic context (GDP, PMI, inflation), use qualitative language only
- Every figure MUST be traceable to our transaction database
- Include source attribution for all statistics

## DATA PROVIDED FOR THIS REPORT:
[INSERT ACTUAL DATA SUMMARY HERE]
"""

        structure_guidance = """
STRUCTURE ORDER:
1. Headline statistics (2-3 key numbers)
2. Executive narrative
3. Transaction volume section
4. Price performance section
5. Geographic breakdown
6. Segment analysis
7. Forward look / opportunities
"""

        language_guidance = """
LANGUAGE CHECKLIST:
- [ ] Used "record" or "highest" where data supports
- [ ] Included YoY % for all major metrics
- [ ] Separated off-plan vs ready analysis
- [ ] Named specific areas (not just "some areas")
- [ ] Balanced positive trends with measured language
- [ ] Cited data source for all figures
"""

        differentiation_points = [
            "180-area granularity vs city-wide aggregates",
            "Real transaction data (488K+ records) vs surveys",
            "Daily data freshness vs quarterly reports",
            "Emerging hotspot identification",
            "Mid-market segment depth (93% of market)"
        ]

        data_requirements = [
            "Total transaction count for period",
            "YoY transaction change %",
            "Off-plan vs ready transaction counts",
            "Average price per sqft by property type",
            "Price change % (quarterly and annual)",
            "Top 10 areas by volume",
            "Top 10 areas by price growth",
            "Luxury segment (5M+) transaction count",
            "Monthly transaction trend data"
        ]

        return OptimizedPrompt(
            content_type="market_report",
            prompt_text=prompt_text,
            structure_guidance=structure_guidance,
            language_guidance=language_guidance,
            differentiation_points=differentiation_points,
            data_requirements=data_requirements
        )

    def get_area_guide_prompt(self, area_context: Dict) -> OptimizedPrompt:
        """
        Generate optimized prompt for area guides.

        Args:
            area_context: Dict with keys like 'area_name', 'data_summary'
        """
        area_name = area_context.get('area_name', 'Dubai Marina')

        prompt_text = f"""
You are a senior real estate analyst creating a comprehensive guide for {area_name}, Dubai.

## GUIDE STRUCTURE (Based on competitor best practices + our unique depth)

### 1. Area Overview & Positioning
- Opening statement positioning the area in Dubai's market
- Key statistics as headline grabbers (transaction volume, avg price)
- Market segment positioning (luxury/mid-market/affordable)

### 2. Transaction Activity Analysis
- Total transactions in analysis period
- YoY change in activity
- Off-plan vs ready breakdown
- Monthly/quarterly trends
- Comparison to Dubai average

### 3. Price Analysis
- Average price per sqft
- Price range (min to max)
- YoY price change
- Quarterly price movement
- Price comparison to neighboring areas (OUR ADVANTAGE)

### 4. Property Type Breakdown
- Apartments vs Villas mix
- Most active property types
- Price performance by type

### 5. Developer Presence
- Active developers in the area
- Market share breakdown
- Recent/upcoming projects

### 6. Investment Perspective
- Yield indicators where calculable
- Price trajectory analysis
- Opportunity identification (OUR UNIQUE VALUE)
- Comparison to similar areas

### 7. Key Takeaways
- Bullet points summarizing the opportunity
- Who this area suits (investors, end-users, etc.)
- Forward-looking assessment

## LANGUAGE PATTERNS

1. **Positioning**: "One of Dubai's most [adjective] communities"
2. **Data-Driven**: "with X transactions totaling AED Xbn"
3. **Comparative**: "outperforming the Dubai average by X%"
4. **Opportunity Framing**: "presenting an opportunity for..."

## DIFFERENTIATION (Include These)

1. **Transaction-Level Data**: Real sales, not asking prices
2. **Price Arbitrage Analysis**: Compare to similar neighboring areas
3. **Emerging vs Established**: Where is this area in its growth cycle?
4. **Developer Momentum**: Who is betting on this area?

## DATA SOURCING RULES

- ONLY cite figures from the data provided
- Include source attribution
- Flag any data gaps

## DATA PROVIDED FOR {area_name.upper()}:
[INSERT ACTUAL DATA SUMMARY HERE]
"""

        structure_guidance = """
STRUCTURE ORDER:
1. Area positioning statement with key stats
2. Transaction activity deep-dive
3. Price analysis with comparisons
4. Property type breakdown
5. Developer analysis
6. Investment perspective
7. Key takeaways
"""

        language_guidance = """
LANGUAGE CHECKLIST:
- [ ] Positioned area within Dubai market context
- [ ] Included comparison to Dubai average
- [ ] Named specific developers active in area
- [ ] Provided price per sqft figures
- [ ] Identified target buyer/investor profile
- [ ] Cited data source for all figures
"""

        differentiation_points = [
            "Transaction-level granularity",
            "Price comparison to neighboring areas",
            "Developer market share analysis",
            "Investment yield indicators",
            "Growth trajectory positioning"
        ]

        data_requirements = [
            "Total transaction count for area",
            "Average price per sqft",
            "Price range (min, max)",
            "YoY transaction change",
            "YoY price change",
            "Off-plan vs ready split",
            "Property type breakdown",
            "Top developers by volume",
            "Monthly trend data"
        ]

        return OptimizedPrompt(
            content_type="area_guide",
            prompt_text=prompt_text,
            structure_guidance=structure_guidance,
            language_guidance=language_guidance,
            differentiation_points=differentiation_points,
            data_requirements=data_requirements
        )

    def get_developer_profile_prompt(self, developer_context: Dict) -> OptimizedPrompt:
        """
        Generate optimized prompt for developer profiles.

        Args:
            developer_context: Dict with keys like 'developer_name', 'data_summary'
        """
        developer_name = developer_context.get('developer_name', 'Emaar')

        prompt_text = f"""
You are a senior real estate analyst creating a comprehensive developer profile for {developer_name}.

## PROFILE STRUCTURE

### 1. Developer Overview
- Market position statement
- Key statistics (total transactions, market share)
- Primary focus areas and segments

### 2. Market Share Analysis
- Transaction volume and value
- Market share vs competitors
- Trend over analysis period

### 3. Geographic Footprint
- Areas where developer is most active
- Transaction concentration by area
- Emerging areas of focus

### 4. Product Mix
- Property types offered
- Price segment positioning
- Off-plan vs ready inventory

### 5. Price Positioning
- Average price per sqft
- Price range
- Comparison to market average

### 6. Performance Metrics
- Sales velocity
- Price performance over time
- Off-plan to handover trajectory (if data available)

### 7. Investment Consideration
- Developer track record summary
- Areas of strength
- Key projects to watch

## LANGUAGE PATTERNS

1. **Market Position**: "commanding X% market share"
2. **Geographic**: "with strongest presence in [areas]"
3. **Comparative**: "outperforming market average by X%"
4. **Trend-Based**: "showing accelerating/decelerating momentum"

## DIFFERENTIATION

1. **Transaction-Level Analysis**: Real sales data, not PR numbers
2. **Market Share Tracking**: Position vs competitors
3. **Area Concentration**: Where are they winning?
4. **Price Performance**: How do their properties perform?

## DATA PROVIDED FOR {developer_name.upper()}:
[INSERT ACTUAL DATA SUMMARY HERE]
"""

        return OptimizedPrompt(
            content_type="developer_profile",
            prompt_text=prompt_text,
            structure_guidance="See prompt structure sections",
            language_guidance="See prompt language patterns",
            differentiation_points=[
                "Transaction-level sales data",
                "Market share tracking over time",
                "Geographic concentration analysis",
                "Price performance vs market"
            ],
            data_requirements=[
                "Total transaction count",
                "Market share %",
                "Top areas by volume",
                "Average price per sqft",
                "YoY changes",
                "Property type breakdown"
            ]
        )

    def get_luxury_report_prompt(self, luxury_context: Dict) -> OptimizedPrompt:
        """
        Generate optimized prompt for luxury market reports.

        Args:
            luxury_context: Dict with keys like 'year', 'threshold', 'data_summary'
        """
        year = luxury_context.get('year', 2025)
        threshold = luxury_context.get('threshold', 5_000_000)
        threshold_display = f"AED {threshold/1_000_000:.0f}M+"

        prompt_text = f"""
You are a senior real estate analyst creating a luxury market report for Dubai ({threshold_display} segment), {year}.

## REPORT STRUCTURE (Based on Knight Frank Prime Global Cities style)

### 1. Executive Summary
- Total luxury transactions ({threshold_display})
- Total value
- Average transaction value
- Comparison to previous period

### 2. Ultra-Luxury Analysis (if AED 10M+ significant)
- Count of ultra-luxury transactions
- Highest transaction values
- Key locations

### 3. Geographic Concentration
- Top areas for luxury transactions
- Value concentration by area
- Emerging luxury destinations

### 4. Property Type Analysis
- Villas vs apartments in luxury segment
- Average sizes
- Price per sqft by type

### 5. Developer Analysis
- Top developers in luxury segment
- Market share in luxury
- Premium positioning

### 6. Global Context
- Dubai's position in global luxury market
- NOTE: Use qualitative comparisons unless specific data available
- "Competing with London, New York for ultra-high-net-worth buyers"

### 7. Outlook
- Luxury market trajectory
- Emerging premium locations
- Investment considerations

## LANGUAGE PATTERNS (Knight Frank style)

1. **Premium Positioning**: "Dubai's prime residential market"
2. **Global Context**: "attracting high-net-worth individuals seeking..."
3. **Value Emphasis**: "commanding premium prices"
4. **Exclusivity**: "limited inventory of..."

## DIFFERENTIATION

1. **Transaction-Level Data**: Actual luxury sales, not listings
2. **Complete Coverage**: All {threshold_display} transactions, not samples
3. **Area Granularity**: Which communities lead luxury?
4. **Developer Analysis**: Who dominates luxury segment?

## DATA PROVIDED:
[INSERT ACTUAL DATA SUMMARY HERE]
"""

        return OptimizedPrompt(
            content_type="luxury_report",
            prompt_text=prompt_text,
            structure_guidance="See prompt structure sections",
            language_guidance="Premium, aspirational language; global context",
            differentiation_points=[
                f"Complete {threshold_display} transaction coverage",
                "Actual sales data, not listings",
                "Area-level luxury concentration",
                "Developer luxury market share"
            ],
            data_requirements=[
                f"Luxury transaction count ({threshold_display})",
                "Total luxury value",
                "Ultra-luxury count (10M+)",
                "Top areas by luxury volume",
                "Top developers in luxury",
                "Highest transactions",
                "Property type breakdown"
            ]
        )

    def get_offplan_report_prompt(self, offplan_context: Dict) -> OptimizedPrompt:
        """
        Generate optimized prompt for off-plan market reports.

        Args:
            offplan_context: Dict with keys like 'period', 'year', 'data_summary'
        """
        period = offplan_context.get('period', 'Q4')
        year = offplan_context.get('year', 2025)

        prompt_text = f"""
You are a senior real estate analyst creating an off-plan market report for Dubai, {period} {year}.

## REPORT STRUCTURE (Based on CBRE style - they emphasize off-plan heavily)

### 1. Executive Summary
- Off-plan transaction count
- Share of total market (currently ~70%)
- YoY change
- Key trends

### 2. Off-Plan vs Ready Comparison
- Transaction volumes
- Value comparison
- Growth rate comparison
- Market share shift

### 3. Geographic Analysis
- Top areas for off-plan sales
- Emerging off-plan hotspots
- Developer launch locations

### 4. Developer Activity
- Top developers by off-plan volume
- New launch activity
- Payment plan trends (qualitative)

### 5. Price Dynamics
- Average off-plan price per sqft
- Off-plan vs ready price differential
- Price trends over time

### 6. Property Types
- Off-plan apartments vs villas
- Most active configurations
- Premium vs affordable off-plan

### 7. Market Assessment
- Off-plan market health indicators
- Speculative activity signals (CBRE style - measured concern)
- Forward outlook

## LANGUAGE PATTERNS (CBRE style)

1. **Market Share Focus**: "representing X% of total transactions"
2. **Trend Analysis**: "continuing the shift towards off-plan"
3. **Cautious Tone**: "while positive, warrants monitoring for..."
4. **Supply Context**: "with limited ready inventory..."

## DIFFERENTIATION

1. **Complete Off-Plan Data**: All registered off-plan transactions
2. **Developer Launch Tracking**: Who is most active?
3. **Area-Level Analysis**: Where is off-plan concentrated?
4. **Ready vs Off-Plan Dynamics**: Price and volume differentials

## DATA PROVIDED:
[INSERT ACTUAL DATA SUMMARY HERE]
"""

        return OptimizedPrompt(
            content_type="offplan_report",
            prompt_text=prompt_text,
            structure_guidance="See prompt structure sections",
            language_guidance="CBRE style - data-driven with cautious optimism",
            differentiation_points=[
                "Complete off-plan transaction coverage",
                "Developer launch tracking",
                "Off-plan vs ready price dynamics",
                "Area-level off-plan concentration"
            ],
            data_requirements=[
                "Off-plan transaction count",
                "Off-plan share of total %",
                "YoY off-plan change",
                "Top areas for off-plan",
                "Top developers in off-plan",
                "Off-plan avg price per sqft",
                "Off-plan vs ready price differential"
            ]
        )

    def get_prompt_for_content_type(self, content_type: str, context: Dict) -> OptimizedPrompt:
        """
        Get the appropriate optimized prompt for a content type.

        Args:
            content_type: One of 'market_report', 'area_guide', 'developer_profile',
                         'luxury_report', 'offplan_report'
            context: Dict with relevant context for the content type

        Returns:
            OptimizedPrompt with all guidance
        """
        prompt_methods = {
            'market_report': self.get_market_report_prompt,
            'area_guide': self.get_area_guide_prompt,
            'developer_profile': self.get_developer_profile_prompt,
            'luxury_report': self.get_luxury_report_prompt,
            'offplan_report': self.get_offplan_report_prompt
        }

        method = prompt_methods.get(content_type)
        if not method:
            raise ValueError(f"Unknown content type: {content_type}. "
                           f"Valid types: {list(prompt_methods.keys())}")

        return method(context)

    def get_data_sourcing_disclaimer(self) -> str:
        """Get the standard data sourcing disclaimer to include in content."""
        return """
---
**Data Source & Methodology**

This analysis is based on Property Monitor transaction data, which captures
registered sales transactions from the Dubai Land Department. All figures
represent actual recorded transactions, not asking prices or survey estimates.

- **Data Coverage**: 488,000+ transaction records
- **Geographic Granularity**: 180+ areas analyzed
- **Update Frequency**: Daily transaction data
- **Price Data**: Actual transaction values, not listings

For macroeconomic context, qualitative assessments are provided. Specific
economic indicators (GDP, PMI, inflation) will be added in future updates
pending data source integration.
---
"""


# Create singleton instance
prompt_optimizer = PromptOptimizer()


if __name__ == "__main__":
    # Demo the prompt optimizer
    optimizer = PromptOptimizer()

    print("=" * 60)
    print("PROMPT OPTIMIZER DEMO")
    print("=" * 60)

    # Get market report prompt
    market_prompt = optimizer.get_market_report_prompt({
        'period': 'Q4',
        'year': 2025
    })

    print("\n--- MARKET REPORT PROMPT ---")
    print(f"Content Type: {market_prompt.content_type}")
    print(f"\nData Requirements:")
    for req in market_prompt.data_requirements:
        print(f"  - {req}")
    print(f"\nDifferentiation Points:")
    for point in market_prompt.differentiation_points:
        print(f"  - {point}")

    # Get area guide prompt
    area_prompt = optimizer.get_area_guide_prompt({
        'area_name': 'Dubai Marina'
    })

    print("\n--- AREA GUIDE PROMPT ---")
    print(f"Content Type: {area_prompt.content_type}")
    print(f"\nData Requirements:")
    for req in area_prompt.data_requirements:
        print(f"  - {req}")

    print("\n" + "=" * 60)
    print("DATA SOURCING DISCLAIMER:")
    print("=" * 60)
    print(optimizer.get_data_sourcing_disclaimer())
