# src/analytics/market_intelligence/opportunity_detector.py
"""
Opportunity Detector - Identifies investment opportunities

Detects:
- Emerging hotspots (areas with accelerating transaction growth)
- Undervalued areas (lower price per sqm than comparable neighbors)
- Price arbitrage opportunities (same property type, different prices)
- Developer momentum (market share trends)
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime
import duckdb


@dataclass
class EmergingHotspot:
    """An area showing accelerating growth"""
    area: str
    current_tx_count: int
    previous_tx_count: int
    tx_growth_rate: float
    current_avg_price: float
    previous_avg_price: float
    price_growth_rate: float
    momentum_score: float  # Combined tx + price growth


@dataclass
class UndervaluedArea:
    """An area priced below comparable neighbors"""
    area: str
    avg_price_sqm: float
    comparable_areas: List[str]
    comparable_avg_price_sqm: float
    price_discount: float  # Negative = undervalued
    tx_count: int
    quality_indicators: Dict[str, Any]


@dataclass
class PriceArbitrage:
    """Same property type with different prices across areas"""
    property_type: str
    rooms: str
    high_price_area: str
    high_price_avg: float
    low_price_area: str
    low_price_avg: float
    price_difference: float
    arbitrage_percentage: float


@dataclass
class DeveloperMomentum:
    """Developer market share trend"""
    developer: str
    current_market_share: float
    previous_market_share: float
    market_share_change: float
    trend: str  # 'gaining', 'stable', 'losing'
    current_tx_count: int
    focus_areas: List[str]


@dataclass
class OpportunityResults:
    """Complete opportunity detection results"""
    period_info: Dict[str, Any]
    emerging_hotspots: List[EmergingHotspot]
    undervalued_areas: List[UndervaluedArea]
    price_arbitrage: List[PriceArbitrage]
    developer_momentum: List[DeveloperMomentum]


class OpportunityDetector:
    """
    Identifies investment opportunities based on transaction patterns.

    All calculations are based on Property Monitor transaction data.
    """

    def __init__(self, connection: duckdb.DuckDBPyConnection):
        self.con = connection

    def detect_opportunities(
        self,
        year: int,
        period_type: str,
        period_number: int,
        lookback_months: int = 6
    ) -> OpportunityResults:
        """
        Run all opportunity detection for a given period.

        Args:
            year: Year to analyze
            period_type: 'monthly', 'quarterly', 'semi_annual', 'annual'
            period_number: Period number within the year
            lookback_months: Months to use for historical comparison

        Returns:
            OpportunityResults with all detected opportunities
        """
        start_date, end_date = self._get_period_dates(year, period_type, period_number)
        prev_start, prev_end = self._get_previous_period_dates(start_date, end_date)

        period_info = {
            'year': year,
            'period_type': period_type,
            'period_number': period_number,
            'start_date': start_date,
            'end_date': end_date,
            'comparison_start': prev_start,
            'comparison_end': prev_end
        }

        return OpportunityResults(
            period_info=period_info,
            emerging_hotspots=self._find_emerging_hotspots(start_date, end_date, prev_start, prev_end),
            undervalued_areas=self._find_undervalued_areas(start_date, end_date),
            price_arbitrage=self._find_price_arbitrage(start_date, end_date),
            developer_momentum=self._get_developer_momentum(start_date, end_date, prev_start, prev_end)
        )

    def _get_period_dates(self, year: int, period_type: str, period_number: int) -> tuple:
        """Convert period to date range."""
        if period_type == 'monthly':
            start_date = f"{year}-{period_number:02d}-01"
            if period_number == 12:
                end_date = f"{year}-12-31"
            else:
                next_month = period_number + 1
                end_date = f"{year}-{next_month:02d}-01"
        elif period_type == 'quarterly':
            start_month = (period_number - 1) * 3 + 1
            end_month = start_month + 2
            start_date = f"{year}-{start_month:02d}-01"
            if end_month == 12:
                end_date = f"{year}-12-31"
            else:
                end_date = f"{year}-{end_month + 1:02d}-01"
        elif period_type == 'semi_annual':
            if period_number == 1:
                start_date = f"{year}-01-01"
                end_date = f"{year}-07-01"
            else:
                start_date = f"{year}-07-01"
                end_date = f"{year + 1}-01-01"
        else:  # annual
            start_date = f"{year}-01-01"
            end_date = f"{year + 1}-01-01"

        return start_date, end_date

    def _get_previous_period_dates(self, start_date: str, end_date: str) -> tuple:
        """Get the equivalent previous period for comparison."""
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        period_days = (end_dt - start_dt).days

        prev_end_dt = start_dt
        prev_start_dt = prev_end_dt - (end_dt - start_dt)

        return prev_start_dt.strftime('%Y-%m-%d'), prev_end_dt.strftime('%Y-%m-%d')

    def _find_emerging_hotspots(
        self,
        start_date: str,
        end_date: str,
        prev_start: str,
        prev_end: str,
        min_tx: int = 20,
        min_growth: float = 15.0
    ) -> List[EmergingHotspot]:
        """
        Find areas with accelerating growth (both volume and price).

        Emerging = significant transaction growth + price appreciation
        """
        query = f"""
            WITH current_period AS (
                SELECT
                    area_name_en,
                    COUNT(*) as tx_count,
                    AVG(actual_worth) as avg_price,
                    AVG(meter_sale_price) as avg_price_sqm
                FROM transactions_clean
                WHERE instance_date >= '{start_date}'
                  AND instance_date < '{end_date}'
                GROUP BY area_name_en
                HAVING COUNT(*) >= {min_tx}
            ),
            previous_period AS (
                SELECT
                    area_name_en,
                    COUNT(*) as tx_count,
                    AVG(actual_worth) as avg_price,
                    AVG(meter_sale_price) as avg_price_sqm
                FROM transactions_clean
                WHERE instance_date >= '{prev_start}'
                  AND instance_date < '{prev_end}'
                GROUP BY area_name_en
            )
            SELECT
                c.area_name_en,
                c.tx_count as current_tx,
                COALESCE(p.tx_count, 0) as previous_tx,
                c.avg_price as current_price,
                COALESCE(p.avg_price, c.avg_price) as previous_price,
                CASE
                    WHEN COALESCE(p.tx_count, 0) > 0
                    THEN ((c.tx_count - p.tx_count) * 100.0 / p.tx_count)
                    ELSE 100
                END as tx_growth,
                CASE
                    WHEN COALESCE(p.avg_price, 0) > 0
                    THEN ((c.avg_price - p.avg_price) * 100.0 / p.avg_price)
                    ELSE 0
                END as price_growth
            FROM current_period c
            LEFT JOIN previous_period p ON c.area_name_en = p.area_name_en
            WHERE COALESCE(p.tx_count, 0) >= 10  -- Needs historical baseline
            ORDER BY tx_growth DESC
        """

        df = self.con.execute(query).df()

        hotspots = []
        for _, row in df.iterrows():
            tx_growth = float(row['tx_growth'])
            price_growth = float(row['price_growth'])

            # Only include if both are positive and tx growth meets threshold
            if tx_growth >= min_growth and price_growth > 0:
                # Momentum score = weighted combination
                momentum_score = (tx_growth * 0.6) + (price_growth * 0.4)

                hotspots.append(EmergingHotspot(
                    area=row['area_name_en'],
                    current_tx_count=int(row['current_tx']),
                    previous_tx_count=int(row['previous_tx']),
                    tx_growth_rate=tx_growth,
                    current_avg_price=float(row['current_price']),
                    previous_avg_price=float(row['previous_price']),
                    price_growth_rate=price_growth,
                    momentum_score=momentum_score
                ))

        # Sort by momentum score
        hotspots.sort(key=lambda x: x.momentum_score, reverse=True)
        return hotspots[:10]

    def _find_undervalued_areas(
        self,
        start_date: str,
        end_date: str,
        min_tx: int = 30
    ) -> List[UndervaluedArea]:
        """
        Find areas priced significantly below comparable neighbors.

        Uses price per sqm as the comparison metric.
        """
        # Get all areas with sufficient transactions
        query = f"""
            SELECT
                area_name_en,
                COUNT(*) as tx_count,
                AVG(meter_sale_price) as avg_price_sqm,
                AVG(actual_worth) as avg_price,
                COUNT(DISTINCT property_type_en) as property_type_count,
                SUM(CASE WHEN reg_type_en = 'Off-Plan' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as offplan_pct,
                SUM(CASE WHEN is_luxury THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as luxury_pct
            FROM transactions_clean
            WHERE instance_date >= '{start_date}'
              AND instance_date < '{end_date}'
              AND meter_sale_price > 0
            GROUP BY area_name_en
            HAVING COUNT(*) >= {min_tx}
            ORDER BY avg_price_sqm
        """

        df = self.con.execute(query).df()

        if df.empty:
            return []

        # Calculate market-wide median for comparison
        market_median_sqm = df['avg_price_sqm'].median()

        undervalued = []
        for _, row in df.iterrows():
            area_price = float(row['avg_price_sqm'])

            # Find comparable areas (within 30% transaction count)
            tx_count = int(row['tx_count'])
            comparable_df = df[
                (df['tx_count'] >= tx_count * 0.7) &
                (df['tx_count'] <= tx_count * 1.3) &
                (df['area_name_en'] != row['area_name_en'])
            ]

            if len(comparable_df) < 3:
                continue

            comparable_avg = comparable_df['avg_price_sqm'].mean()
            price_discount = ((area_price - comparable_avg) / comparable_avg) * 100

            # Only include if meaningfully undervalued (>10% below comparables)
            if price_discount < -10:
                undervalued.append(UndervaluedArea(
                    area=row['area_name_en'],
                    avg_price_sqm=area_price,
                    comparable_areas=comparable_df['area_name_en'].tolist()[:5],
                    comparable_avg_price_sqm=comparable_avg,
                    price_discount=price_discount,
                    tx_count=tx_count,
                    quality_indicators={
                        'property_type_diversity': int(row['property_type_count']),
                        'offplan_percentage': float(row['offplan_pct']),
                        'luxury_percentage': float(row['luxury_pct']),
                        'vs_market_median': ((area_price - market_median_sqm) / market_median_sqm) * 100
                    }
                ))

        # Sort by discount (most undervalued first)
        undervalued.sort(key=lambda x: x.price_discount)
        return undervalued[:10]

    def _find_price_arbitrage(
        self,
        start_date: str,
        end_date: str,
        min_tx_per_area: int = 10,
        min_arbitrage_pct: float = 20.0
    ) -> List[PriceArbitrage]:
        """
        Find same property type/config with significantly different prices across areas.

        This helps identify potential value investments or premium overpayment.
        """
        query = f"""
            WITH area_type_prices AS (
                SELECT
                    area_name_en,
                    property_type_en,
                    rooms_en,
                    COUNT(*) as tx_count,
                    AVG(actual_worth) as avg_price,
                    AVG(meter_sale_price) as avg_price_sqm
                FROM transactions_clean
                WHERE instance_date >= '{start_date}'
                  AND instance_date < '{end_date}'
                  AND property_type_en IS NOT NULL
                  AND rooms_en IS NOT NULL
                GROUP BY area_name_en, property_type_en, rooms_en
                HAVING COUNT(*) >= {min_tx_per_area}
            ),
            type_stats AS (
                SELECT
                    property_type_en,
                    rooms_en,
                    COUNT(DISTINCT area_name_en) as area_count,
                    MAX(avg_price) as max_price,
                    MIN(avg_price) as min_price,
                    AVG(avg_price) as overall_avg
                FROM area_type_prices
                GROUP BY property_type_en, rooms_en
                HAVING COUNT(DISTINCT area_name_en) >= 3
            )
            SELECT
                ts.property_type_en,
                ts.rooms_en,
                ts.max_price,
                ts.min_price,
                ts.overall_avg,
                ((ts.max_price - ts.min_price) / ts.min_price) * 100 as arbitrage_pct,
                (SELECT area_name_en FROM area_type_prices atp
                 WHERE atp.property_type_en = ts.property_type_en
                   AND atp.rooms_en = ts.rooms_en
                 ORDER BY avg_price DESC LIMIT 1) as high_price_area,
                (SELECT area_name_en FROM area_type_prices atp
                 WHERE atp.property_type_en = ts.property_type_en
                   AND atp.rooms_en = ts.rooms_en
                 ORDER BY avg_price ASC LIMIT 1) as low_price_area
            FROM type_stats ts
            WHERE ((ts.max_price - ts.min_price) / ts.min_price) * 100 >= {min_arbitrage_pct}
            ORDER BY arbitrage_pct DESC
            LIMIT 15
        """

        df = self.con.execute(query).df()

        arbitrage = []
        for _, row in df.iterrows():
            arbitrage.append(PriceArbitrage(
                property_type=row['property_type_en'],
                rooms=row['rooms_en'],
                high_price_area=row['high_price_area'],
                high_price_avg=float(row['max_price']),
                low_price_area=row['low_price_area'],
                low_price_avg=float(row['min_price']),
                price_difference=float(row['max_price'] - row['min_price']),
                arbitrage_percentage=float(row['arbitrage_pct'])
            ))

        return arbitrage

    def _get_developer_momentum(
        self,
        start_date: str,
        end_date: str,
        prev_start: str,
        prev_end: str,
        min_tx: int = 20
    ) -> List[DeveloperMomentum]:
        """
        Calculate developer market share trends.

        Identifies developers gaining or losing market share.
        """
        query = f"""
            WITH current_period AS (
                SELECT
                    master_project_en,
                    COUNT(*) as tx_count,
                    SUM(actual_worth) as total_value
                FROM transactions_clean
                WHERE instance_date >= '{start_date}'
                  AND instance_date < '{end_date}'
                  AND master_project_en IS NOT NULL
                  AND master_project_en != ''
                GROUP BY master_project_en
            ),
            previous_period AS (
                SELECT
                    master_project_en,
                    COUNT(*) as tx_count,
                    SUM(actual_worth) as total_value
                FROM transactions_clean
                WHERE instance_date >= '{prev_start}'
                  AND instance_date < '{prev_end}'
                  AND master_project_en IS NOT NULL
                  AND master_project_en != ''
                GROUP BY master_project_en
            ),
            current_total AS (
                SELECT SUM(tx_count) as total FROM current_period
            ),
            previous_total AS (
                SELECT SUM(tx_count) as total FROM previous_period
            ),
            developer_areas AS (
                SELECT
                    master_project_en,
                    ARRAY_AGG(DISTINCT area_name_en) as areas
                FROM transactions_clean
                WHERE instance_date >= '{start_date}'
                  AND instance_date < '{end_date}'
                  AND master_project_en IS NOT NULL
                GROUP BY master_project_en
            )
            SELECT
                c.master_project_en as developer,
                c.tx_count as current_tx,
                COALESCE(p.tx_count, 0) as previous_tx,
                (c.tx_count * 100.0 / ct.total) as current_share,
                CASE
                    WHEN pt.total > 0
                    THEN (COALESCE(p.tx_count, 0) * 100.0 / pt.total)
                    ELSE 0
                END as previous_share,
                da.areas as focus_areas
            FROM current_period c
            CROSS JOIN current_total ct
            CROSS JOIN previous_total pt
            LEFT JOIN previous_period p ON c.master_project_en = p.master_project_en
            LEFT JOIN developer_areas da ON c.master_project_en = da.master_project_en
            WHERE c.tx_count >= {min_tx}
            ORDER BY c.tx_count DESC
        """

        df = self.con.execute(query).df()

        momentum = []
        for _, row in df.iterrows():
            current_share = float(row['current_share'])
            previous_share = float(row['previous_share'])
            share_change = current_share - previous_share

            # Determine trend
            if share_change > 0.5:
                trend = 'gaining'
            elif share_change < -0.5:
                trend = 'losing'
            else:
                trend = 'stable'

            # Handle array from DuckDB
            focus_areas = row['focus_areas']
            if isinstance(focus_areas, list):
                areas_list = focus_areas[:5]
            else:
                areas_list = []

            momentum.append(DeveloperMomentum(
                developer=row['developer'],
                current_market_share=current_share,
                previous_market_share=previous_share,
                market_share_change=share_change,
                trend=trend,
                current_tx_count=int(row['current_tx']),
                focus_areas=areas_list
            ))

        # Sort by market share change (biggest gainers first)
        momentum.sort(key=lambda x: x.market_share_change, reverse=True)
        return momentum[:15]

    def format_for_prompt(self, results: OpportunityResults) -> str:
        """
        Format opportunity results for injection into content prompt.

        Returns:
            Formatted string ready for prompt injection
        """
        sections = []

        # Emerging hotspots
        if results.emerging_hotspots:
            section = "**Emerging Hotspots** (Accelerating Growth):\n"
            for spot in results.emerging_hotspots[:5]:
                section += f"- {spot.area}: Transaction volume +{spot.tx_growth_rate:.1f}%, prices +{spot.price_growth_rate:.1f}% (momentum score: {spot.momentum_score:.1f})\n"
            sections.append(section)

        # Undervalued areas
        if results.undervalued_areas:
            section = "**Value Opportunities** (Below Comparable Areas):\n"
            for area in results.undervalued_areas[:5]:
                section += f"- {area.area}: {area.price_discount:.1f}% below comparables (AED {area.avg_price_sqm:,.0f}/sqm vs AED {area.comparable_avg_price_sqm:,.0f}/sqm)\n"
            sections.append(section)

        # Price arbitrage
        if results.price_arbitrage:
            section = "**Price Arbitrage** (Same Type, Different Prices):\n"
            for arb in results.price_arbitrage[:5]:
                section += f"- {arb.property_type} {arb.rooms}: {arb.arbitrage_percentage:.0f}% spread ({arb.low_price_area} vs {arb.high_price_area})\n"
            sections.append(section)

        # Developer momentum
        gainers = [d for d in results.developer_momentum if d.trend == 'gaining']
        if gainers:
            section = "**Developer Momentum** (Market Share Gainers):\n"
            for dev in gainers[:5]:
                section += f"- {dev.developer}: {dev.market_share_change:+.2f}pp market share ({dev.current_market_share:.1f}%)\n"
            sections.append(section)

        if not sections:
            return "No significant investment opportunities detected in this period."

        return "\n".join(sections)
