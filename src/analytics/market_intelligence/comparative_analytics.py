# src/analytics/market_intelligence/comparative_analytics.py
"""
Comparative Analytics - Cross-sectional analysis and profiling

Provides:
- Area DNA profiling (transaction mix, buyer behavior indicators)
- Comparable area identification
- Investment yield proxies
- Luxury concentration analysis
- Buyer profile indicators
"""

from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum
import duckdb


class MarketSegment(Enum):
    """Market segment positioning"""
    AFFORDABLE = "affordable"
    MID_MARKET = "mid-market"
    MID_LUXURY = "mid-luxury"
    LUXURY = "luxury"
    ULTRA_LUXURY = "ultra-luxury"


@dataclass
class AreaDNA:
    """Complete DNA profile for an area"""
    area_name: str
    market_segment: MarketSegment
    segment_confidence: float

    # Transaction mix
    transaction_mix: Dict[str, float]  # property_type -> percentage
    room_distribution: Dict[str, float]  # rooms -> percentage

    # Price metrics
    avg_price: float
    avg_price_sqm: float
    price_range: Tuple[float, float]  # min, max

    # Buyer indicators
    offplan_preference: float  # % off-plan
    avg_unit_size_sqm: float
    luxury_penetration: float  # % above 5M

    # Activity metrics
    total_transactions: int
    unique_projects: int
    unique_developers: int

    # Comparable areas
    comparable_areas: List[str]


@dataclass
class AreaComparison:
    """Comparison between areas"""
    area_1: str
    area_2: str
    price_difference_pct: float
    volume_difference_pct: float
    segment_match: bool
    similarity_score: float  # 0-1


@dataclass
class DeveloperProfile:
    """Developer profile based on transaction data"""
    developer_name: str
    market_segment_focus: MarketSegment
    primary_areas: List[str]
    property_type_focus: Dict[str, float]
    avg_transaction_value: float
    offplan_focus: float  # % of their sales that are off-plan
    total_transactions: int
    market_share: float


@dataclass
class ComparativeResults:
    """Complete comparative analysis results"""
    analysis_scope: Dict[str, Any]
    area_profiles: List[AreaDNA]
    area_comparisons: List[AreaComparison]
    top_developer_profiles: List[DeveloperProfile]
    market_concentration: Dict[str, Any]


class ComparativeAnalytics:
    """
    Provides cross-sectional analysis for areas and developers.

    All analysis is based on Property Monitor transaction data.
    """

    # Price thresholds for market segments (AED)
    SEGMENT_THRESHOLDS = {
        'affordable': (0, 1_000_000),
        'mid_market': (1_000_000, 3_000_000),
        'mid_luxury': (3_000_000, 5_000_000),
        'luxury': (5_000_000, 10_000_000),
        'ultra_luxury': (10_000_000, float('inf'))
    }

    def __init__(self, connection: duckdb.DuckDBPyConnection):
        self.con = connection

    def get_area_dna(
        self,
        area_name: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> AreaDNA:
        """
        Generate complete DNA profile for an area.

        Args:
            area_name: Name of the area to profile
            start_date: Optional start date filter
            end_date: Optional end date filter

        Returns:
            AreaDNA with complete area profile
        """
        date_filter = self._build_date_filter(start_date, end_date)

        # Main metrics query
        query = f"""
            SELECT
                COUNT(*) as tx_count,
                AVG(actual_worth) as avg_price,
                AVG(meter_sale_price) as avg_price_sqm,
                MIN(actual_worth) as min_price,
                MAX(actual_worth) as max_price,
                AVG(procedure_area) as avg_size_sqm,
                COUNT(DISTINCT project_name_en) as unique_projects,
                COUNT(DISTINCT master_project_en) as unique_developers,
                SUM(CASE WHEN reg_type_en = 'Off-Plan' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as offplan_pct,
                SUM(CASE WHEN is_luxury THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as luxury_pct
            FROM transactions_clean
            WHERE area_name_en = '{area_name}'
              {date_filter}
        """

        df = self.con.execute(query).df()

        if df.empty or df.iloc[0]['tx_count'] == 0:
            return self._empty_area_dna(area_name)

        row = df.iloc[0]

        # Property type distribution
        type_query = f"""
            SELECT
                property_type_en,
                COUNT(*) as tx_count,
                COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as pct
            FROM transactions_clean
            WHERE area_name_en = '{area_name}'
              {date_filter}
            GROUP BY property_type_en
            ORDER BY tx_count DESC
        """
        type_df = self.con.execute(type_query).df()
        transaction_mix = {r['property_type_en']: float(r['pct']) for _, r in type_df.iterrows()}

        # Room distribution
        room_query = f"""
            SELECT
                rooms_en,
                COUNT(*) as tx_count,
                COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as pct
            FROM transactions_clean
            WHERE area_name_en = '{area_name}'
              AND rooms_en IS NOT NULL
              {date_filter}
            GROUP BY rooms_en
            ORDER BY tx_count DESC
        """
        room_df = self.con.execute(room_query).df()
        room_distribution = {r['rooms_en']: float(r['pct']) for _, r in room_df.iterrows()}

        # Determine market segment
        avg_price = float(row['avg_price'])
        segment = self._determine_segment(avg_price)

        # Find comparable areas
        comparable_areas = self._find_comparable_areas(area_name, avg_price, int(row['tx_count']))

        return AreaDNA(
            area_name=area_name,
            market_segment=segment,
            segment_confidence=0.8,  # Could be refined
            transaction_mix=transaction_mix,
            room_distribution=room_distribution,
            avg_price=avg_price,
            avg_price_sqm=float(row['avg_price_sqm']) if row['avg_price_sqm'] else 0,
            price_range=(float(row['min_price']), float(row['max_price'])),
            offplan_preference=float(row['offplan_pct']),
            avg_unit_size_sqm=float(row['avg_size_sqm']) if row['avg_size_sqm'] else 0,
            luxury_penetration=float(row['luxury_pct']),
            total_transactions=int(row['tx_count']),
            unique_projects=int(row['unique_projects']),
            unique_developers=int(row['unique_developers']),
            comparable_areas=comparable_areas
        )

    def _build_date_filter(
        self,
        start_date: Optional[str],
        end_date: Optional[str]
    ) -> str:
        """Build date filter clause."""
        clauses = []
        if start_date:
            clauses.append(f"instance_date >= '{start_date}'")
        if end_date:
            clauses.append(f"instance_date < '{end_date}'")

        if clauses:
            return " AND " + " AND ".join(clauses)
        return ""

    def _determine_segment(self, avg_price: float) -> MarketSegment:
        """Determine market segment from average price."""
        if avg_price < 1_000_000:
            return MarketSegment.AFFORDABLE
        elif avg_price < 3_000_000:
            return MarketSegment.MID_MARKET
        elif avg_price < 5_000_000:
            return MarketSegment.MID_LUXURY
        elif avg_price < 10_000_000:
            return MarketSegment.LUXURY
        else:
            return MarketSegment.ULTRA_LUXURY

    def _find_comparable_areas(
        self,
        area_name: str,
        avg_price: float,
        tx_count: int,
        limit: int = 5
    ) -> List[str]:
        """Find areas with similar characteristics."""
        # Find areas with similar price and volume
        price_range_low = avg_price * 0.7
        price_range_high = avg_price * 1.3
        volume_range_low = tx_count * 0.5
        volume_range_high = tx_count * 2.0

        query = f"""
            SELECT
                area_name_en,
                COUNT(*) as tx_count,
                AVG(actual_worth) as avg_price,
                ABS(AVG(actual_worth) - {avg_price}) / {avg_price} as price_diff
            FROM transactions_clean
            WHERE area_name_en != '{area_name}'
            GROUP BY area_name_en
            HAVING COUNT(*) BETWEEN {volume_range_low} AND {volume_range_high}
               AND AVG(actual_worth) BETWEEN {price_range_low} AND {price_range_high}
            ORDER BY price_diff
            LIMIT {limit}
        """

        df = self.con.execute(query).df()
        return df['area_name_en'].tolist() if not df.empty else []

    def _empty_area_dna(self, area_name: str) -> AreaDNA:
        """Return empty DNA for areas with no data."""
        return AreaDNA(
            area_name=area_name,
            market_segment=MarketSegment.MID_MARKET,
            segment_confidence=0,
            transaction_mix={},
            room_distribution={},
            avg_price=0,
            avg_price_sqm=0,
            price_range=(0, 0),
            offplan_preference=0,
            avg_unit_size_sqm=0,
            luxury_penetration=0,
            total_transactions=0,
            unique_projects=0,
            unique_developers=0,
            comparable_areas=[]
        )

    def compare_areas(
        self,
        area_1: str,
        area_2: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> AreaComparison:
        """
        Compare two areas directly.
        """
        dna_1 = self.get_area_dna(area_1, start_date, end_date)
        dna_2 = self.get_area_dna(area_2, start_date, end_date)

        # Price difference
        if dna_2.avg_price > 0:
            price_diff = ((dna_1.avg_price - dna_2.avg_price) / dna_2.avg_price) * 100
        else:
            price_diff = 0

        # Volume difference
        if dna_2.total_transactions > 0:
            volume_diff = ((dna_1.total_transactions - dna_2.total_transactions) / dna_2.total_transactions) * 100
        else:
            volume_diff = 0

        # Segment match
        segment_match = dna_1.market_segment == dna_2.market_segment

        # Similarity score (simple version)
        similarity = 1.0
        similarity -= abs(price_diff) / 200  # Penalize price difference
        similarity -= abs(volume_diff) / 200  # Penalize volume difference
        if not segment_match:
            similarity -= 0.2
        similarity = max(0, min(1, similarity))

        return AreaComparison(
            area_1=area_1,
            area_2=area_2,
            price_difference_pct=price_diff,
            volume_difference_pct=volume_diff,
            segment_match=segment_match,
            similarity_score=similarity
        )

    def get_developer_profile(
        self,
        developer_name: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> DeveloperProfile:
        """
        Generate profile for a developer.
        """
        date_filter = self._build_date_filter(start_date, end_date)

        # Main metrics
        query = f"""
            WITH dev_stats AS (
                SELECT
                    COUNT(*) as tx_count,
                    AVG(actual_worth) as avg_price,
                    SUM(CASE WHEN reg_type_en = 'Off-Plan' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as offplan_pct
                FROM transactions_clean
                WHERE master_project_en = '{developer_name}'
                  {date_filter}
            ),
            market_total AS (
                SELECT COUNT(*) as total FROM transactions_clean WHERE 1=1 {date_filter}
            )
            SELECT
                ds.*,
                (ds.tx_count * 100.0 / mt.total) as market_share
            FROM dev_stats ds, market_total mt
        """

        df = self.con.execute(query).df()

        if df.empty or df.iloc[0]['tx_count'] == 0:
            return DeveloperProfile(
                developer_name=developer_name,
                market_segment_focus=MarketSegment.MID_MARKET,
                primary_areas=[],
                property_type_focus={},
                avg_transaction_value=0,
                offplan_focus=0,
                total_transactions=0,
                market_share=0
            )

        row = df.iloc[0]

        # Primary areas
        area_query = f"""
            SELECT area_name_en, COUNT(*) as tx_count
            FROM transactions_clean
            WHERE master_project_en = '{developer_name}'
              {date_filter}
            GROUP BY area_name_en
            ORDER BY tx_count DESC
            LIMIT 5
        """
        area_df = self.con.execute(area_query).df()
        primary_areas = area_df['area_name_en'].tolist()

        # Property type focus
        type_query = f"""
            SELECT
                property_type_en,
                COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as pct
            FROM transactions_clean
            WHERE master_project_en = '{developer_name}'
              {date_filter}
            GROUP BY property_type_en
        """
        type_df = self.con.execute(type_query).df()
        property_type_focus = {r['property_type_en']: float(r['pct']) for _, r in type_df.iterrows()}

        return DeveloperProfile(
            developer_name=developer_name,
            market_segment_focus=self._determine_segment(float(row['avg_price'])),
            primary_areas=primary_areas,
            property_type_focus=property_type_focus,
            avg_transaction_value=float(row['avg_price']),
            offplan_focus=float(row['offplan_pct']),
            total_transactions=int(row['tx_count']),
            market_share=float(row['market_share'])
        )

    def get_market_concentration(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Analyze market concentration metrics.
        """
        date_filter = self._build_date_filter(start_date, end_date)

        # Area concentration
        area_query = f"""
            SELECT
                area_name_en,
                COUNT(*) as tx_count,
                SUM(actual_worth) as total_value
            FROM transactions_clean
            WHERE 1=1 {date_filter}
            GROUP BY area_name_en
            ORDER BY tx_count DESC
        """
        area_df = self.con.execute(area_query).df()

        total_tx = area_df['tx_count'].sum()
        top_5_areas_share = (area_df.head(5)['tx_count'].sum() / total_tx * 100) if total_tx > 0 else 0
        top_10_areas_share = (area_df.head(10)['tx_count'].sum() / total_tx * 100) if total_tx > 0 else 0

        # Developer concentration
        dev_query = f"""
            SELECT
                master_project_en,
                COUNT(*) as tx_count
            FROM transactions_clean
            WHERE master_project_en IS NOT NULL
              AND master_project_en != ''
              {date_filter}
            GROUP BY master_project_en
            ORDER BY tx_count DESC
        """
        dev_df = self.con.execute(dev_query).df()

        total_dev_tx = dev_df['tx_count'].sum()
        top_5_devs_share = (dev_df.head(5)['tx_count'].sum() / total_dev_tx * 100) if total_dev_tx > 0 else 0
        top_10_devs_share = (dev_df.head(10)['tx_count'].sum() / total_dev_tx * 100) if total_dev_tx > 0 else 0

        # Segment distribution
        segment_query = f"""
            SELECT
                CASE
                    WHEN actual_worth < 1000000 THEN 'affordable'
                    WHEN actual_worth < 3000000 THEN 'mid_market'
                    WHEN actual_worth < 5000000 THEN 'mid_luxury'
                    WHEN actual_worth < 10000000 THEN 'luxury'
                    ELSE 'ultra_luxury'
                END as segment,
                COUNT(*) as tx_count,
                COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as pct
            FROM transactions_clean
            WHERE 1=1 {date_filter}
            GROUP BY segment
            ORDER BY tx_count DESC
        """
        segment_df = self.con.execute(segment_query).df()
        segment_distribution = {r['segment']: float(r['pct']) for _, r in segment_df.iterrows()}

        return {
            'area_concentration': {
                'top_5_share': top_5_areas_share,
                'top_10_share': top_10_areas_share,
                'top_areas': area_df.head(5)['area_name_en'].tolist()
            },
            'developer_concentration': {
                'top_5_share': top_5_devs_share,
                'top_10_share': top_10_devs_share,
                'top_developers': dev_df.head(5)['master_project_en'].tolist()
            },
            'segment_distribution': segment_distribution,
            'total_transactions': int(total_tx)
        }

    def format_area_dna_for_prompt(self, dna: AreaDNA) -> str:
        """
        Format area DNA for injection into content prompt.
        """
        sections = []

        # Market positioning
        section = f"**Market Positioning:** {dna.market_segment.value.replace('_', '-').title()}\n"
        section += f"- Average transaction: AED {dna.avg_price:,.0f}\n"
        section += f"- Average price/sqm: AED {dna.avg_price_sqm:,.0f}\n"
        sections.append(section)

        # Transaction mix
        if dna.transaction_mix:
            section = "**Transaction Mix:**\n"
            for prop_type, pct in sorted(dna.transaction_mix.items(), key=lambda x: x[1], reverse=True)[:4]:
                section += f"- {prop_type}: {pct:.1f}%\n"
            sections.append(section)

        # Buyer indicators
        section = "**Buyer Profile Indicators:**\n"
        section += f"- Off-plan preference: {dna.offplan_preference:.1f}%\n"
        section += f"- Average unit size: {dna.avg_unit_size_sqm:.0f} sqm\n"
        section += f"- Luxury penetration (5M+): {dna.luxury_penetration:.1f}%\n"
        sections.append(section)

        # Activity
        section = "**Activity Metrics:**\n"
        section += f"- Total transactions: {dna.total_transactions:,}\n"
        section += f"- Active projects: {dna.unique_projects}\n"
        section += f"- Active developers: {dna.unique_developers}\n"
        sections.append(section)

        # Comparable areas
        if dna.comparable_areas:
            section = f"**Comparable Areas:** {', '.join(dna.comparable_areas)}\n"
            sections.append(section)

        return "\n".join(sections)
