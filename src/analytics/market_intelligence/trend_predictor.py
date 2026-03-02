# src/analytics/market_intelligence/trend_predictor.py
"""
Trend Predictor - Forward-looking analysis based on historical patterns

Analyzes:
- Price momentum (3/6/12 month trajectories)
- Seasonality patterns (peak vs low activity months)
- Market cycle positioning (growth/peak/correction/recovery)
- Volume trajectory (transaction count trends)
"""

from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import duckdb


class MarketCyclePhase(Enum):
    """Market cycle phases"""
    GROWTH = "growth"
    PEAK = "peak"
    CORRECTION = "correction"
    RECOVERY = "recovery"
    STABLE = "stable"


class TrajectoryDirection(Enum):
    """Price/volume trajectory direction"""
    ACCELERATING = "accelerating"
    DECELERATING = "decelerating"
    STABLE = "stable"
    VOLATILE = "volatile"


@dataclass
class PriceMomentum:
    """Price momentum over multiple time windows"""
    area: Optional[str]
    property_type: Optional[str]
    current_avg_price: float
    momentum_3m: Optional[float]  # % change
    momentum_6m: Optional[float]
    momentum_12m: Optional[float]
    trajectory: TrajectoryDirection
    consistency_score: float  # 0-1, higher = more consistent direction


@dataclass
class SeasonalityPattern:
    """Seasonal patterns in transaction activity"""
    area: Optional[str]
    peak_months: List[str]
    low_months: List[str]
    peak_vs_low_ratio: float
    monthly_indices: Dict[str, float]  # Month name -> index (100 = average)
    current_month_position: str  # 'peak', 'low', 'normal'


@dataclass
class CyclePosition:
    """Market cycle position assessment"""
    area: Optional[str]
    phase: MarketCyclePhase
    phase_confidence: float  # 0-1
    supporting_indicators: Dict[str, Any]
    phase_duration_months: int  # How long in current phase


@dataclass
class TrendResults:
    """Complete trend analysis results"""
    analysis_date: str
    scope: Dict[str, Any]  # area, property_type filters
    price_momentum: PriceMomentum
    volume_momentum: Dict[str, Any]
    seasonality: SeasonalityPattern
    cycle_position: CyclePosition


class TrendPredictor:
    """
    Analyzes historical patterns to identify trends and cycles.

    All analysis is based on Property Monitor transaction data.
    Forward-looking statements should be qualified in content.
    """

    MONTH_NAMES = [
        'January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December'
    ]

    def __init__(self, connection: duckdb.DuckDBPyConnection):
        self.con = connection

    def _get_period_end_date(self, year: int, period_type, period_number: int) -> str:
        """Get the end date for a given period."""
        # Handle both enum and string period types
        pt_value = period_type.value if hasattr(period_type, 'value') else str(period_type)

        if pt_value == 'monthly':
            # Last day of the month
            import calendar
            last_day = calendar.monthrange(year, period_number)[1]
            return f"{year}-{period_number:02d}-{last_day}"
        elif pt_value == 'quarterly':
            end_month = period_number * 3
            import calendar
            last_day = calendar.monthrange(year, end_month)[1]
            return f"{year}-{end_month:02d}-{last_day}"
        elif pt_value == 'semi_annual':
            end_month = period_number * 6
            import calendar
            last_day = calendar.monthrange(year, end_month)[1]
            return f"{year}-{end_month:02d}-{last_day}"
        else:  # annual
            return f"{year}-12-31"

    def analyze_period_trends(
        self,
        year: int,
        period_type,
        period_number: int,
        area_name: Optional[str] = None,
        property_type: Optional[str] = None
    ) -> TrendResults:
        """
        Analyze trends for a specific period (e.g., Q3 2024).

        Args:
            year: Year for the analysis
            period_type: PeriodType enum (monthly, quarterly, etc.)
            period_number: Period number within the year
            area_name: Optional area filter
            property_type: Optional property type filter

        Returns:
            TrendResults with trend analysis as of period end
        """
        as_of_date = self._get_period_end_date(year, period_type, period_number)
        return self.analyze_trends(area_name, property_type, as_of_date)

    def analyze_trends(
        self,
        area_name: Optional[str] = None,
        property_type: Optional[str] = None,
        as_of_date: Optional[str] = None
    ) -> TrendResults:
        """
        Run comprehensive trend analysis.

        Args:
            area_name: Optional area filter (None = market-wide)
            property_type: Optional property type filter
            as_of_date: Analysis date (default: latest data)

        Returns:
            TrendResults with all trend analysis
        """
        if as_of_date is None:
            as_of_date = datetime.now().strftime('%Y-%m-%d')

        scope = {
            'area': area_name,
            'property_type': property_type,
            'as_of_date': as_of_date
        }

        return TrendResults(
            analysis_date=as_of_date,
            scope=scope,
            price_momentum=self._calculate_price_momentum(area_name, property_type, as_of_date),
            volume_momentum=self._calculate_volume_momentum(area_name, property_type, as_of_date),
            seasonality=self._analyze_seasonality(area_name, property_type),
            cycle_position=self._assess_cycle_position(area_name, property_type, as_of_date)
        )

    def _build_filter_clause(
        self,
        area_name: Optional[str],
        property_type: Optional[str]
    ) -> str:
        """Build WHERE clause for area/property type filters."""
        clauses = []
        if area_name:
            clauses.append(f"area_name_en = '{area_name}'")
        if property_type:
            clauses.append(f"property_type_en = '{property_type}'")

        if clauses:
            return " AND " + " AND ".join(clauses)
        return ""

    def _calculate_price_momentum(
        self,
        area_name: Optional[str],
        property_type: Optional[str],
        as_of_date: str
    ) -> PriceMomentum:
        """
        Calculate price momentum over 3, 6, and 12 month windows.
        """
        filter_clause = self._build_filter_clause(area_name, property_type)

        # Get monthly average prices for last 13 months
        # Note: instance_date is VARCHAR in the database, so we cast it
        query = f"""
            WITH monthly_prices AS (
                SELECT
                    DATE_TRUNC('month', instance_date::DATE) as month,
                    AVG(actual_worth) as avg_price,
                    AVG(meter_sale_price) as avg_price_sqm,
                    COUNT(*) as tx_count
                FROM transactions_clean
                WHERE instance_date::DATE <= '{as_of_date}'::DATE
                  AND instance_date::DATE >= '{as_of_date}'::DATE - INTERVAL '13 months'
                  {filter_clause}
                GROUP BY DATE_TRUNC('month', instance_date::DATE)
                HAVING COUNT(*) >= 10
                ORDER BY month DESC
            )
            SELECT * FROM monthly_prices
        """

        df = self.con.execute(query).df()

        if df.empty or len(df) < 2:
            return PriceMomentum(
                area=area_name,
                property_type=property_type,
                current_avg_price=0,
                momentum_3m=None,
                momentum_6m=None,
                momentum_12m=None,
                trajectory=TrajectoryDirection.STABLE,
                consistency_score=0
            )

        current_price = float(df.iloc[0]['avg_price'])

        # Calculate momentum for each window
        momentum_3m = None
        momentum_6m = None
        momentum_12m = None

        if len(df) >= 4:  # Need at least 4 months for 3-month comparison
            price_3m_ago = float(df.iloc[3]['avg_price'])
            momentum_3m = ((current_price - price_3m_ago) / price_3m_ago) * 100

        if len(df) >= 7:
            price_6m_ago = float(df.iloc[6]['avg_price'])
            momentum_6m = ((current_price - price_6m_ago) / price_6m_ago) * 100

        if len(df) >= 13:
            price_12m_ago = float(df.iloc[12]['avg_price'])
            momentum_12m = ((current_price - price_12m_ago) / price_12m_ago) * 100

        # Determine trajectory
        trajectory, consistency = self._determine_trajectory(df)

        return PriceMomentum(
            area=area_name,
            property_type=property_type,
            current_avg_price=current_price,
            momentum_3m=momentum_3m,
            momentum_6m=momentum_6m,
            momentum_12m=momentum_12m,
            trajectory=trajectory,
            consistency_score=consistency
        )

    def _determine_trajectory(self, monthly_df) -> Tuple[TrajectoryDirection, float]:
        """
        Determine price trajectory direction from monthly data.

        Returns:
            Tuple of (trajectory direction, consistency score 0-1)
        """
        if len(monthly_df) < 4:
            return TrajectoryDirection.STABLE, 0.0

        # Calculate month-over-month changes
        prices = monthly_df['avg_price'].tolist()
        mom_changes = []
        for i in range(len(prices) - 1):
            if prices[i + 1] > 0:
                change = (prices[i] - prices[i + 1]) / prices[i + 1]
                mom_changes.append(change)

        if not mom_changes:
            return TrajectoryDirection.STABLE, 0.0

        # Count direction consistency
        positive_count = sum(1 for c in mom_changes if c > 0.01)  # >1% growth
        negative_count = sum(1 for c in mom_changes if c < -0.01)  # >1% decline
        neutral_count = len(mom_changes) - positive_count - negative_count

        total = len(mom_changes)

        # Check for acceleration (recent changes larger than older)
        recent_avg = sum(mom_changes[:3]) / min(3, len(mom_changes))
        older_avg = sum(mom_changes[3:6]) / max(1, min(3, len(mom_changes) - 3)) if len(mom_changes) > 3 else recent_avg

        if positive_count >= total * 0.7:
            if recent_avg > older_avg * 1.2:
                return TrajectoryDirection.ACCELERATING, positive_count / total
            return TrajectoryDirection.STABLE, positive_count / total
        elif negative_count >= total * 0.7:
            return TrajectoryDirection.DECELERATING, negative_count / total
        elif abs(positive_count - negative_count) / total > 0.5:
            return TrajectoryDirection.VOLATILE, 0.3
        else:
            return TrajectoryDirection.STABLE, neutral_count / total

    def _calculate_volume_momentum(
        self,
        area_name: Optional[str],
        property_type: Optional[str],
        as_of_date: str
    ) -> Dict[str, Any]:
        """Calculate transaction volume momentum."""
        filter_clause = self._build_filter_clause(area_name, property_type)

        query = f"""
            WITH monthly_volume AS (
                SELECT
                    DATE_TRUNC('month', instance_date::DATE) as month,
                    COUNT(*) as tx_count,
                    SUM(actual_worth) as total_value
                FROM transactions_clean
                WHERE instance_date::DATE <= '{as_of_date}'::DATE
                  AND instance_date::DATE >= '{as_of_date}'::DATE - INTERVAL '13 months'
                  {filter_clause}
                GROUP BY DATE_TRUNC('month', instance_date::DATE)
                ORDER BY month DESC
            )
            SELECT * FROM monthly_volume
        """

        df = self.con.execute(query).df()

        if df.empty or len(df) < 2:
            return {
                'current_monthly_volume': 0,
                'volume_3m_change': None,
                'volume_6m_change': None,
                'volume_12m_change': None,
                'trend': 'insufficient_data'
            }

        current_volume = int(df.iloc[0]['tx_count'])

        result = {
            'current_monthly_volume': current_volume,
            'volume_3m_change': None,
            'volume_6m_change': None,
            'volume_12m_change': None,
            'trend': 'stable'
        }

        if len(df) >= 4:
            vol_3m_ago = int(df.iloc[3]['tx_count'])
            if vol_3m_ago > 0:
                result['volume_3m_change'] = ((current_volume - vol_3m_ago) / vol_3m_ago) * 100

        if len(df) >= 7:
            vol_6m_ago = int(df.iloc[6]['tx_count'])
            if vol_6m_ago > 0:
                result['volume_6m_change'] = ((current_volume - vol_6m_ago) / vol_6m_ago) * 100

        if len(df) >= 13:
            vol_12m_ago = int(df.iloc[12]['tx_count'])
            if vol_12m_ago > 0:
                result['volume_12m_change'] = ((current_volume - vol_12m_ago) / vol_12m_ago) * 100

        # Determine trend
        if result['volume_3m_change'] and result['volume_3m_change'] > 15:
            result['trend'] = 'increasing'
        elif result['volume_3m_change'] and result['volume_3m_change'] < -15:
            result['trend'] = 'decreasing'

        return result

    def _analyze_seasonality(
        self,
        area_name: Optional[str],
        property_type: Optional[str]
    ) -> SeasonalityPattern:
        """
        Analyze seasonal patterns in transaction activity.

        Uses 2+ years of data to identify reliable patterns.
        """
        filter_clause = self._build_filter_clause(area_name, property_type)

        # Note: instance_date is VARCHAR, so we cast it to DATE
        query = f"""
            SELECT
                EXTRACT(MONTH FROM instance_date::DATE) as month_num,
                COUNT(*) as tx_count,
                AVG(actual_worth) as avg_price
            FROM transactions_clean
            WHERE instance_date::DATE >= '2023-01-01'::DATE
              {filter_clause}
            GROUP BY EXTRACT(MONTH FROM instance_date::DATE)
            ORDER BY month_num
        """

        df = self.con.execute(query).df()

        if df.empty or len(df) < 6:
            return SeasonalityPattern(
                area=area_name,
                peak_months=['March', 'October'],  # Default Dubai patterns
                low_months=['July', 'August'],
                peak_vs_low_ratio=1.3,
                monthly_indices={},
                current_month_position='normal'
            )

        # Calculate monthly indices (100 = average)
        avg_monthly = df['tx_count'].mean()
        monthly_indices = {}

        for _, row in df.iterrows():
            month_num = int(row['month_num'])
            month_name = self.MONTH_NAMES[month_num - 1]
            index = (row['tx_count'] / avg_monthly) * 100
            monthly_indices[month_name] = float(index)

        # Find peak and low months
        sorted_months = sorted(monthly_indices.items(), key=lambda x: x[1], reverse=True)
        peak_months = [m[0] for m in sorted_months[:3]]
        low_months = [m[0] for m in sorted_months[-3:]]

        # Peak vs low ratio
        peak_avg = sum(monthly_indices[m] for m in peak_months) / 3
        low_avg = sum(monthly_indices[m] for m in low_months) / 3
        peak_vs_low = peak_avg / low_avg if low_avg > 0 else 1.0

        # Current month position
        current_month = self.MONTH_NAMES[datetime.now().month - 1]
        current_index = monthly_indices.get(current_month, 100)
        if current_index >= 110:
            position = 'peak'
        elif current_index <= 90:
            position = 'low'
        else:
            position = 'normal'

        return SeasonalityPattern(
            area=area_name,
            peak_months=peak_months,
            low_months=low_months,
            peak_vs_low_ratio=peak_vs_low,
            monthly_indices=monthly_indices,
            current_month_position=position
        )

    def _assess_cycle_position(
        self,
        area_name: Optional[str],
        property_type: Optional[str],
        as_of_date: str
    ) -> CyclePosition:
        """
        Assess current position in market cycle.

        Uses multiple indicators:
        - Price momentum direction and strength
        - Volume trends
        - Historical comparison
        """
        filter_clause = self._build_filter_clause(area_name, property_type)

        # Get quarterly data for cycle analysis
        query = f"""
            WITH quarterly AS (
                SELECT
                    DATE_TRUNC('quarter', instance_date::DATE) as quarter,
                    COUNT(*) as tx_count,
                    AVG(actual_worth) as avg_price,
                    SUM(actual_worth) as total_value
                FROM transactions_clean
                WHERE instance_date::DATE <= '{as_of_date}'::DATE
                  AND instance_date::DATE >= '{as_of_date}'::DATE - INTERVAL '24 months'
                  {filter_clause}
                GROUP BY DATE_TRUNC('quarter', instance_date::DATE)
                ORDER BY quarter DESC
            )
            SELECT * FROM quarterly
        """

        df = self.con.execute(query).df()

        if df.empty or len(df) < 3:
            return CyclePosition(
                area=area_name,
                phase=MarketCyclePhase.STABLE,
                phase_confidence=0.3,
                supporting_indicators={'data_points': 'insufficient'},
                phase_duration_months=0
            )

        # Calculate quarter-over-quarter changes
        qoq_price_changes = []
        qoq_volume_changes = []

        for i in range(len(df) - 1):
            current = df.iloc[i]
            previous = df.iloc[i + 1]

            if previous['avg_price'] > 0:
                price_change = (current['avg_price'] - previous['avg_price']) / previous['avg_price']
                qoq_price_changes.append(price_change)

            if previous['tx_count'] > 0:
                volume_change = (current['tx_count'] - previous['tx_count']) / previous['tx_count']
                qoq_volume_changes.append(volume_change)

        # Determine phase based on patterns
        recent_price_trend = sum(qoq_price_changes[:2]) / min(2, len(qoq_price_changes)) if qoq_price_changes else 0
        recent_volume_trend = sum(qoq_volume_changes[:2]) / min(2, len(qoq_volume_changes)) if qoq_volume_changes else 0

        indicators = {
            'recent_price_trend_qoq': recent_price_trend * 100,
            'recent_volume_trend_qoq': recent_volume_trend * 100,
            'quarters_analyzed': len(df)
        }

        # Phase determination logic
        if recent_price_trend > 0.03 and recent_volume_trend > 0.05:
            phase = MarketCyclePhase.GROWTH
            confidence = min(0.9, 0.5 + abs(recent_price_trend) + abs(recent_volume_trend))
        elif recent_price_trend > 0.02 and recent_volume_trend < -0.05:
            phase = MarketCyclePhase.PEAK
            confidence = 0.6
        elif recent_price_trend < -0.02:
            phase = MarketCyclePhase.CORRECTION
            confidence = min(0.8, 0.5 + abs(recent_price_trend))
        elif recent_price_trend > 0 and recent_volume_trend > 0.1:
            phase = MarketCyclePhase.RECOVERY
            confidence = 0.6
        else:
            phase = MarketCyclePhase.STABLE
            confidence = 0.5

        # Estimate phase duration (simplified)
        consistent_quarters = 1
        for change in qoq_price_changes[1:]:
            if (change > 0 and recent_price_trend > 0) or (change < 0 and recent_price_trend < 0):
                consistent_quarters += 1
            else:
                break

        return CyclePosition(
            area=area_name,
            phase=phase,
            phase_confidence=confidence,
            supporting_indicators=indicators,
            phase_duration_months=consistent_quarters * 3
        )

    def format_for_prompt(self, results: TrendResults) -> str:
        """
        Format trend results for injection into content prompt.

        Returns:
            Formatted string ready for prompt injection
        """
        sections = []

        # Price momentum
        pm = results.price_momentum
        if pm.current_avg_price > 0:
            section = "**Price Momentum:**\n"
            if pm.momentum_3m is not None:
                section += f"- 3-month: {pm.momentum_3m:+.1f}%\n"
            if pm.momentum_6m is not None:
                section += f"- 6-month: {pm.momentum_6m:+.1f}%\n"
            if pm.momentum_12m is not None:
                section += f"- 12-month: {pm.momentum_12m:+.1f}%\n"
            section += f"- Trajectory: {pm.trajectory.value.title()} (consistency: {pm.consistency_score:.0%})\n"
            sections.append(section)

        # Volume momentum
        vm = results.volume_momentum
        if vm.get('current_monthly_volume', 0) > 0:
            section = "**Volume Trends:**\n"
            section += f"- Current monthly volume: {vm['current_monthly_volume']:,}\n"
            if vm.get('volume_3m_change') is not None:
                section += f"- 3-month change: {vm['volume_3m_change']:+.1f}%\n"
            section += f"- Trend: {vm.get('trend', 'stable').title()}\n"
            sections.append(section)

        # Seasonality
        season = results.seasonality
        if season.monthly_indices:
            section = "**Seasonality Pattern:**\n"
            section += f"- Peak activity months: {', '.join(season.peak_months)}\n"
            section += f"- Lower activity months: {', '.join(season.low_months)}\n"
            section += f"- Current position: {season.current_month_position.title()}\n"
            sections.append(section)

        # Cycle position
        cycle = results.cycle_position
        section = "**Market Cycle Position:**\n"
        section += f"- Phase: {cycle.phase.value.title()} (confidence: {cycle.phase_confidence:.0%})\n"
        if cycle.phase_duration_months > 0:
            section += f"- Duration in phase: ~{cycle.phase_duration_months} months\n"
        sections.append(section)

        if not sections:
            return "Insufficient data for trend analysis."

        return "\n".join(sections) + "\n\nNOTE: Forward-looking statements should be qualified with 'based on historical patterns'"
