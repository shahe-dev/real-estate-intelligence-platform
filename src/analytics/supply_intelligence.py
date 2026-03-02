"""
Supply Intelligence Module

Provides high-level supply intelligence methods for market analysis,
opportunity detection, and risk assessment using supply-demand correlation data.

Author: Dubai Real Estate Intelligence Team
Date: January 9, 2026
"""

from typing import Dict, List, Optional, Tuple
import duckdb
import pandas as pd
from dataclasses import dataclass
from datetime import datetime


@dataclass
class SupplyAlert:
    """Data class for supply-related alerts and insights"""
    area: str
    alert_type: str  # 'OVERSUPPLY', 'UNDERSUPPLY', 'OPPORTUNITY', 'RISK'
    severity: str  # 'HIGH', 'MODERATE', 'LOW'
    message: str
    metrics: Dict


class SupplyIntelligence:
    """
    Supply Intelligence Engine

    Provides market intelligence based on supply-demand correlation analysis.
    Integrates with existing analytics modules for comprehensive insights.
    """

    def __init__(self, db_path: str = 'data/database/property_monitor.db'):
        """
        Initialize Supply Intelligence engine

        Args:
            db_path: Path to DuckDB database
        """
        self.db_path = db_path

    def _get_connection(self, read_only: bool = True) -> duckdb.DuckDBPyConnection:
        """Get database connection"""
        return duckdb.connect(self.db_path, read_only=read_only)

    def detect_supply_saturation(
        self,
        threshold: float = 3.0,
        min_supply_units: int = 500
    ) -> pd.DataFrame:
        """
        Detect oversaturated markets based on supply-demand ratio

        Args:
            threshold: Supply-demand ratio threshold (default: 3.0)
            min_supply_units: Minimum off-plan units to consider

        Returns:
            DataFrame with oversaturated areas and risk metrics
        """
        con = self._get_connection()

        query = f"""
        SELECT
            area,
            market_balance,
            supply_offplan_units,
            demand_offplan_tx,
            supply_demand_ratio,
            oversupply_risk,
            price_yoy_change_pct,
            tx_yoy_change_pct,
            supply_developers,
            near_term_supply,

            -- Risk scoring
            CASE
                WHEN supply_demand_ratio > 10 AND price_yoy_change_pct < -10 THEN 'CRITICAL'
                WHEN supply_demand_ratio > 5 AND price_yoy_change_pct < -5 THEN 'HIGH'
                WHEN supply_demand_ratio > {threshold} THEN 'MODERATE'
                ELSE 'LOW'
            END as saturation_severity

        FROM metrics_supply_demand_area
        WHERE supply_demand_ratio > {threshold}
          AND supply_offplan_units >= {min_supply_units}
        ORDER BY supply_demand_ratio DESC
        """

        result = con.execute(query).df()
        con.close()

        return result

    def identify_emerging_hotspots(
        self,
        min_projects: int = 5,
        min_tx_growth: float = 20.0,
        max_sd_ratio: float = 3.0
    ) -> pd.DataFrame:
        """
        Identify emerging market hotspots

        Criteria:
        - New supply (5+ projects)
        - Growing demand (>20% YoY transaction growth)
        - Not oversupplied (SD ratio < 3.0)
        - Multiple reputable developers

        Args:
            min_projects: Minimum off-plan projects
            min_tx_growth: Minimum YoY transaction growth %
            max_sd_ratio: Maximum supply-demand ratio

        Returns:
            DataFrame with emerging hotspot areas
        """
        con = self._get_connection()

        query = f"""
        SELECT
            area,
            opportunity_classification,
            market_balance,
            supply_offplan_projects,
            supply_offplan_units,
            demand_offplan_tx,
            supply_demand_ratio,
            price_yoy_change_pct,
            tx_yoy_change_pct,
            supply_developers,
            near_term_supply,

            -- Growth momentum score
            ROUND(
                (tx_yoy_change_pct * 0.4) +
                (price_yoy_change_pct * 0.3) +
                (supply_developers * 2) +
                (CASE WHEN supply_demand_ratio < 1.0 THEN 20 ELSE 0 END)
            , 1) as momentum_score

        FROM metrics_supply_demand_area
        WHERE supply_offplan_projects >= {min_projects}
          AND tx_yoy_change_pct > {min_tx_growth}
          AND (supply_demand_ratio < {max_sd_ratio} OR supply_demand_ratio IS NULL)
          AND supply_developers >= 3
        ORDER BY momentum_score DESC
        """

        result = con.execute(query).df()
        con.close()

        return result

    def score_developer_reliability(
        self,
        developer: Optional[str] = None,
        min_completed_projects: int = 2
    ) -> pd.DataFrame:
        """
        Score developer reliability based on track record

        Args:
            developer: Specific developer name (None for all)
            min_completed_projects: Minimum completed projects to score

        Returns:
            DataFrame with developer reliability scores and metrics
        """
        con = self._get_connection()

        developer_filter = ""
        if developer:
            developer_filter = f"AND developer = '{developer}'"

        query = f"""
        SELECT
            developer,
            delivery_track_record,
            market_segment,

            -- Supply metrics
            supply_active_projects,
            supply_offplan_projects,
            supply_completed_projects,
            supply_offplan_units,

            -- Performance metrics
            supply_avg_completion_pct,
            completion_rate_pct,
            sales_velocity_pct,

            -- Diversification
            supply_areas_active,
            geographic_diversification_score,
            product_diversification_score,

            -- Demand metrics
            demand_transaction_count,
            demand_sales_volume,
            demand_avg_price,

            -- Composite reliability score (0-100)
            LEAST(100, GREATEST(0,
                CASE delivery_track_record
                    WHEN 'Highly Reliable' THEN 90
                    WHEN 'Reliable' THEN 75
                    WHEN 'Moderate' THEN 50
                    WHEN 'Unproven' THEN 25
                    ELSE 10
                END +
                (completion_rate_pct * 0.1) +
                (geographic_diversification_score * 0.5) -
                (CASE WHEN sales_velocity_pct < 5 THEN 10 ELSE 0 END)
            )) as reliability_score

        FROM metrics_developer_performance
        WHERE supply_completed_projects >= {min_completed_projects}
          {developer_filter}
        ORDER BY reliability_score DESC, supply_offplan_units DESC
        """

        result = con.execute(query).df()
        con.close()

        return result

    def forecast_delivery_waves(
        self,
        start_quarter: str = 'Q1 2026',
        quarters_ahead: int = 8
    ) -> pd.DataFrame:
        """
        Forecast upcoming delivery waves by quarter

        Args:
            start_quarter: Starting quarter (e.g., 'Q1 2026')
            quarters_ahead: Number of quarters to forecast

        Returns:
            DataFrame with quarterly delivery forecasts
        """
        con = self._get_connection()

        # Parse start quarter
        start_q = int(start_quarter[1])  # Quarter number (1-4)
        start_y = int(start_quarter[4:])  # Year

        query = f"""
        WITH sorted_quarters AS (
            SELECT
                delivery_quarter,
                projects_delivering,
                total_units_delivering,
                total_residential_units,
                total_commercial_units,
                areas_delivering,
                avg_completion_pct,
                top_area_by_units,
                CAST(SUBSTRING(delivery_quarter, 4) AS INTEGER) as year,
                CAST(SUBSTRING(delivery_quarter, 2, 1) AS INTEGER) as quarter
            FROM metrics_supply_quarterly
        )
        SELECT
            delivery_quarter,
            projects_delivering,
            total_units_delivering,
            total_residential_units,
            total_commercial_units,
            areas_delivering,
            avg_completion_pct,
            top_area_by_units,

            -- Supply wave intensity (relative to average)
            ROUND(
                total_units_delivering * 100.0 /
                AVG(total_units_delivering) OVER (),
                1
            ) as wave_intensity_pct

        FROM sorted_quarters
        WHERE (year > {start_y}) OR (year = {start_y} AND quarter >= {start_q})
        ORDER BY year, quarter
        LIMIT {quarters_ahead}
        """

        result = con.execute(query).df()
        con.close()

        return result

    def find_arbitrage_opportunities(
        self,
        min_price_growth: float = 10.0,
        max_supply_ratio: float = 2.0,
        max_developers: int = 10
    ) -> pd.DataFrame:
        """
        Find investment arbitrage opportunities

        Criteria:
        - High demand (strong price growth)
        - Limited supply (low SD ratio)
        - Low competition (few developers)

        Args:
            min_price_growth: Minimum YoY price growth %
            max_supply_ratio: Maximum supply-demand ratio
            max_developers: Maximum number of developers

        Returns:
            DataFrame with arbitrage opportunities ranked by potential
        """
        con = self._get_connection()

        query = f"""
        SELECT
            area,
            opportunity_classification,
            market_balance,
            supply_offplan_units,
            demand_offplan_tx,
            supply_demand_ratio,
            price_yoy_change_pct,
            demand_avg_price,
            demand_price_per_sqm,
            supply_developers,
            near_term_supply,

            -- Arbitrage potential score (0-100)
            LEAST(100, GREATEST(0,
                (price_yoy_change_pct * 1.5) +
                ((2.0 - COALESCE(supply_demand_ratio, 2.0)) * 20) +
                ((10 - supply_developers) * 3) +
                (CASE WHEN near_term_supply < 1000 THEN 15 ELSE 0 END)
            )) as arbitrage_score

        FROM metrics_supply_demand_area
        WHERE price_yoy_change_pct > {min_price_growth}
          AND (supply_demand_ratio < {max_supply_ratio} OR supply_demand_ratio IS NULL)
          AND supply_developers <= {max_developers}
          AND demand_offplan_tx > 10  -- Minimum liquidity
        ORDER BY arbitrage_score DESC
        """

        result = con.execute(query).df()
        con.close()

        return result

    def get_area_intelligence(self, area: str) -> Dict:
        """
        Get comprehensive supply-demand intelligence for specific area

        Args:
            area: Area name (master_development)

        Returns:
            Dictionary with comprehensive area intelligence
        """
        con = self._get_connection()

        # Get supply-demand metrics
        sd_metrics = con.execute(f"""
            SELECT * FROM metrics_supply_demand_area
            WHERE area = '{area}'
        """).fetchone()

        if not sd_metrics:
            con.close()
            return {"error": f"Area '{area}' not found in database"}

        # Get quarterly delivery forecast
        timeline = con.execute(f"""
            SELECT
                delivery_quarter,
                projects_delivering,
                units_delivering,
                active_developers
            FROM metrics_supply_timeline
            WHERE master_development = '{area}'
              AND delivery_quarter >= 'Q1 2026'
            ORDER BY delivery_quarter
            LIMIT 8
        """).df()

        # Get top developers in area
        developers = con.execute(f"""
            SELECT
                developer,
                COUNT(*) as projects,
                SUM(total_units) as units,
                AVG(completion_percentage) as avg_completion
            FROM supply_projects_clean
            WHERE master_development = '{area}'
              AND is_offplan = TRUE
            GROUP BY developer
            ORDER BY units DESC
            LIMIT 5
        """).df()

        # Get market opportunity score
        opportunity = con.execute(f"""
            SELECT
                opportunity_classification,
                opportunity_score,
                investment_timing
            FROM metrics_market_opportunities
            WHERE area = '{area}'
        """).fetchone()

        con.close()

        # Convert to column names (DuckDB returns tuples with column names in description)
        sd_cols = [
            'area', 'demand_tx_count', 'demand_volume_aed', 'demand_avg_price',
            'demand_price_per_sqm', 'demand_offplan_tx', 'demand_ready_tx',
            'price_yoy_change_pct', 'tx_yoy_change_pct', 'supply_projects',
            'supply_offplan_projects', 'supply_completed_projects',
            'supply_under_construction', 'supply_launched_projects',
            'supply_total_units', 'supply_offplan_units', 'supply_completed_units',
            'supply_residential_units', 'supply_2026', 'supply_2027', 'supply_2028',
            'supply_developers', 'supply_avg_completion_pct', 'supply_earliest_delivery',
            'supply_latest_delivery', 'supply_demand_ratio', 'market_balance',
            'tx_per_project', 'units_per_developer', 'near_term_supply',
            'opportunity_classification', 'oversupply_risk', 'price_decline_risk'
        ]

        return {
            "area": area,
            "supply_demand_metrics": dict(zip(sd_cols, sd_metrics)) if sd_metrics else {},
            "delivery_timeline": timeline.to_dict('records') if not timeline.empty else [],
            "top_developers": developers.to_dict('records') if not developers.empty else [],
            "opportunity": {
                "classification": opportunity[0] if opportunity else None,
                "score": opportunity[1] if opportunity else None,
                "timing": opportunity[2] if opportunity else None
            },
            "generated_at": datetime.now().isoformat()
        }

    def generate_market_alerts(
        self,
        saturation_threshold: float = 3.0,
        price_decline_threshold: float = -10.0
    ) -> List[SupplyAlert]:
        """
        Generate market alerts for oversupply, undersupply, and opportunities

        Args:
            saturation_threshold: SD ratio threshold for oversupply alerts
            price_decline_threshold: Price decline % for risk alerts

        Returns:
            List of SupplyAlert objects
        """
        con = self._get_connection()
        alerts = []

        # Oversupply alerts
        oversupply = con.execute(f"""
            SELECT
                area,
                supply_offplan_units,
                demand_offplan_tx,
                supply_demand_ratio,
                price_yoy_change_pct
            FROM metrics_supply_demand_area
            WHERE supply_demand_ratio > {saturation_threshold}
              AND supply_offplan_units > 500
            ORDER BY supply_demand_ratio DESC
            LIMIT 10
        """).fetchall()

        for row in oversupply:
            severity = 'HIGH' if row[3] > 10 else 'MODERATE'
            alerts.append(SupplyAlert(
                area=row[0],
                alert_type='OVERSUPPLY',
                severity=severity,
                message=f"{row[0]}: {int(row[1])} units for {int(row[2])} transactions (ratio: {row[3]:.1f})",
                metrics={
                    'supply_units': row[1],
                    'demand_tx': row[2],
                    'sd_ratio': row[3],
                    'price_yoy': row[4]
                }
            ))

        # High opportunity alerts
        opportunities = con.execute("""
            SELECT
                area,
                opportunity_score,
                supply_offplan_units,
                demand_offplan_tx,
                price_yoy_change_pct
            FROM metrics_market_opportunities
            WHERE opportunity_classification IN ('High Opportunity', 'Moderate Opportunity')
            ORDER BY opportunity_score DESC
            LIMIT 5
        """).fetchall()

        for row in opportunities:
            alerts.append(SupplyAlert(
                area=row[0],
                alert_type='OPPORTUNITY',
                severity='HIGH' if row[1] > 80 else 'MODERATE',
                message=f"{row[0]}: High opportunity (score: {row[1]:.0f}) - {int(row[2])} units, {int(row[3])} transactions",
                metrics={
                    'opportunity_score': row[1],
                    'supply_units': row[2],
                    'demand_tx': row[3],
                    'price_yoy': row[4]
                }
            ))

        # Price decline risk alerts
        price_risks = con.execute(f"""
            SELECT
                area,
                price_yoy_change_pct,
                supply_offplan_units,
                supply_demand_ratio
            FROM metrics_supply_demand_area
            WHERE price_yoy_change_pct < {price_decline_threshold}
              AND demand_offplan_tx > 20
            ORDER BY price_yoy_change_pct
            LIMIT 5
        """).fetchall()

        for row in price_risks:
            alerts.append(SupplyAlert(
                area=row[0],
                alert_type='RISK',
                severity='HIGH',
                message=f"{row[0]}: Price declining {row[1]:.1f}% YoY with {int(row[2])} units in pipeline",
                metrics={
                    'price_yoy': row[1],
                    'supply_units': row[2],
                    'sd_ratio': row[3]
                }
            ))

        con.close()
        return alerts


# Convenience functions for common use cases
def get_oversupplied_areas(threshold: float = 3.0) -> pd.DataFrame:
    """Quick access to oversupplied areas"""
    si = SupplyIntelligence()
    return si.detect_supply_saturation(threshold=threshold)


def get_emerging_hotspots() -> pd.DataFrame:
    """Quick access to emerging market hotspots"""
    si = SupplyIntelligence()
    return si.identify_emerging_hotspots()


def get_reliable_developers(min_projects: int = 5) -> pd.DataFrame:
    """Quick access to reliable developers"""
    si = SupplyIntelligence()
    return si.score_developer_reliability(min_completed_projects=min_projects)


def get_delivery_forecast(quarters: int = 8) -> pd.DataFrame:
    """Quick access to delivery forecast"""
    si = SupplyIntelligence()
    return si.forecast_delivery_waves(quarters_ahead=quarters)
