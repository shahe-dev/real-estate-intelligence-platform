# src/visualization/agents/data_analyst.py
"""
DataAnalystAgent - Transforms raw metrics into chart-ready data structures.

Responsible for:
- Querying metrics from the database
- Transforming data for specific chart types
- Calculating derived metrics (percentages, changes, rankings)
- Preparing time-series data with proper labels
"""

import duckdb
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
from datetime import datetime
from enum import Enum


class MetricType(Enum):
    """Types of metrics for chart generation."""
    TRANSACTION_VOLUME = "transaction_volume"
    TRANSACTION_COUNT = "transaction_count"
    AVERAGE_PRICE = "average_price"
    PRICE_PER_SQM = "price_per_sqm"
    MARKET_SHARE = "market_share"
    SEGMENT_DISTRIBUTION = "segment_distribution"


class DataAnalystAgent:
    """
    Agent for preparing chart-ready data from database metrics.

    Transforms raw database queries into structured data suitable
    for visualization by the chart classes.
    """

    def __init__(self, db_path: str = None):
        """Initialize with database connection."""
        if db_path is None:
            base_dir = Path(__file__).parent.parent.parent.parent
            db_path = base_dir / "data" / "database" / "property_monitor.db"

        self.db_path = str(db_path)
        self.conn = None
        self._connect()

    def _connect(self):
        """Establish database connection."""
        try:
            self.conn = duckdb.connect(self.db_path, read_only=True)
        except Exception as e:
            print(f"Warning: Could not connect to database: {e}")
            self.conn = None

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def prepare_monthly_trend(
        self,
        year: int,
        metric: str = 'transaction_count',
        segment: str = None
    ) -> Dict[str, Any]:
        """
        Prepare monthly trend data for line charts.

        Args:
            year: Year to analyze
            metric: 'transaction_count', 'total_volume', or 'avg_price'
            segment: Optional filter ('luxury', 'offplan', etc.)

        Returns:
            Chart-ready data with labels and values
        """
        if not self.conn:
            return self._empty_trend_data()

        # Build query based on segment (DuckDB syntax with VARCHAR date cast)
        where_clause = f"WHERE YEAR(instance_date::DATE) = {year}"

        if segment == 'luxury':
            where_clause += " AND is_luxury = TRUE"
        elif segment == 'offplan':
            where_clause += " AND reg_type_en = 'Off-Plan'"
        elif segment == 'ready':
            where_clause += " AND reg_type_en = 'Existing'"

        query = f"""
            SELECT
                MONTH(instance_date::DATE) as month,
                COUNT(*) as transaction_count,
                SUM(actual_worth) as total_volume,
                AVG(actual_worth) as avg_price
            FROM transactions_all
            {where_clause}
            GROUP BY MONTH(instance_date::DATE)
            ORDER BY month
        """

        try:
            result = self.conn.execute(query).fetchall()

            # Map month numbers to names
            month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                          'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

            # Get column names for mapping
            col_names = ['month', 'transaction_count', 'total_volume', 'avg_price']
            metric_idx = col_names.index(metric) if metric in col_names else 1

            labels = []
            values = []

            for row in result:
                month_idx = int(row[0]) - 1
                labels.append(month_names[month_idx])
                values.append(row[metric_idx] or 0)

            return {
                'labels': labels,
                'values': values,
                'name': self._format_metric_name(metric),
                'year': year,
                'segment': segment
            }

        except Exception as e:
            print(f"Error preparing monthly trend: {e}")
            return self._empty_trend_data()

    def prepare_quarterly_comparison(
        self,
        year: int,
        quarter: int,
        metric: str = 'transaction_count'
    ) -> Dict[str, Any]:
        """
        Prepare quarter-over-quarter comparison data.

        Returns current quarter vs previous quarter data.
        """
        if not self.conn:
            return self._empty_comparison_data()

        # Get current and previous quarter date ranges
        current_start, current_end = self._get_quarter_dates(year, quarter)
        prev_year, prev_quarter = (year, quarter - 1) if quarter > 1 else (year - 1, 4)
        prev_start, prev_end = self._get_quarter_dates(prev_year, prev_quarter)

        col_names = ['month', 'transaction_count', 'total_volume', 'avg_price']
        metric_idx = col_names.index(metric) if metric in col_names else 1

        def run_query(start, end):
            query = f"""
                SELECT
                    MONTH(instance_date::DATE) as month,
                    COUNT(*) as transaction_count,
                    SUM(actual_worth) as total_volume,
                    AVG(actual_worth) as avg_price
                FROM transactions_all
                WHERE instance_date::DATE BETWEEN '{start}'::DATE AND '{end}'::DATE
                GROUP BY MONTH(instance_date::DATE)
                ORDER BY month
            """
            return self.conn.execute(query).fetchall()

        try:
            current_rows = run_query(current_start, current_end)
            prev_rows = run_query(prev_start, prev_end)

            month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                          'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

            # Map to labels
            current_data = {'labels': [], 'values': []}
            for row in current_rows:
                month_idx = int(row[0]) - 1
                current_data['labels'].append(month_names[month_idx])
                current_data['values'].append(row[metric_idx] or 0)

            prev_data = {'labels': [], 'values': []}
            for row in prev_rows:
                month_idx = int(row[0]) - 1
                prev_data['labels'].append(month_names[month_idx])
                prev_data['values'].append(row[metric_idx] or 0)

            return {
                'current': current_data,
                'previous': prev_data,
                'current_label': f'Q{quarter} {year}',
                'previous_label': f'Q{prev_quarter} {prev_year}',
            }

        except Exception as e:
            print(f"Error preparing quarterly comparison: {e}")
            return self._empty_comparison_data()

    def prepare_top_areas(
        self,
        year: int = None,
        quarter: int = None,
        metric: str = 'transaction_count',
        limit: int = 10,
        segment: str = None
    ) -> Dict[str, Any]:
        """
        Prepare top areas data for horizontal bar charts.

        Args:
            year: Year filter
            quarter: Quarter filter (optional)
            metric: Ranking metric
            limit: Number of top areas
            segment: Optional segment filter

        Returns:
            Chart-ready data with area names and values
        """
        if not self.conn:
            return self._empty_ranking_data()

        where_clauses = ["area_name_en IS NOT NULL"]

        if year:
            if quarter:
                start, end = self._get_quarter_dates(year, quarter)
                where_clauses.append(f"instance_date::DATE BETWEEN '{start}'::DATE AND '{end}'::DATE")
            else:
                where_clauses.append(f"YEAR(instance_date::DATE) = {year}")

        if segment == 'luxury':
            where_clauses.append("is_luxury = TRUE")
        elif segment == 'offplan':
            where_clauses.append("off_plan = TRUE")

        where_clause = " AND ".join(where_clauses)

        query = f"""
            SELECT
                area_name_en,
                COUNT(*) as transaction_count,
                SUM(actual_worth) as total_volume,
                AVG(actual_worth) as avg_price
            FROM transactions_all
            WHERE {where_clause}
            GROUP BY area_name_en
            ORDER BY {metric} DESC
            LIMIT {limit}
        """

        try:
            result = self.conn.execute(query).fetchall()
            col_names = ['area_name_en', 'transaction_count', 'total_volume', 'avg_price']
            metric_idx = col_names.index(metric) if metric in col_names else 1

            return {
                'labels': [row[0] for row in result],
                'values': [row[metric_idx] or 0 for row in result],
                'metric': metric,
                'metric_label': self._format_metric_name(metric)
            }

        except Exception as e:
            print(f"Error preparing top areas: {e}")
            return self._empty_ranking_data()

    def prepare_top_developers(
        self,
        year: int = None,
        quarter: int = None,
        metric: str = 'transaction_count',
        limit: int = 10,
        segment: str = None
    ) -> Dict[str, Any]:
        """Prepare top developers data for horizontal bar charts."""
        if not self.conn:
            return self._empty_ranking_data()

        where_clauses = ["nearest_landmark_en IS NOT NULL", "nearest_landmark_en != ''"]

        if year:
            if quarter:
                start, end = self._get_quarter_dates(year, quarter)
                where_clauses.append(f"instance_date::DATE BETWEEN '{start}'::DATE AND '{end}'::DATE")
            else:
                where_clauses.append(f"YEAR(instance_date::DATE) = {year}")

        if segment == 'luxury':
            where_clauses.append("is_luxury = TRUE")
        elif segment == 'offplan':
            where_clauses.append("off_plan = TRUE")

        where_clause = " AND ".join(where_clauses)

        query = f"""
            SELECT
                nearest_landmark_en as developer,
                COUNT(*) as transaction_count,
                SUM(actual_worth) as total_volume,
                AVG(actual_worth) as avg_price
            FROM transactions_all
            WHERE {where_clause}
            GROUP BY nearest_landmark_en
            ORDER BY {metric} DESC
            LIMIT {limit}
        """

        try:
            result = self.conn.execute(query).fetchall()
            col_names = ['developer', 'transaction_count', 'total_volume', 'avg_price']
            metric_idx = col_names.index(metric) if metric in col_names else 1

            return {
                'labels': [row[0] for row in result],
                'values': [row[metric_idx] or 0 for row in result],
                'metric': metric,
                'metric_label': self._format_metric_name(metric)
            }

        except Exception as e:
            print(f"Error preparing top developers: {e}")
            return self._empty_ranking_data()

    def prepare_market_segments(
        self,
        year: int = None,
        quarter: int = None,
        segment_type: str = 'offplan_ready'
    ) -> Dict[str, Any]:
        """
        Prepare market segment distribution for pie charts.

        Args:
            year: Year filter
            quarter: Quarter filter
            segment_type: 'offplan_ready', 'luxury_tiers', or 'property_types'

        Returns:
            Chart-ready data with labels and values
        """
        if not self.conn:
            return self._empty_segment_data()

        where_clauses = []

        if year:
            if quarter:
                start, end = self._get_quarter_dates(year, quarter)
                where_clauses.append(f"instance_date::DATE BETWEEN '{start}'::DATE AND '{end}'::DATE")
            else:
                where_clauses.append(f"YEAR(instance_date::DATE) = {year}")

        where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        if segment_type == 'offplan_ready':
            query = f"""
                SELECT
                    CASE WHEN off_plan = TRUE THEN 'Off-Plan' ELSE 'Ready' END as segment,
                    COUNT(*) as count
                FROM transactions_all
                {where_clause}
                GROUP BY off_plan
            """
        elif segment_type == 'luxury_tiers':
            query = f"""
                SELECT
                    CASE
                        WHEN actual_worth >= 10000000 THEN 'Ultra Luxury (10M+)'
                        WHEN actual_worth >= 5000000 THEN 'Luxury (5-10M)'
                        WHEN actual_worth >= 2000000 THEN 'Mid-Range (2-5M)'
                        ELSE 'Standard (<2M)'
                    END as segment,
                    COUNT(*) as count
                FROM transactions_all
                {where_clause}
                GROUP BY segment
                ORDER BY
                    CASE segment
                        WHEN 'Ultra Luxury (10M+)' THEN 1
                        WHEN 'Luxury (5-10M)' THEN 2
                        WHEN 'Mid-Range (2-5M)' THEN 3
                        ELSE 4
                    END
            """
        elif segment_type == 'property_types':
            query = f"""
                SELECT
                    property_type_en as segment,
                    COUNT(*) as count
                FROM transactions_all
                {where_clause}
                GROUP BY property_type_en
                ORDER BY count DESC
            """
        else:
            return self._empty_segment_data()

        try:
            result = self.conn.execute(query).fetchall()

            return {
                'labels': [row[0] for row in result if row[0]],
                'values': [row[1] for row in result if row[0]],
                'segment_type': segment_type
            }

        except Exception as e:
            print(f"Error preparing market segments: {e}")
            return self._empty_segment_data()

    def prepare_price_distribution(
        self,
        year: int = None,
        quarter: int = None,
        bins: int = 10
    ) -> Dict[str, Any]:
        """Prepare price distribution data for histogram-style charts."""
        if not self.conn:
            return self._empty_distribution_data()

        where_clauses = ["actual_worth > 0"]

        if year:
            if quarter:
                start, end = self._get_quarter_dates(year, quarter)
                where_clauses.append(f"instance_date::DATE BETWEEN '{start}'::DATE AND '{end}'::DATE")
            else:
                where_clauses.append(f"YEAR(instance_date::DATE) = {year}")

        where_clause = " AND ".join(where_clauses)

        query = f"""
            SELECT actual_worth
            FROM transactions_all
            WHERE {where_clause}
        """

        try:
            result = self.conn.execute(query).fetchall()
            prices = [row[0] for row in result]

            if not prices:
                return self._empty_distribution_data()

            import numpy as np
            counts, bin_edges = np.histogram(prices, bins=bins)

            labels = []
            for i in range(len(bin_edges) - 1):
                label = f"{bin_edges[i]/1e6:.1f}M-{bin_edges[i+1]/1e6:.1f}M"
                labels.append(label)

            return {
                'labels': labels,
                'values': counts.tolist(),
                'bin_edges': bin_edges.tolist()
            }

        except Exception as e:
            print(f"Error preparing price distribution: {e}")
            return self._empty_distribution_data()

    # Helper methods

    def _get_quarter_dates(self, year: int, quarter: int) -> Tuple[str, str]:
        """Get start and end dates for a quarter."""
        quarter_starts = {1: '01-01', 2: '04-01', 3: '07-01', 4: '10-01'}
        quarter_ends = {1: '03-31', 2: '06-30', 3: '09-30', 4: '12-31'}
        return f"{year}-{quarter_starts[quarter]}", f"{year}-{quarter_ends[quarter]}"

    def _format_metric_name(self, metric: str) -> str:
        """Format metric name for display."""
        names = {
            'transaction_count': 'Transaction Count',
            'total_volume': 'Total Volume (AED)',
            'avg_price': 'Average Price (AED)',
        }
        return names.get(metric, metric.replace('_', ' ').title())

    def _empty_trend_data(self) -> Dict[str, Any]:
        return {'labels': [], 'values': [], 'name': '', 'year': None}

    def _empty_comparison_data(self) -> Dict[str, Any]:
        return {'current': {'labels': [], 'values': []}, 'previous': {'labels': [], 'values': []}}

    def _empty_ranking_data(self) -> Dict[str, Any]:
        return {'labels': [], 'values': [], 'metric': '', 'metric_label': ''}

    def _empty_segment_data(self) -> Dict[str, Any]:
        return {'labels': [], 'values': [], 'segment_type': ''}

    def _empty_distribution_data(self) -> Dict[str, Any]:
        return {'labels': [], 'values': [], 'bin_edges': []}
