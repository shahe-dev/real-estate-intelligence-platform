# src/analytics/report_calculator.py

"""
Real Estate Report Calculator - Pure Numerical Analysis
Performs all calculations for real estate reports WITHOUT using AI.

This module calculates comprehensive metrics from the Property Monitor DuckDB database
including period comparisons, market trends, and performance analytics.
"""

import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from enum import Enum
import calendar

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import duckdb
import pandas as pd
import numpy as np

from config.bigquery_settings import bq_settings


class PeriodType(Enum):
    """Period type enumeration for report calculations"""
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    SEMI_ANNUAL = "semi_annual"
    ANNUAL = "annual"


class ReportCalculator:
    """
    Calculate comprehensive real estate metrics from Property Monitor DuckDB database.

    This class provides pure numerical analysis without AI, supporting:
    - Period-based metrics (monthly, quarterly, semi-annual, annual)
    - Year-over-year and month-over-month comparisons
    - Market segmentation (luxury, off-plan, property types)
    - Top performers analysis
    - Statistical aggregations

    Database: data/database/property_monitor.db
    """

    def __init__(self, db_path: Optional[str] = None, read_only: bool = True):
        """
        Initialize calculator with database connection.

        Args:
            db_path: Path to DuckDB database. Defaults to PM_DB_PATH from settings.
            read_only: If True, opens database in read-only mode (default: True)
        """
        self.db_path = db_path or str(bq_settings.PM_DB_PATH)
        self.con = duckdb.connect(self.db_path, read_only=read_only)
        self._owns_connection = True

    def __del__(self):
        """Cleanup database connection"""
        if hasattr(self, '_owns_connection') and self._owns_connection:
            self.close()

    def close(self):
        """Close database connection"""
        if hasattr(self, 'con'):
            self.con.close()

    # =====================================================================
    # PERIOD CALCULATION HELPERS
    # =====================================================================

    def _get_period_dates(
        self,
        year: int,
        period_type: PeriodType,
        period_number: int
    ) -> Tuple[str, str]:
        """
        Calculate start and end dates for a given period.

        Args:
            year: Year for the period
            period_type: Type of period (monthly, quarterly, etc.)
            period_number: Period number within the year

        Returns:
            Tuple of (start_date, end_date) as ISO format strings

        Raises:
            ValueError: If period_number is invalid for the period_type
        """
        if period_type == PeriodType.MONTHLY:
            if not 1 <= period_number <= 12:
                raise ValueError(f"Monthly period_number must be 1-12, got {period_number}")
            start_month = period_number
            end_month = period_number
            start_day = 1
            end_day = calendar.monthrange(year, end_month)[1]

        elif period_type == PeriodType.QUARTERLY:
            if not 1 <= period_number <= 4:
                raise ValueError(f"Quarterly period_number must be 1-4, got {period_number}")
            start_month = (period_number - 1) * 3 + 1
            end_month = start_month + 2
            start_day = 1
            end_day = calendar.monthrange(year, end_month)[1]

        elif period_type == PeriodType.SEMI_ANNUAL:
            if not 1 <= period_number <= 2:
                raise ValueError(f"Semi-annual period_number must be 1-2, got {period_number}")
            start_month = (period_number - 1) * 6 + 1
            end_month = start_month + 5
            start_day = 1
            end_day = calendar.monthrange(year, end_month)[1]

        elif period_type == PeriodType.ANNUAL:
            if period_number != 1:
                raise ValueError(f"Annual period_number must be 1, got {period_number}")
            start_month = 1
            end_month = 12
            start_day = 1
            end_day = 31

        else:
            raise ValueError(f"Invalid period_type: {period_type}")

        start_date = f"{year}-{start_month:02d}-{start_day:02d}"
        end_date = f"{year}-{end_month:02d}-{end_day:02d}"

        return start_date, end_date

    def _get_previous_period(
        self,
        year: int,
        period_type: PeriodType,
        period_number: int
    ) -> Tuple[int, int]:
        """
        Get the previous period year and number.

        Args:
            year: Current year
            period_type: Type of period
            period_number: Current period number

        Returns:
            Tuple of (previous_year, previous_period_number)
        """
        if period_type == PeriodType.MONTHLY:
            if period_number == 1:
                return year - 1, 12
            return year, period_number - 1

        elif period_type == PeriodType.QUARTERLY:
            if period_number == 1:
                return year - 1, 4
            return year, period_number - 1

        elif period_type == PeriodType.SEMI_ANNUAL:
            if period_number == 1:
                return year - 1, 2
            return year, period_number - 1

        elif period_type == PeriodType.ANNUAL:
            return year - 1, 1

        raise ValueError(f"Invalid period_type: {period_type}")

    def _get_yoy_period(
        self,
        year: int,
        period_type: PeriodType,
        period_number: int
    ) -> Tuple[int, int]:
        """
        Get the year-over-year comparison period (same period, previous year).

        Args:
            year: Current year
            period_type: Type of period
            period_number: Current period number

        Returns:
            Tuple of (previous_year, same_period_number)
        """
        return year - 1, period_number

    # =====================================================================
    # CORE METRIC CALCULATIONS
    # =====================================================================

    def calculate_period_metrics(
        self,
        year: int,
        period_type: PeriodType,
        period_number: int,
        area_filter: Optional[str] = None,
        property_type_filter: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Calculate comprehensive metrics for a specific period.

        Args:
            year: Year for the period
            period_type: Type of period (monthly, quarterly, semi_annual, annual)
            period_number: Period number within the year
            area_filter: Optional area name to filter results
            property_type_filter: Optional property type to filter results

        Returns:
            Dictionary containing:
                - period_info: Period metadata (dates, type, etc.)
                - transaction_metrics: Count, volume, averages
                - price_metrics: Average, median, price per sqm
                - market_segments: Luxury, off-plan, property type breakdowns
                - statistical_metrics: Min, max, quartiles, std dev
        """
        start_date, end_date = self._get_period_dates(year, period_type, period_number)

        # Build filter clauses
        filters = [
            f"instance_date >= '{start_date}'",
            f"instance_date <= '{end_date}'"
        ]

        if area_filter:
            filters.append(f"area_name_en = '{area_filter}'")
        if property_type_filter:
            filters.append(f"property_type_en = '{property_type_filter}'")

        where_clause = " AND ".join(filters)

        # Main metrics query
        query = f"""
        SELECT
            -- Transaction counts
            COUNT(*) as total_transactions,
            COUNT(DISTINCT area_name_en) as unique_areas,
            COUNT(DISTINCT project_name_en) as unique_projects,
            COUNT(DISTINCT master_project_en) as unique_developers,

            -- Price metrics
            SUM(actual_worth) as total_sales_volume,
            AVG(actual_worth) as avg_price,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY actual_worth) as median_price,
            MIN(actual_worth) as min_price,
            MAX(actual_worth) as max_price,
            STDDEV(actual_worth) as price_stddev,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY actual_worth) as price_q1,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY actual_worth) as price_q3,

            -- Size metrics
            AVG(procedure_area) as avg_size_sqm,
            AVG(meter_sale_price) as avg_price_per_sqm,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY meter_sale_price) as median_price_per_sqm,

            -- Market segments
            SUM(CASE WHEN is_luxury THEN 1 ELSE 0 END) as luxury_count,
            SUM(CASE WHEN is_luxury THEN actual_worth ELSE 0 END) as luxury_volume,
            SUM(CASE WHEN reg_type_en = 'Off-Plan' THEN 1 ELSE 0 END) as offplan_count,
            SUM(CASE WHEN reg_type_en = 'Off-Plan' THEN actual_worth ELSE 0 END) as offplan_volume,
            SUM(CASE WHEN reg_type_en = 'Existing' THEN 1 ELSE 0 END) as ready_count,
            SUM(CASE WHEN reg_type_en = 'Existing' THEN actual_worth ELSE 0 END) as ready_volume

        FROM transactions_clean
        WHERE {where_clause}
        """

        result = self.con.execute(query).fetchone()

        if not result or result[0] == 0:
            return self._empty_period_metrics(year, period_type, period_number,
                                             start_date, end_date)

        # Parse results - unpack tuple carefully
        (total_transactions, unique_areas, unique_projects, unique_developers,
         total_sales_volume, avg_price, median_price, min_price, max_price, price_stddev,
         price_q1, price_q3, avg_size_sqm, avg_price_per_sqm, median_price_per_sqm,
         luxury_count, luxury_volume, offplan_count, offplan_volume,
         ready_count, ready_volume) = result

        total_tx = total_transactions
        total_volume = total_sales_volume or 0
        luxury_count = luxury_count or 0
        offplan_count = offplan_count or 0
        ready_count = ready_count or 0

        metrics = {
            'period_info': {
                'year': year,
                'period_type': period_type.value,
                'period_number': period_number,
                'start_date': start_date,
                'end_date': end_date,
                'area_filter': area_filter,
                'property_type_filter': property_type_filter
            },
            'transaction_metrics': {
                'total_transactions': total_tx,
                'unique_areas': unique_areas,
                'unique_projects': unique_projects,
                'unique_developers': unique_developers,
                'total_sales_volume': total_volume,
                'avg_transaction_size': total_volume / total_tx if total_tx > 0 else 0
            },
            'price_metrics': {
                'avg_price': avg_price,
                'median_price': median_price,
                'min_price': min_price,
                'max_price': max_price,
                'price_stddev': price_stddev,
                'price_q1': price_q1,
                'price_q3': price_q3,
                'avg_size_sqm': avg_size_sqm,
                'avg_price_per_sqm': avg_price_per_sqm,
                'median_price_per_sqm': median_price_per_sqm
            },
            'market_segments': {
                'luxury': {
                    'count': luxury_count,
                    'volume': luxury_volume or 0,
                    'percentage': (luxury_count / total_tx * 100) if total_tx > 0 else 0,
                    'avg_price': (luxury_volume / luxury_count) if luxury_count > 0 else 0
                },
                'offplan': {
                    'count': offplan_count,
                    'volume': offplan_volume or 0,
                    'percentage': (offplan_count / total_tx * 100) if total_tx > 0 else 0,
                    'avg_price': (offplan_volume / offplan_count) if offplan_count > 0 else 0
                },
                'ready': {
                    'count': ready_count,
                    'volume': ready_volume or 0,
                    'percentage': (ready_count / total_tx * 100) if total_tx > 0 else 0,
                    'avg_price': (ready_volume / ready_count) if ready_count > 0 else 0
                }
            },
            'statistical_metrics': {
                'price_range': max_price - min_price if max_price and min_price else 0,
                'price_iqr': price_q3 - price_q1 if price_q3 and price_q1 else 0,
                'coefficient_of_variation': (price_stddev / avg_price * 100) if avg_price and price_stddev else 0
            }
        }

        # Add property type distribution
        metrics['property_type_distribution'] = self._get_property_type_distribution(
            start_date, end_date, area_filter, property_type_filter
        )

        return metrics

    def _empty_period_metrics(
        self,
        year: int,
        period_type: PeriodType,
        period_number: int,
        start_date: str,
        end_date: str
    ) -> Dict[str, Any]:
        """Return empty metrics structure when no data available"""
        return {
            'period_info': {
                'year': year,
                'period_type': period_type.value,
                'period_number': period_number,
                'start_date': start_date,
                'end_date': end_date,
                'area_filter': None,
                'property_type_filter': None
            },
            'transaction_metrics': {
                'total_transactions': 0,
                'unique_areas': 0,
                'unique_projects': 0,
                'unique_developers': 0,
                'total_sales_volume': 0,
                'avg_transaction_size': 0
            },
            'price_metrics': {
                'avg_price': 0,
                'median_price': 0,
                'min_price': 0,
                'max_price': 0,
                'price_stddev': 0,
                'price_q1': 0,
                'price_q3': 0,
                'avg_size_sqm': 0,
                'avg_price_per_sqm': 0,
                'median_price_per_sqm': 0
            },
            'market_segments': {
                'luxury': {'count': 0, 'volume': 0, 'percentage': 0, 'avg_price': 0},
                'offplan': {'count': 0, 'volume': 0, 'percentage': 0, 'avg_price': 0},
                'ready': {'count': 0, 'volume': 0, 'percentage': 0, 'avg_price': 0}
            },
            'statistical_metrics': {
                'price_range': 0,
                'price_iqr': 0,
                'coefficient_of_variation': 0
            },
            'property_type_distribution': []
        }

    def _get_property_type_distribution(
        self,
        start_date: str,
        end_date: str,
        area_filter: Optional[str] = None,
        property_type_filter: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get property type distribution for a period"""
        filters = [
            f"instance_date >= '{start_date}'",
            f"instance_date <= '{end_date}'"
        ]

        if area_filter:
            filters.append(f"area_name_en = '{area_filter}'")
        if property_type_filter:
            filters.append(f"property_type_en = '{property_type_filter}'")

        where_clause = " AND ".join(filters)

        query = f"""
        SELECT
            property_type_en,
            COUNT(*) as count,
            SUM(actual_worth) as volume,
            AVG(actual_worth) as avg_price,
            AVG(meter_sale_price) as avg_price_sqm
        FROM transactions_clean
        WHERE {where_clause}
        GROUP BY property_type_en
        ORDER BY count DESC
        """

        results = self.con.execute(query).fetchall()

        # Calculate total for percentages
        total = sum(row[1] for row in results)

        return [
            {
                'property_type': row[0],
                'count': row[1],
                'volume': row[2],
                'percentage': (row[1] / total * 100) if total > 0 else 0,
                'avg_price': row[3],
                'avg_price_sqm': row[4]
            }
            for row in results
        ]

    # =====================================================================
    # COMPARISON CALCULATIONS
    # =====================================================================

    def get_comparison_metrics(
        self,
        current_year: int,
        current_period_type: PeriodType,
        current_period_number: int,
        comparison_type: str = "mom",  # "mom" or "yoy"
        area_filter: Optional[str] = None,
        property_type_filter: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Compare current period with previous period (MoM) or year-over-year (YoY).

        Args:
            current_year: Current period year
            current_period_type: Type of period
            current_period_number: Current period number
            comparison_type: "mom" for month-over-month or "yoy" for year-over-year
            area_filter: Optional area filter
            property_type_filter: Optional property type filter

        Returns:
            Dictionary with current metrics, comparison metrics, and change percentages
        """
        # Get current period metrics
        current_metrics = self.calculate_period_metrics(
            current_year, current_period_type, current_period_number,
            area_filter, property_type_filter
        )

        # Determine comparison period
        if comparison_type == "mom":
            comp_year, comp_period = self._get_previous_period(
                current_year, current_period_type, current_period_number
            )
        elif comparison_type == "yoy":
            comp_year, comp_period = self._get_yoy_period(
                current_year, current_period_type, current_period_number
            )
        else:
            raise ValueError(f"Invalid comparison_type: {comparison_type}. Use 'mom' or 'yoy'")

        # Get comparison period metrics
        comparison_metrics = self.calculate_period_metrics(
            comp_year, current_period_type, comp_period,
            area_filter, property_type_filter
        )

        # Calculate changes
        changes = self._calculate_metric_changes(current_metrics, comparison_metrics)

        return {
            'comparison_type': comparison_type,
            'current_period': current_metrics,
            'comparison_period': comparison_metrics,
            'changes': changes
        }

    def _calculate_metric_changes(
        self,
        current: Dict[str, Any],
        previous: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate percentage and absolute changes between two periods"""

        def pct_change(current_val, previous_val):
            """Calculate percentage change, handling edge cases"""
            if previous_val == 0 or previous_val is None:
                return None
            if current_val is None:
                return None
            return ((current_val - previous_val) / previous_val) * 100

        def abs_change(current_val, previous_val):
            """Calculate absolute change"""
            if current_val is None or previous_val is None:
                return None
            return current_val - previous_val

        curr_tx = current['transaction_metrics']
        prev_tx = previous['transaction_metrics']
        curr_price = current['price_metrics']
        prev_price = previous['price_metrics']
        curr_seg = current['market_segments']
        prev_seg = previous['market_segments']

        return {
            'transaction_changes': {
                'total_transactions': {
                    'absolute': abs_change(curr_tx['total_transactions'], prev_tx['total_transactions']),
                    'percentage': pct_change(curr_tx['total_transactions'], prev_tx['total_transactions'])
                },
                'total_sales_volume': {
                    'absolute': abs_change(curr_tx['total_sales_volume'], prev_tx['total_sales_volume']),
                    'percentage': pct_change(curr_tx['total_sales_volume'], prev_tx['total_sales_volume'])
                }
            },
            'price_changes': {
                'avg_price': {
                    'absolute': abs_change(curr_price['avg_price'], prev_price['avg_price']),
                    'percentage': pct_change(curr_price['avg_price'], prev_price['avg_price'])
                },
                'median_price': {
                    'absolute': abs_change(curr_price['median_price'], prev_price['median_price']),
                    'percentage': pct_change(curr_price['median_price'], prev_price['median_price'])
                },
                'avg_price_per_sqm': {
                    'absolute': abs_change(curr_price['avg_price_per_sqm'], prev_price['avg_price_per_sqm']),
                    'percentage': pct_change(curr_price['avg_price_per_sqm'], prev_price['avg_price_per_sqm'])
                }
            },
            'segment_changes': {
                'luxury_count': {
                    'absolute': abs_change(curr_seg['luxury']['count'], prev_seg['luxury']['count']),
                    'percentage': pct_change(curr_seg['luxury']['count'], prev_seg['luxury']['count'])
                },
                'offplan_count': {
                    'absolute': abs_change(curr_seg['offplan']['count'], prev_seg['offplan']['count']),
                    'percentage': pct_change(curr_seg['offplan']['count'], prev_seg['offplan']['count'])
                },
                'offplan_percentage': {
                    'absolute': abs_change(curr_seg['offplan']['percentage'], prev_seg['offplan']['percentage']),
                    'percentage': None  # Already a percentage
                }
            }
        }

    # =====================================================================
    # TOP PERFORMERS ANALYSIS
    # =====================================================================

    def get_top_performers(
        self,
        year: int,
        period_type: PeriodType,
        period_number: int,
        metric: str = "transaction_count",
        category: str = "areas",
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get top performers for a specific metric and category.

        Args:
            year: Period year
            period_type: Type of period
            period_number: Period number
            metric: Metric to rank by:
                - "transaction_count": Total transactions
                - "sales_volume": Total sales value
                - "avg_price": Average transaction price
                - "price_growth": Price growth vs previous period
            category: What to rank:
                - "areas": Geographic areas
                - "developers": Developer companies
                - "projects": Individual projects
                - "property_types": Property types
            limit: Number of top performers to return

        Returns:
            List of dictionaries with performer details and metrics
        """
        start_date, end_date = self._get_period_dates(year, period_type, period_number)

        # Build query based on category
        if category == "areas":
            group_field = "area_name_en"
            name_field = "area_name_en as name"
        elif category == "developers":
            group_field = "master_project_en"
            name_field = "master_project_en as name"
        elif category == "projects":
            group_field = "project_name_en"
            name_field = "project_name_en as name"
        elif category == "property_types":
            group_field = "property_type_en"
            name_field = "property_type_en as name"
        else:
            raise ValueError(f"Invalid category: {category}")

        # Build ORDER BY clause based on metric
        if metric == "transaction_count":
            order_by = "transaction_count DESC"
        elif metric == "sales_volume":
            order_by = "sales_volume DESC"
        elif metric == "avg_price":
            order_by = "avg_price DESC"
        elif metric == "price_growth":
            # For price growth, we need to calculate vs previous period
            return self._get_top_price_growth_performers(
                year, period_type, period_number, category, limit
            )
        else:
            raise ValueError(f"Invalid metric: {metric}")

        query = f"""
        SELECT
            {name_field},
            COUNT(*) as transaction_count,
            SUM(actual_worth) as sales_volume,
            AVG(actual_worth) as avg_price,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY actual_worth) as median_price,
            AVG(meter_sale_price) as avg_price_sqm,
            SUM(CASE WHEN is_luxury THEN 1 ELSE 0 END) as luxury_count,
            SUM(CASE WHEN reg_type_en = 'Off-Plan' THEN 1 ELSE 0 END) as offplan_count,
            COUNT(DISTINCT CASE WHEN {group_field} = 'area_name_en' THEN NULL ELSE project_name_en END) as unique_projects
        FROM transactions_clean
        WHERE instance_date >= '{start_date}'
          AND instance_date <= '{end_date}'
          AND {group_field} IS NOT NULL
          AND {group_field} != ''
        GROUP BY {group_field}
        HAVING COUNT(*) >= 5
        ORDER BY {order_by}
        LIMIT {limit}
        """

        results = self.con.execute(query).fetchall()

        return [
            {
                'rank': i + 1,
                'name': row[0],
                'transaction_count': row[1],
                'sales_volume': row[2],
                'avg_price': row[3],
                'median_price': row[4],
                'avg_price_sqm': row[5],
                'luxury_count': row[6],
                'luxury_percentage': (row[6] / row[1] * 100) if row[1] > 0 else 0,
                'offplan_count': row[7],
                'offplan_percentage': (row[7] / row[1] * 100) if row[1] > 0 else 0,
                'unique_projects': row[8] if category == "areas" else None
            }
            for i, row in enumerate(results)
        ]

    def _get_top_price_growth_performers(
        self,
        year: int,
        period_type: PeriodType,
        period_number: int,
        category: str,
        limit: int
    ) -> List[Dict[str, Any]]:
        """Get top performers by price growth"""
        start_date, end_date = self._get_period_dates(year, period_type, period_number)
        prev_year, prev_period = self._get_previous_period(year, period_type, period_number)
        prev_start_date, prev_end_date = self._get_period_dates(prev_year, period_type, prev_period)

        if category == "areas":
            group_field = "area_name_en"
        elif category == "developers":
            group_field = "master_project_en"
        elif category == "projects":
            group_field = "project_name_en"
        elif category == "property_types":
            group_field = "property_type_en"
        else:
            raise ValueError(f"Invalid category: {category}")

        query = f"""
        WITH current_period AS (
            SELECT
                {group_field} as name,
                COUNT(*) as current_count,
                AVG(actual_worth) as current_avg_price
            FROM transactions_clean
            WHERE instance_date >= '{start_date}'
              AND instance_date <= '{end_date}'
              AND {group_field} IS NOT NULL
              AND {group_field} != ''
            GROUP BY {group_field}
            HAVING COUNT(*) >= 5
        ),
        previous_period AS (
            SELECT
                {group_field} as name,
                COUNT(*) as previous_count,
                AVG(actual_worth) as previous_avg_price
            FROM transactions_clean
            WHERE instance_date >= '{prev_start_date}'
              AND instance_date <= '{prev_end_date}'
              AND {group_field} IS NOT NULL
              AND {group_field} != ''
            GROUP BY {group_field}
            HAVING COUNT(*) >= 5
        )
        SELECT
            c.name,
            c.current_count,
            c.current_avg_price,
            p.previous_avg_price,
            ((c.current_avg_price - p.previous_avg_price) / p.previous_avg_price * 100) as price_growth_pct,
            (c.current_avg_price - p.previous_avg_price) as price_growth_abs
        FROM current_period c
        INNER JOIN previous_period p ON c.name = p.name
        WHERE p.previous_avg_price > 0
        ORDER BY price_growth_pct DESC
        LIMIT {limit}
        """

        results = self.con.execute(query).fetchall()

        return [
            {
                'rank': i + 1,
                'name': row[0],
                'current_transaction_count': row[1],
                'current_avg_price': row[2],
                'previous_avg_price': row[3],
                'price_growth_percentage': row[4],
                'price_growth_absolute': row[5]
            }
            for i, row in enumerate(results)
        ]

    # =====================================================================
    # ADDITIONAL ANALYSIS METHODS
    # =====================================================================

    def get_area_summary(
        self,
        area_name: str,
        year: int,
        period_type: PeriodType,
        period_number: int
    ) -> Dict[str, Any]:
        """
        Get comprehensive summary for a specific area.

        Args:
            area_name: Name of the area
            year: Period year
            period_type: Type of period
            period_number: Period number

        Returns:
            Detailed area metrics including property type breakdown,
            top projects, and market positioning
        """
        # Get period metrics for the area
        metrics = self.calculate_period_metrics(
            year, period_type, period_number, area_filter=area_name
        )

        # Get top projects in this area
        start_date, end_date = self._get_period_dates(year, period_type, period_number)

        top_projects_query = f"""
        SELECT
            project_name_en,
            master_project_en as developer,
            COUNT(*) as transaction_count,
            AVG(actual_worth) as avg_price,
            AVG(meter_sale_price) as avg_price_sqm,
            SUM(CASE WHEN reg_type_en = 'Off-Plan' THEN 1 ELSE 0 END) as offplan_count
        FROM transactions_clean
        WHERE area_name_en = '{area_name}'
          AND instance_date >= '{start_date}'
          AND instance_date <= '{end_date}'
          AND project_name_en IS NOT NULL
          AND project_name_en != ''
        GROUP BY project_name_en, master_project_en
        HAVING COUNT(*) >= 3
        ORDER BY transaction_count DESC
        LIMIT 10
        """

        top_projects = self.con.execute(top_projects_query).fetchall()

        metrics['top_projects'] = [
            {
                'project_name': row[0],
                'developer': row[1],
                'transaction_count': row[2],
                'avg_price': row[3],
                'avg_price_sqm': row[4],
                'offplan_count': row[5],
                'offplan_percentage': (row[5] / row[2] * 100) if row[2] > 0 else 0
            }
            for row in top_projects
        ]

        return metrics

    def get_market_overview(
        self,
        year: int,
        period_type: PeriodType,
        period_number: int
    ) -> Dict[str, Any]:
        """
        Get overall market overview for a period.

        Args:
            year: Period year
            period_type: Type of period
            period_number: Period number

        Returns:
            Comprehensive market overview with all key metrics
        """
        metrics = self.calculate_period_metrics(year, period_type, period_number)

        # Add top performers across all categories
        metrics['top_areas'] = self.get_top_performers(
            year, period_type, period_number,
            metric="transaction_count", category="areas", limit=10
        )

        metrics['top_developers'] = self.get_top_performers(
            year, period_type, period_number,
            metric="transaction_count", category="developers", limit=10
        )

        metrics['top_projects'] = self.get_top_performers(
            year, period_type, period_number,
            metric="transaction_count", category="projects", limit=10
        )

        return metrics

    def get_time_series(
        self,
        start_year: int,
        start_period: int,
        end_year: int,
        end_period: int,
        period_type: PeriodType,
        area_filter: Optional[str] = None,
        property_type_filter: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get time series data across multiple periods.

        Args:
            start_year: Starting year
            start_period: Starting period number
            end_year: Ending year
            end_period: Ending period number
            period_type: Type of period
            area_filter: Optional area filter
            property_type_filter: Optional property type filter

        Returns:
            List of period metrics in chronological order
        """
        periods = []
        current_year = start_year
        current_period = start_period

        # Generate all periods in range
        while (current_year < end_year) or (current_year == end_year and current_period <= end_period):
            try:
                metrics = self.calculate_period_metrics(
                    current_year, period_type, current_period,
                    area_filter, property_type_filter
                )
                periods.append(metrics)
            except ValueError:
                # Invalid period, skip
                pass

            # Move to next period
            max_periods = {
                PeriodType.MONTHLY: 12,
                PeriodType.QUARTERLY: 4,
                PeriodType.SEMI_ANNUAL: 2,
                PeriodType.ANNUAL: 1
            }

            if current_period >= max_periods[period_type]:
                current_year += 1
                current_period = 1
            else:
                current_period += 1

        return periods


def example_usage():
    """Example usage of ReportCalculator"""
    calculator = ReportCalculator()

    try:
        # Example 1: Get monthly metrics
        print("=" * 80)
        print("EXAMPLE 1: Monthly Metrics for November 2024")
        print("=" * 80)

        monthly_metrics = calculator.calculate_period_metrics(
            year=2024,
            period_type=PeriodType.MONTHLY,
            period_number=11
        )

        print(f"\nTotal Transactions: {monthly_metrics['transaction_metrics']['total_transactions']:,}")
        print(f"Total Sales Volume: AED {monthly_metrics['transaction_metrics']['total_sales_volume']:,.2f}")
        print(f"Average Price: AED {monthly_metrics['price_metrics']['avg_price']:,.2f}")
        print(f"Median Price: AED {monthly_metrics['price_metrics']['median_price']:,.2f}")
        print(f"Luxury Count: {monthly_metrics['market_segments']['luxury']['count']:,} "
              f"({monthly_metrics['market_segments']['luxury']['percentage']:.1f}%)")

        # Example 2: Year-over-year comparison
        print("\n" + "=" * 80)
        print("EXAMPLE 2: Year-over-Year Comparison (Q4 2024 vs Q4 2023)")
        print("=" * 80)

        yoy_comparison = calculator.get_comparison_metrics(
            current_year=2024,
            current_period_type=PeriodType.QUARTERLY,
            current_period_number=4,
            comparison_type="yoy"
        )

        tx_change = yoy_comparison['changes']['transaction_changes']['total_transactions']
        price_change = yoy_comparison['changes']['price_changes']['avg_price']

        print(f"\nTransaction Count Change: {tx_change['absolute']:+,.0f} ({tx_change['percentage']:+.1f}%)")
        print(f"Average Price Change: AED {price_change['absolute']:+,.2f} ({price_change['percentage']:+.1f}%)")

        # Example 3: Top performing areas
        print("\n" + "=" * 80)
        print("EXAMPLE 3: Top 5 Areas by Transaction Count (November 2024)")
        print("=" * 80)

        top_areas = calculator.get_top_performers(
            year=2024,
            period_type=PeriodType.MONTHLY,
            period_number=11,
            metric="transaction_count",
            category="areas",
            limit=5
        )

        for area in top_areas:
            print(f"\n{area['rank']}. {area['name']}")
            print(f"   Transactions: {area['transaction_count']:,}")
            print(f"   Avg Price: AED {area['avg_price']:,.2f}")
            print(f"   Off-plan: {area['offplan_percentage']:.1f}%")

    finally:
        calculator.close()


if __name__ == "__main__":
    example_usage()
