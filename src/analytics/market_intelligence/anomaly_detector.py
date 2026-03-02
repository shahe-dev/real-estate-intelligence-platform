# src/analytics/market_intelligence/anomaly_detector.py
"""
Anomaly Detector - Identifies newsworthy data points

Detects:
- Record-breaking transactions in a period
- Volume spikes vs historical average
- Price anomalies (significantly above/below area average)
- New developer market entries
- Developer activity surges
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
import duckdb


@dataclass
class RecordTransaction:
    """A record-breaking transaction"""
    transaction_id: str
    area: str
    price: float
    property_type: str
    rooms: str
    date: str
    developer: Optional[str]
    rank: int


@dataclass
class VolumeSpike:
    """An area with unusual transaction volume"""
    area: str
    current_volume: int
    historical_avg: float
    spike_percentage: float
    current_value: float


@dataclass
class NewDeveloper:
    """A developer with recent market entry"""
    developer: str
    first_sale_date: str
    transaction_count: int
    total_value: float
    areas_active: int


@dataclass
class AnomalyResults:
    """Complete anomaly detection results"""
    period_info: Dict[str, Any]
    record_transactions: List[RecordTransaction]
    ultra_luxury_records: List[RecordTransaction]
    volume_spikes: List[VolumeSpike]
    volume_drops: List[VolumeSpike]
    new_developers: List[NewDeveloper]
    developer_surges: List[Dict[str, Any]]


class AnomalyDetector:
    """
    Detects anomalies and newsworthy data points in transaction data.

    All calculations are based on Property Monitor transaction data.
    """

    def __init__(self, connection: duckdb.DuckDBPyConnection):
        self.con = connection

    def detect_anomalies(
        self,
        year: int,
        period_type: str,
        period_number: int,
        lookback_months: int = 6
    ) -> AnomalyResults:
        """
        Run all anomaly detection for a given period.

        Args:
            year: Year to analyze
            period_type: 'monthly', 'quarterly', 'semi_annual', 'annual'
            period_number: Period number within the year
            lookback_months: Months to use for historical comparison

        Returns:
            AnomalyResults with all detected anomalies
        """
        start_date, end_date = self._get_period_dates(year, period_type, period_number)

        period_info = {
            'year': year,
            'period_type': period_type,
            'period_number': period_number,
            'start_date': start_date,
            'end_date': end_date,
            'lookback_months': lookback_months
        }

        return AnomalyResults(
            period_info=period_info,
            record_transactions=self._get_record_transactions(start_date, end_date, limit=10),
            ultra_luxury_records=self._get_ultra_luxury_records(start_date, end_date, limit=5),
            volume_spikes=self._get_volume_spikes(start_date, end_date, lookback_months),
            volume_drops=self._get_volume_drops(start_date, end_date, lookback_months),
            new_developers=self._get_new_developers(start_date, end_date, lookback_months),
            developer_surges=self._get_developer_surges(start_date, end_date, lookback_months)
        )

    def _get_period_dates(self, year: int, period_type: str, period_number: int) -> tuple:
        """Convert period to date range."""
        if period_type == 'monthly':
            start_date = f"{year}-{period_number:02d}-01"
            if period_number == 12:
                end_date = f"{year}-12-31"
            else:
                # Last day of month
                next_month = period_number + 1
                end_date = f"{year}-{next_month:02d}-01"
                # Subtract one day (handled in query with <)
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

    def _get_record_transactions(
        self,
        start_date: str,
        end_date: str,
        limit: int = 10
    ) -> List[RecordTransaction]:
        """Get the highest-value transactions in the period."""
        query = f"""
            SELECT
                transaction_id,
                area_name_en,
                actual_worth,
                property_type_en,
                rooms_en,
                instance_date,
                master_project_en
            FROM transactions_clean
            WHERE instance_date >= '{start_date}'
              AND instance_date < '{end_date}'
            ORDER BY actual_worth DESC
            LIMIT {limit}
        """

        df = self.con.execute(query).df()

        records = []
        for i, row in df.iterrows():
            records.append(RecordTransaction(
                transaction_id=str(row['transaction_id']),
                area=row['area_name_en'],
                price=float(row['actual_worth']),
                property_type=row['property_type_en'],
                rooms=row['rooms_en'] or 'N/A',
                date=str(row['instance_date']),
                developer=row['master_project_en'] if row['master_project_en'] else None,
                rank=i + 1
            ))

        return records

    def _get_ultra_luxury_records(
        self,
        start_date: str,
        end_date: str,
        limit: int = 5,
        threshold: float = 10_000_000
    ) -> List[RecordTransaction]:
        """Get ultra-luxury (10M+) record transactions."""
        query = f"""
            SELECT
                transaction_id,
                area_name_en,
                actual_worth,
                property_type_en,
                rooms_en,
                instance_date,
                master_project_en
            FROM transactions_clean
            WHERE instance_date >= '{start_date}'
              AND instance_date < '{end_date}'
              AND actual_worth >= {threshold}
            ORDER BY actual_worth DESC
            LIMIT {limit}
        """

        df = self.con.execute(query).df()

        records = []
        for i, row in df.iterrows():
            records.append(RecordTransaction(
                transaction_id=str(row['transaction_id']),
                area=row['area_name_en'],
                price=float(row['actual_worth']),
                property_type=row['property_type_en'],
                rooms=row['rooms_en'] or 'N/A',
                date=str(row['instance_date']),
                developer=row['master_project_en'] if row['master_project_en'] else None,
                rank=i + 1
            ))

        return records

    def _get_volume_spikes(
        self,
        start_date: str,
        end_date: str,
        lookback_months: int,
        spike_threshold: float = 30.0
    ) -> List[VolumeSpike]:
        """
        Find areas with transaction volume significantly above historical average.

        Args:
            spike_threshold: Minimum % above average to be considered a spike
        """
        # Calculate lookback start date
        lookback_start = self._subtract_months(start_date, lookback_months)

        query = f"""
            WITH current_period AS (
                SELECT
                    area_name_en,
                    COUNT(*) as current_volume,
                    SUM(actual_worth) as current_value
                FROM transactions_clean
                WHERE instance_date >= '{start_date}'
                  AND instance_date < '{end_date}'
                GROUP BY area_name_en
            ),
            historical AS (
                SELECT
                    area_name_en,
                    COUNT(*) * 1.0 / {lookback_months} as monthly_avg
                FROM transactions_clean
                WHERE instance_date >= '{lookback_start}'
                  AND instance_date < '{start_date}'
                GROUP BY area_name_en
            )
            SELECT * FROM (
                SELECT
                    c.area_name_en,
                    c.current_volume,
                    COALESCE(h.monthly_avg, 0) as historical_avg,
                    c.current_value,
                    CASE
                        WHEN COALESCE(h.monthly_avg, 0) > 0
                        THEN ((c.current_volume - h.monthly_avg) / h.monthly_avg) * 100
                        ELSE 100
                    END as spike_pct
                FROM current_period c
                LEFT JOIN historical h ON c.area_name_en = h.area_name_en
                WHERE COALESCE(h.monthly_avg, 0) > 5  -- Minimum baseline activity
            ) subq
            WHERE spike_pct >= {spike_threshold}
            ORDER BY spike_pct DESC
            LIMIT 10
        """

        df = self.con.execute(query).df()

        spikes = []
        for _, row in df.iterrows():
            spikes.append(VolumeSpike(
                area=row['area_name_en'],
                current_volume=int(row['current_volume']),
                historical_avg=float(row['historical_avg']),
                spike_percentage=float(row['spike_pct']),
                current_value=float(row['current_value'])
            ))

        return spikes

    def _get_volume_drops(
        self,
        start_date: str,
        end_date: str,
        lookback_months: int,
        drop_threshold: float = -30.0
    ) -> List[VolumeSpike]:
        """Find areas with transaction volume significantly below historical average."""
        lookback_start = self._subtract_months(start_date, lookback_months)

        query = f"""
            WITH current_period AS (
                SELECT
                    area_name_en,
                    COUNT(*) as current_volume,
                    SUM(actual_worth) as current_value
                FROM transactions_clean
                WHERE instance_date >= '{start_date}'
                  AND instance_date < '{end_date}'
                GROUP BY area_name_en
            ),
            historical AS (
                SELECT
                    area_name_en,
                    COUNT(*) * 1.0 / {lookback_months} as monthly_avg
                FROM transactions_clean
                WHERE instance_date >= '{lookback_start}'
                  AND instance_date < '{start_date}'
                GROUP BY area_name_en
            )
            SELECT * FROM (
                SELECT
                    h.area_name_en,
                    COALESCE(c.current_volume, 0) as current_volume,
                    h.monthly_avg as historical_avg,
                    COALESCE(c.current_value, 0) as current_value,
                    ((COALESCE(c.current_volume, 0) - h.monthly_avg) / h.monthly_avg) * 100 as drop_pct
                FROM historical h
                LEFT JOIN current_period c ON h.area_name_en = c.area_name_en
                WHERE h.monthly_avg > 10  -- Areas with meaningful historical activity
            ) subq
            WHERE drop_pct <= {drop_threshold}
            ORDER BY drop_pct ASC
            LIMIT 10
        """

        df = self.con.execute(query).df()

        drops = []
        for _, row in df.iterrows():
            drops.append(VolumeSpike(
                area=row['area_name_en'],
                current_volume=int(row['current_volume']),
                historical_avg=float(row['historical_avg']),
                spike_percentage=float(row['drop_pct']),
                current_value=float(row['current_value'])
            ))

        return drops

    def _get_new_developers(
        self,
        start_date: str,
        end_date: str,
        lookback_months: int
    ) -> List[NewDeveloper]:
        """
        Find developers who made their first sale recently.

        Identifies developers whose first transaction in our database
        falls within the analysis period.
        """
        lookback_start = self._subtract_months(start_date, lookback_months)

        query = f"""
            WITH developer_first_sale AS (
                SELECT
                    master_project_en,
                    MIN(instance_date) as first_sale_date
                FROM transactions_clean
                WHERE master_project_en IS NOT NULL
                  AND master_project_en != ''
                GROUP BY master_project_en
            ),
            new_devs AS (
                SELECT master_project_en, first_sale_date
                FROM developer_first_sale
                WHERE first_sale_date >= '{lookback_start}'
            ),
            dev_stats AS (
                SELECT
                    t.master_project_en,
                    nd.first_sale_date,
                    COUNT(*) as tx_count,
                    SUM(actual_worth) as total_value,
                    COUNT(DISTINCT area_name_en) as areas_active
                FROM transactions_clean t
                INNER JOIN new_devs nd ON t.master_project_en = nd.master_project_en
                WHERE t.instance_date >= '{start_date}'
                  AND t.instance_date < '{end_date}'
                GROUP BY t.master_project_en, nd.first_sale_date
            )
            SELECT *
            FROM dev_stats
            WHERE tx_count >= 5  -- Meaningful activity threshold
            ORDER BY tx_count DESC
            LIMIT 10
        """

        df = self.con.execute(query).df()

        new_devs = []
        for _, row in df.iterrows():
            new_devs.append(NewDeveloper(
                developer=row['master_project_en'],
                first_sale_date=str(row['first_sale_date']),
                transaction_count=int(row['tx_count']),
                total_value=float(row['total_value']),
                areas_active=int(row['areas_active'])
            ))

        return new_devs

    def _get_developer_surges(
        self,
        start_date: str,
        end_date: str,
        lookback_months: int,
        surge_threshold: float = 50.0
    ) -> List[Dict[str, Any]]:
        """
        Find established developers with unusual activity surge.

        Different from new developers - these are established players
        whose activity has increased significantly.
        """
        lookback_start = self._subtract_months(start_date, lookback_months)

        query = f"""
            WITH current_activity AS (
                SELECT
                    master_project_en,
                    COUNT(*) as current_count,
                    SUM(actual_worth) as current_value
                FROM transactions_clean
                WHERE instance_date >= '{start_date}'
                  AND instance_date < '{end_date}'
                  AND master_project_en IS NOT NULL
                  AND master_project_en != ''
                GROUP BY master_project_en
            ),
            historical_activity AS (
                SELECT
                    master_project_en,
                    COUNT(*) * 1.0 / {lookback_months} as monthly_avg
                FROM transactions_clean
                WHERE instance_date >= '{lookback_start}'
                  AND instance_date < '{start_date}'
                  AND master_project_en IS NOT NULL
                  AND master_project_en != ''
                GROUP BY master_project_en
            )
            SELECT * FROM (
                SELECT
                    c.master_project_en as developer,
                    c.current_count,
                    h.monthly_avg as historical_monthly_avg,
                    c.current_value,
                    ((c.current_count - h.monthly_avg) / h.monthly_avg) * 100 as surge_pct
                FROM current_activity c
                INNER JOIN historical_activity h ON c.master_project_en = h.master_project_en
                WHERE h.monthly_avg >= 10  -- Established developers only
            ) subq
            WHERE surge_pct >= {surge_threshold}
            ORDER BY surge_pct DESC
            LIMIT 10
        """

        df = self.con.execute(query).df()

        surges = []
        for _, row in df.iterrows():
            surges.append({
                'developer': row['developer'],
                'current_count': int(row['current_count']),
                'historical_monthly_avg': float(row['historical_monthly_avg']),
                'current_value': float(row['current_value']),
                'surge_percentage': float(row['surge_pct'])
            })

        return surges

    def _subtract_months(self, date_str: str, months: int) -> str:
        """Subtract months from a date string."""
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        # Simple month subtraction
        new_month = dt.month - months
        new_year = dt.year
        while new_month <= 0:
            new_month += 12
            new_year -= 1
        return f"{new_year}-{new_month:02d}-{dt.day:02d}"

    def format_for_prompt(self, results: AnomalyResults) -> str:
        """
        Format anomaly results for injection into content prompt.

        Returns:
            Formatted string ready for prompt injection
        """
        sections = []

        # Record transactions
        if results.record_transactions:
            section = "**Record Transactions (Highest Value Sales):**\n"
            for tx in results.record_transactions[:5]:
                section += f"- #{tx.rank}: AED {tx.price:,.0f} - {tx.property_type} in {tx.area}"
                if tx.developer:
                    section += f" ({tx.developer})"
                section += "\n"
            sections.append(section)

        # Ultra-luxury records
        if results.ultra_luxury_records:
            section = "**Ultra-Luxury Segment (AED 10M+):**\n"
            for tx in results.ultra_luxury_records[:3]:
                section += f"- AED {tx.price:,.0f} {tx.property_type} in {tx.area}\n"
            sections.append(section)

        # Volume spikes
        if results.volume_spikes:
            section = "**Transaction Volume Spikes:**\n"
            for spike in results.volume_spikes[:5]:
                section += f"- {spike.area}: +{spike.spike_percentage:.1f}% above historical ({spike.current_volume:,} vs avg {spike.historical_avg:.0f})\n"
            sections.append(section)

        # New developers
        if results.new_developers:
            section = "**New Market Entrants:**\n"
            for dev in results.new_developers[:3]:
                section += f"- {dev.developer}: {dev.transaction_count} sales since {dev.first_sale_date}, AED {dev.total_value:,.0f} total\n"
            sections.append(section)

        # Developer surges
        if results.developer_surges:
            section = "**Developer Activity Surges:**\n"
            for surge in results.developer_surges[:3]:
                section += f"- {surge['developer']}: +{surge['surge_percentage']:.1f}% vs historical average\n"
            sections.append(section)

        if not sections:
            return "No significant anomalies detected in this period."

        return "\n".join(sections)
