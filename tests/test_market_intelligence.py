# tests/test_market_intelligence.py
"""
Comprehensive tests for Market Intelligence modules.
Validates calculations against direct SQL queries to ensure accuracy.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
from datetime import datetime
from pathlib import Path

# Import config for database path
from config.bigquery_settings import bq_settings

# Import intelligence modules
from src.analytics.market_intelligence import (
    AnomalyDetector,
    OpportunityDetector,
    TrendPredictor,
    ComparativeAnalytics,
    MarketIntelligenceEngine
)
from src.analytics.report_calculator import PeriodType


class IntelligenceValidator:
    """Validates intelligence calculations against direct SQL queries."""

    def __init__(self, db_path: str = None):
        self.db_path = db_path or bq_settings.PM_DB_PATH
        self.con = duckdb.connect(self.db_path, read_only=True)
        self.validation_results = []
        self.errors = []

    def log_validation(self, test_name: str, expected: any, actual: any, passed: bool, details: str = ""):
        """Log a validation result."""
        result = {
            'test': test_name,
            'expected': expected,
            'actual': actual,
            'passed': passed,
            'details': details
        }
        self.validation_results.append(result)

        status = "[PASS]" if passed else "[FAIL]"
        print(f"  {status}: {test_name}")
        if not passed:
            print(f"    Expected: {expected}")
            print(f"    Actual: {actual}")
            if details:
                print(f"    Details: {details}")

    def validate_anomaly_detector(self, year: int = 2024, period_type: PeriodType = PeriodType.QUARTERLY, period_number: int = 3):
        """Validate AnomalyDetector calculations."""
        print(f"\n{'='*60}")
        print(f"VALIDATING: AnomalyDetector")
        print(f"Period: {year} {period_type.value} {period_number}")
        print(f"{'='*60}")

        detector = AnomalyDetector(self.con)

        # Get period dates - MUST match the detector's _get_period_dates logic
        # The detector uses end_date as first day of next period with < comparison
        if period_type == PeriodType.QUARTERLY:
            start_month = (period_number - 1) * 3 + 1
            end_month = start_month + 2
            start_date = f"{year}-{start_month:02d}-01"
            if end_month == 12:
                end_date = f"{year + 1}-01-01"
            else:
                end_date = f"{year}-{end_month + 1:02d}-01"
        elif period_type == PeriodType.ANNUAL:
            start_date = f"{year}-01-01"
            end_date = f"{year + 1}-01-01"
        else:  # monthly
            start_date = f"{year}-{period_number:02d}-01"
            if period_number == 12:
                end_date = f"{year + 1}-01-01"
            else:
                end_date = f"{year}-{period_number + 1:02d}-01"

        print(f"Date range: {start_date} to {end_date}")

        try:
            # Convert PeriodType enum to string for detector (it expects 'quarterly', not PeriodType.QUARTERLY)
            period_type_str = period_type.value if hasattr(period_type, 'value') else period_type
            results = detector.detect_anomalies(year, period_type_str, period_number)

            # Validate record transactions
            print("\n--- Record Transactions ---")
            direct_records = self.con.execute(f"""
                SELECT area_name_en, actual_worth, property_type_en
                FROM transactions_clean
                WHERE instance_date >= '{start_date}' AND instance_date < '{end_date}'
                ORDER BY actual_worth DESC
                LIMIT 5
            """).df()

            if results.record_transactions and not direct_records.empty:
                top_record = results.record_transactions[0]
                direct_top = direct_records.iloc[0]

                # Validate top record price matches (dataclass attributes)
                self.log_validation(
                    "Top record transaction price",
                    direct_top['actual_worth'],
                    top_record.price,
                    abs(direct_top['actual_worth'] - top_record.price) < 1,
                    f"Area: {top_record.area}"
                )

                # Validate area matches
                self.log_validation(
                    "Top record transaction area",
                    direct_top['area_name_en'],
                    top_record.area,
                    direct_top['area_name_en'] == top_record.area
                )
            else:
                self.log_validation(
                    "Record transactions found",
                    "Some records",
                    "None" if not results.record_transactions else f"{len(results.record_transactions)} records",
                    bool(results.record_transactions) == (not direct_records.empty)
                )

            # Validate volume spikes
            print("\n--- Volume Spikes ---")
            if results.volume_spikes:
                for spike in results.volume_spikes[:2]:
                    # Verify the current volume (dataclass attributes)
                    # Match spike detector's query: instance_date is VARCHAR, using string comparison
                    current_vol = self.con.execute(f"""
                        SELECT COUNT(*) as vol
                        FROM transactions_clean
                        WHERE area_name_en = '{spike.area}'
                          AND instance_date >= '{start_date}'
                          AND instance_date < '{end_date}'
                    """).fetchone()[0]

                    self.log_validation(
                        f"Volume for {spike.area}",
                        current_vol,
                        spike.current_volume,
                        current_vol == spike.current_volume,
                        f"Spike: {spike.spike_percentage:.1f}%"
                    )
            else:
                print("  No volume spikes detected (may be normal)")

            # Validate new developers
            print("\n--- New Developers ---")
            if results.new_developers:
                for dev in results.new_developers[:2]:
                    # Verify first sale date (dataclass attributes)
                    first_sale = self.con.execute(f"""
                        SELECT MIN(instance_date) as first_date
                        FROM transactions_clean
                        WHERE master_project_en = '{dev.developer.replace("'", "''")}'
                    """).fetchone()[0]

                    self.log_validation(
                        f"First sale date for {dev.developer[:30]}...",
                        str(first_sale),
                        dev.first_sale_date,
                        str(first_sale) == dev.first_sale_date
                    )
            else:
                print("  No new developers detected in period")

            return True

        except Exception as e:
            self.errors.append(f"AnomalyDetector: {str(e)}")
            print(f"  ERROR: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    def validate_opportunity_detector(self, year: int = 2024, period_type: PeriodType = PeriodType.QUARTERLY, period_number: int = 3):
        """Validate OpportunityDetector calculations."""
        print(f"\n{'='*60}")
        print(f"VALIDATING: OpportunityDetector")
        print(f"{'='*60}")

        detector = OpportunityDetector(self.con)

        try:
            results = detector.detect_opportunities(year, period_type, period_number)

            # Validate emerging hotspots
            print("\n--- Emerging Hotspots ---")
            if results.emerging_hotspots:
                for hotspot in results.emerging_hotspots[:2]:
                    # Cross-check growth calculation (dataclass attributes)
                    print(f"  Hotspot: {hotspot.area}")
                    print(f"    Growth Rate: {hotspot.tx_growth_rate:.1f}%")
                    print(f"    Current Volume: {hotspot.current_tx_count}")

                    # Verify volume is reasonable
                    actual_vol = self.con.execute(f"""
                        SELECT COUNT(*) FROM transactions_clean
                        WHERE area_name_en = '{hotspot.area}'
                    """).fetchone()[0]

                    self.log_validation(
                        f"Current volume for {hotspot.area}",
                        "Reasonable (>0)",
                        hotspot.current_tx_count,
                        hotspot.current_tx_count > 0 and hotspot.current_tx_count <= actual_vol
                    )
            else:
                print("  No emerging hotspots detected")

            # Validate developer momentum
            print("\n--- Developer Momentum ---")
            if results.developer_momentum:
                for dev in results.developer_momentum[:3]:
                    # dataclass attributes
                    print(f"  {dev.developer}: {dev.trend} (change: {dev.market_share_change:+.2f}%)")

                    # Verify developer exists and has transactions
                    dev_count = self.con.execute(f"""
                        SELECT COUNT(*) FROM transactions_clean
                        WHERE master_project_en = '{dev.developer.replace("'", "''")}'
                    """).fetchone()[0]

                    self.log_validation(
                        f"Developer {dev.developer[:25]}... has transactions",
                        ">0",
                        dev_count,
                        dev_count > 0
                    )
            else:
                print("  No developer momentum data")

            return True

        except Exception as e:
            self.errors.append(f"OpportunityDetector: {str(e)}")
            print(f"  ERROR: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    def validate_trend_predictor(self, year: int = 2024, period_type: PeriodType = PeriodType.QUARTERLY, period_number: int = 3):
        """Validate TrendPredictor calculations."""
        print(f"\n{'='*60}")
        print(f"VALIDATING: TrendPredictor")
        print(f"{'='*60}")

        predictor = TrendPredictor(self.con)

        try:
            # Use analyze_period_trends for period-based analysis
            results = predictor.analyze_period_trends(year, period_type, period_number)

            # Validate price momentum (dataclass attributes)
            print("\n--- Price Momentum ---")
            pm = results.price_momentum
            print(f"  3-month: {pm.momentum_3m if pm.momentum_3m else 'N/A'}%")
            print(f"  6-month: {pm.momentum_6m if pm.momentum_6m else 'N/A'}%")
            print(f"  12-month: {pm.momentum_12m if pm.momentum_12m else 'N/A'}%")
            print(f"  Trajectory: {pm.trajectory.value}")

            # Verify momentum calculations make sense (not extreme)
            momentum_values = [
                ('3_month', pm.momentum_3m),
                ('6_month', pm.momentum_6m),
                ('12_month', pm.momentum_12m)
            ]
            for period, value in momentum_values:
                if value is not None:
                    self.log_validation(
                        f"Price momentum {period} is reasonable",
                        "Between -50% and +100%",
                        f"{value:.1f}%",
                        -50 <= value <= 100,
                        "Extreme values may indicate calculation error"
                    )

            # Validate seasonality (dataclass)
            print("\n--- Seasonality ---")
            seas = results.seasonality
            print(f"  Peak Months: {seas.peak_months}")
            print(f"  Low Months: {seas.low_months}")

            if seas.peak_months:
                self.log_validation(
                    "Peak months are valid month names",
                    "Valid months",
                    seas.peak_months,
                    all(m in ['January','February','March','April','May','June',
                              'July','August','September','October','November','December']
                        for m in seas.peak_months)
                )

            # Validate cycle position (dataclass)
            print(f"\n--- Market Cycle ---")
            cycle = results.cycle_position
            print(f"  Position: {cycle.phase.value}")
            print(f"  Confidence: {cycle.phase_confidence*100:.0f}%")

            valid_positions = ['growth', 'peak', 'correction', 'recovery', 'stable']
            self.log_validation(
                "Cycle position is valid",
                f"One of {valid_positions}",
                cycle.phase.value,
                cycle.phase.value in valid_positions
            )

            return True

        except Exception as e:
            self.errors.append(f"TrendPredictor: {str(e)}")
            print(f"  ERROR: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    def validate_comparative_analytics(self, area_name: str = "Dubai Marina"):
        """Validate ComparativeAnalytics calculations."""
        print(f"\n{'='*60}")
        print(f"VALIDATING: ComparativeAnalytics")
        print(f"Area: {area_name}")
        print(f"{'='*60}")

        analytics = ComparativeAnalytics(self.con)

        try:
            # Test area DNA
            print("\n--- Area DNA ---")
            area_dna = analytics.get_area_dna(area_name)

            if area_dna:
                print(f"  Market Segment: {area_dna.market_segment}")
                print(f"  Avg Price: AED {area_dna.avg_price:,.0f}")
                print(f"  Total Transactions: {area_dna.total_transactions}")

                # Verify against direct query
                direct_stats = self.con.execute(f"""
                    SELECT
                        COUNT(*) as tx_count,
                        AVG(actual_worth) as avg_price
                    FROM transactions_clean
                    WHERE area_name_en = '{area_name}'
                """).df()

                if not direct_stats.empty:
                    direct_tx = int(direct_stats.iloc[0]['tx_count'])
                    direct_avg = direct_stats.iloc[0]['avg_price']

                    self.log_validation(
                        f"Transaction count for {area_name}",
                        direct_tx,
                        area_dna.total_transactions,
                        area_dna.total_transactions == direct_tx
                    )

                    # Allow small rounding difference
                    self.log_validation(
                        f"Average price for {area_name}",
                        f"AED {direct_avg:,.0f}",
                        f"AED {area_dna.avg_price:,.0f}",
                        abs(direct_avg - area_dna.avg_price) < 100,  # Within AED 100
                        "Small rounding differences acceptable"
                    )

                # Validate transaction mix sums to ~100%
                mix = area_dna.transaction_mix
                total_mix = sum(mix.values())
                self.log_validation(
                    "Transaction mix sums to ~100%",
                    "~100%",
                    f"{total_mix:.1f}%",
                    95 <= total_mix <= 105,  # Allow small rounding
                    f"Mix: {mix}"
                )

                # Validate comparable areas exist
                print(f"\n  Comparable Areas: {area_dna.comparable_areas}")
                for comp_area in area_dna.comparable_areas[:2]:
                    area_exists = self.con.execute(f"""
                        SELECT COUNT(*) FROM transactions_clean
                        WHERE area_name_en = '{comp_area}'
                    """).fetchone()[0]

                    self.log_validation(
                        f"Comparable area '{comp_area}' exists",
                        ">0 transactions",
                        area_exists,
                        area_exists > 0
                    )
            else:
                print(f"  No DNA found for {area_name}")
                self.log_validation(
                    f"Area DNA for {area_name}",
                    "Should exist",
                    "None returned",
                    False
                )

            return True

        except Exception as e:
            self.errors.append(f"ComparativeAnalytics: {str(e)}")
            print(f"  ERROR: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    def validate_engine_integration(self, year: int = 2024, period_type: PeriodType = PeriodType.QUARTERLY, period_number: int = 3):
        """Validate MarketIntelligenceEngine integration."""
        print(f"\n{'='*60}")
        print(f"VALIDATING: MarketIntelligenceEngine (Full Integration)")
        print(f"{'='*60}")

        engine = MarketIntelligenceEngine(self.con)

        try:
            # Test market report intelligence
            print("\n--- Market Report Intelligence ---")
            market_intel = engine.get_market_report_intelligence(year, period_type, period_number)

            self.log_validation(
                "Market report has primary insights",
                ">0 characters",
                f"{len(market_intel.primary_insights)} chars",
                len(market_intel.primary_insights) > 0
            )

            self.log_validation(
                "Market report has supporting data",
                "dict with keys",
                f"{list(market_intel.supporting_data.keys()) if market_intel.supporting_data else 'Empty'}",
                bool(market_intel.supporting_data)
            )

            # Check for hallucination indicators in output
            insights = market_intel.primary_insights.lower()
            hallucination_phrases = [
                "according to sources",
                "experts predict",
                "it is believed",
                "expected to reach",
                "will likely"
            ]

            found_hallucinations = [p for p in hallucination_phrases if p in insights]
            self.log_validation(
                "No hallucination phrases in output",
                "No matches",
                f"Found: {found_hallucinations}" if found_hallucinations else "None found",
                len(found_hallucinations) == 0
            )

            # Test area guide intelligence
            print("\n--- Area Guide Intelligence ---")
            area_intel = engine.get_area_guide_intelligence("Business Bay", "2024-01-01", "2024-12-31")

            self.log_validation(
                "Area guide has primary insights",
                ">0 characters",
                f"{len(area_intel.primary_insights)} chars",
                len(area_intel.primary_insights) > 0
            )

            # Test luxury report intelligence
            print("\n--- Luxury Report Intelligence ---")
            luxury_intel = engine.get_luxury_report_intelligence(year, period_type, period_number)

            self.log_validation(
                "Luxury report has primary insights",
                ">0 characters",
                f"{len(luxury_intel.primary_insights)} chars",
                len(luxury_intel.primary_insights) > 0
            )

            return True

        except Exception as e:
            self.errors.append(f"MarketIntelligenceEngine: {str(e)}")
            print(f"  ERROR: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    def print_summary(self):
        """Print validation summary."""
        print(f"\n{'='*60}")
        print("VALIDATION SUMMARY")
        print(f"{'='*60}")

        passed = sum(1 for r in self.validation_results if r['passed'])
        failed = sum(1 for r in self.validation_results if not r['passed'])
        total = len(self.validation_results)

        print(f"\nTotal Tests: {total}")
        print(f"Passed: {passed} ({passed/total*100:.1f}%)" if total > 0 else "No tests run")
        print(f"Failed: {failed}")

        if self.errors:
            print(f"\nModule Errors ({len(self.errors)}):")
            for err in self.errors:
                print(f"  - {err}")

        if failed > 0:
            print(f"\nFailed Tests:")
            for r in self.validation_results:
                if not r['passed']:
                    print(f"  - {r['test']}")
                    print(f"    Expected: {r['expected']}")
                    print(f"    Actual: {r['actual']}")

        return passed, failed, total


def main():
    """Run all validations."""
    print("="*60)
    print("MARKET INTELLIGENCE VALIDATION SUITE")
    print("="*60)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Check database exists
    db_path = bq_settings.PM_DB_PATH
    print(f"Database: {db_path}")

    if not Path(db_path).exists():
        print(f"\nERROR: Database not found at {db_path}")
        print("Please ensure the database is available.")
        return 1

    validator = IntelligenceValidator(db_path)

    # Run validations
    validator.validate_anomaly_detector(2024, PeriodType.QUARTERLY, 3)
    validator.validate_opportunity_detector(2024, PeriodType.QUARTERLY, 3)
    validator.validate_trend_predictor(2024, PeriodType.QUARTERLY, 3)
    validator.validate_comparative_analytics("Dubai Marina")
    validator.validate_comparative_analytics("Business Bay")
    validator.validate_engine_integration(2024, PeriodType.QUARTERLY, 3)

    # Print summary
    passed, failed, total = validator.print_summary()

    print(f"\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Return exit code
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    exit(main())
