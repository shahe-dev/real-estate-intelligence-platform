"""
Data Validation QA Agent
Validates database accuracy against verified Excel benchmark data.
Ensures all supply-demand analyses use correct transaction counts.
"""

import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime
from typing import Dict, List
import sys

# Configuration
VERIFIED_EXCEL_PATH = Path("data/generated_content/PR Article Data/emerging_hotspots_deep_dive_shared.xlsx")
PM_DB_PATH = Path("data/database/property_monitor.db")
CORRECT_TABLE = "transactions_clean"  # ALWAYS USE THIS
INCORRECT_TABLE = "transactions_all"  # NEVER USE THIS (has duplicates)

class DataValidationAgent:
    """
    QA Agent that validates database transaction counts against verified benchmarks.

    CRITICAL: Property Monitor data MUST use 'transactions_clean' table.
    - transactions_clean: Verified, deduplicated, accurate ✅
    - transactions_all: Raw data with duplicates (5-6x inflated) ❌
    """

    def __init__(self, verified_excel_path: Path = VERIFIED_EXCEL_PATH, db_path: Path = PM_DB_PATH):
        self.verified_excel_path = verified_excel_path
        self.db_path = db_path
        self.results = {}

    def load_verified_data(self) -> pd.DataFrame:
        """Load verified benchmark data from Excel"""
        print(f"[OK] Loading verified data from: {self.verified_excel_path}")
        df = pd.read_excel(self.verified_excel_path, sheet_name='All Areas')
        print(f"[OK] Loaded {len(df)} areas from verified Excel")
        return df

    def query_database_transactions(self, table_name: str, areas: List[str] = None) -> pd.DataFrame:
        """Query transaction counts from database"""
        con = duckdb.connect(str(self.db_path), read_only=True)

        area_filter = ""
        if areas:
            area_list = "', '".join(areas)
            area_filter = f"AND area_name_en IN ('{area_list}')"

        query = f"""
        SELECT
            area_name_en as area,
            COUNT(CASE WHEN CAST(SUBSTRING(instance_date, 1, 4) AS INTEGER) = 2023 THEN 1 END) as tx_2023,
            COUNT(CASE WHEN CAST(SUBSTRING(instance_date, 1, 4) AS INTEGER) = 2024 THEN 1 END) as tx_2024,
            COUNT(CASE WHEN CAST(SUBSTRING(instance_date, 1, 4) AS INTEGER) = 2025 THEN 1 END) as tx_2025
        FROM {table_name}
        WHERE 1=1 {area_filter}
        GROUP BY area_name_en
        ORDER BY area_name_en
        """

        df = con.execute(query).df()
        con.close()
        return df.set_index('area')

    def compare_data(self, verified: pd.DataFrame, current: pd.DataFrame, year: int = 2024) -> Dict:
        """Compare verified vs current data"""
        verified_col = f'transactions_{year}'
        current_col = f'tx_{year}'

        discrepancies = []
        exact_matches = []

        for area in verified.index:
            if area not in current.index:
                discrepancies.append({
                    'area': area,
                    'verified': verified.loc[area, verified_col],
                    'current': None,
                    'diff': None,
                    'issue': 'MISSING_FROM_DATABASE'
                })
                continue

            v_count = verified.loc[area, verified_col]
            c_count = current.loc[area, current_col]
            diff = c_count - v_count

            if diff != 0:
                diff_pct = (abs(diff) / v_count * 100) if v_count > 0 else 999
                discrepancies.append({
                    'area': area,
                    'verified': int(v_count),
                    'current': int(c_count),
                    'diff': int(diff),
                    'diff_pct': round(diff_pct, 1),
                    'issue': 'COUNT_MISMATCH'
                })
            else:
                exact_matches.append(area)

        return {
            'year': year,
            'total_areas': len(verified),
            'exact_matches': len(exact_matches),
            'discrepancies_count': len(discrepancies),
            'discrepancies': discrepancies,
            'match_rate_pct': (len(exact_matches) / len(verified) * 100) if len(verified) > 0 else 0
        }

    def validate_table(self, table_name: str) -> Dict:
        """Validate a specific database table against verified data"""
        print(f"\n{'='*60}")
        print(f"VALIDATING TABLE: {table_name}")
        print(f"{'='*60}")

        verified_df = self.load_verified_data()

        # Sample 10 high-volume areas for validation
        sample_areas = ['Business Bay', 'Dubai Marina', 'Jumeirah Village Circle',
                       'Downtown Dubai', 'Palm Jumeirah', 'Dubai Hills Estate',
                       'Arabian Ranches', 'Jumeirah Lake Towers', 'DAMAC Hills',
                       'Dubai Sports City']

        verified_sample = verified_df[verified_df['area_name_en'].isin(sample_areas)]
        verified_sample = verified_sample.set_index('area_name_en')

        current_df = self.query_database_transactions(table_name, sample_areas)

        # Compare for each year
        results_2023 = self.compare_data(verified_sample, current_df, year=2023)
        results_2024 = self.compare_data(verified_sample, current_df, year=2024)
        results_2025 = self.compare_data(verified_sample, current_df, year=2025)

        # Print results
        for year_results in [results_2023, results_2024, results_2025]:
            year = year_results['year']
            match_rate = year_results['match_rate_pct']

            if match_rate == 100:
                print(f"\n[PASS] {year}: {match_rate:.1f}% MATCH - PERFECT!")
            elif match_rate >= 95:
                print(f"\n[WARN] {year}: {match_rate:.1f}% MATCH - ACCEPTABLE")
            else:
                print(f"\n[FAIL] {year}: {match_rate:.1f}% MATCH - FAILED")

            if year_results['discrepancies']:
                print(f"   Top discrepancies:")
                for disc in year_results['discrepancies'][:3]:
                    print(f"   - {disc['area']}: Verified={disc['verified']}, Current={disc['current']}, Diff={disc['diff']:+d} ({disc['diff_pct']:.1f}%)")

        all_match = all([
            results_2023['match_rate_pct'] == 100,
            results_2024['match_rate_pct'] == 100,
            results_2025['match_rate_pct'] == 100
        ])

        return {
            'table_name': table_name,
            'passed': all_match,
            'results_2023': results_2023,
            'results_2024': results_2024,
            'results_2025': results_2025,
            'overall_match_rate': (results_2023['match_rate_pct'] + results_2024['match_rate_pct'] + results_2025['match_rate_pct']) / 3
        }

    def scan_codebase_for_table_usage(self) -> Dict:
        """Scan codebase for incorrect table usage"""
        print(f"\n{'='*60}")
        print("SCANNING CODEBASE FOR TABLE USAGE")
        print(f"{'='*60}")

        files_to_check = [
            'src/api/pm_api.py',
            'src/analytics/supply_intelligence.py',
            'src/analytics/calculator.py',
            'src/etl/loader.py',
            'src/etl/bigquery_loader.py',
            'src/metrics/calculator.py'
        ]

        issues_found = []
        files_clean = []

        for file_path in files_to_check:
            path = Path(file_path)
            if not path.exists():
                continue

            with open(path, 'r', encoding='utf-8') as f:
                content = f.read()

            # Check for problematic patterns
            uses_all = 'transactions_all' in content or 'FROM transactions' in content
            uses_clean = 'transactions_clean' in content

            if uses_all:
                # Count occurrences
                count_all = content.count('transactions_all') + content.count('FROM transactions ')

                issues_found.append({
                    'file': file_path,
                    'issue': f'Uses incorrect table (transactions_all or bare transactions)',
                    'occurrences': count_all,
                    'severity': 'CRITICAL'
                })
                print(f"[FAIL] {file_path}: Uses transactions_all ({count_all} occurrences)")
            elif uses_clean:
                files_clean.append(file_path)
                print(f"[PASS] {file_path}: Uses transactions_clean (CORRECT)")
            else:
                print(f"[WARN] {file_path}: No transaction table usage found")

        return {
            'files_checked': len(files_to_check),
            'issues_found': len(issues_found),
            'files_clean': len(files_clean),
            'issues': issues_found
        }

    def run_full_validation(self) -> Dict:
        """Run complete validation suite"""
        print(f"\n{'#'*60}")
        print("DATA VALIDATION QA AGENT - FULL SUITE")
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'#'*60}")

        # Validate both tables to show the difference
        results_clean = self.validate_table('transactions_clean')
        results_all = self.validate_table('transactions_all')

        # Scan codebase
        codebase_scan = self.scan_codebase_for_table_usage()

        # Summary
        print(f"\n{'='*60}")
        print("VALIDATION SUMMARY")
        print(f"{'='*60}")

        print(f"\nTable Accuracy:")
        print(f"  - transactions_clean: {'[PASS] 100% match' if results_clean['passed'] else '[FAIL]'} - Match rate: {results_clean['overall_match_rate']:.1f}%")
        print(f"  - transactions_all: {'[FAIL] INCORRECT (has duplicates)' if not results_all['passed'] else '[WARN] Unexpected'} - Match rate: {results_all['overall_match_rate']:.1f}%")

        print(f"\nCodebase Issues:")
        print(f"  - Files using incorrect table: {codebase_scan['issues_found']}")
        print(f"  - Files using correct table: {codebase_scan['files_clean']}")

        if codebase_scan['issues']:
            print(f"\n  CRITICAL ISSUES FOUND:")
            for issue in codebase_scan['issues']:
                print(f"    - {issue['file']}: {issue['occurrences']} occurrences")

        all_passed = results_clean['passed'] and codebase_scan['issues_found'] == 0

        print(f"\n{'='*60}")
        if all_passed:
            print("[PASS] ALL VALIDATION CHECKS PASSED")
        else:
            print("[FAIL] VALIDATION FAILED - ACTION REQUIRED")
            if codebase_scan['issues_found'] > 0:
                print(f"   -> {codebase_scan['issues_found']} files need fixing")
        print(f"{'='*60}\n")

        return {
            'timestamp': datetime.now().isoformat(),
            'all_passed': all_passed,
            'results_clean': results_clean,
            'results_all': results_all,
            'codebase_scan': codebase_scan
        }


def main():
    """Run data validation as standalone script"""
    agent = DataValidationAgent()
    results = agent.run_full_validation()
    sys.exit(0 if results['all_passed'] else 1)


if __name__ == "__main__":
    main()
