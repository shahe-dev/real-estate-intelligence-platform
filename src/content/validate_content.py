# src/content/validate_content.py

"""
Content Validation Script - Verify LLM-Generated Numbers Against Database
This script extracts numbers from generated content and validates them against actual database values.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import re
from typing import Dict, List, Tuple
from src.utils.db import get_db
from config.settings import settings


class ContentValidator:
    """Validates LLM-generated content against database facts"""

    def __init__(self):
        self.con = get_db()
        self.tolerance_percent = 1.0  # Allow 1% difference for rounding

    def extract_numbers_from_content(self, content: str) -> Dict[str, List[int]]:
        """Extract all AED values and numbers from content"""

        # Extract AED amounts (e.g., "AED 1,234,567" or "AED 1234567")
        aed_pattern = r'AED\s*([\d,]+(?:\.\d+)?)'
        aed_matches = re.findall(aed_pattern, content, re.IGNORECASE)
        aed_values = [float(m.replace(',', '')) for m in aed_matches]

        # Extract transaction counts and other numbers
        # Look for patterns like "1,234 transactions" or "total of 5,678"
        count_pattern = r'(\d{1,3}(?:,\d{3})*)\s*(?:transactions?|properties|units|sales)'
        count_matches = re.findall(count_pattern, content, re.IGNORECASE)
        count_values = [int(m.replace(',', '')) for m in count_matches]

        # Extract percentages
        percent_pattern = r'([\d.]+)%'
        percent_matches = re.findall(percent_pattern, content)
        percent_values = [float(m) for m in percent_matches]

        return {
            'aed_values': aed_values,
            'count_values': count_values,
            'percent_values': percent_values
        }

    def get_area_facts(self, area_name: str, year_from=None, year_to=None) -> Dict:
        """Get ground truth data for an area from database"""

        # Build date filter
        date_filter = ""
        if year_from and year_to:
            date_filter = f"AND transaction_year BETWEEN {year_from} AND {year_to}"
        elif year_from:
            date_filter = f"AND transaction_year >= {year_from}"
        elif year_to:
            date_filter = f"AND transaction_year <= {year_to}"

        try:
            # Get main metrics
            metrics = self.con.execute(f"""
                SELECT
                    total_transactions,
                    avg_price,
                    median_price,
                    min_price,
                    max_price,
                    avg_price_sqm,
                    avg_size_sqm,
                    luxury_count
                FROM metrics_area
                WHERE area_name_en = '{area_name}'
            """).df()

            if metrics.empty:
                return None

            metrics_dict = metrics.to_dict('records')[0]

            # Get actual transaction count from raw data (for comparison)
            actual_tx_count = self.con.execute(f"""
                SELECT COUNT(*) as actual_count
                FROM transactions_clean
                WHERE area_name_en = '{area_name}'
                  AND trans_group_en = 'Sales'
                  {date_filter}
            """).df()

            if not actual_tx_count.empty:
                metrics_dict['actual_transaction_count'] = actual_tx_count.iloc[0]['actual_count']

            # Get property type counts for validation
            prop_types = self.con.execute(f"""
                SELECT
                    rooms_en,
                    tx_count,
                    avg_price,
                    avg_price_sqm
                FROM metrics_property_types
                WHERE area_name_en = '{area_name}'
                ORDER BY tx_count DESC
                LIMIT 5
            """).df()

            metrics_dict['property_types'] = prop_types.to_dict('records') if not prop_types.empty else []

            # Get recent price changes
            price_changes = self.con.execute(f"""
                SELECT
                    pct_change_mom
                FROM metrics_price_changes
                WHERE area_name_en = '{area_name}'
                  AND pct_change_mom IS NOT NULL
                ORDER BY transaction_year DESC, transaction_month DESC
                LIMIT 1
            """).df()

            if not price_changes.empty:
                metrics_dict['recent_mom_change'] = price_changes.iloc[0]['pct_change_mom']

            return metrics_dict

        except Exception as e:
            print(f"Error fetching area facts: {e}")
            return None

    def validate_number(self, extracted: float, actual: float, tolerance_pct: float = None) -> Tuple[bool, str]:
        """Check if extracted number matches actual within tolerance"""

        if tolerance_pct is None:
            tolerance_pct = self.tolerance_percent

        if actual == 0:
            return extracted == 0, f"Expected 0, got {extracted}"

        diff_percent = abs((extracted - actual) / actual) * 100

        if diff_percent <= tolerance_pct:
            return True, f"Match within {diff_percent:.2f}% tolerance"
        else:
            return False, f"Mismatch: {diff_percent:.2f}% difference (extracted: {extracted:,.0f}, actual: {actual:,.0f})"

    def validate_area_content(self, content: str, area_name: str, year_from=None, year_to=None) -> Dict:
        """Validate all numbers in area content against database"""

        print(f"\n{'='*70}")
        print(f"VALIDATING CONTENT FOR: {area_name}")
        print(f"{'='*70}\n")

        # Get ground truth
        facts = self.get_area_facts(area_name, year_from, year_to)
        if not facts:
            return {
                'valid': False,
                'error': f'No data found for area: {area_name}'
            }

        # Extract numbers from content
        extracted = self.extract_numbers_from_content(content)

        results = {
            'area': area_name,
            'valid': True,
            'checks': [],
            'warnings': [],
            'errors': [],
            'facts': facts,
            'extracted': extracted
        }

        print("GROUND TRUTH FROM DATABASE:")
        print(f"  Total Transactions: {facts.get('actual_transaction_count', facts['total_transactions']):,}")
        print(f"  Average Price: AED {facts['avg_price']:,.0f}")
        print(f"  Median Price: AED {facts['median_price']:,.0f}")
        print(f"  Price Range: AED {facts['min_price']:,.0f} to AED {facts['max_price']:,.0f}")
        print(f"  Avg Price/sqm: AED {facts['avg_price_sqm']:,.0f}")
        print(f"  Luxury Count: {facts['luxury_count']:,}")

        print(f"\nEXTRACTED FROM CONTENT:")
        print(f"  AED Values: {[f'{v:,.0f}' for v in extracted['aed_values'][:10]]}")
        print(f"  Count Values: {[f'{v:,}' for v in extracted['count_values'][:10]]}")
        print(f"  Percent Values: {extracted['percent_values'][:10]}")

        print(f"\n{'='*70}")
        print("VALIDATION RESULTS:")
        print(f"{'='*70}\n")

        # Check 1: Average Price
        avg_price_found = False
        for aed_val in extracted['aed_values']:
            is_valid, msg = self.validate_number(aed_val, facts['avg_price'], tolerance_pct=2.0)
            if is_valid:
                avg_price_found = True
                results['checks'].append(f"[OK] Average Price: {msg}")
                print(f"[OK] PASS - Average Price: {msg}")
                break

        if not avg_price_found:
            error = f"[ERROR] Average Price (AED {facts['avg_price']:,.0f}) not found in content"
            results['errors'].append(error)
            results['valid'] = False
            print(f"[ERROR] FAIL - {error}")

        # Check 2: Median Price
        median_price_found = False
        for aed_val in extracted['aed_values']:
            is_valid, msg = self.validate_number(aed_val, facts['median_price'], tolerance_pct=2.0)
            if is_valid:
                median_price_found = True
                results['checks'].append(f"[OK] Median Price: {msg}")
                print(f"[OK] PASS - Median Price: {msg}")
                break

        if not median_price_found:
            warning = f"[WARNING] Median Price (AED {facts['median_price']:,.0f}) not found - may be optional"
            results['warnings'].append(warning)
            print(f"[WARNING] WARN - {warning}")

        # Check 3: Price per sqm
        price_sqm_found = False
        for aed_val in extracted['aed_values']:
            is_valid, msg = self.validate_number(aed_val, facts['avg_price_sqm'], tolerance_pct=2.0)
            if is_valid:
                price_sqm_found = True
                results['checks'].append(f"[OK] Price per sqm: {msg}")
                print(f"[OK] PASS - Price per sqm: {msg}")
                break

        if not price_sqm_found:
            error = f"[ERROR] Price per sqm (AED {facts['avg_price_sqm']:,.0f}) not found in content"
            results['errors'].append(error)
            results['valid'] = False
            print(f"[ERROR] FAIL - {error}")

        # Check 4: Transaction count
        tx_count_actual = facts.get('actual_transaction_count', facts['total_transactions'])
        tx_count_found = False
        for count_val in extracted['count_values']:
            is_valid, msg = self.validate_number(count_val, tx_count_actual, tolerance_pct=5.0)
            if is_valid:
                tx_count_found = True
                results['checks'].append(f"[OK] Transaction Count: {msg}")
                print(f"[OK] PASS - Transaction Count: {msg}")
                break

        if not tx_count_found:
            warning = f"[WARNING] Transaction Count ({tx_count_actual:,}) not found - may not be mentioned"
            results['warnings'].append(warning)
            print(f"[WARNING] WARN - {warning}")

        # Check 5: Price range (min/max)
        min_found = any(self.validate_number(v, facts['min_price'], 5.0)[0] for v in extracted['aed_values'])
        max_found = any(self.validate_number(v, facts['max_price'], 5.0)[0] for v in extracted['aed_values'])

        if min_found and max_found:
            results['checks'].append("[OK] Price Range: Both min and max found")
            print("[OK] PASS - Price Range: Both min and max found")
        elif min_found or max_found:
            results['warnings'].append("[WARNING] Partial Price Range: Only one of min/max found")
            print("[WARNING] WARN - Partial Price Range found")
        else:
            results['warnings'].append("[WARNING] Price Range: Not mentioned (optional)")
            print("[WARNING] WARN - Price Range not mentioned")

        # Check 6: Luxury count
        if facts['luxury_count'] > 0:
            luxury_found = any(self.validate_number(v, facts['luxury_count'], 10.0)[0] for v in extracted['count_values'])
            if luxury_found:
                results['checks'].append(f"[OK] Luxury Count: Found and matches")
                print("[OK] PASS - Luxury Count matches")
            else:
                results['warnings'].append(f"[WARNING] Luxury Count ({facts['luxury_count']:,}) not mentioned")
                print(f"[WARNING] WARN - Luxury Count not mentioned")

        # Check 7: Property type prices (if mentioned)
        for prop_type in facts.get('property_types', [])[:3]:
            prop_price_found = any(self.validate_number(v, prop_type['avg_price'], 5.0)[0] for v in extracted['aed_values'])
            if prop_price_found:
                results['checks'].append(f"[OK] Property Type ({prop_type['rooms_en']}): Price matches")
                print(f"[OK] PASS - Property Type ({prop_type['rooms_en']}) price matches")

        # Check 8: Percentage changes
        if 'recent_mom_change' in facts and facts['recent_mom_change'] is not None:
            mom_change = abs(facts['recent_mom_change'])
            pct_found = any(abs(abs(v) - mom_change) < 1.0 for v in extracted['percent_values'])
            if pct_found:
                results['checks'].append(f"[OK] Price Change: Found matching percentage")
                print("[OK] PASS - Price change percentage matches")

        # Summary
        print(f"\n{'='*70}")
        print("SUMMARY:")
        print(f"{'='*70}")
        print(f"Total Checks Passed: {len(results['checks'])}")
        print(f"Warnings: {len(results['warnings'])}")
        print(f"Errors: {len(results['errors'])}")
        print(f"Overall Validation: {'[OK] PASSED' if results['valid'] else '[ERROR] FAILED'}")
        print(f"{'='*70}\n")

        return results

    def validate_file(self, filepath: str) -> Dict:
        """Validate a generated content file"""

        filepath = Path(filepath)
        if not filepath.exists():
            return {'valid': False, 'error': 'File not found'}

        # Read content
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        # Extract area name from filename or content
        area_name = None

        # Try to get from filename (e.g., "area_guide_downtown_dubai.md")
        if 'area_guide_' in filepath.name:
            area_name = filepath.name.replace('area_guide_', '').replace('.md', '').replace('_', ' ').title()

        # Try to get from content metadata
        metadata_match = re.search(r'^area:\s*(.+)$', content, re.MULTILINE)
        if metadata_match:
            area_name = metadata_match.group(1).strip()

        # Try to get from H1 title
        if not area_name:
            title_match = re.search(r'^#\s+(.+?)\s+Real Estate', content, re.MULTILINE)
            if title_match:
                area_name = title_match.group(1).strip()

        if not area_name:
            return {'valid': False, 'error': 'Could not extract area name from file'}

        return self.validate_area_content(content, area_name)


def main():
    """CLI for content validation"""

    print("\n" + "="*70)
    print("CONTENT VALIDATION TOOL")
    print("Verify LLM-Generated Numbers Against Database")
    print("="*70 + "\n")

    print("Options:")
    print("1. Validate a specific file")
    print("2. Validate all files in content directory")
    print("3. Validate specific area (enter area name)")
    print("4. Quick test (validate latest generated file)")
    print("\n0. Exit")

    choice = input("\nEnter choice (0-4): ").strip()

    validator = ContentValidator()

    if choice == '1':
        # Validate specific file
        filepath = input("Enter file path: ").strip()
        results = validator.validate_file(filepath)

        if not results.get('valid'):
            print(f"\n[ERROR] VALIDATION FAILED")
            if 'error' in results:
                print(f"Error: {results['error']}")

    elif choice == '2':
        # Validate all files
        content_dir = settings.CONTENT_OUTPUT_DIR
        if not content_dir.exists():
            print(f"[ERROR] Content directory not found: {content_dir}")
            return

        md_files = list(content_dir.glob("area_guide_*.md"))
        print(f"\nFound {len(md_files)} area guide files\n")

        passed = 0
        failed = 0

        for filepath in md_files:
            results = validator.validate_file(filepath)
            if results.get('valid'):
                passed += 1
            else:
                failed += 1

        print(f"\n{'='*70}")
        print(f"BATCH VALIDATION COMPLETE")
        print(f"{'='*70}")
        print(f"Passed: {passed}")
        print(f"Failed: {failed}")
        print(f"Total: {passed + failed}")
        print(f"{'='*70}\n")

    elif choice == '3':
        # Validate by area name
        area_name = input("Enter area name (e.g., 'Downtown Dubai'): ").strip()

        # Ask for date range
        print("\nDate Range (optional):")
        year_from_input = input("Start year (press Enter to skip): ").strip()
        year_to_input = input("End year (press Enter to skip): ").strip()

        year_from = int(year_from_input) if year_from_input else None
        year_to = int(year_to_input) if year_to_input else None

        # Try to find the file first
        content_dir = settings.CONTENT_OUTPUT_DIR
        filepath = content_dir / f"area_guide_{area_name.lower().replace(' ', '_')}.md"

        if filepath.exists():
            print(f"\nFound file: {filepath}")
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
        else:
            print(f"\nNo file found. Fetching data directly from database...")
            content = "Mock content for validation (no actual content to validate)"

        results = validator.validate_area_content(content, area_name, year_from, year_to)

        if not results.get('valid'):
            print(f"\n[ERROR] VALIDATION FAILED")

    elif choice == '4':
        # Quick test - validate most recent file
        content_dir = settings.CONTENT_OUTPUT_DIR
        if not content_dir.exists():
            print(f"[ERROR] Content directory not found: {content_dir}")
            return

        md_files = sorted(content_dir.glob("area_guide_*.md"), key=lambda p: p.stat().st_mtime, reverse=True)

        if not md_files:
            print("[ERROR] No area guide files found")
            return

        latest_file = md_files[0]
        print(f"Validating latest file: {latest_file.name}\n")

        results = validator.validate_file(latest_file)

        if not results.get('valid'):
            print(f"\n[ERROR] VALIDATION FAILED")
        else:
            print(f"\n[SUCCESS] VALIDATION PASSED")

    elif choice == '0':
        print("Goodbye!")
        return

    else:
        print("Invalid choice")


if __name__ == "__main__":
    main()
