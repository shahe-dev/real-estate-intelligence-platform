"""
Emerging Hotspots Analysis - Dubai Real Estate Transaction Data
Analyzes transaction trends across ALL areas for 2023, 2024, and 2025
with misleading growth detection and comprehensive metrics.
"""

import duckdb
import pandas as pd
import json
from pathlib import Path
from typing import Dict, List, Tuple
from datetime import datetime


class EmergingHotspotsAnalyzer:
    """Analyze real estate transaction trends with misleading growth detection."""

    def __init__(self, db_path: str):
        """Initialize analyzer with database connection.

        Args:
            db_path: Path to DuckDB database file
        """
        self.db_path = db_path
        self.conn = duckdb.connect(db_path, read_only=True)

    def extract_transaction_data(self) -> pd.DataFrame:
        """Extract transaction counts and values for all areas (2023-2025).

        Returns:
            DataFrame with transaction metrics by area and year
        """
        query = """
        WITH yearly_data AS (
            SELECT
                area_name_en,
                YEAR(instance_date::DATE) as year,
                COUNT(*) as transaction_count,
                AVG(actual_worth) as avg_transaction_value,
                MEDIAN(actual_worth) as median_transaction_value,
                SUM(actual_worth) as total_value
            FROM transactions_clean
            WHERE instance_date IS NOT NULL
                AND area_name_en IS NOT NULL
                AND YEAR(instance_date::DATE) IN (2023, 2024, 2025)
            GROUP BY area_name_en, YEAR(instance_date::DATE)
        )
        SELECT
            area_name_en,
            year,
            transaction_count,
            avg_transaction_value,
            median_transaction_value,
            total_value
        FROM yearly_data
        ORDER BY area_name_en, year
        """

        df = self.conn.execute(query).df()
        print(f"Extracted {len(df)} area-year combinations")
        return df

    def pivot_and_calculate_growth(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pivot data and calculate YoY growth rates.

        Args:
            df: Raw transaction data by area and year

        Returns:
            DataFrame with growth calculations and metrics
        """
        # Pivot transaction counts
        counts_pivot = df.pivot(
            index='area_name_en',
            columns='year',
            values='transaction_count'
        ).fillna(0).astype(int)

        # Pivot average values
        avg_values_pivot = df.pivot(
            index='area_name_en',
            columns='year',
            values='avg_transaction_value'
        ).fillna(0)

        # Ensure all year columns exist
        for year in [2023, 2024, 2025]:
            if year not in counts_pivot.columns:
                counts_pivot[year] = 0
            if year not in avg_values_pivot.columns:
                avg_values_pivot[year] = 0

        # Sort columns
        counts_pivot = counts_pivot[[2023, 2024, 2025]]
        avg_values_pivot = avg_values_pivot[[2023, 2024, 2025]]

        # Create result DataFrame
        result = pd.DataFrame(index=counts_pivot.index)

        # Transaction counts
        result['transactions_2023'] = counts_pivot[2023]
        result['transactions_2024'] = counts_pivot[2024]
        result['transactions_2025'] = counts_pivot[2025]

        # Average transaction values
        result['avg_value_2023'] = avg_values_pivot[2023].round(2)
        result['avg_value_2024'] = avg_values_pivot[2024].round(2)
        result['avg_value_2025'] = avg_values_pivot[2025].round(2)

        # Calculate YoY growth rates (2024 vs 2023)
        result['yoy_growth_2024_vs_2023_pct'] = self._calculate_growth_rate(
            counts_pivot[2023], counts_pivot[2024]
        )

        # Calculate YoY growth rates (2025 vs 2024)
        result['yoy_growth_2025_vs_2024_pct'] = self._calculate_growth_rate(
            counts_pivot[2024], counts_pivot[2025]
        )

        # Calculate compound growth (2023 -> 2025)
        result['compound_growth_2023_2025_pct'] = self._calculate_compound_growth(
            counts_pivot[2023], counts_pivot[2025], periods=2
        )

        # Absolute changes
        result['absolute_change_2024'] = counts_pivot[2024] - counts_pivot[2023]
        result['absolute_change_2025'] = counts_pivot[2025] - counts_pivot[2024]
        result['absolute_change_total'] = counts_pivot[2025] - counts_pivot[2023]

        # Reset index to make area_name_en a column
        result = result.reset_index()
        result = result.rename(columns={'index': 'area_name_en'})

        return result

    def _calculate_growth_rate(self, base: pd.Series, current: pd.Series) -> pd.Series:
        """Calculate percentage growth rate with safety for zero division.

        Args:
            base: Base period values
            current: Current period values

        Returns:
            Growth rate percentage (rounded to 2 decimals)
        """
        growth = pd.Series(index=base.index, dtype=float)

        # Where base is 0 but current > 0, it's infinite growth (use 999999 as marker)
        mask_zero_base = (base == 0) & (current > 0)
        growth[mask_zero_base] = 999999.0

        # Where base is 0 and current is 0, growth is 0
        mask_both_zero = (base == 0) & (current == 0)
        growth[mask_both_zero] = 0.0

        # Normal calculation where base > 0
        mask_normal = base > 0
        growth[mask_normal] = ((current[mask_normal] - base[mask_normal]) / base[mask_normal] * 100)

        return growth.round(2)

    def _calculate_compound_growth(self, start: pd.Series, end: pd.Series, periods: int) -> pd.Series:
        """Calculate compound annual growth rate (CAGR).

        Args:
            start: Starting period values
            end: Ending period values
            periods: Number of periods

        Returns:
            CAGR percentage (rounded to 2 decimals)
        """
        cagr = pd.Series(index=start.index, dtype=float)

        # Where start is 0, mark as infinite growth
        mask_zero_start = (start == 0) & (end > 0)
        cagr[mask_zero_start] = 999999.0

        # Where both are 0, growth is 0
        mask_both_zero = (start == 0) & (end == 0)
        cagr[mask_both_zero] = 0.0

        # Normal CAGR calculation where start > 0
        mask_normal = start > 0
        cagr[mask_normal] = (((end[mask_normal] / start[mask_normal]) ** (1 / periods) - 1) * 100)

        return cagr.round(2)

    def add_misleading_flags(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add misleading_flag and misleading_reason columns.

        Args:
            df: DataFrame with transaction and growth data

        Returns:
            DataFrame with misleading flags and reasons
        """
        df = df.copy()
        df['misleading_flag'] = False
        df['misleading_reason'] = ''

        reasons = []

        for idx, row in df.iterrows():
            reason_parts = []
            is_misleading = False

            # Check 1: Small sample in base year (2023)
            if row['transactions_2023'] < 50 and row['transactions_2023'] > 0:
                reason_parts.append(f"Small 2023 base ({int(row['transactions_2023'])} transactions)")
                is_misleading = True

            # Check 2: High growth % but low absolute change (2024)
            if (abs(row['yoy_growth_2024_vs_2023_pct']) > 200 and
                abs(row['absolute_change_2024']) < 100):
                reason_parts.append(
                    f"High growth % ({row['yoy_growth_2024_vs_2023_pct']:.1f}%) "
                    f"but only {int(abs(row['absolute_change_2024']))} transaction change (2024)"
                )
                is_misleading = True

            # Check 3: High growth % but low absolute change (2025)
            if (abs(row['yoy_growth_2025_vs_2024_pct']) > 200 and
                abs(row['absolute_change_2025']) < 100):
                reason_parts.append(
                    f"High growth % ({row['yoy_growth_2025_vs_2024_pct']:.1f}%) "
                    f"but only {int(abs(row['absolute_change_2025']))} transaction change (2025)"
                )
                is_misleading = True

            # Check 4: Any year has < 10 transactions
            low_volume_years = []
            if row['transactions_2023'] < 10:
                low_volume_years.append(f"2023 ({int(row['transactions_2023'])})")
            if row['transactions_2024'] < 10:
                low_volume_years.append(f"2024 ({int(row['transactions_2024'])})")
            if row['transactions_2025'] < 10:
                low_volume_years.append(f"2025 ({int(row['transactions_2025'])})")

            if low_volume_years:
                reason_parts.append(f"Very low volume: {', '.join(low_volume_years)}")
                is_misleading = True

            # Check 5: Infinite growth marker (from zero base)
            if row['yoy_growth_2024_vs_2023_pct'] == 999999.0:
                reason_parts.append("No 2023 transactions (infinite growth)")
                is_misleading = True
            if row['yoy_growth_2025_vs_2024_pct'] == 999999.0:
                reason_parts.append("No 2024 transactions (infinite growth)")
                is_misleading = True

            df.at[idx, 'misleading_flag'] = is_misleading
            df.at[idx, 'misleading_reason'] = '; '.join(reason_parts) if reason_parts else ''

        return df

    def export_to_excel(self, df: pd.DataFrame, output_path: str) -> None:
        """Export analysis to Excel with multiple sheets.

        Args:
            df: Complete analysis DataFrame
            output_path: Path to output Excel file
        """
        # Create output directory if needed
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Sheet 1: Complete data sorted by compound growth
            df_complete = df.sort_values('compound_growth_2023_2025_pct', ascending=False)
            df_complete.to_excel(writer, sheet_name='All Areas', index=False)

            # Sheet 2: Top growth areas (non-misleading)
            df_legit_growth = df[~df['misleading_flag']].copy()
            df_legit_growth = df_legit_growth.sort_values('compound_growth_2023_2025_pct', ascending=False).head(50)
            df_legit_growth.to_excel(writer, sheet_name='Top 50 Legitimate Growth', index=False)

            # Sheet 3: Misleading growth cases
            df_misleading = df[df['misleading_flag']].copy()
            df_misleading = df_misleading.sort_values('compound_growth_2023_2025_pct', ascending=False)
            df_misleading.to_excel(writer, sheet_name='Misleading Growth', index=False)

            # Sheet 4: High volume areas (2024 > 500 transactions)
            df_high_volume = df[df['transactions_2024'] > 500].copy()
            df_high_volume = df_high_volume.sort_values('compound_growth_2023_2025_pct', ascending=False)
            df_high_volume.to_excel(writer, sheet_name='High Volume Areas', index=False)

            # Sheet 5: Summary statistics
            summary_stats = self._generate_summary_stats(df)
            summary_stats.to_excel(writer, sheet_name='Summary Statistics', index=True)

            # Sheet 6: Declining markets
            df_declining = df[df['compound_growth_2023_2025_pct'] < -10].copy()
            df_declining = df_declining.sort_values('compound_growth_2023_2025_pct', ascending=True)
            df_declining.to_excel(writer, sheet_name='Declining Markets', index=False)

        print(f"Excel file exported: {output_path}")

    def export_to_json(self, df: pd.DataFrame, output_path: str) -> None:
        """Export analysis to JSON format.

        Args:
            df: Complete analysis DataFrame
            output_path: Path to output JSON file
        """
        # Create output directory if needed
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        # Replace infinite growth markers with "N/A" string for JSON
        df_json = df.copy()
        df_json = df_json.replace([999999.0, -999999.0], 'INFINITE')

        # Convert to dict with proper structure
        export_data = {
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'total_areas': len(df),
                'areas_with_misleading_growth': int(df['misleading_flag'].sum()),
                'areas_with_legitimate_growth': int((~df['misleading_flag']).sum())
            },
            'areas': df_json.to_dict(orient='records'),
            'summary_statistics': self._generate_summary_stats(df).to_dict()
        }

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)

        print(f"JSON file exported: {output_path}")

    def _generate_summary_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate summary statistics for the analysis.

        Args:
            df: Complete analysis DataFrame

        Returns:
            DataFrame with summary statistics
        """
        stats = {
            'Total Areas Analyzed': [len(df)],
            'Areas with Misleading Growth': [int(df['misleading_flag'].sum())],
            'Areas with Legitimate Growth': [int((~df['misleading_flag']).sum())],
            '---': [''],
            'Total Transactions 2023': [int(df['transactions_2023'].sum())],
            'Total Transactions 2024': [int(df['transactions_2024'].sum())],
            'Total Transactions 2025': [int(df['transactions_2025'].sum())],
            '----': [''],
            'Avg Compound Growth (All)': [f"{df['compound_growth_2023_2025_pct'].mean():.2f}%"],
            'Avg Compound Growth (Legit)': [f"{df[~df['misleading_flag']]['compound_growth_2023_2025_pct'].mean():.2f}%"],
            'Median Compound Growth (All)': [f"{df['compound_growth_2023_2025_pct'].median():.2f}%"],
            '-----': [''],
            'Areas with 2024 Growth > 50%': [int((df['yoy_growth_2024_vs_2023_pct'] > 50).sum())],
            'Areas with 2025 Growth > 50%': [int((df['yoy_growth_2025_vs_2024_pct'] > 50).sum())],
            'Areas with Declining Transactions': [int((df['compound_growth_2023_2025_pct'] < 0).sum())],
            '------': [''],
            'Highest Compound Growth Area': [df.loc[df['compound_growth_2023_2025_pct'].idxmax(), 'area_name_en']],
            'Highest Compound Growth %': [f"{df['compound_growth_2023_2025_pct'].max():.2f}%"],
            'Lowest Compound Growth Area': [df.loc[df['compound_growth_2023_2025_pct'].idxmin(), 'area_name_en']],
            'Lowest Compound Growth %': [f"{df['compound_growth_2023_2025_pct'].min():.2f}%"],
        }

        return pd.DataFrame.from_dict(stats, orient='index', columns=['Value'])

    def print_summary(self, df: pd.DataFrame) -> None:
        """Print comprehensive summary statistics to console.

        Args:
            df: Complete analysis DataFrame
        """
        print("\n" + "="*80)
        print("EMERGING HOTSPOTS ANALYSIS - SUMMARY REPORT")
        print("="*80)

        print(f"\nTotal Areas Analyzed: {len(df)}")
        print(f"Areas with Misleading Growth Flags: {df['misleading_flag'].sum()}")
        print(f"Areas with Legitimate Growth: {(~df['misleading_flag']).sum()}")

        print("\n" + "-"*80)
        print("TRANSACTION VOLUME OVERVIEW")
        print("-"*80)
        print(f"2023 Total Transactions: {df['transactions_2023'].sum():,}")
        print(f"2024 Total Transactions: {df['transactions_2024'].sum():,}")
        print(f"2025 Total Transactions: {df['transactions_2025'].sum():,}")
        print(f"\nOverall Market Growth (2023-2025): {((df['transactions_2025'].sum() / df['transactions_2023'].sum() - 1) * 100):.2f}%")

        print("\n" + "-"*80)
        print("GROWTH RATE STATISTICS")
        print("-"*80)

        # Filter out infinite growth for meaningful stats
        df_finite = df[df['compound_growth_2023_2025_pct'] != 999999.0].copy()

        print(f"Average Compound Growth (All Areas): {df_finite['compound_growth_2023_2025_pct'].mean():.2f}%")
        print(f"Median Compound Growth (All Areas): {df_finite['compound_growth_2023_2025_pct'].median():.2f}%")
        print(f"Average Compound Growth (Legitimate Only): {df_finite[~df_finite['misleading_flag']]['compound_growth_2023_2025_pct'].mean():.2f}%")

        print("\n" + "-"*80)
        print("TOP 10 AREAS - LEGITIMATE COMPOUND GROWTH (2023-2025)")
        print("-"*80)

        top_legit = df[~df['misleading_flag']].nlargest(10, 'compound_growth_2023_2025_pct')
        for idx, row in enumerate(top_legit.itertuples(), 1):
            print(f"{idx:2d}. {row.area_name_en[:40]:40s} | "
                  f"{row.compound_growth_2023_2025_pct:7.2f}% | "
                  f"Transactions: {row.transactions_2023:4d} -> {row.transactions_2024:4d} -> {row.transactions_2025:4d}")

        print("\n" + "-"*80)
        print("TOP 10 MISLEADING GROWTH CASES")
        print("-"*80)

        top_misleading = df[df['misleading_flag']].nlargest(10, 'compound_growth_2023_2025_pct')
        for idx, row in enumerate(top_misleading.itertuples(), 1):
            growth_display = f"{row.compound_growth_2023_2025_pct:.2f}%" if row.compound_growth_2023_2025_pct != 999999.0 else "INFINITE"
            print(f"{idx:2d}. {row.area_name_en[:40]:40s} | {growth_display:>10s}")
            print(f"    Reason: {row.misleading_reason}")

        print("\n" + "-"*80)
        print("DECLINING MARKETS (Compound Growth < -10%)")
        print("-"*80)

        declining = df[df['compound_growth_2023_2025_pct'] < -10].nsmallest(10, 'compound_growth_2023_2025_pct')
        for idx, row in enumerate(declining.itertuples(), 1):
            print(f"{idx:2d}. {row.area_name_en[:40]:40s} | "
                  f"{row.compound_growth_2023_2025_pct:7.2f}% | "
                  f"Transactions: {row.transactions_2023:4d} -> {row.transactions_2024:4d} -> {row.transactions_2025:4d}")

        print("\n" + "-"*80)
        print("HIGH VOLUME MARKETS (2024 > 500 transactions)")
        print("-"*80)

        high_volume = df[df['transactions_2024'] > 500].nlargest(10, 'transactions_2024')
        print(f"Total high-volume areas: {len(df[df['transactions_2024'] > 500])}")
        print("\nTop 10 by volume:")
        for idx, row in enumerate(high_volume.itertuples(), 1):
            print(f"{idx:2d}. {row.area_name_en[:40]:40s} | "
                  f"{row.transactions_2024:5,d} txns | "
                  f"Growth: {row.compound_growth_2023_2025_pct:6.2f}%")

        print("\n" + "="*80)
        print("Analysis complete!")
        print("="*80 + "\n")

    def run_analysis(self, excel_path: str, json_path: str) -> pd.DataFrame:
        """Run complete analysis pipeline.

        Args:
            excel_path: Path for Excel output
            json_path: Path for JSON output

        Returns:
            Complete analysis DataFrame
        """
        print("Starting Emerging Hotspots Analysis...")
        print(f"Database: {self.db_path}\n")

        # Step 1: Extract data
        print("Step 1: Extracting transaction data...")
        raw_data = self.extract_transaction_data()

        # Step 2: Calculate growth metrics
        print("Step 2: Calculating growth metrics...")
        analysis_df = self.pivot_and_calculate_growth(raw_data)

        # Step 3: Add misleading flags
        print("Step 3: Adding misleading growth detection...")
        analysis_df = self.add_misleading_flags(analysis_df)

        # Step 4: Export to Excel
        print("Step 4: Exporting to Excel...")
        self.export_to_excel(analysis_df, excel_path)

        # Step 5: Export to JSON
        print("Step 5: Exporting to JSON...")
        self.export_to_json(analysis_df, json_path)

        # Step 6: Print summary
        self.print_summary(analysis_df)

        return analysis_df

    def close(self):
        """Close database connection."""
        self.conn.close()


def main():
    """Main execution function."""
    # Paths
    PROJECT_ROOT = Path(__file__).parent.parent
    DB_PATH = str(PROJECT_ROOT / "data" / "database" / "property_monitor.db")
    EXCEL_OUTPUT = str(PROJECT_ROOT / "data" / "generated_content" / "emerging_hotspots_deep_dive.xlsx")
    JSON_OUTPUT = str(PROJECT_ROOT / "data" / "generated_content" / "emerging_hotspots_data.json")

    # Run analysis
    analyzer = EmergingHotspotsAnalyzer(DB_PATH)

    try:
        results_df = analyzer.run_analysis(EXCEL_OUTPUT, JSON_OUTPUT)
        print(f"\nResults DataFrame shape: {results_df.shape}")
        print(f"Columns: {', '.join(results_df.columns)}")
    finally:
        analyzer.close()


if __name__ == "__main__":
    main()
