# src/content/generate.py

"""
Content Generation CLI
Supports both DLD and Property Monitor data sources
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


def select_data_source():
    """Let user select data source"""
    print("\n" + "=" * 60)
    print("SELECT DATA SOURCE")
    print("=" * 60)
    print("\n1. Property Monitor (Recommended)")
    print("   - 483K+ transactions (2023-2025)")
    print("   - Includes off-plan data, developer info")
    print("   - More recent and accurate")
    print("\n2. DLD Raw Data (Legacy)")
    print("   - Historical data (2002-2024)")
    print("   - Dubai Land Department records")
    print("\n0. Exit")

    choice = input("\nSelect data source (1-2, default=1): ").strip() or "1"
    return choice


def run_property_monitor_menu():
    """Property Monitor content generation menu"""
    import duckdb
    from config.bigquery_settings import bq_settings
    from src.content.pm_generator import PMContentGenerator, generate_pm_content_batch

    print("\n" + "=" * 60)
    print("PROPERTY MONITOR CONTENT GENERATOR")
    print("=" * 60)
    print("\nWhat would you like to generate?\n")
    print("1. Area guide (single area)")
    print("2. Area guides (top 10 areas)")
    print("3. Monthly market report")
    print("4. Developer profile")
    print("5. Off-plan market report")
    print("6. Luxury market summary")
    print("\n0. Back to data source selection")

    choice = input("\nEnter choice (0-6): ").strip()

    if choice == '0':
        return True  # Go back

    con = duckdb.connect(str(bq_settings.PM_DB_PATH), read_only=True)
    generator = PMContentGenerator()

    if choice == '1':
        # Single area guide
        areas = con.execute("""
            SELECT area_name_en, total_transactions, avg_price
            FROM metrics_area
            ORDER BY total_transactions DESC
            LIMIT 20
        """).df()

        print("\nTop 20 areas by transaction volume:")
        print("-" * 60)
        for i, row in areas.iterrows():
            print(f"{i+1:2}. {row['area_name_en']:35} ({row['total_transactions']:,} txns, avg AED {row['avg_price']:,.0f})")

        area_choice = input("\nEnter area number or name: ").strip()

        if area_choice.isdigit():
            idx = int(area_choice) - 1
            if 0 <= idx < len(areas):
                area_name = areas.iloc[idx]['area_name_en']
            else:
                print("Invalid selection")
                return False
        else:
            area_name = area_choice

        # Date range options
        print("\nDate Range Options:")
        print("1. All Property Monitor data (2023-2025)")
        print("2. 2024 data only")
        print("3. 2025 data only")
        print("4. Custom range")

        date_choice = input("\nChoose (1-4, default=1): ").strip() or "1"

        year_from = None
        year_to = None

        if date_choice == "2":
            year_from = 2024
            year_to = 2024
        elif date_choice == "3":
            year_from = 2025
            year_to = 2025
        elif date_choice == "4":
            year_from = int(input("Start year: ").strip() or "2023")
            year_to = int(input("End year: ").strip() or "2025")

        generator.generate_area_guide(area_name, year_from, year_to)

    elif choice == '2':
        # Top 10 areas batch
        generate_pm_content_batch()

    elif choice == '3':
        # Monthly report
        print("\nAvailable data: 2023-2025")
        year = int(input("Year (2024): ").strip() or "2024")
        month = int(input("Month (1-12): ").strip() or str(datetime.now().month))
        generator.generate_market_report(year, month)

    elif choice == '4':
        # Developer profile
        developers = con.execute("""
            SELECT developer, total_transactions, projects_count, avg_price
            FROM metrics_developers
            ORDER BY total_transactions DESC
            LIMIT 20
        """).df()

        print("\nTop 20 developers:")
        print("-" * 70)
        for i, row in developers.iterrows():
            print(f"{i+1:2}. {row['developer']:35} ({row['total_transactions']:,} sales, {row['projects_count']} projects)")

        dev_choice = input("\nEnter developer number or name: ").strip()

        if dev_choice.isdigit():
            idx = int(dev_choice) - 1
            if 0 <= idx < len(developers):
                dev_name = developers.iloc[idx]['developer']
            else:
                print("Invalid selection")
                return False
        else:
            dev_name = dev_choice

        generator.generate_developer_report(dev_name)

    elif choice == '5':
        # Off-plan report
        print("\nAvailable years: 2023, 2024, 2025")
        year = int(input("Year (2024): ").strip() or "2024")
        generator.generate_offplan_report(year)

    elif choice == '6':
        # Luxury summary
        print("\nLuxury Market Summary (5M+ AED)")
        print("-" * 60)

        luxury = con.execute("""
            SELECT
                area_name_en,
                total_luxury_transactions,
                avg_luxury_price,
                highest_sale
            FROM metrics_luxury_summary
            ORDER BY total_luxury_transactions DESC
            LIMIT 15
        """).df()

        print(f"\n{'Area':<30} {'Transactions':>12} {'Avg Price':>15} {'Highest Sale':>15}")
        print("-" * 75)
        for _, row in luxury.iterrows():
            print(f"{row['area_name_en']:<30} {row['total_luxury_transactions']:>12,} {row['avg_luxury_price']:>15,.0f} {row['highest_sale']:>15,.0f}")

    return False


def run_dld_menu():
    """DLD (legacy) content generation menu"""
    from src.content.generator import ContentGenerator, generate_content_batch
    from src.utils.db import get_db

    print("\n" + "=" * 60)
    print("DLD CONTENT GENERATOR (Legacy)")
    print("=" * 60)
    print("\nWhat would you like to generate?\n")
    print("1. Area guide (single area)")
    print("2. Area guides (top 10 areas)")
    print("3. Monthly market report")
    print("4. Property type comparison")
    print("5. Luxury market report")
    print("\n0. Back to data source selection")

    choice = input("\nEnter choice (0-5): ").strip()

    if choice == '0':
        return True

    generator = ContentGenerator()
    con = get_db(read_only=True)

    if choice == '1':
        areas = con.execute("""
            SELECT area_name_en FROM metrics_area
            ORDER BY total_transactions DESC
            LIMIT 20
        """).df()

        print("\nTop 20 areas:")
        for i, area in enumerate(areas['area_name_en'], 1):
            print(f"{i}. {area}")

        area_choice = input("\nEnter area number or name: ").strip()

        if area_choice.isdigit():
            area_name = areas.iloc[int(area_choice)-1]['area_name_en']
        else:
            area_name = area_choice

        print("\nDate Range Options:")
        print("1. All available data (2002-2025)")
        print("2. Recent data only (2020-2025)")
        print("3. Custom range")

        date_choice = input("\nChoose (1-3, default=1): ").strip() or "1"

        year_from = None
        year_to = None

        if date_choice == "2":
            year_from = 2020
            year_to = 2025
        elif date_choice == "3":
            year_from = int(input("Start year: ").strip() or "")
            year_to = int(input("End year: ").strip() or "")

        generator.generate_area_guide(area_name, year_from, year_to)

    elif choice == '2':
        generate_content_batch()

    elif choice == '3':
        year = int(input("Year (2024): ").strip() or "2024")
        month = int(input("Month (1-12): ").strip())
        generator.generate_monthly_market_report(year, month)

    elif choice == '4':
        print("\nAvailable: Studio, 1 B/R, 2 B/R, 3 B/R, 4 B/R, 5 B/R")
        room_types = input("Enter room types (comma-separated): ").strip().split(',')
        room_types = [rt.strip() for rt in room_types]
        generator.generate_property_comparison(room_types)

    elif choice == '5':
        year = int(input("Year (2024): ").strip() or "2024")

        luxury_data = con.execute(f"""
            SELECT
                area_name_en,
                luxury_tx_count,
                avg_luxury_price,
                max_luxury_price
            FROM metrics_luxury
            WHERE transaction_year = {year}
            ORDER BY luxury_tx_count DESC
            LIMIT 10
        """).df()

        print(f"\nLuxury Market {year}:")
        print(luxury_data.to_string())

    return False


def main():
    """Main entry point"""
    from datetime import datetime

    print("\n" + "=" * 60)
    print("DUBAI REAL ESTATE CONTENT GENERATOR")
    print("=" * 60)

    while True:
        source_choice = select_data_source()

        if source_choice == '0':
            print("\nGoodbye!")
            break
        elif source_choice == '1':
            go_back = run_property_monitor_menu()
            if not go_back:
                break
        elif source_choice == '2':
            go_back = run_dld_menu()
            if not go_back:
                break
        else:
            print("Invalid choice")


if __name__ == "__main__":
    main()
