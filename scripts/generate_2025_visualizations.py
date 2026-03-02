# scripts/generate_2025_visualizations.py

"""
2025 Dubai Real Estate Annual Report - Comprehensive Visualization Package

Generates professional static charts for:
1. YoY Comparison & Price Segments
2. Geographic & Developer Analysis
3. Market Intelligence Visuals
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
import json
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.ticker import FuncFormatter
import numpy as np
from datetime import datetime

from config.bigquery_settings import bq_settings
from config.settings import settings

# Professional color palette (Property Monitor branded)
COLORS = {
    'primary': '#1E3A5F',      # Dark blue
    'secondary': '#3498DB',    # Light blue
    'accent': '#E74C3C',       # Red for highlights
    'success': '#27AE60',      # Green
    'warning': '#F39C12',      # Orange
    'neutral': '#95A5A6',      # Gray
    'gold': '#F1C40F',         # Gold for luxury
    'purple': '#9B59B6',       # Purple
}

# Extended palette for multi-series
PALETTE = [
    '#1E3A5F', '#3498DB', '#27AE60', '#E74C3C', '#F39C12',
    '#9B59B6', '#1ABC9C', '#E67E22', '#34495E', '#16A085'
]

# Price segment colors (gradient from affordable to luxury)
SEGMENT_COLORS = {
    'Under 1M': '#27AE60',    # Green - affordable
    '1M-2M': '#3498DB',       # Blue - mid-market
    '2M-5M': '#F39C12',       # Orange - upper mid
    '5M-10M': '#E74C3C',      # Red - luxury
    '10M-20M': '#9B59B6',     # Purple - ultra-luxury
    '20M+': '#F1C40F',        # Gold - super prime
}


def setup_style():
    """Configure matplotlib style for professional charts."""
    plt.style.use('seaborn-v0_8-whitegrid')
    plt.rcParams.update({
        'font.family': 'sans-serif',
        'font.sans-serif': ['Arial', 'Helvetica', 'DejaVu Sans'],
        'font.size': 11,
        'axes.titlesize': 14,
        'axes.titleweight': 'bold',
        'axes.labelsize': 11,
        'xtick.labelsize': 10,
        'ytick.labelsize': 10,
        'legend.fontsize': 10,
        'figure.titlesize': 16,
        'figure.titleweight': 'bold',
        'axes.spines.top': False,
        'axes.spines.right': False,
        'axes.grid': True,
        'grid.alpha': 0.3,
    })


def format_aed(x, pos):
    """Format number as AED millions/billions."""
    if x >= 1e9:
        return f'AED {x/1e9:.1f}B'
    elif x >= 1e6:
        return f'AED {x/1e6:.0f}M'
    elif x >= 1e3:
        return f'AED {x/1e3:.0f}K'
    return f'AED {x:.0f}'


def format_number(x, pos):
    """Format large numbers with K/M suffix."""
    if x >= 1e6:
        return f'{x/1e6:.1f}M'
    elif x >= 1e3:
        return f'{x/1e3:.0f}K'
    return f'{x:.0f}'


class Report2025Visualizer:
    """Generates comprehensive visualizations for 2025 Annual Report."""

    def __init__(self):
        self.db_path = bq_settings.PM_DB_PATH
        self.con = duckdb.connect(str(self.db_path), read_only=True)
        self.output_dir = settings.CONTENT_OUTPUT_DIR / 'charts' / '2025_annual'
        self.output_dir.mkdir(parents=True, exist_ok=True)
        setup_style()

    def generate_all(self):
        """Generate all visualizations."""
        print("="*70)
        print("2025 ANNUAL REPORT VISUALIZATION PACKAGE")
        print("="*70)
        print(f"Output directory: {self.output_dir}")
        print()

        charts = []

        # 1. YoY Comparison Charts
        print("1. YoY COMPARISON CHARTS")
        charts.append(self.chart_yoy_metrics_comparison())
        charts.append(self.chart_monthly_comparison_overlay())

        # 2. Price Segment Charts
        print("\n2. PRICE SEGMENT CHARTS")
        charts.append(self.chart_price_segments_pyramid())
        charts.append(self.chart_price_segment_donut())

        # 3. Geographic Analysis
        print("\n3. GEOGRAPHIC ANALYSIS")
        charts.append(self.chart_top_areas_horizontal())
        charts.append(self.chart_area_bubble())

        # 4. Developer Analysis
        print("\n4. DEVELOPER ANALYSIS")
        charts.append(self.chart_developer_market_share())
        charts.append(self.chart_developer_treemap())

        # 5. Market Intelligence
        print("\n5. MARKET INTELLIGENCE")
        charts.append(self.chart_emerging_hotspots())
        charts.append(self.chart_price_momentum())
        charts.append(self.chart_record_transactions())

        # 6. Market Segments
        print("\n6. MARKET SEGMENTS")
        charts.append(self.chart_offplan_vs_ready())
        charts.append(self.chart_monthly_trend())

        print("\n" + "="*70)
        print("VISUALIZATION COMPLETE")
        print("="*70)
        print(f"\nGenerated {len([c for c in charts if c])} charts in: {self.output_dir}")

        return charts

    # =========================================================================
    # 1. YoY COMPARISON CHARTS
    # =========================================================================

    def chart_yoy_metrics_comparison(self):
        """Grouped bar chart comparing 2024 vs 2025 key metrics."""
        print("  - YoY Metrics Comparison...")

        data = self.con.execute("""
            SELECT
                EXTRACT(YEAR FROM instance_date::DATE) as year,
                COUNT(*) as transactions,
                SUM(actual_worth) as total_value,
                AVG(actual_worth) as avg_price,
                COUNT(CASE WHEN actual_worth >= 5000000 THEN 1 END) as luxury_count
            FROM transactions_clean
            WHERE instance_date >= '2024-01-01' AND instance_date < '2026-01-01'
            GROUP BY EXTRACT(YEAR FROM instance_date::DATE)
            ORDER BY year
        """).df()

        if len(data) < 2:
            print("    [SKIP] Insufficient data")
            return None

        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle('2025 vs 2024: Key Market Metrics Comparison', fontsize=16, fontweight='bold', y=1.02)

        metrics = [
            ('transactions', 'Transaction Volume', format_number, axes[0, 0]),
            ('total_value', 'Total Sales Value', format_aed, axes[0, 1]),
            ('avg_price', 'Average Price', format_aed, axes[1, 0]),
            ('luxury_count', 'Luxury Transactions (5M+)', format_number, axes[1, 1]),
        ]

        for col, title, formatter, ax in metrics:
            x = np.arange(2)
            values = data[col].values

            bars = ax.bar(x, values, color=[COLORS['neutral'], COLORS['primary']], width=0.6)

            # Add value labels
            for bar, val in zip(bars, values):
                height = bar.get_height()
                ax.annotate(formatter(val, None),
                           xy=(bar.get_x() + bar.get_width()/2, height),
                           xytext=(0, 5), textcoords='offset points',
                           ha='center', va='bottom', fontweight='bold', fontsize=11)

            # Add change percentage
            if values[0] > 0:
                pct_change = ((values[1] - values[0]) / values[0]) * 100
                color = COLORS['success'] if pct_change > 0 else COLORS['accent']
                ax.annotate(f'{pct_change:+.1f}%',
                           xy=(1, values[1]),
                           xytext=(30, 0), textcoords='offset points',
                           ha='left', va='center', fontweight='bold', fontsize=12, color=color)

            ax.set_xticks(x)
            ax.set_xticklabels(['2024', '2025'], fontweight='bold')
            ax.set_title(title, fontsize=12, pad=10)
            ax.yaxis.set_major_formatter(FuncFormatter(formatter))

        plt.tight_layout()
        path = self.output_dir / 'yoy_metrics_comparison.png'
        plt.savefig(path, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close()
        print(f"    [OK] {path.name}")
        return str(path)

    def chart_monthly_comparison_overlay(self):
        """Line chart with 2024 vs 2025 monthly overlay."""
        print("  - Monthly Comparison Overlay...")

        data = self.con.execute("""
            SELECT
                EXTRACT(YEAR FROM instance_date::DATE) as year,
                EXTRACT(MONTH FROM instance_date::DATE) as month,
                COUNT(*) as transactions
            FROM transactions_clean
            WHERE instance_date >= '2024-01-01' AND instance_date < '2026-01-01'
            GROUP BY year, month
            ORDER BY year, month
        """).df()

        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

        fig, ax = plt.subplots(figsize=(14, 6))

        data_2024 = data[data['year'] == 2024].sort_values('month')
        data_2025 = data[data['year'] == 2025].sort_values('month')

        ax.plot(range(len(data_2024)), data_2024['transactions'].values,
               marker='o', linewidth=2.5, color=COLORS['neutral'],
               label='2024', markersize=8)
        ax.fill_between(range(len(data_2024)), data_2024['transactions'].values,
                       alpha=0.1, color=COLORS['neutral'])

        ax.plot(range(len(data_2025)), data_2025['transactions'].values,
               marker='o', linewidth=2.5, color=COLORS['primary'],
               label='2025', markersize=8)
        ax.fill_between(range(len(data_2025)), data_2025['transactions'].values,
                       alpha=0.2, color=COLORS['primary'])

        ax.set_xticks(range(12))
        ax.set_xticklabels(months)
        ax.set_xlabel('Month', fontweight='bold')
        ax.set_ylabel('Transaction Count', fontweight='bold')
        ax.set_title('Monthly Transaction Volume: 2024 vs 2025', fontsize=14, fontweight='bold', pad=15)
        ax.yaxis.set_major_formatter(FuncFormatter(format_number))
        ax.legend(loc='upper left', frameon=True, fancybox=True, shadow=True)

        plt.tight_layout()
        path = self.output_dir / 'monthly_yoy_overlay.png'
        plt.savefig(path, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close()
        print(f"    [OK] {path.name}")
        return str(path)

    # =========================================================================
    # 2. PRICE SEGMENT CHARTS
    # =========================================================================

    def chart_price_segments_pyramid(self):
        """Horizontal bar chart as pyramid showing price segments."""
        print("  - Price Segments Pyramid...")

        data = self.con.execute("""
            SELECT
                CASE
                    WHEN actual_worth < 1000000 THEN 'Under 1M'
                    WHEN actual_worth < 2000000 THEN '1M-2M'
                    WHEN actual_worth < 5000000 THEN '2M-5M'
                    WHEN actual_worth < 10000000 THEN '5M-10M'
                    WHEN actual_worth < 20000000 THEN '10M-20M'
                    ELSE '20M+'
                END as segment,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as pct
            FROM transactions_clean
            WHERE instance_date >= '2025-01-01' AND instance_date < '2026-01-01'
            GROUP BY segment
        """).df()

        # Sort by segment order
        segment_order = ['Under 1M', '1M-2M', '2M-5M', '5M-10M', '10M-20M', '20M+']
        data['sort_order'] = data['segment'].apply(lambda x: segment_order.index(x))
        data = data.sort_values('sort_order')

        fig, ax = plt.subplots(figsize=(12, 7))

        colors = [SEGMENT_COLORS.get(seg, COLORS['neutral']) for seg in data['segment']]
        bars = ax.barh(data['segment'], data['count'], color=colors, height=0.7)

        # Add value labels
        for bar, count, pct in zip(bars, data['count'], data['pct']):
            width = bar.get_width()
            ax.annotate(f'{count:,} ({pct:.1f}%)',
                       xy=(width, bar.get_y() + bar.get_height()/2),
                       xytext=(5, 0), textcoords='offset points',
                       ha='left', va='center', fontweight='bold', fontsize=11)

        ax.set_xlabel('Number of Transactions', fontweight='bold')
        ax.set_title('2025 Market by Price Segment', fontsize=14, fontweight='bold', pad=15)
        ax.xaxis.set_major_formatter(FuncFormatter(format_number))

        # Add segment labels on the left
        ax.set_yticklabels([f'{seg}' for seg in data['segment']], fontweight='bold')

        plt.tight_layout()
        path = self.output_dir / 'price_segments_pyramid.png'
        plt.savefig(path, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close()
        print(f"    [OK] {path.name}")
        return str(path)

    def chart_price_segment_donut(self):
        """Donut chart for price segment distribution."""
        print("  - Price Segment Donut...")

        data = self.con.execute("""
            SELECT
                CASE
                    WHEN actual_worth < 1000000 THEN 'Under 1M'
                    WHEN actual_worth < 2000000 THEN '1M-2M'
                    WHEN actual_worth < 5000000 THEN '2M-5M'
                    WHEN actual_worth < 10000000 THEN '5M-10M'
                    WHEN actual_worth < 20000000 THEN '10M-20M'
                    ELSE '20M+'
                END as segment,
                COUNT(*) as count
            FROM transactions_clean
            WHERE instance_date >= '2025-01-01' AND instance_date < '2026-01-01'
            GROUP BY segment
        """).df()

        segment_order = ['Under 1M', '1M-2M', '2M-5M', '5M-10M', '10M-20M', '20M+']
        data['sort_order'] = data['segment'].apply(lambda x: segment_order.index(x))
        data = data.sort_values('sort_order')

        fig, ax = plt.subplots(figsize=(10, 10))

        colors = [SEGMENT_COLORS.get(seg, COLORS['neutral']) for seg in data['segment']]

        wedges, texts, autotexts = ax.pie(
            data['count'], labels=data['segment'], colors=colors,
            autopct='%1.1f%%', startangle=90, pctdistance=0.75,
            wedgeprops=dict(width=0.5, edgecolor='white', linewidth=2)
        )

        for autotext in autotexts:
            autotext.set_fontweight('bold')
            autotext.set_fontsize(10)

        # Center text
        total = data['count'].sum()
        ax.text(0, 0, f'{total:,}\nTransactions', ha='center', va='center',
               fontsize=14, fontweight='bold')

        ax.set_title('2025 Price Segment Distribution', fontsize=14, fontweight='bold', pad=20)

        plt.tight_layout()
        path = self.output_dir / 'price_segment_donut.png'
        plt.savefig(path, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close()
        print(f"    [OK] {path.name}")
        return str(path)

    # =========================================================================
    # 3. GEOGRAPHIC ANALYSIS
    # =========================================================================

    def chart_top_areas_horizontal(self):
        """Horizontal bar chart of top 10 areas."""
        print("  - Top Areas Horizontal Bar...")

        data = self.con.execute("""
            SELECT
                area_name_en,
                COUNT(*) as transactions,
                AVG(actual_worth) as avg_price
            FROM transactions_clean
            WHERE instance_date >= '2025-01-01' AND instance_date < '2026-01-01'
              AND area_name_en IS NOT NULL
            GROUP BY area_name_en
            ORDER BY transactions DESC
            LIMIT 10
        """).df()

        fig, ax = plt.subplots(figsize=(12, 8))

        colors = [COLORS['primary'] if i == 0 else COLORS['secondary']
                 for i in range(len(data))]

        y_pos = range(len(data))
        bars = ax.barh(y_pos, data['transactions'], color=colors, height=0.7)

        ax.set_yticks(y_pos)
        ax.set_yticklabels(data['area_name_en'], fontsize=11)
        ax.invert_yaxis()

        # Add value labels
        for bar, tx in zip(bars, data['transactions']):
            ax.annotate(f'{tx:,}',
                       xy=(bar.get_width(), bar.get_y() + bar.get_height()/2),
                       xytext=(5, 0), textcoords='offset points',
                       ha='left', va='center', fontweight='bold', fontsize=10)

        ax.set_xlabel('Number of Transactions', fontweight='bold')
        ax.set_title('Top 10 Areas by Transaction Volume (2025)', fontsize=14, fontweight='bold', pad=15)
        ax.xaxis.set_major_formatter(FuncFormatter(format_number))

        plt.tight_layout()
        path = self.output_dir / 'top_areas_horizontal.png'
        plt.savefig(path, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close()
        print(f"    [OK] {path.name}")
        return str(path)

    def chart_area_bubble(self):
        """Bubble chart: X=transactions, Y=avg price, size=total value."""
        print("  - Area Bubble Chart...")

        data = self.con.execute("""
            SELECT
                area_name_en,
                COUNT(*) as transactions,
                AVG(actual_worth) as avg_price,
                SUM(actual_worth) as total_value,
                COUNT(CASE WHEN reg_type_en LIKE '%Off%' THEN 1 END) * 100.0 / COUNT(*) as offplan_pct
            FROM transactions_clean
            WHERE instance_date >= '2025-01-01' AND instance_date < '2026-01-01'
              AND area_name_en IS NOT NULL
            GROUP BY area_name_en
            HAVING COUNT(*) >= 1000
            ORDER BY transactions DESC
            LIMIT 15
        """).df()

        fig, ax = plt.subplots(figsize=(14, 10))

        # Normalize sizes for visualization
        sizes = (data['total_value'] / data['total_value'].max()) * 2000 + 100

        # Color by off-plan percentage
        scatter = ax.scatter(
            data['transactions'],
            data['avg_price'],
            s=sizes,
            c=data['offplan_pct'],
            cmap='RdYlBu_r',
            alpha=0.7,
            edgecolors='white',
            linewidths=2
        )

        # Add labels for top areas
        for _, row in data.head(8).iterrows():
            ax.annotate(
                row['area_name_en'],
                xy=(row['transactions'], row['avg_price']),
                xytext=(10, 10), textcoords='offset points',
                fontsize=9, fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8)
            )

        cbar = plt.colorbar(scatter, ax=ax, label='Off-Plan %', shrink=0.8)
        cbar.ax.set_ylabel('Off-Plan %', fontweight='bold')

        ax.set_xlabel('Transaction Count', fontweight='bold')
        ax.set_ylabel('Average Price (AED)', fontweight='bold')
        ax.set_title('Area Performance: Volume vs Price (Bubble Size = Total Value)',
                    fontsize=14, fontweight='bold', pad=15)
        ax.xaxis.set_major_formatter(FuncFormatter(format_number))
        ax.yaxis.set_major_formatter(FuncFormatter(format_aed))

        plt.tight_layout()
        path = self.output_dir / 'area_bubble_chart.png'
        plt.savefig(path, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close()
        print(f"    [OK] {path.name}")
        return str(path)

    # =========================================================================
    # 4. DEVELOPER ANALYSIS
    # =========================================================================

    def chart_developer_market_share(self):
        """Horizontal bar chart of developer market share."""
        print("  - Developer Market Share...")

        data = self.con.execute("""
            SELECT
                master_project_en as developer,
                COUNT(*) as transactions,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM transactions_clean
                    WHERE instance_date >= '2025-01-01' AND instance_date < '2026-01-01'
                    AND master_project_en IS NOT NULL), 1) as market_share
            FROM transactions_clean
            WHERE instance_date >= '2025-01-01' AND instance_date < '2026-01-01'
              AND master_project_en IS NOT NULL
              AND master_project_en != ''
            GROUP BY master_project_en
            ORDER BY transactions DESC
            LIMIT 10
        """).df()

        fig, ax = plt.subplots(figsize=(12, 8))

        colors = [PALETTE[i % len(PALETTE)] for i in range(len(data))]
        y_pos = range(len(data))

        bars = ax.barh(y_pos, data['market_share'], color=colors, height=0.7)

        ax.set_yticks(y_pos)
        ax.set_yticklabels(data['developer'], fontsize=11)
        ax.invert_yaxis()

        # Add value labels
        for bar, share, tx in zip(bars, data['market_share'], data['transactions']):
            ax.annotate(f'{share:.1f}% ({tx:,} sales)',
                       xy=(bar.get_width(), bar.get_y() + bar.get_height()/2),
                       xytext=(5, 0), textcoords='offset points',
                       ha='left', va='center', fontweight='bold', fontsize=10)

        ax.set_xlabel('Market Share (%)', fontweight='bold')
        ax.set_title('Top 10 Developers by Market Share (2025)', fontsize=14, fontweight='bold', pad=15)

        plt.tight_layout()
        path = self.output_dir / 'developer_market_share.png'
        plt.savefig(path, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close()
        print(f"    [OK] {path.name}")
        return str(path)

    def chart_developer_treemap(self):
        """Treemap of developer market share (using nested rectangles)."""
        print("  - Developer Treemap...")

        try:
            import squarify
        except ImportError:
            print("    [SKIP] squarify not installed")
            return None

        data = self.con.execute("""
            SELECT
                master_project_en as developer,
                COUNT(*) as transactions
            FROM transactions_clean
            WHERE instance_date >= '2025-01-01' AND instance_date < '2026-01-01'
              AND master_project_en IS NOT NULL
              AND master_project_en != ''
            GROUP BY master_project_en
            ORDER BY transactions DESC
            LIMIT 15
        """).df()

        fig, ax = plt.subplots(figsize=(14, 10))

        colors = [PALETTE[i % len(PALETTE)] for i in range(len(data))]

        squarify.plot(
            sizes=data['transactions'].values,
            label=[f"{d}\n{t:,}" for d, t in zip(data['developer'], data['transactions'])],
            color=colors,
            alpha=0.8,
            ax=ax,
            text_kwargs={'fontsize': 9, 'fontweight': 'bold', 'wrap': True}
        )

        ax.axis('off')
        ax.set_title('Developer Market Share Treemap (2025)', fontsize=14, fontweight='bold', pad=15)

        plt.tight_layout()
        path = self.output_dir / 'developer_treemap.png'
        plt.savefig(path, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close()
        print(f"    [OK] {path.name}")
        return str(path)

    # =========================================================================
    # 5. MARKET INTELLIGENCE
    # =========================================================================

    def chart_emerging_hotspots(self):
        """Bar chart showing emerging area growth rates."""
        print("  - Emerging Hotspots...")

        # Calculate YoY growth for areas
        data = self.con.execute("""
            WITH area_2024 AS (
                SELECT area_name_en, COUNT(*) as tx_2024
                FROM transactions_clean
                WHERE instance_date >= '2024-01-01' AND instance_date < '2025-01-01'
                GROUP BY area_name_en
            ),
            area_2025 AS (
                SELECT area_name_en, COUNT(*) as tx_2025
                FROM transactions_clean
                WHERE instance_date >= '2025-01-01' AND instance_date < '2026-01-01'
                GROUP BY area_name_en
            )
            SELECT
                a25.area_name_en,
                COALESCE(a24.tx_2024, 0) as tx_2024,
                a25.tx_2025,
                CASE
                    WHEN COALESCE(a24.tx_2024, 0) > 0
                    THEN ((a25.tx_2025 - a24.tx_2024) * 100.0 / a24.tx_2024)
                    ELSE 100
                END as growth_pct
            FROM area_2025 a25
            LEFT JOIN area_2024 a24 ON a25.area_name_en = a24.area_name_en
            WHERE a25.tx_2025 >= 500
              AND COALESCE(a24.tx_2024, 0) >= 100
            ORDER BY growth_pct DESC
            LIMIT 10
        """).df()

        fig, ax = plt.subplots(figsize=(12, 8))

        colors = [COLORS['success'] if g > 0 else COLORS['accent'] for g in data['growth_pct']]
        y_pos = range(len(data))

        bars = ax.barh(y_pos, data['growth_pct'], color=colors, height=0.7)

        ax.set_yticks(y_pos)
        ax.set_yticklabels(data['area_name_en'], fontsize=11)
        ax.invert_yaxis()
        ax.axvline(x=0, color='black', linewidth=0.5)

        # Add value labels
        for bar, pct in zip(bars, data['growth_pct']):
            ax.annotate(f'{pct:+.0f}%',
                       xy=(bar.get_width(), bar.get_y() + bar.get_height()/2),
                       xytext=(5 if pct > 0 else -30, 0), textcoords='offset points',
                       ha='left' if pct > 0 else 'right', va='center',
                       fontweight='bold', fontsize=10)

        ax.set_xlabel('YoY Transaction Growth (%)', fontweight='bold')
        ax.set_title('Top Emerging Areas by Transaction Growth (2024 to 2025)',
                    fontsize=14, fontweight='bold', pad=15)

        plt.tight_layout()
        path = self.output_dir / 'emerging_hotspots.png'
        plt.savefig(path, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close()
        print(f"    [OK] {path.name}")
        return str(path)

    def chart_price_momentum(self):
        """Area chart showing quarterly price momentum."""
        print("  - Price Momentum...")

        data = self.con.execute("""
            SELECT
                DATE_TRUNC('quarter', instance_date::DATE) as quarter,
                AVG(actual_worth) as avg_price,
                COUNT(*) as tx_count
            FROM transactions_clean
            WHERE instance_date >= '2024-01-01' AND instance_date < '2026-01-01'
            GROUP BY quarter
            ORDER BY quarter
        """).df()

        fig, ax = plt.subplots(figsize=(12, 6))

        ax.fill_between(range(len(data)), data['avg_price'], alpha=0.3, color=COLORS['primary'])
        ax.plot(range(len(data)), data['avg_price'], marker='o', linewidth=2.5,
               color=COLORS['primary'], markersize=10)

        # Add value annotations
        for i, (price, quarter) in enumerate(zip(data['avg_price'], data['quarter'])):
            ax.annotate(f'AED {price/1e6:.2f}M',
                       xy=(i, price), xytext=(0, 15), textcoords='offset points',
                       ha='center', fontweight='bold', fontsize=10)

        labels = []
        for q in data['quarter']:
            if hasattr(q, 'year') and hasattr(q, 'month'):
                quarter_num = (q.month - 1) // 3 + 1
                labels.append(f"{q.year} Q{quarter_num}")
            else:
                labels.append(str(q)[:7])
        ax.set_xticks(range(len(data)))
        ax.set_xticklabels(labels, rotation=45, ha='right')

        ax.set_ylabel('Average Price (AED)', fontweight='bold')
        ax.set_title('Quarterly Price Momentum (2024-2025)', fontsize=14, fontweight='bold', pad=15)
        ax.yaxis.set_major_formatter(FuncFormatter(format_aed))

        plt.tight_layout()
        path = self.output_dir / 'price_momentum.png'
        plt.savefig(path, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close()
        print(f"    [OK] {path.name}")
        return str(path)

    def chart_record_transactions(self):
        """Infographic-style chart of record transactions."""
        print("  - Record Transactions...")

        data = self.con.execute("""
            SELECT
                area_name_en,
                property_type_en,
                actual_worth,
                master_project_en
            FROM transactions_clean
            WHERE instance_date >= '2025-01-01' AND instance_date < '2026-01-01'
            ORDER BY actual_worth DESC
            LIMIT 5
        """).df()

        fig, ax = plt.subplots(figsize=(12, 6))

        colors = [COLORS['gold'], '#C0C0C0', '#CD7F32', COLORS['primary'], COLORS['secondary']]
        y_pos = range(len(data))

        bars = ax.barh(y_pos, data['actual_worth'], color=colors, height=0.6)

        ax.set_yticks(y_pos)
        ax.set_yticklabels([f"#{i+1}" for i in range(len(data))], fontsize=14, fontweight='bold')
        ax.invert_yaxis()

        # Add detailed labels
        for bar, row in zip(bars, data.itertuples()):
            ax.annotate(
                f'AED {row.actual_worth/1e6:.0f}M\n{row.property_type_en} in {row.area_name_en}',
                xy=(bar.get_width(), bar.get_y() + bar.get_height()/2),
                xytext=(10, 0), textcoords='offset points',
                ha='left', va='center', fontsize=11, fontweight='bold'
            )

        ax.set_xlabel('Transaction Value (AED)', fontweight='bold')
        ax.set_title('Top 5 Record Transactions in 2025', fontsize=14, fontweight='bold', pad=15)
        ax.xaxis.set_major_formatter(FuncFormatter(format_aed))

        plt.tight_layout()
        path = self.output_dir / 'record_transactions.png'
        plt.savefig(path, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close()
        print(f"    [OK] {path.name}")
        return str(path)

    # =========================================================================
    # 6. MARKET SEGMENTS
    # =========================================================================

    def chart_offplan_vs_ready(self):
        """Stacked bar showing off-plan vs ready monthly trend."""
        print("  - Off-Plan vs Ready Trend...")

        data = self.con.execute("""
            SELECT
                EXTRACT(MONTH FROM instance_date::DATE) as month,
                COUNT(CASE WHEN reg_type_en LIKE '%Off%' THEN 1 END) as offplan,
                COUNT(CASE WHEN reg_type_en NOT LIKE '%Off%' THEN 1 END) as ready
            FROM transactions_clean
            WHERE instance_date >= '2025-01-01' AND instance_date < '2026-01-01'
            GROUP BY month
            ORDER BY month
        """).df()

        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

        fig, ax = plt.subplots(figsize=(14, 6))

        x = np.arange(len(data))
        width = 0.7

        ax.bar(x, data['offplan'], width, label='Off-Plan', color=COLORS['primary'])
        ax.bar(x, data['ready'], width, bottom=data['offplan'], label='Ready', color=COLORS['secondary'])

        ax.set_xticks(x)
        ax.set_xticklabels([months[int(m)-1] for m in data['month']])
        ax.set_xlabel('Month', fontweight='bold')
        ax.set_ylabel('Transaction Count', fontweight='bold')
        ax.set_title('Off-Plan vs Ready Transactions by Month (2025)', fontsize=14, fontweight='bold', pad=15)
        ax.yaxis.set_major_formatter(FuncFormatter(format_number))
        ax.legend(loc='upper right', frameon=True, fancybox=True)

        plt.tight_layout()
        path = self.output_dir / 'offplan_vs_ready.png'
        plt.savefig(path, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close()
        print(f"    [OK] {path.name}")
        return str(path)

    def chart_monthly_trend(self):
        """Line chart of monthly transaction trend."""
        print("  - Monthly Trend...")

        data = self.con.execute("""
            SELECT
                EXTRACT(MONTH FROM instance_date::DATE) as month,
                COUNT(*) as transactions,
                SUM(actual_worth) as total_value,
                AVG(actual_worth) as avg_price
            FROM transactions_clean
            WHERE instance_date >= '2025-01-01' AND instance_date < '2026-01-01'
            GROUP BY month
            ORDER BY month
        """).df()

        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

        fig, ax1 = plt.subplots(figsize=(14, 6))

        # Transaction volume bars
        ax1.bar(range(len(data)), data['transactions'], color=COLORS['primary'], alpha=0.7,
               label='Transaction Count')
        ax1.set_ylabel('Transaction Count', color=COLORS['primary'], fontweight='bold')
        ax1.tick_params(axis='y', labelcolor=COLORS['primary'])
        ax1.yaxis.set_major_formatter(FuncFormatter(format_number))

        # Average price line on secondary axis
        ax2 = ax1.twinx()
        ax2.plot(range(len(data)), data['avg_price'], marker='o', linewidth=2.5,
                color=COLORS['accent'], label='Average Price')
        ax2.set_ylabel('Average Price (AED)', color=COLORS['accent'], fontweight='bold')
        ax2.tick_params(axis='y', labelcolor=COLORS['accent'])
        ax2.yaxis.set_major_formatter(FuncFormatter(format_aed))

        ax1.set_xticks(range(len(data)))
        ax1.set_xticklabels([months[int(m)-1] for m in data['month']])
        ax1.set_xlabel('Month', fontweight='bold')
        ax1.set_title('Monthly Transaction Volume & Average Price (2025)',
                     fontsize=14, fontweight='bold', pad=15)

        # Combined legend
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', frameon=True)

        plt.tight_layout()
        path = self.output_dir / 'monthly_trend.png'
        plt.savefig(path, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close()
        print(f"    [OK] {path.name}")
        return str(path)


if __name__ == "__main__":
    visualizer = Report2025Visualizer()
    visualizer.generate_all()
