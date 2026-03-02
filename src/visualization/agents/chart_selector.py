# src/visualization/agents/chart_selector.py
"""
ChartSelectorAgent - Intelligently selects optimal chart types for data.

Responsible for:
- Analyzing data characteristics
- Selecting appropriate chart types
- Configuring chart parameters
- Generating Chart.js configurations
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum


class ChartType(Enum):
    """Available chart types."""
    LINE = "line"
    BAR = "bar"
    HORIZONTAL_BAR = "horizontal_bar"
    STACKED_BAR = "stacked_bar"
    GROUPED_BAR = "grouped_bar"
    PIE = "pie"
    DONUT = "donut"
    SCATTER = "scatter"
    AREA = "area"


@dataclass
class ChartConfig:
    """Configuration for a chart."""
    chart_type: ChartType
    title: str
    data: Dict[str, Any]
    options: Dict[str, Any]
    static_options: Dict[str, Any]  # matplotlib-specific
    interactive_options: Dict[str, Any]  # Chart.js-specific


class ChartSelectorAgent:
    """
    Agent for selecting and configuring optimal chart types.

    Uses heuristics based on:
    - Data shape (time-series, categorical, hierarchical)
    - Number of data points
    - Data relationships (comparison, composition, distribution)
    - Report context
    """

    # Chart type recommendations by data pattern
    CHART_RECOMMENDATIONS = {
        'trend': ChartType.LINE,
        'comparison': ChartType.GROUPED_BAR,
        'ranking': ChartType.HORIZONTAL_BAR,
        'composition': ChartType.PIE,
        'distribution': ChartType.BAR,
        'proportion': ChartType.DONUT,
        'time_series': ChartType.LINE,
        'correlation': ChartType.SCATTER,
    }

    # Report type to chart mappings
    REPORT_CHARTS = {
        'market': [
            ('monthly_trend', ChartType.LINE, 'Transaction Trend'),
            ('market_segments', ChartType.DONUT, 'Market Segments'),
            ('top_areas', ChartType.HORIZONTAL_BAR, 'Top Areas'),
            ('price_comparison', ChartType.GROUPED_BAR, 'Price Comparison'),
        ],
        'area_guide': [
            ('price_trend', ChartType.LINE, 'Price Trend'),
            ('property_types', ChartType.PIE, 'Property Type Mix'),
            ('developer_share', ChartType.HORIZONTAL_BAR, 'Developer Market Share'),
        ],
        'luxury': [
            ('luxury_trend', ChartType.LINE, 'Luxury Transaction Trend'),
            ('luxury_tiers', ChartType.DONUT, 'Luxury Tiers Distribution'),
            ('top_luxury_areas', ChartType.HORIZONTAL_BAR, 'Top Luxury Areas'),
            ('ultra_luxury_trend', ChartType.AREA, 'Ultra-Luxury Segment'),
        ],
        'offplan': [
            ('offplan_vs_ready', ChartType.STACKED_BAR, 'Off-Plan vs Ready'),
            ('developer_pipeline', ChartType.HORIZONTAL_BAR, 'Developer Pipeline'),
            ('offplan_trend', ChartType.LINE, 'Off-Plan Trend'),
        ],
        'developer': [
            ('transaction_volume', ChartType.LINE, 'Transaction Volume'),
            ('project_areas', ChartType.STACKED_BAR, 'Projects by Area'),
            ('price_range', ChartType.BAR, 'Price Range Distribution'),
        ],
    }

    def __init__(self):
        """Initialize the chart selector agent."""
        pass

    def select_chart_type(
        self,
        data: Dict[str, Any],
        context: str = None,
        data_pattern: str = None
    ) -> ChartType:
        """
        Select optimal chart type based on data and context.

        Args:
            data: The data to visualize
            context: Report context ('market', 'luxury', etc.)
            data_pattern: Explicit data pattern hint

        Returns:
            Recommended ChartType
        """
        # Use explicit pattern if provided
        if data_pattern and data_pattern in self.CHART_RECOMMENDATIONS:
            return self.CHART_RECOMMENDATIONS[data_pattern]

        # Analyze data characteristics
        labels = data.get('labels', [])
        values = data.get('values', [])
        series = data.get('series', [])
        stacks = data.get('stacks', [])
        groups = data.get('groups', [])

        # Multiple series suggests comparison or trend
        if series and len(series) > 1:
            if self._is_time_series(labels):
                return ChartType.LINE
            return ChartType.GROUPED_BAR

        # Stacked data suggests composition
        if stacks:
            return ChartType.STACKED_BAR

        # Grouped data suggests comparison
        if groups:
            return ChartType.GROUPED_BAR

        # Time series data
        if self._is_time_series(labels):
            return ChartType.LINE

        # Few categories (2-6) with proportions
        if 2 <= len(labels) <= 6 and self._looks_like_proportions(values):
            return ChartType.DONUT

        # Rankings (many items to compare)
        if len(labels) > 5:
            return ChartType.HORIZONTAL_BAR

        # Default to vertical bar
        return ChartType.BAR

    def get_charts_for_report(
        self,
        report_type: str,
        data_available: Dict[str, Dict[str, Any]]
    ) -> List[ChartConfig]:
        """
        Get recommended charts for a report type.

        Args:
            report_type: Type of report ('market', 'luxury', etc.)
            data_available: Dictionary of available data by chart name

        Returns:
            List of ChartConfig objects
        """
        charts = []
        chart_specs = self.REPORT_CHARTS.get(report_type, [])

        for chart_name, chart_type, title in chart_specs:
            if chart_name in data_available:
                data = data_available[chart_name]

                config = self.create_chart_config(
                    chart_type=chart_type,
                    title=title,
                    data=data,
                    chart_name=chart_name
                )
                charts.append(config)

        return charts

    def create_chart_config(
        self,
        chart_type: ChartType,
        title: str,
        data: Dict[str, Any],
        chart_name: str = None
    ) -> ChartConfig:
        """
        Create a complete chart configuration.

        Args:
            chart_type: Type of chart
            title: Chart title
            data: Chart data
            chart_name: Identifier for the chart

        Returns:
            ChartConfig object
        """
        # Base options
        options = {
            'title': title,
            'chart_name': chart_name,
        }

        # Static (matplotlib) options
        static_options = self._get_matplotlib_options(chart_type, data)

        # Interactive (Chart.js) options
        interactive_options = self._get_chartjs_options(chart_type, data)

        return ChartConfig(
            chart_type=chart_type,
            title=title,
            data=data,
            options=options,
            static_options=static_options,
            interactive_options=interactive_options
        )

    def _get_matplotlib_options(
        self,
        chart_type: ChartType,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Get matplotlib-specific rendering options."""
        options = {
            'show_values': True,
            'value_format': 'number',
        }

        if chart_type == ChartType.LINE:
            options.update({
                'show_markers': True,
                'fill_area': False,
                'show_trend': False,
            })
        elif chart_type in (ChartType.PIE, ChartType.DONUT):
            options.update({
                'show_percentages': True,
                'explode_largest': True,
            })
        elif chart_type == ChartType.HORIZONTAL_BAR:
            options.update({
                'show_rank': True,
                'use_area_colors': 'area' in str(data.get('metric', '')).lower()
            })

        # Detect if values are currency
        values = data.get('values', [])
        if values and max(values) > 100000:
            options['value_format'] = 'currency'

        return options

    def _get_chartjs_options(
        self,
        chart_type: ChartType,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Get Chart.js-specific configuration."""
        from ..config.colors import get_chartjs_colors, COLORS
        from ..config.styles import CHARTJS_OPTIONS

        options = CHARTJS_OPTIONS.copy()

        # Chart type specific configurations
        if chart_type == ChartType.LINE:
            options['elements'] = {
                'line': {'tension': 0.3},
                'point': {'radius': 4, 'hoverRadius': 6}
            }
        elif chart_type in (ChartType.PIE, ChartType.DONUT):
            options['plugins']['legend']['position'] = 'right'
            if 'scales' in options:
                del options['scales']
            if chart_type == ChartType.DONUT:
                options['cutout'] = '50%'
        elif chart_type == ChartType.HORIZONTAL_BAR:
            options['indexAxis'] = 'y'

        # Add colors
        n_items = len(data.get('labels', []))
        options['colors'] = get_chartjs_colors(n_items)

        return options

    def _is_time_series(self, labels: List[str]) -> bool:
        """Check if labels represent time series."""
        time_indicators = [
            'jan', 'feb', 'mar', 'apr', 'may', 'jun',
            'jul', 'aug', 'sep', 'oct', 'nov', 'dec',
            'q1', 'q2', 'q3', 'q4',
            'h1', 'h2',
            '2023', '2024', '2025'
        ]

        if not labels:
            return False

        sample = str(labels[0]).lower()
        return any(ind in sample for ind in time_indicators)

    def _looks_like_proportions(self, values: List[float]) -> bool:
        """Check if values look like parts of a whole."""
        if not values or len(values) < 2:
            return False

        total = sum(values)
        if total == 0:
            return False

        # Check if values could be percentages or counts adding to a total
        normalized = [v / total for v in values]
        return all(0 < n < 1 for n in normalized)

    def suggest_insights(
        self,
        chart_type: ChartType,
        data: Dict[str, Any]
    ) -> List[str]:
        """
        Suggest key insights to highlight for a chart.

        Args:
            chart_type: Type of chart
            data: Chart data

        Returns:
            List of insight prompts for the storyteller agent
        """
        insights = []
        values = data.get('values', [])
        labels = data.get('labels', [])

        if not values:
            return insights

        if chart_type == ChartType.LINE:
            # Trend insights
            if len(values) >= 2:
                change = (values[-1] - values[0]) / values[0] * 100 if values[0] else 0
                insights.append(f"Overall trend: {'up' if change > 0 else 'down'} {abs(change):.1f}%")

                # Peak detection
                max_idx = values.index(max(values))
                insights.append(f"Peak at {labels[max_idx]} with value {max(values):,.0f}")

        elif chart_type == ChartType.HORIZONTAL_BAR:
            # Top performer
            if labels and values:
                insights.append(f"Leader: {labels[0]} with {values[0]:,.0f}")

                # Gap analysis
                if len(values) >= 2:
                    gap = (values[0] - values[1]) / values[1] * 100 if values[1] else 0
                    insights.append(f"Lead margin: {gap:.1f}% over second place")

        elif chart_type in (ChartType.PIE, ChartType.DONUT):
            # Dominant segment
            total = sum(values)
            if total > 0:
                max_idx = values.index(max(values))
                pct = values[max_idx] / total * 100
                insights.append(f"Dominant: {labels[max_idx]} at {pct:.1f}%")

        return insights
