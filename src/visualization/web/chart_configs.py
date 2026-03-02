# src/visualization/web/chart_configs.py
"""
Chart.js configuration generator for interactive web visualizations.

Generates JSON configurations that can be used by the frontend
Chart.js library to render interactive charts.
"""

from typing import Dict, List, Any, Optional
from ..agents.chart_selector import ChartType
from ..config.colors import (
    COLORS, get_palette, hex_to_rgba,
    get_segment_colors, get_chartjs_colors
)
from ..config.styles import CHARTJS_OPTIONS, format_currency, format_number


class ChartJSConfigGenerator:
    """
    Generator for Chart.js compatible configurations.

    Produces JSON configurations that can be directly consumed
    by Chart.js on the frontend.
    """

    def __init__(self):
        """Initialize the config generator."""
        self.default_options = CHARTJS_OPTIONS.copy()

    def generate(
        self,
        chart_type: ChartType,
        data: Dict[str, Any],
        title: str = None,
        custom_options: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Generate Chart.js configuration.

        Args:
            chart_type: Type of chart
            data: Chart data with labels and values
            title: Chart title
            custom_options: Override default options

        Returns:
            Chart.js configuration dictionary
        """
        # Map chart types to Chart.js types
        type_map = {
            ChartType.LINE: 'line',
            ChartType.BAR: 'bar',
            ChartType.HORIZONTAL_BAR: 'bar',
            ChartType.STACKED_BAR: 'bar',
            ChartType.GROUPED_BAR: 'bar',
            ChartType.PIE: 'pie',
            ChartType.DONUT: 'doughnut',
            ChartType.SCATTER: 'scatter',
            ChartType.AREA: 'line',
        }

        chartjs_type = type_map.get(chart_type, 'bar')

        # Build configuration
        config = {
            'type': chartjs_type,
            'data': self._build_data(chart_type, data),
            'options': self._build_options(chart_type, title, custom_options)
        }

        return config

    def generate_line_config(
        self,
        data: Dict[str, Any],
        title: str = None,
        fill: bool = False,
        tension: float = 0.3
    ) -> Dict[str, Any]:
        """Generate configuration for line chart."""
        labels = data.get('labels', [])
        series = data.get('series', [])
        palette = get_palette()

        datasets = []

        if series:
            for i, s in enumerate(series):
                color = s.get('color') or palette[i % len(palette)]
                datasets.append({
                    'label': s['name'],
                    'data': s['values'],
                    'borderColor': color,
                    'backgroundColor': hex_to_rgba(color, 0.1) if fill else 'transparent',
                    'borderWidth': 2,
                    'fill': fill,
                    'tension': tension,
                    'pointRadius': 4,
                    'pointHoverRadius': 6,
                    'pointBackgroundColor': '#ffffff',
                    'pointBorderColor': color,
                    'pointBorderWidth': 2,
                })
        else:
            color = COLORS['primary']
            datasets.append({
                'label': data.get('name', 'Value'),
                'data': data.get('values', []),
                'borderColor': color,
                'backgroundColor': hex_to_rgba(color, 0.1) if fill else 'transparent',
                'borderWidth': 2,
                'fill': fill,
                'tension': tension,
                'pointRadius': 4,
                'pointHoverRadius': 6,
            })

        return {
            'type': 'line',
            'data': {
                'labels': labels,
                'datasets': datasets
            },
            'options': self._get_line_options(title)
        }

    def generate_bar_config(
        self,
        data: Dict[str, Any],
        title: str = None,
        horizontal: bool = False,
        stacked: bool = False
    ) -> Dict[str, Any]:
        """Generate configuration for bar chart."""
        labels = data.get('labels', [])
        palette = get_palette()

        # Handle grouped/stacked bars
        groups = data.get('groups', [])
        stacks = data.get('stacks', [])

        if groups:
            datasets = []
            for i, group in enumerate(groups):
                color = group.get('color') or palette[i % len(palette)]
                datasets.append({
                    'label': group['name'],
                    'data': group['values'],
                    'backgroundColor': hex_to_rgba(color, 0.8),
                    'borderColor': color,
                    'borderWidth': 1,
                })
        elif stacks:
            datasets = []
            for i, stack in enumerate(stacks):
                color = stack.get('color') or palette[i % len(palette)]
                datasets.append({
                    'label': stack['name'],
                    'data': stack['values'],
                    'backgroundColor': hex_to_rgba(color, 0.8),
                    'borderColor': color,
                    'borderWidth': 1,
                    'stack': 'stack0',
                })
        else:
            colors = data.get('colors') or [COLORS['primary']] * len(labels)
            datasets = [{
                'label': data.get('name', 'Value'),
                'data': data.get('values', []),
                'backgroundColor': [hex_to_rgba(c, 0.8) for c in colors],
                'borderColor': colors,
                'borderWidth': 1,
            }]

        config = {
            'type': 'bar',
            'data': {
                'labels': labels,
                'datasets': datasets
            },
            'options': self._get_bar_options(title, horizontal, stacked)
        }

        return config

    def generate_pie_config(
        self,
        data: Dict[str, Any],
        title: str = None,
        donut: bool = False
    ) -> Dict[str, Any]:
        """Generate configuration for pie/donut chart."""
        labels = data.get('labels', [])
        values = data.get('values', [])
        colors = data.get('colors') or get_palette(len(labels))

        segment_type = data.get('segment_type')
        if segment_type:
            segment_colors = get_segment_colors()
            color_map = {
                'offplan_ready': {
                    'Off-Plan': segment_colors['offplan'],
                    'Ready': segment_colors['ready'],
                },
                'luxury_tiers': {
                    'Ultra Luxury (10M+)': segment_colors['ultra_luxury'],
                    'Luxury (5-10M)': segment_colors['luxury'],
                    'Mid-Range (2-5M)': segment_colors['mid_range'],
                    'Standard (<2M)': segment_colors['affordable'],
                }
            }
            if segment_type in color_map:
                colors = [color_map[segment_type].get(label, colors[i]) for i, label in enumerate(labels)]

        config = {
            'type': 'doughnut' if donut else 'pie',
            'data': {
                'labels': labels,
                'datasets': [{
                    'data': values,
                    'backgroundColor': [hex_to_rgba(c, 0.8) for c in colors],
                    'borderColor': colors,
                    'borderWidth': 2,
                    'hoverOffset': 10,
                }]
            },
            'options': self._get_pie_options(title, donut)
        }

        return config

    def _build_data(
        self,
        chart_type: ChartType,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Build Chart.js data object."""
        if chart_type == ChartType.LINE or chart_type == ChartType.AREA:
            config = self.generate_line_config(
                data,
                fill=(chart_type == ChartType.AREA)
            )
            return config['data']

        elif chart_type in (ChartType.BAR, ChartType.HORIZONTAL_BAR):
            config = self.generate_bar_config(
                data,
                horizontal=(chart_type == ChartType.HORIZONTAL_BAR)
            )
            return config['data']

        elif chart_type == ChartType.STACKED_BAR:
            config = self.generate_bar_config(data, stacked=True)
            return config['data']

        elif chart_type == ChartType.GROUPED_BAR:
            config = self.generate_bar_config(data)
            return config['data']

        elif chart_type in (ChartType.PIE, ChartType.DONUT):
            config = self.generate_pie_config(
                data,
                donut=(chart_type == ChartType.DONUT)
            )
            return config['data']

        # Default
        return {
            'labels': data.get('labels', []),
            'datasets': [{
                'data': data.get('values', []),
                'backgroundColor': get_chartjs_colors(len(data.get('labels', [])))['backgroundColor']
            }]
        }

    def _build_options(
        self,
        chart_type: ChartType,
        title: str = None,
        custom_options: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Build Chart.js options object."""
        options = self.default_options.copy()

        # Add title
        if title:
            options['plugins'] = options.get('plugins', {}).copy()
            options['plugins']['title'] = {
                'display': True,
                'text': title,
                'font': {'size': 16, 'weight': 'bold'},
                'padding': {'bottom': 20}
            }

        # Chart type specific options
        if chart_type == ChartType.HORIZONTAL_BAR:
            options['indexAxis'] = 'y'

        elif chart_type == ChartType.STACKED_BAR:
            options['scales'] = {
                'x': {'stacked': True},
                'y': {'stacked': True}
            }

        elif chart_type in (ChartType.PIE, ChartType.DONUT):
            if 'scales' in options:
                del options['scales']
            options['plugins']['legend']['position'] = 'right'
            if chart_type == ChartType.DONUT:
                options['cutout'] = '50%'

        # Apply custom options
        if custom_options:
            options = self._deep_merge(options, custom_options)

        return options

    def _get_line_options(self, title: str = None) -> Dict[str, Any]:
        """Get options for line chart."""
        options = self.default_options.copy()

        options['elements'] = {
            'line': {'tension': 0.3},
            'point': {'radius': 4, 'hoverRadius': 6}
        }

        options['interaction'] = {
            'intersect': False,
            'mode': 'index'
        }

        if title:
            options['plugins']['title'] = {
                'display': True,
                'text': title,
                'font': {'size': 16, 'weight': 'bold'}
            }

        return options

    def _get_bar_options(
        self,
        title: str = None,
        horizontal: bool = False,
        stacked: bool = False
    ) -> Dict[str, Any]:
        """Get options for bar chart."""
        options = self.default_options.copy()

        if horizontal:
            options['indexAxis'] = 'y'

        if stacked:
            options['scales'] = {
                'x': {'stacked': True, 'grid': {'display': False}},
                'y': {'stacked': True, 'grid': {'color': COLORS['grid']}}
            }

        if title:
            options['plugins']['title'] = {
                'display': True,
                'text': title,
                'font': {'size': 16, 'weight': 'bold'}
            }

        return options

    def _get_pie_options(
        self,
        title: str = None,
        donut: bool = False
    ) -> Dict[str, Any]:
        """Get options for pie/donut chart."""
        options = {
            'responsive': True,
            'maintainAspectRatio': True,
            'plugins': {
                'legend': {
                    'position': 'right',
                    'labels': {
                        'usePointStyle': True,
                        'padding': 15,
                        'font': {'size': 12}
                    }
                },
                'tooltip': {
                    'callbacks': {
                        'label': 'function(context) { '
                            'let label = context.label || \"\"; '
                            'let value = context.raw || 0; '
                            'let total = context.dataset.data.reduce((a, b) => a + b, 0); '
                            'let percentage = ((value / total) * 100).toFixed(1); '
                            'return label + \": \" + value.toLocaleString() + \" (\" + percentage + \"%)\"; '
                        '}'
                    }
                }
            }
        }

        if donut:
            options['cutout'] = '50%'

        if title:
            options['plugins']['title'] = {
                'display': True,
                'text': title,
                'font': {'size': 16, 'weight': 'bold'}
            }

        return options

    def _deep_merge(self, base: Dict, override: Dict) -> Dict:
        """Deep merge two dictionaries."""
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        return result

    def to_json(self, config: Dict[str, Any]) -> str:
        """Convert configuration to JSON string."""
        import json
        return json.dumps(config, indent=2)
