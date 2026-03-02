# src/visualization/charts/line_chart.py
"""
Line chart implementation for time-series data visualization.
"""

from typing import Dict, List, Any, Optional
import matplotlib.pyplot as plt
import numpy as np

from .base import BaseChart
from ..config.colors import COLORS, get_palette, get_trend_color
from ..config.styles import format_currency, format_number, FONTS


class LineChart(BaseChart):
    """
    Line chart for time-series data like transaction trends, price movements.

    Supports:
    - Single or multiple series
    - Area fill under line
    - Trend indicators
    - Year-over-year comparison
    - Data point markers with tooltips
    """

    def __init__(self):
        super().__init__(chart_type='line')

    def render(
        self,
        data: Dict[str, Any],
        title: str = None,
        xlabel: str = None,
        ylabel: str = None,
        show_markers: bool = True,
        fill_area: bool = False,
        show_trend: bool = False,
        value_format: str = 'number',
        figsize: tuple = None,
        **kwargs
    ) -> 'LineChart':
        """
        Render line chart with time-series data.

        Args:
            data: Dictionary containing:
                - labels: List of x-axis labels (dates/periods)
                - series: List of dicts with 'name', 'values', and optional 'color'
                OR
                - values: Single series values (for simple charts)
            title: Chart title
            xlabel: X-axis label
            ylabel: Y-axis label
            show_markers: Show data point markers
            fill_area: Fill area under line
            show_trend: Show trend line
            value_format: 'number', 'currency', or 'percentage'
            figsize: Custom figure size

        Returns:
            self for method chaining
        """
        self.setup_figure(title=title, figsize=figsize)

        labels = data.get('labels', [])
        x = np.arange(len(labels))
        palette = get_palette()

        # Handle single series or multiple series
        if 'series' in data:
            series_list = data['series']
        else:
            # Single series format
            series_list = [{
                'name': data.get('name', 'Value'),
                'values': data.get('values', []),
                'color': data.get('color')
            }]

        lines = []
        for i, series in enumerate(series_list):
            color = series.get('color') or palette[i % len(palette)]
            values = series['values']

            # Plot line
            line_kwargs = {
                'color': color,
                'linewidth': self.style.get('linewidth', 2.5),
                'label': series['name'],
            }

            if show_markers:
                line_kwargs['marker'] = self.style.get('marker', 'o')
                line_kwargs['markersize'] = self.style.get('markersize', 6)
                line_kwargs['markerfacecolor'] = 'white'
                line_kwargs['markeredgecolor'] = color
                line_kwargs['markeredgewidth'] = 2

            line, = self.ax.plot(x, values, **line_kwargs)
            lines.append(line)

            # Optional area fill
            if fill_area:
                self.ax.fill_between(
                    x, values, 0,
                    color=color,
                    alpha=self.style.get('fill_alpha', 0.1)
                )

            # Optional trend line
            if show_trend and len(values) > 1:
                z = np.polyfit(x, values, 1)
                p = np.poly1d(z)
                self.ax.plot(
                    x, p(x),
                    color=color,
                    linestyle='--',
                    alpha=0.5,
                    linewidth=1.5
                )

        # Configure axes
        self.ax.set_xticks(x)
        self.ax.set_xticklabels(labels)
        self.format_x_axis(rotation=45 if len(labels) > 6 else 0, ha='right' if len(labels) > 6 else 'center')
        self.format_y_axis(fmt=value_format)
        self.set_labels(xlabel=xlabel, ylabel=ylabel)
        self.configure_grid(show=True, axis='y')

        # Add legend if multiple series
        if len(series_list) > 1:
            self.add_legend()

        return self

    def render_comparison(
        self,
        current_data: Dict[str, Any],
        previous_data: Dict[str, Any],
        title: str = None,
        xlabel: str = None,
        ylabel: str = None,
        current_label: str = 'Current Period',
        previous_label: str = 'Previous Period',
        value_format: str = 'number',
        **kwargs
    ) -> 'LineChart':
        """
        Render comparison chart showing current vs previous period.

        Args:
            current_data: Current period data with 'labels' and 'values'
            previous_data: Previous period data with 'labels' and 'values'
            title: Chart title
            current_label: Label for current series
            previous_label: Label for previous series
            value_format: Number formatting

        Returns:
            self for method chaining
        """
        combined_data = {
            'labels': current_data.get('labels', []),
            'series': [
                {
                    'name': current_label,
                    'values': current_data.get('values', []),
                    'color': COLORS['primary']
                },
                {
                    'name': previous_label,
                    'values': previous_data.get('values', []),
                    'color': COLORS['neutral']
                }
            ]
        }

        return self.render(
            combined_data,
            title=title,
            xlabel=xlabel,
            ylabel=ylabel,
            value_format=value_format,
            show_markers=True,
            **kwargs
        )

    def render_with_annotations(
        self,
        data: Dict[str, Any],
        annotations: List[Dict[str, Any]],
        title: str = None,
        **kwargs
    ) -> 'LineChart':
        """
        Render line chart with key point annotations.

        Args:
            data: Chart data
            annotations: List of dicts with 'index', 'text', and optional 'color'
            title: Chart title

        Returns:
            self for method chaining
        """
        self.render(data, title=title, **kwargs)

        values = data.get('values', [])
        if 'series' in data and data['series']:
            values = data['series'][0]['values']

        for ann in annotations:
            idx = ann['index']
            if 0 <= idx < len(values):
                self.add_annotation(
                    idx,
                    values[idx],
                    ann['text'],
                    fontsize=FONTS['annotation_size'],
                    color=ann.get('color', COLORS['accent']),
                    fontweight='bold',
                    xytext=(0, 10),
                    textcoords='offset points',
                    ha='center',
                    va='bottom',
                    bbox=dict(
                        boxstyle='round,pad=0.3',
                        facecolor='white',
                        edgecolor=ann.get('color', COLORS['accent']),
                        alpha=0.9
                    )
                )

        return self
