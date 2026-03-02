# src/visualization/charts/bar_chart.py
"""
Bar chart implementations for rankings, comparisons, and distributions.
"""

from typing import Dict, List, Any, Optional, Tuple
import matplotlib.pyplot as plt
import numpy as np

from .base import BaseChart
from ..config.colors import COLORS, get_palette, get_trend_color, get_area_color
from ..config.styles import format_currency, format_number, format_percentage, FONTS


class BarChart(BaseChart):
    """
    Vertical and horizontal bar charts for rankings and comparisons.

    Supports:
    - Vertical and horizontal orientation
    - Grouped bars for comparisons
    - Stacked bars for composition
    - Value labels on bars
    - Color-coded by category or value
    """

    def __init__(self, horizontal: bool = False):
        chart_type = 'horizontal_bar' if horizontal else 'bar'
        super().__init__(chart_type=chart_type)
        self.horizontal = horizontal

    def render(
        self,
        data: Dict[str, Any],
        title: str = None,
        xlabel: str = None,
        ylabel: str = None,
        show_values: bool = True,
        value_format: str = 'number',
        color_by_value: bool = False,
        figsize: tuple = None,
        **kwargs
    ) -> 'BarChart':
        """
        Render simple bar chart.

        Args:
            data: Dictionary containing:
                - labels: Category labels
                - values: Corresponding values
                - colors: Optional list of colors per bar
            title: Chart title
            xlabel: X-axis label
            ylabel: Y-axis label
            show_values: Show value labels on bars
            value_format: 'number', 'currency', or 'percentage'
            color_by_value: Color bars by positive/negative value
            figsize: Custom figure size

        Returns:
            self for method chaining
        """
        self.setup_figure(title=title, figsize=figsize)

        labels = data.get('labels', [])
        values = data.get('values', [])
        colors = data.get('colors', None)

        if colors is None:
            if color_by_value:
                colors = [get_trend_color(v) for v in values]
            else:
                colors = [COLORS['primary']] * len(values)

        x = np.arange(len(labels))
        bar_size = self.style.get('bar_width', 0.7) if not self.horizontal else self.style.get('bar_height', 0.6)

        if self.horizontal:
            bars = self.ax.barh(
                x, values,
                height=bar_size,
                color=colors,
                edgecolor=self.style.get('edge_color', 'white'),
                linewidth=self.style.get('edge_width', 1)
            )
            self.ax.set_yticks(x)
            self.ax.set_yticklabels(labels)
            self.ax.invert_yaxis()  # Highest value at top

            if show_values:
                for bar, value in zip(bars, values):
                    width = bar.get_width()
                    if value_format == 'currency':
                        label = format_currency(width)
                    elif value_format == 'percentage':
                        label = format_percentage(width)
                    else:
                        label = format_number(width)

                    self.ax.annotate(
                        label,
                        xy=(width, bar.get_y() + bar.get_height() / 2),
                        xytext=(5, 0),
                        textcoords='offset points',
                        ha='left',
                        va='center',
                        fontsize=FONTS['annotation_size'],
                        color=COLORS['text_secondary']
                    )
        else:
            bars = self.ax.bar(
                x, values,
                width=bar_size,
                color=colors,
                edgecolor=self.style.get('edge_color', 'white'),
                linewidth=self.style.get('edge_width', 1)
            )
            self.ax.set_xticks(x)
            self.ax.set_xticklabels(labels)
            self.format_x_axis(rotation=45 if len(labels) > 5 else 0, ha='right' if len(labels) > 5 else 'center')

            if show_values:
                self.add_value_labels(bars, fmt=value_format)

        self.format_y_axis(fmt=value_format) if not self.horizontal else self.format_y_axis(fmt='number')
        self.set_labels(xlabel=xlabel, ylabel=ylabel)
        self.configure_grid(show=True, axis='x' if self.horizontal else 'y')

        return self

    def render_grouped(
        self,
        data: Dict[str, Any],
        title: str = None,
        xlabel: str = None,
        ylabel: str = None,
        show_values: bool = False,
        value_format: str = 'number',
        figsize: tuple = None,
        **kwargs
    ) -> 'BarChart':
        """
        Render grouped bar chart for comparisons.

        Args:
            data: Dictionary containing:
                - labels: Category labels
                - groups: List of dicts with 'name', 'values', and optional 'color'
            title: Chart title

        Returns:
            self for method chaining
        """
        self.setup_figure(title=title, figsize=figsize)

        labels = data.get('labels', [])
        groups = data.get('groups', [])
        n_groups = len(groups)
        palette = get_palette()

        x = np.arange(len(labels))
        total_width = 0.8
        bar_width = total_width / n_groups

        for i, group in enumerate(groups):
            offset = (i - n_groups / 2 + 0.5) * bar_width
            color = group.get('color') or palette[i % len(palette)]

            bars = self.ax.bar(
                x + offset,
                group['values'],
                bar_width,
                label=group['name'],
                color=color,
                edgecolor=self.style.get('edge_color', 'white'),
                linewidth=self.style.get('edge_width', 0.5)
            )

        self.ax.set_xticks(x)
        self.ax.set_xticklabels(labels)
        self.format_x_axis(rotation=45 if len(labels) > 5 else 0, ha='right' if len(labels) > 5 else 'center')
        self.format_y_axis(fmt=value_format)
        self.set_labels(xlabel=xlabel, ylabel=ylabel)
        self.configure_grid(show=True, axis='y')
        self.add_legend()

        return self

    def render_stacked(
        self,
        data: Dict[str, Any],
        title: str = None,
        xlabel: str = None,
        ylabel: str = None,
        show_totals: bool = True,
        value_format: str = 'number',
        figsize: tuple = None,
        **kwargs
    ) -> 'BarChart':
        """
        Render stacked bar chart for composition analysis.

        Args:
            data: Dictionary containing:
                - labels: Category labels
                - stacks: List of dicts with 'name', 'values', and optional 'color'
            title: Chart title
            show_totals: Show total values on top

        Returns:
            self for method chaining
        """
        self.setup_figure(title=title, figsize=figsize)

        labels = data.get('labels', [])
        stacks = data.get('stacks', [])
        palette = get_palette()

        x = np.arange(len(labels))
        bar_width = self.style.get('bar_width', 0.7)

        bottoms = np.zeros(len(labels))

        for i, stack in enumerate(stacks):
            color = stack.get('color') or palette[i % len(palette)]
            values = np.array(stack['values'])

            self.ax.bar(
                x, values,
                bar_width,
                bottom=bottoms,
                label=stack['name'],
                color=color,
                edgecolor=self.style.get('edge_color', 'white'),
                linewidth=self.style.get('edge_width', 0.5)
            )

            bottoms += values

        if show_totals:
            for i, total in enumerate(bottoms):
                if value_format == 'currency':
                    label = format_currency(total)
                elif value_format == 'percentage':
                    label = format_percentage(total)
                else:
                    label = format_number(total)

                self.ax.annotate(
                    label,
                    xy=(i, total),
                    xytext=(0, 5),
                    textcoords='offset points',
                    ha='center',
                    va='bottom',
                    fontsize=FONTS['annotation_size'],
                    fontweight='bold',
                    color=COLORS['text']
                )

        self.ax.set_xticks(x)
        self.ax.set_xticklabels(labels)
        self.format_x_axis(rotation=45 if len(labels) > 5 else 0, ha='right' if len(labels) > 5 else 'center')
        self.format_y_axis(fmt=value_format)
        self.set_labels(xlabel=xlabel, ylabel=ylabel)
        self.configure_grid(show=True, axis='y')
        self.add_legend()

        return self

    def render_top_performers(
        self,
        data: Dict[str, Any],
        title: str = None,
        metric_label: str = 'Value',
        value_format: str = 'number',
        show_rank: bool = True,
        use_area_colors: bool = False,
        figsize: tuple = None,
        **kwargs
    ) -> 'BarChart':
        """
        Render horizontal bar chart optimized for top performers/rankings.

        Args:
            data: Dictionary containing:
                - labels: Names (areas, developers, etc.)
                - values: Metric values
            title: Chart title
            metric_label: Label for the value axis
            show_rank: Prepend rank number to labels
            use_area_colors: Use area-specific colors

        Returns:
            self for method chaining
        """
        self.horizontal = True
        self.chart_type = 'horizontal_bar'
        self.style = self.style.copy()
        self.style.update({'figsize': figsize or (10, 6)})

        labels = data.get('labels', [])
        values = data.get('values', [])

        if show_rank:
            labels = [f"{i+1}. {label}" for i, label in enumerate(labels)]

        if use_area_colors:
            colors = [get_area_color(label.split('. ')[-1] if show_rank else label) for label in data.get('labels', [])]
        else:
            colors = None

        modified_data = {
            'labels': labels,
            'values': values,
            'colors': colors
        }

        return self.render(
            modified_data,
            title=title,
            xlabel=metric_label,
            ylabel=None,
            show_values=True,
            value_format=value_format,
            figsize=figsize,
            **kwargs
        )
