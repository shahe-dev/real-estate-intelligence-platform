# src/visualization/charts/base.py
"""
Base chart class with common functionality for all chart types.
"""

import os
import io
import base64
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for server use
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

from ..config.colors import COLORS, get_palette, get_trend_color
from ..config.styles import (
    get_style, get_matplotlib_style, format_currency,
    format_number, format_percentage, FONTS
)


class BaseChart(ABC):
    """
    Abstract base class for all chart types.

    Provides common functionality for:
    - Figure setup and styling
    - Title and label formatting
    - Legend configuration
    - Export to file or base64
    - Annotation support
    """

    def __init__(self, chart_type: str = 'bar'):
        self.chart_type = chart_type
        self.style = get_style(chart_type)
        self.fig = None
        self.ax = None
        self.annotations = []
        self._apply_matplotlib_style()

    def _apply_matplotlib_style(self):
        """Apply consistent matplotlib styling."""
        style = get_matplotlib_style()
        for key, value in style.items():
            plt.rcParams[key] = value

    def setup_figure(self, title: str = None, figsize: Tuple[int, int] = None) -> Tuple[plt.Figure, plt.Axes]:
        """Create and configure the figure and axes."""
        if figsize is None:
            figsize = self.style.get('figsize', (10, 6))

        self.fig, self.ax = plt.subplots(figsize=figsize)

        if title:
            self.ax.set_title(
                title,
                fontsize=FONTS['title_size'],
                fontweight=FONTS['weight_bold'],
                pad=15
            )

        return self.fig, self.ax

    def set_labels(self, xlabel: str = None, ylabel: str = None):
        """Set axis labels with consistent styling."""
        if xlabel:
            self.ax.set_xlabel(
                xlabel,
                fontsize=FONTS['label_size'],
                color=COLORS['text']
            )
        if ylabel:
            self.ax.set_ylabel(
                ylabel,
                fontsize=FONTS['label_size'],
                color=COLORS['text']
            )

    def configure_grid(self, show: bool = True, axis: str = 'y', alpha: float = 0.3):
        """Configure grid lines."""
        if show:
            self.ax.grid(True, axis=axis, alpha=alpha, linestyle='--', color=COLORS['grid'])
        else:
            self.ax.grid(False)

    def add_legend(self, loc: str = None, **kwargs):
        """Add legend with consistent styling."""
        loc = loc or self.style.get('legend_loc', 'best')

        legend_kwargs = {
            'loc': loc,
            'fontsize': FONTS['legend_size'],
            'framealpha': 0.9,
            'edgecolor': COLORS['grid'],
        }
        legend_kwargs.update(kwargs)

        if 'legend_bbox' in self.style:
            legend_kwargs['bbox_to_anchor'] = self.style['legend_bbox']

        self.ax.legend(**legend_kwargs)

    def add_annotation(self, x, y, text: str, **kwargs):
        """Add text annotation to chart."""
        default_kwargs = {
            'fontsize': FONTS['annotation_size'],
            'color': COLORS['text_secondary'],
            'ha': 'center',
            'va': 'bottom',
        }
        default_kwargs.update(kwargs)

        annotation = self.ax.annotate(text, (x, y), **default_kwargs)
        self.annotations.append(annotation)
        return annotation

    def add_value_labels(self, bars, fmt: str = 'number', **kwargs):
        """Add value labels to bar charts."""
        for bar in bars:
            height = bar.get_height()
            if height == 0:
                continue

            if fmt == 'currency':
                label = format_currency(height)
            elif fmt == 'percentage':
                label = format_percentage(height)
            else:
                label = format_number(height)

            self.add_annotation(
                bar.get_x() + bar.get_width() / 2,
                height,
                label,
                **kwargs
            )

    def format_y_axis(self, fmt: str = 'number'):
        """Format Y-axis ticks."""
        if fmt == 'currency':
            self.ax.yaxis.set_major_formatter(
                ticker.FuncFormatter(lambda x, p: format_currency(x))
            )
        elif fmt == 'percentage':
            self.ax.yaxis.set_major_formatter(
                ticker.FuncFormatter(lambda x, p: format_percentage(x))
            )
        elif fmt == 'number':
            self.ax.yaxis.set_major_formatter(
                ticker.FuncFormatter(lambda x, p: format_number(x))
            )

    def format_x_axis(self, rotation: int = 0, ha: str = 'center'):
        """Format X-axis ticks."""
        plt.xticks(rotation=rotation, ha=ha)

    def finalize(self):
        """Finalize chart layout."""
        self.fig.tight_layout()

    def save(self, filepath: str, dpi: int = 150, transparent: bool = False):
        """Save chart to file."""
        self.finalize()

        # Ensure directory exists
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)

        self.fig.savefig(
            filepath,
            dpi=dpi,
            transparent=transparent,
            bbox_inches='tight',
            pad_inches=0.2
        )
        plt.close(self.fig)
        return filepath

    def to_base64(self, fmt: str = 'png', dpi: int = 150) -> str:
        """Export chart as base64 encoded string for embedding."""
        self.finalize()

        buffer = io.BytesIO()
        self.fig.savefig(
            buffer,
            format=fmt,
            dpi=dpi,
            bbox_inches='tight',
            pad_inches=0.2
        )
        buffer.seek(0)

        image_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
        plt.close(self.fig)

        return f"data:image/{fmt};base64,{image_base64}"

    def to_markdown(self, alt_text: str = "Chart", fmt: str = 'png', dpi: int = 150) -> str:
        """Export chart as markdown image with embedded base64."""
        base64_data = self.to_base64(fmt=fmt, dpi=dpi)
        return f"![{alt_text}]({base64_data})"

    @abstractmethod
    def render(self, data: Dict[str, Any], **kwargs) -> 'BaseChart':
        """
        Render the chart with provided data.
        Must be implemented by subclasses.

        Args:
            data: Chart data dictionary
            **kwargs: Additional chart-specific options

        Returns:
            self for method chaining
        """
        pass

    def close(self):
        """Close the figure and free resources."""
        if self.fig:
            plt.close(self.fig)
            self.fig = None
            self.ax = None
