# src/visualization/charts/pie_chart.py
"""
Pie and donut chart implementations for market segment visualization.
"""

from typing import Dict, List, Any, Optional
import matplotlib.pyplot as plt
import numpy as np

from .base import BaseChart
from ..config.colors import COLORS, get_palette, get_segment_colors
from ..config.styles import format_currency, format_number, format_percentage, FONTS


class PieChart(BaseChart):
    """
    Pie and donut charts for market composition and segment analysis.

    Supports:
    - Standard pie charts
    - Donut charts with center text
    - Exploded segments for emphasis
    - Percentage and value labels
    - Custom color schemes for segments
    """

    def __init__(self, donut: bool = False):
        chart_type = 'donut' if donut else 'pie'
        super().__init__(chart_type=chart_type)
        self.donut = donut

    def render(
        self,
        data: Dict[str, Any],
        title: str = None,
        show_percentages: bool = True,
        show_values: bool = False,
        value_format: str = 'number',
        explode_largest: bool = False,
        center_text: str = None,
        figsize: tuple = None,
        **kwargs
    ) -> 'PieChart':
        """
        Render pie or donut chart.

        Args:
            data: Dictionary containing:
                - labels: Segment labels
                - values: Segment values
                - colors: Optional list of colors per segment
            title: Chart title
            show_percentages: Show percentage labels
            show_values: Show actual values in labels
            value_format: Format for values ('number', 'currency')
            explode_largest: Slightly explode the largest segment
            center_text: Text to display in center (donut only)
            figsize: Custom figure size

        Returns:
            self for method chaining
        """
        self.setup_figure(title=title, figsize=figsize)

        labels = data.get('labels', [])
        values = data.get('values', [])
        colors = data.get('colors') or get_palette(len(values))

        # Calculate explode effect
        explode = None
        if explode_largest and values:
            max_idx = values.index(max(values))
            explode = [self.style.get('explode_max', 0.05) if i == max_idx else 0 for i in range(len(values))]

        # Configure label format
        def make_autopct(values):
            def autopct(pct):
                total = sum(values)
                val = int(round(pct * total / 100.0))

                if show_values and show_percentages:
                    if value_format == 'currency':
                        return f'{pct:.1f}%\n({format_currency(val)})'
                    else:
                        return f'{pct:.1f}%\n({format_number(val)})'
                elif show_values:
                    return format_currency(val) if value_format == 'currency' else format_number(val)
                elif show_percentages:
                    return f'{pct:.1f}%'
                return ''
            return autopct

        # Pie chart configuration
        pie_kwargs = {
            'labels': None,  # We'll add legend instead
            'colors': colors,
            'autopct': make_autopct(values) if (show_percentages or show_values) else None,
            'startangle': self.style.get('startangle', 90),
            'explode': explode,
            'shadow': self.style.get('shadow', False),
            'textprops': {'fontsize': FONTS['annotation_size']},
        }

        if self.donut:
            pie_kwargs['wedgeprops'] = self.style.get('wedgeprops', {'width': 0.5})
            pie_kwargs['pctdistance'] = self.style.get('pctdistance', 0.75)

        wedges, texts, autotexts = self.ax.pie(values, **pie_kwargs)

        # Style the percentage text
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')

        # Add center text for donut
        if self.donut and center_text:
            self.ax.text(
                0, 0, center_text,
                ha='center', va='center',
                fontsize=FONTS['title_size'],
                fontweight='bold',
                color=COLORS['text']
            )

        # Add legend
        self.ax.legend(
            wedges, labels,
            loc=self.style.get('legend_loc', 'center left'),
            bbox_to_anchor=self.style.get('legend_bbox', (1, 0.5)),
            fontsize=FONTS['legend_size']
        )

        self.ax.set_aspect('equal')

        return self

    def render_market_segments(
        self,
        data: Dict[str, Any],
        title: str = 'Market Segments',
        segment_type: str = 'offplan_ready',
        center_text: str = None,
        figsize: tuple = None,
        **kwargs
    ) -> 'PieChart':
        """
        Render chart for standard market segments with predefined colors.

        Args:
            data: Dictionary with segment data
            title: Chart title
            segment_type: 'offplan_ready', 'luxury_tiers', or 'property_types'
            center_text: Center text for donut chart

        Returns:
            self for method chaining
        """
        segment_colors = get_segment_colors()

        color_maps = {
            'offplan_ready': {
                'Off-Plan': segment_colors['offplan'],
                'Ready': segment_colors['ready'],
            },
            'luxury_tiers': {
                'Ultra Luxury (10M+)': segment_colors['ultra_luxury'],
                'Luxury (5-10M)': segment_colors['luxury'],
                'Mid-Range (2-5M)': segment_colors['mid_range'],
                'Standard (<2M)': segment_colors['affordable'],
            },
            'property_types': {
                'Unit': COLORS['primary'],
                'Villa': COLORS['accent'],
                'Land': COLORS['secondary'],
                'Building': COLORS['neutral'],
            }
        }

        color_map = color_maps.get(segment_type, {})
        labels = data.get('labels', [])
        colors = [color_map.get(label, get_palette()[i % len(get_palette())]) for i, label in enumerate(labels)]

        colored_data = data.copy()
        colored_data['colors'] = colors

        return self.render(
            colored_data,
            title=title,
            show_percentages=True,
            show_values=True,
            value_format='number',
            explode_largest=True,
            center_text=center_text,
            figsize=figsize,
            **kwargs
        )

    def render_with_highlights(
        self,
        data: Dict[str, Any],
        highlight_segments: List[str],
        title: str = None,
        highlight_color: str = None,
        dim_alpha: float = 0.4,
        figsize: tuple = None,
        **kwargs
    ) -> 'PieChart':
        """
        Render pie chart with certain segments highlighted.

        Args:
            data: Chart data
            highlight_segments: List of segment labels to highlight
            title: Chart title
            highlight_color: Color for highlighted segments
            dim_alpha: Alpha for non-highlighted segments

        Returns:
            self for method chaining
        """
        labels = data.get('labels', [])
        base_colors = data.get('colors') or get_palette(len(labels))
        highlight_color = highlight_color or COLORS['accent']

        # Create color list with highlighting
        colors = []
        for i, label in enumerate(labels):
            if label in highlight_segments:
                colors.append(highlight_color)
            else:
                # Dim the non-highlighted segments
                import matplotlib.colors as mcolors
                rgb = mcolors.to_rgb(base_colors[i])
                # Mix with white to dim
                dimmed = tuple(c * dim_alpha + (1 - dim_alpha) for c in rgb)
                colors.append(dimmed)

        colored_data = data.copy()
        colored_data['colors'] = colors

        return self.render(
            colored_data,
            title=title,
            figsize=figsize,
            **kwargs
        )
