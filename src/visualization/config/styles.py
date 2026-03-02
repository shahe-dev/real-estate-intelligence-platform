# src/visualization/config/styles.py
"""
Dubai Real Estate Intelligence - Chart Styling Configuration

Consistent styling for matplotlib charts and Chart.js configurations.
"""

from typing import Dict, Any
from .colors import COLORS


# Base chart dimensions
CHART_DIMENSIONS = {
    'small': (6, 4),       # Small inline charts
    'medium': (10, 6),     # Standard report charts
    'large': (12, 8),      # Full-width charts
    'wide': (14, 5),       # Timeline/trend charts
    'square': (8, 8),      # Pie/donut charts
}

# Font configuration
FONTS = {
    'family': 'Arial, sans-serif',
    'title_size': 14,
    'label_size': 11,
    'tick_size': 10,
    'legend_size': 10,
    'annotation_size': 9,
    'weight_bold': 'bold',
    'weight_normal': 'normal',
}

# Chart-specific styles
CHART_STYLES = {
    'line': {
        'figsize': CHART_DIMENSIONS['wide'],
        'linewidth': 2.5,
        'marker': 'o',
        'markersize': 6,
        'grid': True,
        'grid_alpha': 0.3,
        'fill_alpha': 0.1,
        'legend_loc': 'upper left',
    },
    'bar': {
        'figsize': CHART_DIMENSIONS['medium'],
        'bar_width': 0.7,
        'edge_color': 'white',
        'edge_width': 1,
        'grid': True,
        'grid_alpha': 0.3,
        'legend_loc': 'upper right',
    },
    'horizontal_bar': {
        'figsize': CHART_DIMENSIONS['medium'],
        'bar_height': 0.6,
        'edge_color': 'white',
        'edge_width': 1,
        'grid': True,
        'grid_alpha': 0.3,
        'legend_loc': 'lower right',
    },
    'pie': {
        'figsize': CHART_DIMENSIONS['square'],
        'startangle': 90,
        'explode_max': 0.05,
        'autopct': '%1.1f%%',
        'shadow': False,
        'legend_loc': 'center left',
        'legend_bbox': (1, 0.5),
    },
    'donut': {
        'figsize': CHART_DIMENSIONS['square'],
        'startangle': 90,
        'wedgeprops': {'width': 0.5},
        'autopct': '%1.1f%%',
        'pctdistance': 0.75,
        'legend_loc': 'center left',
        'legend_bbox': (1, 0.5),
    },
    'scatter': {
        'figsize': CHART_DIMENSIONS['medium'],
        'marker_size': 50,
        'alpha': 0.6,
        'grid': True,
        'grid_alpha': 0.3,
    },
    'stacked_bar': {
        'figsize': CHART_DIMENSIONS['medium'],
        'bar_width': 0.7,
        'edge_color': 'white',
        'edge_width': 0.5,
        'grid': True,
        'grid_alpha': 0.3,
        'legend_loc': 'upper right',
    },
}

# Matplotlib rcParams overrides
MATPLOTLIB_STYLE = {
    'figure.facecolor': COLORS['background'],
    'axes.facecolor': COLORS['background'],
    'axes.edgecolor': COLORS['grid'],
    'axes.labelcolor': COLORS['text'],
    'axes.titlecolor': COLORS['text'],
    'axes.grid': True,
    'axes.grid.axis': 'y',
    'grid.color': COLORS['grid'],
    'grid.linestyle': '--',
    'grid.alpha': 0.3,
    'xtick.color': COLORS['text_secondary'],
    'ytick.color': COLORS['text_secondary'],
    'text.color': COLORS['text'],
    'legend.frameon': True,
    'legend.framealpha': 0.9,
    'legend.edgecolor': COLORS['grid'],
    'figure.dpi': 150,
    'savefig.dpi': 150,
    'savefig.bbox': 'tight',
    'savefig.pad_inches': 0.2,
}

# Chart.js global options
CHARTJS_OPTIONS = {
    'responsive': True,
    'maintainAspectRatio': True,
    'plugins': {
        'legend': {
            'position': 'top',
            'labels': {
                'usePointStyle': True,
                'padding': 15,
                'font': {
                    'size': 12,
                    'family': FONTS['family'],
                }
            }
        },
        'tooltip': {
            'backgroundColor': 'rgba(0, 0, 0, 0.8)',
            'titleFont': {'size': 13, 'weight': 'bold'},
            'bodyFont': {'size': 12},
            'padding': 12,
            'cornerRadius': 4,
        },
        'title': {
            'display': True,
            'font': {
                'size': 16,
                'weight': 'bold',
                'family': FONTS['family'],
            },
            'padding': {'bottom': 20},
        }
    },
    'scales': {
        'x': {
            'grid': {'display': False},
            'ticks': {'font': {'size': 11}},
        },
        'y': {
            'grid': {'color': COLORS['grid'], 'drawBorder': False},
            'ticks': {'font': {'size': 11}},
        }
    }
}


def get_style(chart_type: str) -> Dict[str, Any]:
    """Get style configuration for a chart type."""
    return CHART_STYLES.get(chart_type, CHART_STYLES['bar']).copy()


def get_matplotlib_style() -> Dict[str, Any]:
    """Get matplotlib rcParams style dictionary."""
    return MATPLOTLIB_STYLE.copy()


def get_chartjs_options(chart_type: str = None) -> Dict[str, Any]:
    """Get Chart.js options, optionally customized for chart type."""
    options = CHARTJS_OPTIONS.copy()

    if chart_type == 'pie' or chart_type == 'donut':
        options['plugins']['legend']['position'] = 'right'
        del options['scales']  # Pie charts don't use scales
    elif chart_type == 'horizontal_bar':
        options['indexAxis'] = 'y'

    return options


def format_currency(value: float, prefix: str = 'AED ', suffix: str = '') -> str:
    """Format value as currency string."""
    if value >= 1_000_000_000:
        return f"{prefix}{value/1_000_000_000:.1f}B{suffix}"
    elif value >= 1_000_000:
        return f"{prefix}{value/1_000_000:.1f}M{suffix}"
    elif value >= 1_000:
        return f"{prefix}{value/1_000:.1f}K{suffix}"
    return f"{prefix}{value:,.0f}{suffix}"


def format_percentage(value: float, decimal_places: int = 1) -> str:
    """Format value as percentage string."""
    return f"{value:.{decimal_places}f}%"


def format_number(value: float) -> str:
    """Format large numbers with K/M/B suffix."""
    if value >= 1_000_000_000:
        return f"{value/1_000_000_000:.1f}B"
    elif value >= 1_000_000:
        return f"{value/1_000_000:.1f}M"
    elif value >= 1_000:
        return f"{value/1_000:.1f}K"
    return f"{value:,.0f}"
