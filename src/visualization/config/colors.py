# src/visualization/config/colors.py
"""
Dubai Real Estate Intelligence - Brand Color Palette

Consistent color scheme for all visualizations, supporting both
matplotlib (hex) and Chart.js (rgba) formats.
"""

from typing import List, Tuple, Optional


# Primary brand colors
COLORS = {
    # Core brand colors
    'primary': '#1E3A5F',       # Deep navy blue - headers, primary elements
    'secondary': '#4A90A4',     # Teal - secondary elements
    'accent': '#D4AF37',        # Gold - highlights, luxury emphasis

    # Status colors
    'success': '#2E7D32',       # Green - positive trends, growth
    'warning': '#F57C00',       # Orange - caution, moderate changes
    'danger': '#C62828',        # Red - negative trends, declines
    'neutral': '#757575',       # Gray - baselines, neutral data

    # Segment-specific colors
    'offplan': '#4A90A4',       # Teal for off-plan properties
    'ready': '#1E3A5F',         # Navy for ready properties
    'luxury': '#D4AF37',        # Gold for luxury segment
    'ultra_luxury': '#8B7355',  # Bronze for ultra-luxury (10M+)
    'mid_range': '#6B8E7B',     # Sage green for mid-range
    'affordable': '#9E9E9E',    # Gray for affordable

    # Background colors
    'background': '#FFFFFF',    # White background
    'background_alt': '#F5F5F5', # Light gray alternate
    'grid': '#E0E0E0',          # Grid lines
    'text': '#212121',          # Primary text
    'text_secondary': '#616161', # Secondary text

    # Chart-specific
    'trend_up': '#2E7D32',      # Price/volume increase
    'trend_down': '#C62828',    # Price/volume decrease
    'trend_flat': '#757575',    # No significant change
}

# Multi-series palette for charts with many categories
PALETTE = [
    '#1E3A5F',  # Navy
    '#4A90A4',  # Teal
    '#D4AF37',  # Gold
    '#7B9E87',  # Sage
    '#9C6644',  # Brown
    '#6B5B95',  # Purple
    '#88B04B',  # Green
    '#F7CAC9',  # Pink
    '#92A8D1',  # Light blue
    '#F7786B',  # Coral
]

# Area-specific colors (for top areas charts)
AREA_COLORS = {
    'Dubai Marina': '#1E3A5F',
    'Palm Jumeirah': '#D4AF37',
    'Downtown Dubai': '#4A90A4',
    'Business Bay': '#7B9E87',
    'Dubai Hills Estate': '#9C6644',
    'Dubai Creek Harbour': '#6B5B95',
    'JVC': '#88B04B',
    'Jumeirah Village Circle': '#88B04B',
    'Mohammed Bin Rashid City': '#92A8D1',
    'Emirates Hills': '#8B7355',
}


def get_color(name: str) -> str:
    """Get a color by name from the palette."""
    return COLORS.get(name, COLORS['neutral'])


def get_palette(n: int = None) -> List[str]:
    """Get color palette, optionally limited to n colors."""
    if n is None:
        return PALETTE.copy()
    return PALETTE[:min(n, len(PALETTE))]


def hex_to_rgb(hex_color: str) -> Tuple[int, int, int]:
    """Convert hex color to RGB tuple."""
    hex_color = hex_color.lstrip('#')
    return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))


def hex_to_rgba(hex_color: str, alpha: float = 1.0) -> str:
    """Convert hex color to rgba string for Chart.js."""
    r, g, b = hex_to_rgb(hex_color)
    return f'rgba({r}, {g}, {b}, {alpha})'


def get_trend_color(value: float, threshold: float = 0.01) -> str:
    """Get color based on trend direction."""
    if value > threshold:
        return COLORS['trend_up']
    elif value < -threshold:
        return COLORS['trend_down']
    return COLORS['trend_flat']


def get_segment_colors() -> dict:
    """Get colors for market segments."""
    return {
        'offplan': COLORS['offplan'],
        'ready': COLORS['ready'],
        'luxury': COLORS['luxury'],
        'ultra_luxury': COLORS['ultra_luxury'],
        'mid_range': COLORS['mid_range'],
        'affordable': COLORS['affordable'],
    }


def get_chartjs_colors(n: int = None, alpha: float = 0.8) -> dict:
    """Get Chart.js compatible color configuration."""
    palette = get_palette(n)
    return {
        'backgroundColor': [hex_to_rgba(c, alpha) for c in palette],
        'borderColor': palette,
        'hoverBackgroundColor': [hex_to_rgba(c, min(alpha + 0.1, 1.0)) for c in palette],
    }


def get_area_color(area_name: str) -> str:
    """Get color for a specific area, with fallback to palette."""
    if area_name in AREA_COLORS:
        return AREA_COLORS[area_name]
    # Generate consistent color based on area name hash
    idx = hash(area_name) % len(PALETTE)
    return PALETTE[idx]
