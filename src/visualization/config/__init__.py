# src/visualization/config/__init__.py
"""Configuration for visualization styling and chart mappings."""

from .colors import COLORS, get_color, get_palette
from .styles import CHART_STYLES, get_style

__all__ = [
    'COLORS',
    'get_color',
    'get_palette',
    'CHART_STYLES',
    'get_style'
]
