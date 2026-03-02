# src/analytics/citation_intel/__init__.py
"""
Citation Intelligence Module

Analyzes competitor reports to extract patterns and optimize content prompts.
Also identifies unique insights that differentiate our content.
"""

from .pattern_database import PatternDatabase, pattern_db
from .prompt_optimizer import PromptOptimizer, prompt_optimizer

__all__ = [
    'PatternDatabase',
    'pattern_db',
    'PromptOptimizer',
    'prompt_optimizer'
]
