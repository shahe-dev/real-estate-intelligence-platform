# src/analytics/__init__.py

"""
Analytics module for Dubai Real Estate Intelligence Platform
Provides QA validation, GSC integration, content analysis, and optimization tools
"""

from .qa_validator import QAValidator, ValidationResult, ValidationStatus
from .gsc_client import GSCClient, SearchQuery, PagePerformance, SiteOverview, create_client
from .content_reviewer import ContentReviewer, ContentReview, ContentGap, ContentScore
from .content_optimizer import ContentOptimizer, ContentOptimizationPlan, ScreenshotAnalysis, MarketDataEnrichment

__all__ = [
    # QA Validation
    'QAValidator', 'ValidationResult', 'ValidationStatus',
    # GSC Integration
    'GSCClient', 'SearchQuery', 'PagePerformance', 'SiteOverview', 'create_client',
    # Content Review
    'ContentReviewer', 'ContentReview', 'ContentGap', 'ContentScore',
    # Content Optimization
    'ContentOptimizer', 'ContentOptimizationPlan', 'ScreenshotAnalysis', 'MarketDataEnrichment'
]
