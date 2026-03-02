# src/analytics/market_intelligence/__init__.py
"""
Market Intelligence Module

Provides unique insights that differentiate our content from competitors:
- Anomaly detection (record transactions, volume spikes, new developers)
- Opportunity identification (emerging hotspots, undervalued areas, arbitrage)
- Trend prediction (price momentum, seasonality, market cycle position)
- Comparative analytics (area DNA profiles, buyer indicators, comparables)

All calculations are based on Property Monitor transaction data.
"""

from .anomaly_detector import AnomalyDetector
from .opportunity_detector import OpportunityDetector
from .trend_predictor import TrendPredictor
from .comparative_analytics import ComparativeAnalytics
from .engine import MarketIntelligenceEngine

__all__ = [
    'AnomalyDetector',
    'OpportunityDetector',
    'TrendPredictor',
    'ComparativeAnalytics',
    'MarketIntelligenceEngine'
]
