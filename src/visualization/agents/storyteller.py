# src/visualization/agents/storyteller.py
"""
StorytellerAgent - Generates AI-powered annotations and insights for charts.

Responsible for:
- Analyzing chart data for key insights
- Generating legend descriptions
- Creating callout annotations
- Producing executive summaries
"""

import os
from typing import Dict, List, Any, Optional
from anthropic import Anthropic

from .chart_selector import ChartType


class StorytellerAgent:
    """
    Agent for generating AI-powered storytelling elements for visualizations.

    Uses Claude to create:
    - Chart insights and key takeaways
    - Legend descriptions with context
    - Data point callouts
    - Trend interpretations
    """

    def __init__(self, api_key: str = None):
        """
        Initialize storyteller agent with Anthropic client.

        Args:
            api_key: Anthropic API key (defaults to env var)
        """
        self.api_key = api_key or os.getenv('ANTHROPIC_API_KEY')
        self.client = None

        if self.api_key:
            try:
                self.client = Anthropic(api_key=self.api_key)
            except Exception as e:
                print(f"Warning: Could not initialize Anthropic client: {e}")

    def generate_insights(
        self,
        chart_type: ChartType,
        data: Dict[str, Any],
        context: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Generate insights for a chart.

        Args:
            chart_type: Type of chart
            data: Chart data
            context: Additional context (period, segment, etc.)

        Returns:
            Dictionary with insights, legend, and callouts
        """
        if not self.client:
            return self._generate_fallback_insights(chart_type, data, context)

        # Build prompt
        prompt = self._build_insights_prompt(chart_type, data, context)

        try:
            response = self.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=500,
                messages=[{"role": "user", "content": prompt}]
            )

            insights_text = response.content[0].text
            return self._parse_insights_response(insights_text)

        except Exception as e:
            print(f"Error generating insights: {e}")
            return self._generate_fallback_insights(chart_type, data, context)

    def generate_chart_annotation(
        self,
        chart_type: ChartType,
        data: Dict[str, Any],
        highlight_point: int = None
    ) -> str:
        """
        Generate annotation text for a specific data point.

        Args:
            chart_type: Type of chart
            data: Chart data
            highlight_point: Index of point to annotate

        Returns:
            Annotation text string
        """
        values = data.get('values', [])
        labels = data.get('labels', [])

        if not values:
            return ""

        if highlight_point is None:
            # Find the most interesting point (max value)
            highlight_point = values.index(max(values))

        if highlight_point >= len(values):
            return ""

        value = values[highlight_point]
        label = labels[highlight_point] if highlight_point < len(labels) else ""

        # Generate simple annotation
        if chart_type == ChartType.LINE:
            if highlight_point == values.index(max(values)):
                return f"Peak: {self._format_value(value)}"
            elif highlight_point == values.index(min(values)):
                return f"Low: {self._format_value(value)}"
        elif chart_type in (ChartType.BAR, ChartType.HORIZONTAL_BAR):
            if highlight_point == 0:
                return f"#1: {self._format_value(value)}"

        return f"{label}: {self._format_value(value)}"

    def generate_legend_description(
        self,
        chart_type: ChartType,
        data: Dict[str, Any],
        segment: str = None
    ) -> str:
        """
        Generate descriptive text for chart legend.

        Args:
            chart_type: Type of chart
            data: Chart data
            segment: Specific segment to describe

        Returns:
            Legend description text
        """
        labels = data.get('labels', [])
        values = data.get('values', [])

        if not labels or not values:
            return ""

        total = sum(values)

        if chart_type in (ChartType.PIE, ChartType.DONUT):
            # Describe composition
            descriptions = []
            for label, value in zip(labels, values):
                pct = (value / total * 100) if total > 0 else 0
                descriptions.append(f"{label}: {pct:.1f}% ({self._format_value(value)})")
            return " | ".join(descriptions)

        elif chart_type == ChartType.LINE:
            # Describe trend
            if len(values) >= 2:
                change = ((values[-1] - values[0]) / values[0] * 100) if values[0] else 0
                direction = "increased" if change > 0 else "decreased"
                return f"Values {direction} by {abs(change):.1f}% over the period"

        elif chart_type in (ChartType.BAR, ChartType.HORIZONTAL_BAR):
            # Describe ranking
            return f"Showing top {len(labels)} performers"

        return ""

    def generate_executive_summary(
        self,
        charts: List[Dict[str, Any]],
        report_type: str,
        period: str = None
    ) -> str:
        """
        Generate executive summary combining insights from multiple charts.

        Args:
            charts: List of chart data and insights
            report_type: Type of report
            period: Time period covered

        Returns:
            Executive summary text
        """
        if not self.client:
            return self._generate_fallback_summary(charts, report_type, period)

        # Build summary prompt
        prompt = self._build_summary_prompt(charts, report_type, period)

        try:
            response = self.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=300,
                messages=[{"role": "user", "content": prompt}]
            )

            return response.content[0].text.strip()

        except Exception as e:
            print(f"Error generating summary: {e}")
            return self._generate_fallback_summary(charts, report_type, period)

    def _build_insights_prompt(
        self,
        chart_type: ChartType,
        data: Dict[str, Any],
        context: Dict[str, Any] = None
    ) -> str:
        """Build prompt for insights generation."""
        context = context or {}

        labels = data.get('labels', [])
        values = data.get('values', [])

        prompt = f"""Analyze this Dubai real estate market data and provide 2-3 brief, insightful observations.

Chart Type: {chart_type.value}
Data:
Labels: {labels}
Values: {values}
"""

        if context:
            prompt += f"\nContext: {context}"

        prompt += """

Respond in this exact format:
INSIGHT_1: [First key insight, one sentence]
INSIGHT_2: [Second key insight, one sentence]
INSIGHT_3: [Third key insight, one sentence, optional]

Focus on:
- Key trends or patterns
- Notable outliers or peaks
- Actionable observations for real estate professionals
"""
        return prompt

    def _build_summary_prompt(
        self,
        charts: List[Dict[str, Any]],
        report_type: str,
        period: str = None
    ) -> str:
        """Build prompt for executive summary."""
        prompt = f"""Create a 2-3 sentence executive summary for a Dubai real estate {report_type} report.

Period: {period or 'Not specified'}

Key data points from charts:
"""
        for chart in charts:
            chart_name = chart.get('name', 'Chart')
            insights = chart.get('insights', [])
            prompt += f"\n{chart_name}:\n"
            for insight in insights[:2]:
                prompt += f"- {insight}\n"

        prompt += """

Write a concise, professional summary highlighting the most important market trends and implications.
"""
        return prompt

    def _parse_insights_response(self, response: str) -> Dict[str, Any]:
        """Parse AI response into structured insights."""
        insights = []

        for line in response.strip().split('\n'):
            line = line.strip()
            if line.startswith('INSIGHT_'):
                parts = line.split(':', 1)
                if len(parts) > 1:
                    insights.append(parts[1].strip())

        return {
            'insights': insights or [response.strip()],
            'raw_response': response
        }

    def _generate_fallback_insights(
        self,
        chart_type: ChartType,
        data: Dict[str, Any],
        context: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Generate basic insights without AI."""
        insights = []
        values = data.get('values', [])
        labels = data.get('labels', [])

        if not values:
            return {'insights': ['No data available for analysis']}

        total = sum(values)
        avg = total / len(values) if values else 0
        max_val = max(values)
        min_val = min(values)
        max_idx = values.index(max_val)

        if chart_type == ChartType.LINE:
            if len(values) >= 2:
                change = ((values[-1] - values[0]) / values[0] * 100) if values[0] else 0
                direction = "increased" if change > 0 else "decreased"
                insights.append(f"Overall trend: Values {direction} by {abs(change):.1f}%")

            if labels and max_idx < len(labels):
                insights.append(f"Peak recorded at {labels[max_idx]}: {self._format_value(max_val)}")

        elif chart_type in (ChartType.BAR, ChartType.HORIZONTAL_BAR):
            if labels:
                insights.append(f"Top performer: {labels[0]} with {self._format_value(values[0])}")

                if len(values) >= 2:
                    gap = ((values[0] - values[1]) / values[1] * 100) if values[1] else 0
                    insights.append(f"Lead margin: {gap:.1f}% over second place")

        elif chart_type in (ChartType.PIE, ChartType.DONUT):
            if labels and total > 0:
                pct = (max_val / total * 100)
                insights.append(f"Dominant segment: {labels[max_idx]} at {pct:.1f}%")

        if not insights:
            insights.append(f"Average value: {self._format_value(avg)}")

        return {'insights': insights}

    def _generate_fallback_summary(
        self,
        charts: List[Dict[str, Any]],
        report_type: str,
        period: str = None
    ) -> str:
        """Generate basic summary without AI."""
        period_text = f" for {period}" if period else ""

        if report_type == 'market':
            return f"Dubai real estate market report{period_text} shows continued activity across key areas."
        elif report_type == 'luxury':
            return f"Luxury market segment (5M+ AED){period_text} demonstrates strong performance in premium locations."
        elif report_type == 'area_guide':
            return f"Area analysis{period_text} reveals distinct market characteristics and pricing patterns."
        else:
            return f"Market report{period_text} provides insights into current real estate trends."

    def _format_value(self, value: float) -> str:
        """Format numeric value for display."""
        if value >= 1_000_000_000:
            return f"AED {value/1_000_000_000:.1f}B"
        elif value >= 1_000_000:
            return f"AED {value/1_000_000:.1f}M"
        elif value >= 1_000:
            return f"AED {value/1_000:.1f}K"
        return f"{value:,.0f}"
