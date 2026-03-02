# src/visualization/generator.py
"""
VizGenerator - Main orchestrator for visualization generation.

Coordinates all visualization agents to produce complete chart packages
for reports, including both static (matplotlib) and interactive (Chart.js)
outputs with AI-powered storytelling.
"""

from typing import Dict, List, Any, Optional
from pathlib import Path
from dataclasses import dataclass
from enum import Enum

from .agents.data_analyst import DataAnalystAgent
from .agents.chart_selector import ChartSelectorAgent, ChartType, ChartConfig
from .agents.renderer import VizRendererAgent
from .agents.storyteller import StorytellerAgent
from .web.chart_configs import ChartJSConfigGenerator


class OutputFormat(Enum):
    """Output format options."""
    STATIC = "static"           # matplotlib PNG only
    INTERACTIVE = "interactive" # Chart.js JSON only
    BOTH = "both"              # Both formats


@dataclass
class VisualizationResult:
    """Result of a visualization generation."""
    chart_name: str
    title: str
    chart_type: ChartType
    static_image: str = None      # Base64 or file path
    interactive_config: Dict = None  # Chart.js config
    insights: List[str] = None
    legend_description: str = None
    markdown: str = None          # Ready-to-embed markdown


class VizGenerator:
    """
    Main orchestrator for visualization generation.

    Coordinates:
    - DataAnalystAgent: Prepares chart-ready data
    - ChartSelectorAgent: Selects optimal chart types
    - VizRendererAgent: Generates static charts
    - ChartJSConfigGenerator: Creates interactive configs
    - StorytellerAgent: Adds AI-powered insights

    Usage:
        generator = VizGenerator()
        results = generator.generate_report_visualizations(
            report_type='market',
            year=2024,
            quarter=4
        )
    """

    def __init__(
        self,
        db_path: str = None,
        output_dir: str = None,
        enable_ai: bool = True
    ):
        """
        Initialize the visualization generator.

        Args:
            db_path: Path to database
            output_dir: Directory for output files
            enable_ai: Enable AI-powered storytelling
        """
        self.data_analyst = DataAnalystAgent(db_path=db_path)
        self.chart_selector = ChartSelectorAgent()
        self.renderer = VizRendererAgent(output_dir=output_dir)
        self.chartjs_generator = ChartJSConfigGenerator()
        self.storyteller = StorytellerAgent() if enable_ai else None

    def generate_report_visualizations(
        self,
        report_type: str,
        year: int = None,
        quarter: int = None,
        output_format: OutputFormat = OutputFormat.BOTH,
        include_insights: bool = True
    ) -> Dict[str, VisualizationResult]:
        """
        Generate all visualizations for a report type.

        Args:
            report_type: 'market', 'luxury', 'area_guide', 'offplan', 'developer'
            year: Year filter
            quarter: Quarter filter
            output_format: Static, interactive, or both
            include_insights: Generate AI insights

        Returns:
            Dictionary of VisualizationResult by chart name
        """
        results = {}

        # Prepare data based on report type
        data_package = self._prepare_report_data(report_type, year, quarter)

        # Get chart configurations
        configs = self.chart_selector.get_charts_for_report(report_type, data_package)

        # Generate each chart
        for config in configs:
            chart_name = config.options.get('chart_name', 'chart')

            try:
                result = self._generate_single_chart(
                    config=config,
                    output_format=output_format,
                    include_insights=include_insights
                )
                results[chart_name] = result

            except Exception as e:
                print(f"Error generating chart '{chart_name}': {e}")
                results[chart_name] = VisualizationResult(
                    chart_name=chart_name,
                    title=config.title,
                    chart_type=config.chart_type,
                    insights=[f"Error generating chart: {e}"]
                )

        return results

    def generate_chart(
        self,
        chart_type: str,
        data: Dict[str, Any],
        title: str = "Chart",
        output_format: OutputFormat = OutputFormat.BOTH,
        include_insights: bool = True
    ) -> VisualizationResult:
        """
        Generate a single chart from raw data.

        Args:
            chart_type: 'line', 'bar', 'horizontal_bar', 'pie', 'donut', etc.
            data: Chart data with 'labels' and 'values'
            title: Chart title
            output_format: Output format
            include_insights: Generate AI insights

        Returns:
            VisualizationResult
        """
        # Map string to ChartType
        type_map = {
            'line': ChartType.LINE,
            'bar': ChartType.BAR,
            'horizontal_bar': ChartType.HORIZONTAL_BAR,
            'stacked_bar': ChartType.STACKED_BAR,
            'grouped_bar': ChartType.GROUPED_BAR,
            'pie': ChartType.PIE,
            'donut': ChartType.DONUT,
        }

        chart_type_enum = type_map.get(chart_type, ChartType.BAR)

        config = self.chart_selector.create_chart_config(
            chart_type=chart_type_enum,
            title=title,
            data=data,
            chart_name=chart_type
        )

        return self._generate_single_chart(config, output_format, include_insights)

    def generate_trend_chart(
        self,
        year: int,
        metric: str = 'transaction_count',
        segment: str = None,
        title: str = None,
        output_format: OutputFormat = OutputFormat.BOTH
    ) -> VisualizationResult:
        """
        Convenience method for generating trend charts.

        Args:
            year: Year to analyze
            metric: 'transaction_count', 'total_volume', or 'avg_price'
            segment: Optional 'luxury', 'offplan', 'ready'
            title: Chart title

        Returns:
            VisualizationResult
        """
        data = self.data_analyst.prepare_monthly_trend(year, metric, segment)

        if title is None:
            metric_names = {
                'transaction_count': 'Transaction',
                'total_volume': 'Volume',
                'avg_price': 'Average Price'
            }
            segment_text = f" - {segment.title()}" if segment else ""
            title = f"{metric_names.get(metric, metric)} Trend {year}{segment_text}"

        return self.generate_chart('line', data, title, output_format)

    def generate_top_areas_chart(
        self,
        year: int = None,
        quarter: int = None,
        metric: str = 'transaction_count',
        limit: int = 10,
        segment: str = None,
        output_format: OutputFormat = OutputFormat.BOTH
    ) -> VisualizationResult:
        """
        Generate top areas ranking chart.

        Returns:
            VisualizationResult
        """
        data = self.data_analyst.prepare_top_areas(year, quarter, metric, limit, segment)

        period_text = f"Q{quarter} {year}" if quarter else str(year) if year else ""
        segment_text = f" - {segment.title()}" if segment else ""
        title = f"Top {limit} Areas by {data.get('metric_label', 'Transactions')} {period_text}{segment_text}"

        return self.generate_chart('horizontal_bar', data, title, output_format)

    def generate_segment_chart(
        self,
        year: int = None,
        quarter: int = None,
        segment_type: str = 'offplan_ready',
        output_format: OutputFormat = OutputFormat.BOTH
    ) -> VisualizationResult:
        """
        Generate market segment distribution chart.

        Returns:
            VisualizationResult
        """
        data = self.data_analyst.prepare_market_segments(year, quarter, segment_type)

        segment_titles = {
            'offplan_ready': 'Off-Plan vs Ready',
            'luxury_tiers': 'Price Tier Distribution',
            'property_types': 'Property Type Mix'
        }

        period_text = f" - Q{quarter} {year}" if quarter else f" - {year}" if year else ""
        title = f"{segment_titles.get(segment_type, 'Market Segments')}{period_text}"

        return self.generate_chart('donut', data, title, output_format)

    def _prepare_report_data(
        self,
        report_type: str,
        year: int = None,
        quarter: int = None
    ) -> Dict[str, Dict[str, Any]]:
        """Prepare all data needed for a report type."""
        data = {}

        if report_type == 'market':
            data['monthly_trend'] = self.data_analyst.prepare_monthly_trend(
                year or 2024, 'transaction_count'
            )
            data['market_segments'] = self.data_analyst.prepare_market_segments(
                year, quarter, 'offplan_ready'
            )
            data['top_areas'] = self.data_analyst.prepare_top_areas(
                year, quarter, 'transaction_count', 10
            )

        elif report_type == 'luxury':
            data['luxury_trend'] = self.data_analyst.prepare_monthly_trend(
                year or 2024, 'transaction_count', 'luxury'
            )
            data['luxury_tiers'] = self.data_analyst.prepare_market_segments(
                year, quarter, 'luxury_tiers'
            )
            data['top_luxury_areas'] = self.data_analyst.prepare_top_areas(
                year, quarter, 'transaction_count', 10, 'luxury'
            )

        elif report_type == 'offplan':
            data['offplan_trend'] = self.data_analyst.prepare_monthly_trend(
                year or 2024, 'transaction_count', 'offplan'
            )
            data['offplan_vs_ready'] = self.data_analyst.prepare_market_segments(
                year, quarter, 'offplan_ready'
            )
            data['developer_pipeline'] = self.data_analyst.prepare_top_developers(
                year, quarter, 'transaction_count', 10, 'offplan'
            )

        elif report_type == 'area_guide':
            data['price_trend'] = self.data_analyst.prepare_monthly_trend(
                year or 2024, 'avg_price'
            )
            data['property_types'] = self.data_analyst.prepare_market_segments(
                year, quarter, 'property_types'
            )
            data['developer_share'] = self.data_analyst.prepare_top_developers(
                year, quarter, 'transaction_count', 10
            )

        elif report_type == 'developer':
            data['transaction_volume'] = self.data_analyst.prepare_monthly_trend(
                year or 2024, 'total_volume'
            )
            data['project_areas'] = self.data_analyst.prepare_top_areas(
                year, quarter, 'transaction_count', 10
            )

        return data

    def _generate_single_chart(
        self,
        config: ChartConfig,
        output_format: OutputFormat,
        include_insights: bool
    ) -> VisualizationResult:
        """Generate a single chart with all outputs."""
        chart_name = config.options.get('chart_name', 'chart')

        result = VisualizationResult(
            chart_name=chart_name,
            title=config.title,
            chart_type=config.chart_type
        )

        # Generate static image
        if output_format in (OutputFormat.STATIC, OutputFormat.BOTH):
            render_result = self.renderer.render(config, embed_base64=True)
            result.static_image = render_result.get('base64')
            result.markdown = render_result.get('markdown')

        # Generate interactive config
        if output_format in (OutputFormat.INTERACTIVE, OutputFormat.BOTH):
            result.interactive_config = self.chartjs_generator.generate(
                config.chart_type,
                config.data,
                config.title
            )

        # Generate insights
        if include_insights and self.storyteller:
            insights_result = self.storyteller.generate_insights(
                config.chart_type,
                config.data
            )
            result.insights = insights_result.get('insights', [])

            result.legend_description = self.storyteller.generate_legend_description(
                config.chart_type,
                config.data
            )

        return result

    def get_markdown_for_report(
        self,
        results: Dict[str, VisualizationResult]
    ) -> str:
        """
        Generate markdown content with all charts and insights.

        Args:
            results: Dictionary of VisualizationResult

        Returns:
            Markdown string ready for embedding in report
        """
        markdown_parts = []

        for chart_name, result in results.items():
            # Add chart title
            markdown_parts.append(f"\n### {result.title}\n")

            # Add chart image
            if result.markdown:
                markdown_parts.append(result.markdown)
                markdown_parts.append("")

            # Add insights
            if result.insights:
                markdown_parts.append("\n**Key Insights:**")
                for insight in result.insights:
                    markdown_parts.append(f"- {insight}")
                markdown_parts.append("")

            # Add legend description
            if result.legend_description:
                markdown_parts.append(f"*{result.legend_description}*")
                markdown_parts.append("")

        return "\n".join(markdown_parts)

    def close(self):
        """Clean up resources."""
        if self.data_analyst:
            self.data_analyst.close()
