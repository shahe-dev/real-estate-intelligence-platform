# src/visualization/agents/renderer.py
"""
VizRendererAgent - Renders charts using matplotlib for static output.

Responsible for:
- Creating matplotlib figures from chart configurations
- Applying consistent styling
- Exporting to various formats (PNG, SVG, base64)
- Managing chart lifecycle
"""

from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
import os

from .chart_selector import ChartType, ChartConfig
from ..charts import LineChart, BarChart, PieChart


class VizRendererAgent:
    """
    Agent for rendering static visualizations using matplotlib.

    Coordinates with chart classes to produce publication-quality
    static images for embedding in reports.
    """

    def __init__(self, output_dir: str = None):
        """
        Initialize the renderer agent.

        Args:
            output_dir: Directory for saving chart files
        """
        if output_dir is None:
            base_dir = Path(__file__).parent.parent.parent.parent
            output_dir = base_dir / "data" / "charts"

        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def render(
        self,
        config: ChartConfig,
        output_format: str = 'png',
        embed_base64: bool = False
    ) -> Dict[str, Any]:
        """
        Render a chart from configuration.

        Args:
            config: ChartConfig object with all settings
            output_format: 'png', 'svg', or 'both'
            embed_base64: Return base64 encoded image for embedding

        Returns:
            Dictionary with file paths and/or base64 data
        """
        # Select chart class based on type
        chart = self._create_chart(config.chart_type)

        # Render with appropriate method
        result = self._render_chart(chart, config)

        # Export
        output = {'config': config}

        if embed_base64:
            output['base64'] = chart.to_base64(fmt='png')
            output['markdown'] = chart.to_markdown(alt_text=config.title)
        else:
            filename = self._generate_filename(config)
            filepath = self.output_dir / f"{filename}.{output_format}"
            chart.save(str(filepath))
            output['filepath'] = str(filepath)
            output['relative_path'] = f"./charts/{filename}.{output_format}"

        return output

    def render_multiple(
        self,
        configs: List[ChartConfig],
        embed_base64: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Render multiple charts.

        Args:
            configs: List of ChartConfig objects
            embed_base64: Return base64 for embedding

        Returns:
            List of render results
        """
        results = []
        for config in configs:
            try:
                result = self.render(config, embed_base64=embed_base64)
                results.append(result)
            except Exception as e:
                results.append({
                    'config': config,
                    'error': str(e)
                })
        return results

    def render_report_charts(
        self,
        report_type: str,
        data: Dict[str, Dict[str, Any]],
        year: int = None,
        period: str = None,
        embed_base64: bool = True
    ) -> Dict[str, Dict[str, Any]]:
        """
        Render all charts for a report type.

        Args:
            report_type: 'market', 'luxury', 'area_guide', etc.
            data: Dictionary of data by chart name
            year: Year for filename generation
            period: Period identifier (Q1, H2, etc.)
            embed_base64: Embed as base64 for Markdown

        Returns:
            Dictionary of rendered charts by name
        """
        from .chart_selector import ChartSelectorAgent

        selector = ChartSelectorAgent()
        configs = selector.get_charts_for_report(report_type, data)

        results = {}
        for config in configs:
            chart_name = config.options.get('chart_name', 'chart')
            try:
                result = self.render(config, embed_base64=embed_base64)
                results[chart_name] = result
            except Exception as e:
                results[chart_name] = {'error': str(e)}

        return results

    def _create_chart(self, chart_type: ChartType):
        """Create appropriate chart instance for type."""
        if chart_type == ChartType.LINE:
            return LineChart()
        elif chart_type == ChartType.AREA:
            chart = LineChart()
            return chart
        elif chart_type in (ChartType.BAR, ChartType.STACKED_BAR, ChartType.GROUPED_BAR):
            return BarChart(horizontal=False)
        elif chart_type == ChartType.HORIZONTAL_BAR:
            return BarChart(horizontal=True)
        elif chart_type == ChartType.PIE:
            return PieChart(donut=False)
        elif chart_type == ChartType.DONUT:
            return PieChart(donut=True)
        else:
            return BarChart()

    def _render_chart(self, chart, config: ChartConfig):
        """Render chart based on its type."""
        chart_type = config.chart_type
        data = config.data
        title = config.title
        options = config.static_options

        if chart_type == ChartType.LINE:
            chart.render(
                data=data,
                title=title,
                show_markers=options.get('show_markers', True),
                fill_area=options.get('fill_area', False),
                show_trend=options.get('show_trend', False),
                value_format=options.get('value_format', 'number')
            )

        elif chart_type == ChartType.AREA:
            chart.render(
                data=data,
                title=title,
                show_markers=False,
                fill_area=True,
                value_format=options.get('value_format', 'number')
            )

        elif chart_type == ChartType.BAR:
            chart.render(
                data=data,
                title=title,
                show_values=options.get('show_values', True),
                value_format=options.get('value_format', 'number')
            )

        elif chart_type == ChartType.HORIZONTAL_BAR:
            chart.render_top_performers(
                data=data,
                title=title,
                value_format=options.get('value_format', 'number'),
                show_rank=options.get('show_rank', True),
                use_area_colors=options.get('use_area_colors', False)
            )

        elif chart_type == ChartType.GROUPED_BAR:
            chart.render_grouped(
                data=data,
                title=title,
                value_format=options.get('value_format', 'number')
            )

        elif chart_type == ChartType.STACKED_BAR:
            chart.render_stacked(
                data=data,
                title=title,
                show_totals=options.get('show_totals', True),
                value_format=options.get('value_format', 'number')
            )

        elif chart_type == ChartType.PIE:
            chart.render(
                data=data,
                title=title,
                show_percentages=options.get('show_percentages', True),
                explode_largest=options.get('explode_largest', True)
            )

        elif chart_type == ChartType.DONUT:
            segment_type = data.get('segment_type')
            if segment_type:
                chart.render_market_segments(
                    data=data,
                    title=title,
                    segment_type=segment_type
                )
            else:
                chart.render(
                    data=data,
                    title=title,
                    show_percentages=options.get('show_percentages', True),
                    explode_largest=options.get('explode_largest', True)
                )

        return chart

    def _generate_filename(self, config: ChartConfig) -> str:
        """Generate filename for chart."""
        chart_name = config.options.get('chart_name', 'chart')
        title_slug = config.title.lower().replace(' ', '_').replace('/', '_')[:30]
        return f"{chart_name}_{title_slug}"

    def get_chart_as_markdown(
        self,
        config: ChartConfig,
        alt_text: str = None
    ) -> str:
        """
        Render chart and return Markdown image syntax.

        Args:
            config: Chart configuration
            alt_text: Alternative text for image

        Returns:
            Markdown image string with embedded base64
        """
        result = self.render(config, embed_base64=True)
        return result.get('markdown', '')

    def render_trend_chart(
        self,
        data: Dict[str, Any],
        title: str = 'Trend',
        **kwargs
    ) -> Dict[str, Any]:
        """Convenience method for rendering trend charts."""
        from .chart_selector import ChartSelectorAgent

        selector = ChartSelectorAgent()
        config = selector.create_chart_config(
            chart_type=ChartType.LINE,
            title=title,
            data=data,
            chart_name='trend'
        )

        return self.render(config, embed_base64=True)

    def render_ranking_chart(
        self,
        data: Dict[str, Any],
        title: str = 'Top Performers',
        **kwargs
    ) -> Dict[str, Any]:
        """Convenience method for rendering ranking charts."""
        from .chart_selector import ChartSelectorAgent

        selector = ChartSelectorAgent()
        config = selector.create_chart_config(
            chart_type=ChartType.HORIZONTAL_BAR,
            title=title,
            data=data,
            chart_name='ranking'
        )

        return self.render(config, embed_base64=True)

    def render_segment_chart(
        self,
        data: Dict[str, Any],
        title: str = 'Market Segments',
        **kwargs
    ) -> Dict[str, Any]:
        """Convenience method for rendering segment charts."""
        from .chart_selector import ChartSelectorAgent

        selector = ChartSelectorAgent()
        config = selector.create_chart_config(
            chart_type=ChartType.DONUT,
            title=title,
            data=data,
            chart_name='segments'
        )

        return self.render(config, embed_base64=True)
