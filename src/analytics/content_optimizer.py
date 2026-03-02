# src/analytics/content_optimizer.py
"""
Unified Content Optimizer

Combines multiple data sources for comprehensive content optimization:
- Google Search Console data (search queries, CTR, positions)
- Property Monitor market intelligence (trends, opportunities, area DNA)
- Screenshot analysis (content structure, visual hierarchy)
- Current page content analysis

Generates actionable, data-driven recommendations for content improvement.
"""

from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import json
import re
import base64

import duckdb

from .gsc_client import GSCClient, PagePerformance, SearchQuery
from .content_reviewer import ContentReviewer, ContentReview, ContentGap


@dataclass
class RelevantSiteQueries:
    """Site-wide queries that should be targeting this page based on content."""
    matching_queries: List[SearchQuery]  # Queries matching page topic
    missed_opportunities: List[Dict[str, Any]]  # High-value queries page should capture
    cannibalization_risks: List[Dict[str, Any]]  # Queries going to wrong pages
    total_opportunity_impressions: int
    total_opportunity_clicks: int


@dataclass
class ScreenshotAnalysis:
    """Analysis results from page screenshot."""
    content_structure: Dict[str, Any]  # Detected sections, hierarchy
    visual_issues: List[str]  # UX/layout concerns
    content_gaps_visual: List[str]  # Missing visual elements
    above_fold_elements: List[str]  # What's visible without scrolling
    cta_analysis: Dict[str, Any]  # Call-to-action effectiveness
    recommendations: List[str]


@dataclass
class MarketDataEnrichment:
    """Market intelligence from Property Monitor data."""
    relevant_areas: List[Dict[str, Any]]  # Area data for mentioned locations
    relevant_developers: List[Dict[str, Any]]  # Developer data if mentioned
    market_trends: Dict[str, Any]  # Current market trends
    pricing_insights: Dict[str, Any]  # Price benchmarks
    opportunity_areas: List[Dict[str, Any]]  # High-opportunity areas to mention
    data_points_to_add: List[str]  # Specific data points missing from content


@dataclass
class ContentOptimizationPlan:
    """Complete content optimization plan combining all data sources."""
    page_url: str
    page_title: str
    analysis_date: str

    # Source analyses
    gsc_review: Optional[ContentReview]
    site_queries: Optional[RelevantSiteQueries]  # Site-wide relevant queries
    screenshot_analysis: Optional[ScreenshotAnalysis]
    market_enrichment: Optional[MarketDataEnrichment]

    # Combined recommendations
    priority_actions: List[Dict[str, Any]]  # Ranked by impact
    content_additions: List[Dict[str, Any]]  # New sections to add
    content_updates: List[Dict[str, Any]]  # Existing content to modify
    seo_fixes: List[Dict[str, Any]]  # Title, meta, headings
    data_enrichment: List[Dict[str, Any]]  # Market data to incorporate

    # Metrics
    estimated_impact: Dict[str, Any]  # Projected improvements
    effort_estimate: str  # low/medium/high

    def to_markdown(self) -> str:
        """Generate comprehensive markdown report."""
        return ContentOptimizationFormatter.to_markdown(self)

    def to_dict(self) -> Dict[str, Any]:
        """Export as dictionary."""
        return {
            'page_url': self.page_url,
            'page_title': self.page_title,
            'analysis_date': self.analysis_date,
            'priority_actions': self.priority_actions,
            'content_additions': self.content_additions,
            'content_updates': self.content_updates,
            'seo_fixes': self.seo_fixes,
            'data_enrichment': self.data_enrichment,
            'estimated_impact': self.estimated_impact,
            'effort_estimate': self.effort_estimate
        }


class ContentOptimizer:
    """
    Unified content optimizer combining GSC, Property Monitor, and visual analysis.

    Usage:
        optimizer = ContentOptimizer(
            gsc_client=gsc_client,
            db_connection=duckdb_connection  # Optional for market data
        )

        plan = optimizer.optimize_page(
            page_url="https://your-site.com/off-plan-projects/",
            page_content="The actual text content...",
            page_title="Off-Plan Projects in Dubai",
            screenshot_path="path/to/screenshot.png"  # Optional
        )

        print(plan.to_markdown())
    """

    def __init__(
        self,
        gsc_client: GSCClient,
        db_connection: Optional[duckdb.DuckDBPyConnection] = None,
        anthropic_client: Optional[Any] = None  # For screenshot analysis
    ):
        self.gsc = gsc_client
        self.db = db_connection
        self.anthropic = anthropic_client
        self.content_reviewer = ContentReviewer(gsc_client)

        # Initialize market intelligence if DB available
        self.market_intel = None
        if db_connection:
            try:
                from .market_intelligence.engine import MarketIntelligenceEngine
                self.market_intel = MarketIntelligenceEngine(db_connection)
            except ImportError:
                pass

    def optimize_page(
        self,
        page_url: str,
        page_content: str,
        page_title: str,
        meta_description: str = "",
        screenshot_path: Optional[str] = None,
        screenshot_base64: Optional[str] = None,
        h1_tags: Optional[List[str]] = None,
        h2_tags: Optional[List[str]] = None,
        days: int = 90
    ) -> ContentOptimizationPlan:
        """
        Generate comprehensive content optimization plan.

        Args:
            page_url: Full URL of the page
            page_content: Text content of the page
            page_title: Page title tag
            meta_description: Meta description
            screenshot_path: Path to screenshot file (optional)
            screenshot_base64: Base64 encoded screenshot (optional)
            h1_tags: List of H1 tag contents
            h2_tags: List of H2 tag contents
            days: Days of GSC data to analyze

        Returns:
            ContentOptimizationPlan with comprehensive recommendations
        """
        # Extract page path from URL
        from urllib.parse import urlparse
        parsed = urlparse(page_url)
        page_path = parsed.path or '/'

        # 1. Get GSC-based content review
        gsc_review = self.content_reviewer.review_page(
            page_path=page_path,
            page_content=page_content,
            page_title=page_title,
            meta_description=meta_description,
            h1_tags=h1_tags,
            h2_tags=h2_tags,
            days=days
        )

        # 2. Analyze screenshot if provided
        screenshot_analysis = None
        if screenshot_path or screenshot_base64:
            screenshot_analysis = self._analyze_screenshot(
                screenshot_path, screenshot_base64, page_content
            )

        # 3. Enrich with market data if available
        market_enrichment = None
        if self.market_intel:
            market_enrichment = self._enrich_with_market_data(
                page_content, page_title, gsc_review
            )

        # 4. Combine all insights into optimization plan
        plan = self._create_optimization_plan(
            page_url=page_url,
            page_title=page_title,
            gsc_review=gsc_review,
            screenshot_analysis=screenshot_analysis,
            market_enrichment=market_enrichment
        )

        return plan

    def _analyze_screenshot(
        self,
        screenshot_path: Optional[str],
        screenshot_base64: Optional[str],
        page_content: str
    ) -> ScreenshotAnalysis:
        """
        Analyze screenshot for content structure and visual issues.

        Uses Claude's vision capabilities if anthropic client available,
        otherwise returns basic analysis.
        """
        # If no Anthropic client, return placeholder analysis
        if not self.anthropic:
            return ScreenshotAnalysis(
                content_structure={'note': 'Screenshot analysis requires Anthropic API'},
                visual_issues=[],
                content_gaps_visual=[],
                above_fold_elements=[],
                cta_analysis={},
                recommendations=['Provide Anthropic API client for visual analysis']
            )

        # Load screenshot
        if screenshot_path:
            with open(screenshot_path, 'rb') as f:
                image_data = base64.standard_b64encode(f.read()).decode('utf-8')
            media_type = 'image/png' if screenshot_path.endswith('.png') else 'image/jpeg'
        else:
            image_data = screenshot_base64
            media_type = 'image/png'  # Assume PNG

        # Analyze with Claude Vision
        prompt = """Analyze this webpage screenshot for content optimization. Provide:

1. **Content Structure**: Identify the main sections, headings hierarchy, and content blocks visible
2. **Above the Fold**: What content is visible without scrolling? Is the value proposition clear?
3. **Visual Issues**: Any UX problems (cluttered layout, poor hierarchy, missing whitespace)?
4. **Missing Elements**: What content elements should be added (trust signals, CTAs, data visualizations)?
5. **CTA Analysis**: Are calls-to-action visible and compelling?
6. **Recommendations**: Top 5 visual/structural improvements

Format as JSON with keys: content_structure, above_fold_elements, visual_issues, content_gaps_visual, cta_analysis, recommendations"""

        try:
            response = self.anthropic.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=2000,
                messages=[{
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": media_type,
                                "data": image_data
                            }
                        },
                        {"type": "text", "text": prompt}
                    ]
                }]
            )

            # Parse JSON from response
            result_text = response.content[0].text
            # Try to extract JSON from response
            json_match = re.search(r'\{[\s\S]*\}', result_text)
            if json_match:
                result = json.loads(json_match.group())
            else:
                result = {'raw_analysis': result_text}

            return ScreenshotAnalysis(
                content_structure=result.get('content_structure', {}),
                visual_issues=result.get('visual_issues', []),
                content_gaps_visual=result.get('content_gaps_visual', []),
                above_fold_elements=result.get('above_fold_elements', []),
                cta_analysis=result.get('cta_analysis', {}),
                recommendations=result.get('recommendations', [])
            )

        except Exception as e:
            return ScreenshotAnalysis(
                content_structure={'error': str(e)},
                visual_issues=[],
                content_gaps_visual=[],
                above_fold_elements=[],
                cta_analysis={},
                recommendations=[f'Screenshot analysis failed: {str(e)}']
            )

    def _enrich_with_market_data(
        self,
        page_content: str,
        page_title: str,
        gsc_review: ContentReview
    ) -> MarketDataEnrichment:
        """
        Enrich content recommendations with Property Monitor market data.
        """
        if not self.market_intel:
            return MarketDataEnrichment(
                relevant_areas=[],
                relevant_developers=[],
                market_trends={},
                pricing_insights={},
                opportunity_areas=[],
                data_points_to_add=['Connect to Property Monitor database for market data']
            )

        content_lower = page_content.lower()
        title_lower = page_title.lower()

        # Extract areas mentioned in content or queries
        areas_mentioned = self._extract_areas(content_lower, gsc_review)
        developers_mentioned = self._extract_developers(content_lower, gsc_review)

        relevant_areas = []
        relevant_developers = []
        data_points_to_add = []

        # Get area intelligence for mentioned areas
        for area in areas_mentioned[:5]:  # Top 5
            try:
                area_intel = self.market_intel.get_area_guide_intelligence(area)
                area_data = area_intel.supporting_data.get('area_dna', {})
                if area_data:
                    relevant_areas.append({
                        'area': area,
                        'avg_price': area_data.get('avg_price'),
                        'avg_price_sqm': area_data.get('avg_price_sqm'),
                        'transactions': area_data.get('transactions'),
                        'segment': area_data.get('segment'),
                        'insights': area_intel.primary_insights[:500]
                    })

                    # Check if this data is in content
                    if area_data.get('avg_price') and str(int(area_data['avg_price'])) not in page_content:
                        data_points_to_add.append(
                            f"Add pricing data for {area}: AED {area_data['avg_price']:,.0f} average"
                        )
            except Exception:
                pass

        # Get developer intelligence
        for dev in developers_mentioned[:3]:
            try:
                dev_intel = self.market_intel.get_developer_intelligence(dev)
                dev_data = dev_intel.supporting_data.get('profile', {})
                if dev_data:
                    relevant_developers.append({
                        'developer': dev,
                        'market_share': dev_data.get('market_share'),
                        'transactions': dev_data.get('transactions'),
                        'primary_areas': dev_data.get('areas', [])[:3],
                        'segment': dev_data.get('segment')
                    })
            except Exception:
                pass

        # Get market-wide trends
        market_trends = {}
        try:
            market_intel = self.market_intel.get_market_report_intelligence(
                datetime.now().year, 'quarterly', (datetime.now().month - 1) // 3 + 1
            )
            market_trends = market_intel.supporting_data.get('trends', {})
        except Exception:
            pass

        # Identify opportunity areas from GSC queries not covered
        opportunity_areas = []
        for query in gsc_review.gsc_performance.opportunity_queries[:10]:
            # Check if query mentions an area we have data for
            query_lower = query.query.lower()
            for area in self._get_known_areas():
                if area.lower() in query_lower:
                    opportunity_areas.append({
                        'query': query.query,
                        'area': area,
                        'impressions': query.impressions,
                        'position': query.position,
                        'action': f"Add content about {area} to capture '{query.query}' searches"
                    })
                    break

        return MarketDataEnrichment(
            relevant_areas=relevant_areas,
            relevant_developers=relevant_developers,
            market_trends=market_trends,
            pricing_insights={},
            opportunity_areas=opportunity_areas,
            data_points_to_add=data_points_to_add
        )

    def _extract_areas(self, content: str, gsc_review: ContentReview) -> List[str]:
        """Extract area names from content and GSC queries."""
        known_areas = self._get_known_areas()
        found = set()

        # From content
        for area in known_areas:
            if area.lower() in content:
                found.add(area)

        # From GSC queries
        for query in gsc_review.gsc_performance.queries[:50]:
            for area in known_areas:
                if area.lower() in query.query.lower():
                    found.add(area)

        return list(found)

    def _extract_developers(self, content: str, gsc_review: ContentReview) -> List[str]:
        """Extract developer names from content and GSC queries."""
        known_developers = [
            'Emaar', 'DAMAC', 'Nakheel', 'Meraas', 'Dubai Properties',
            'Sobha', 'Danube', 'Azizi', 'Ellington', 'MAG', 'Select Group',
            'Omniyat', 'DIRC', 'Deyaar', 'Union Properties'
        ]
        found = set()

        for dev in known_developers:
            if dev.lower() in content:
                found.add(dev)

        for query in gsc_review.gsc_performance.queries[:50]:
            for dev in known_developers:
                if dev.lower() in query.query.lower():
                    found.add(dev)

        return list(found)

    def _get_known_areas(self) -> List[str]:
        """Get list of known Dubai areas."""
        return [
            'Dubai Marina', 'Downtown Dubai', 'Palm Jumeirah', 'JVC', 'Jumeirah Village Circle',
            'Business Bay', 'Dubai Hills', 'Arabian Ranches', 'DIFC', 'JLT', 'Jumeirah Lake Towers',
            'Dubai Creek Harbour', 'Bluewaters', 'City Walk', 'Al Barsha', 'Jumeirah',
            'Deira', 'Bur Dubai', 'Silicon Oasis', 'Sports City', 'Motor City',
            'Dubai South', 'Damac Hills', 'Emaar South', 'Town Square', 'Remraam',
            'Al Furjan', 'Discovery Gardens', 'International City', 'Dubai Land',
            'Mirdif', 'Al Quoz', 'Jebel Ali', 'Palm Jebel Ali', 'Dubai Islands',
            'Damac Islands', 'The Oasis', 'Emirates Hills', 'The Valley'
        ]

    def _create_optimization_plan(
        self,
        page_url: str,
        page_title: str,
        gsc_review: ContentReview,
        screenshot_analysis: Optional[ScreenshotAnalysis],
        market_enrichment: Optional[MarketDataEnrichment]
    ) -> ContentOptimizationPlan:
        """Combine all analyses into unified optimization plan."""

        priority_actions = []
        content_additions = []
        content_updates = []
        seo_fixes = []
        data_enrichment = []

        # Priority score calculation
        def calc_priority(impact: int, effort: int) -> float:
            return impact / max(effort, 1)

        # 1. Process GSC quick wins (highest priority)
        for i, win in enumerate(gsc_review.quick_wins):
            priority_actions.append({
                'action': win,
                'source': 'GSC Analysis',
                'priority': 1 + i,
                'impact': 'high',
                'effort': 'low'
            })

        # 2. Process content gaps from GSC
        for gap in gsc_review.content_gaps[:10]:
            if gap.gap_type == 'missing_section':
                content_additions.append({
                    'section': gap.query,
                    'reason': f"Ranking for '{gap.query}' at position {gap.position:.1f} with {gap.impressions} impressions",
                    'potential_clicks': gap.potential_clicks,
                    'priority': 'high' if gap.priority == 'high' else 'medium'
                })
            elif gap.gap_type == 'weak_coverage':
                content_updates.append({
                    'topic': gap.query,
                    'action': gap.recommendation,
                    'potential_clicks': gap.potential_clicks
                })
            elif gap.gap_type in ['title_mismatch', 'meta_mismatch']:
                seo_fixes.append({
                    'type': gap.gap_type,
                    'query': gap.query,
                    'recommendation': gap.recommendation,
                    'impressions': gap.impressions
                })

        # 3. Add SEO recommendations
        for rec in gsc_review.title_recommendations:
            seo_fixes.append({
                'type': 'title',
                'recommendation': rec
            })
        for rec in gsc_review.meta_recommendations:
            seo_fixes.append({
                'type': 'meta_description',
                'recommendation': rec
            })

        # 4. Process screenshot analysis
        if screenshot_analysis and screenshot_analysis.recommendations:
            for rec in screenshot_analysis.recommendations[:5]:
                priority_actions.append({
                    'action': rec,
                    'source': 'Visual Analysis',
                    'priority': len(priority_actions) + 1,
                    'impact': 'medium',
                    'effort': 'medium'
                })

            for issue in screenshot_analysis.visual_issues[:3]:
                content_updates.append({
                    'topic': 'Visual/UX',
                    'action': f"Fix: {issue}",
                    'potential_clicks': 0  # UX improvements are harder to quantify
                })

        # 5. Process market data enrichment
        if market_enrichment:
            for data_point in market_enrichment.data_points_to_add[:5]:
                data_enrichment.append({
                    'type': 'pricing_data',
                    'recommendation': data_point,
                    'source': 'Property Monitor'
                })

            for opp in market_enrichment.opportunity_areas[:5]:
                content_additions.append({
                    'section': opp['area'],
                    'reason': opp['action'],
                    'potential_clicks': int(opp['impressions'] * 0.05),  # Estimate 5% CTR
                    'priority': 'high' if opp['impressions'] > 1000 else 'medium',
                    'market_data_available': True
                })

        # Calculate estimated impact
        total_potential_clicks = sum(
            item.get('potential_clicks', 0)
            for item in content_additions + content_updates
        )

        estimated_impact = {
            'potential_additional_clicks': total_potential_clicks,
            'queries_to_improve': len(gsc_review.content_gaps),
            'near_page_1_queries': len(gsc_review.gsc_performance.near_ranking_queries),
            'current_clicks': gsc_review.gsc_performance.total_clicks,
            'current_impressions': gsc_review.gsc_performance.total_impressions,
            'projected_improvement': f"+{total_potential_clicks / max(gsc_review.gsc_performance.total_clicks, 1) * 100:.1f}%" if total_potential_clicks > 0 else "N/A"
        }

        # Estimate effort
        total_changes = len(content_additions) + len(content_updates) + len(seo_fixes)
        if total_changes <= 5:
            effort_estimate = 'low'
        elif total_changes <= 15:
            effort_estimate = 'medium'
        else:
            effort_estimate = 'high'

        return ContentOptimizationPlan(
            page_url=page_url,
            page_title=page_title,
            analysis_date=datetime.now().strftime('%Y-%m-%d'),
            gsc_review=gsc_review,
            site_queries=None,  # Not currently populated
            screenshot_analysis=screenshot_analysis,
            market_enrichment=market_enrichment,
            priority_actions=priority_actions,
            content_additions=content_additions,
            content_updates=content_updates,
            seo_fixes=seo_fixes,
            data_enrichment=data_enrichment,
            estimated_impact=estimated_impact,
            effort_estimate=effort_estimate
        )


class ContentOptimizationFormatter:
    """Formats optimization plans as markdown reports."""

    @staticmethod
    def to_markdown(plan: ContentOptimizationPlan) -> str:
        """Generate comprehensive markdown report."""
        lines = []

        # Header
        lines.append(f"# Content Optimization Plan")
        lines.append(f"\n**Page:** {plan.page_url}")
        lines.append(f"**Title:** {plan.page_title}")
        lines.append(f"**Analysis Date:** {plan.analysis_date}")
        lines.append(f"**Effort Estimate:** {plan.effort_estimate.upper()}")

        # Executive Summary
        lines.append("\n---\n")
        lines.append("## Executive Summary\n")
        impact = plan.estimated_impact
        lines.append(f"| Metric | Current | Potential |")
        lines.append(f"|--------|---------|-----------|")
        lines.append(f"| Clicks | {impact.get('current_clicks', 0):,} | +{impact.get('potential_additional_clicks', 0):,} |")
        lines.append(f"| Impressions | {impact.get('current_impressions', 0):,} | - |")
        lines.append(f"| Queries to Improve | {impact.get('queries_to_improve', 0)} | - |")
        lines.append(f"| Near Page 1 | {impact.get('near_page_1_queries', 0)} | - |")
        if impact.get('projected_improvement'):
            lines.append(f"\n**Projected Traffic Improvement:** {impact['projected_improvement']}")

        # Priority Actions
        if plan.priority_actions:
            lines.append("\n## Priority Actions (Do These First)\n")
            for i, action in enumerate(plan.priority_actions[:10], 1):
                source = action.get('source', 'Analysis')
                impact_level = action.get('impact', 'medium')
                lines.append(f"{i}. **[{impact_level.upper()}]** {action['action']}")
                lines.append(f"   - Source: {source}")

        # SEO Fixes
        if plan.seo_fixes:
            lines.append("\n## SEO Fixes\n")

            title_fixes = [f for f in plan.seo_fixes if f.get('type') == 'title']
            meta_fixes = [f for f in plan.seo_fixes if f.get('type') == 'meta_description']
            other_fixes = [f for f in plan.seo_fixes if f.get('type') not in ['title', 'meta_description']]

            if title_fixes:
                lines.append("### Title Tag")
                for fix in title_fixes:
                    lines.append(f"- {fix.get('recommendation', fix)}")

            if meta_fixes:
                lines.append("\n### Meta Description")
                for fix in meta_fixes:
                    lines.append(f"- {fix.get('recommendation', fix)}")

            if other_fixes:
                lines.append("\n### Other SEO Issues")
                for fix in other_fixes:
                    query = fix.get('query', '')
                    rec = fix.get('recommendation', '')
                    lines.append(f"- **{query}**: {rec}")

        # Content Additions
        if plan.content_additions:
            lines.append("\n## New Sections to Add\n")
            lines.append("| Topic | Reason | Potential Clicks | Priority |")
            lines.append("|-------|--------|-----------------|----------|")
            for item in sorted(plan.content_additions, key=lambda x: x.get('potential_clicks', 0), reverse=True)[:15]:
                topic = item.get('section', 'Unknown')
                reason = item.get('reason', '')[:60] + '...' if len(item.get('reason', '')) > 60 else item.get('reason', '')
                clicks = item.get('potential_clicks', 0)
                priority = item.get('priority', 'medium')
                market_badge = ' [PM Data]' if item.get('market_data_available') else ''
                lines.append(f"| {topic}{market_badge} | {reason} | +{clicks:,} | {priority} |")

        # Content Updates
        if plan.content_updates:
            lines.append("\n## Content Updates Needed\n")
            for item in plan.content_updates[:10]:
                topic = item.get('topic', '')
                action = item.get('action', '')
                lines.append(f"- **{topic}**: {action}")

        # Market Data Enrichment
        if plan.data_enrichment:
            lines.append("\n## Market Data to Add (Property Monitor)\n")
            for item in plan.data_enrichment:
                lines.append(f"- {item.get('recommendation', '')}")

        # Market Intelligence Section
        if plan.market_enrichment:
            me = plan.market_enrichment

            if me.relevant_areas:
                lines.append("\n## Available Market Data\n")
                lines.append("### Area Data")
                for area in me.relevant_areas[:5]:
                    lines.append(f"\n**{area['area']}**")
                    if area.get('avg_price'):
                        lines.append(f"- Average Price: AED {area['avg_price']:,.0f}")
                    if area.get('avg_price_sqm'):
                        lines.append(f"- Price/sqm: AED {area['avg_price_sqm']:,.0f}")
                    if area.get('transactions'):
                        lines.append(f"- Transactions: {area['transactions']:,}")
                    if area.get('segment'):
                        lines.append(f"- Segment: {area['segment']}")

            if me.opportunity_areas:
                lines.append("\n### Opportunity Areas from Search Data")
                for opp in me.opportunity_areas[:5]:
                    lines.append(f"- **{opp['query']}** ({opp['impressions']:,} impressions, position {opp['position']:.1f})")
                    lines.append(f"  - Action: {opp['action']}")

        # Screenshot Analysis
        if plan.screenshot_analysis and plan.screenshot_analysis.content_structure:
            lines.append("\n## Visual Analysis\n")

            if plan.screenshot_analysis.above_fold_elements:
                lines.append("### Above the Fold")
                for elem in plan.screenshot_analysis.above_fold_elements[:5]:
                    lines.append(f"- {elem}")

            if plan.screenshot_analysis.visual_issues:
                lines.append("\n### Visual Issues")
                for issue in plan.screenshot_analysis.visual_issues[:5]:
                    lines.append(f"- {issue}")

            if plan.screenshot_analysis.cta_analysis:
                lines.append("\n### CTA Analysis")
                cta = plan.screenshot_analysis.cta_analysis
                if isinstance(cta, dict):
                    for key, value in cta.items():
                        lines.append(f"- {key}: {value}")

        # GSC Query Data
        if plan.gsc_review:
            lines.append("\n## Top Search Queries\n")
            lines.append("| Query | Impressions | Clicks | Position | Opportunity |")
            lines.append("|-------|-------------|--------|----------|-------------|")
            for q in plan.gsc_review.gsc_performance.top_queries[:15]:
                opp_score = q.opportunity_score
                opp_indicator = "High" if opp_score > 70 else "Medium" if opp_score > 40 else "Low"
                lines.append(f"| {q.query} | {q.impressions:,} | {q.clicks} | {q.position:.1f} | {opp_indicator} |")

        # Footer
        lines.append("\n---\n")
        lines.append("*Report generated by Content Optimizer - combining GSC, Property Monitor, and visual analysis*")

        return '\n'.join(lines)
