# src/analytics/content_reviewer.py
"""
Content Review Analyzer with GSC Integration

Combines Google Search Console data with content analysis to generate
data-driven content optimization recommendations.
"""

from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import json
import re

from .gsc_client import GSCClient, PagePerformance, SearchQuery


@dataclass
class ContentGap:
    """Represents a gap between search intent and content."""
    query: str
    impressions: int
    clicks: int
    position: float
    gap_type: str  # 'missing_section', 'weak_coverage', 'title_mismatch', 'meta_mismatch'
    recommendation: str
    priority: str  # 'high', 'medium', 'low'
    potential_clicks: int  # Estimated additional clicks if fixed


@dataclass
class ContentScore:
    """Scoring for content optimization."""
    query_coverage: float  # 0-100: How well content covers ranking queries
    intent_alignment: float  # 0-100: Title/H1 alignment with top queries
    ctr_efficiency: float  # 0-100: CTR vs expected for positions
    opportunity_index: float  # 0-100: Untapped potential
    overall: float  # Weighted average


@dataclass
class ContentReview:
    """Complete content review with GSC-informed recommendations."""
    page_url: str
    page_path: str
    review_date: str

    # GSC Performance
    gsc_performance: PagePerformance

    # Content Analysis
    content_score: ContentScore
    content_gaps: List[ContentGap]

    # Recommendations
    title_recommendations: List[str]
    meta_recommendations: List[str]
    content_recommendations: List[Dict[str, Any]]
    keyword_opportunities: List[Dict[str, Any]]

    # Quick Wins (lowest effort, highest impact)
    quick_wins: List[str]

    def to_markdown(self) -> str:
        """Generate markdown report."""
        return ContentReviewFormatter.to_markdown(self)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON export."""
        return {
            'page_url': self.page_url,
            'page_path': self.page_path,
            'review_date': self.review_date,
            'performance': {
                'clicks': self.gsc_performance.total_clicks,
                'impressions': self.gsc_performance.total_impressions,
                'ctr': self.gsc_performance.avg_ctr,
                'position': self.gsc_performance.avg_position,
                'date_range': self.gsc_performance.date_range
            },
            'scores': {
                'query_coverage': self.content_score.query_coverage,
                'intent_alignment': self.content_score.intent_alignment,
                'ctr_efficiency': self.content_score.ctr_efficiency,
                'opportunity_index': self.content_score.opportunity_index,
                'overall': self.content_score.overall
            },
            'gaps': [
                {
                    'query': g.query,
                    'type': g.gap_type,
                    'priority': g.priority,
                    'recommendation': g.recommendation,
                    'potential_clicks': g.potential_clicks
                }
                for g in self.content_gaps
            ],
            'recommendations': {
                'title': self.title_recommendations,
                'meta': self.meta_recommendations,
                'content': self.content_recommendations,
                'keywords': self.keyword_opportunities
            },
            'quick_wins': self.quick_wins
        }


class ContentReviewer:
    """
    Generates data-driven content reviews using GSC data.

    Usage:
        reviewer = ContentReviewer(gsc_client)

        # Review a specific page
        review = reviewer.review_page(
            page_path='/off-plan-projects/',
            page_content="The actual content of the page...",
            page_title="Off-Plan Projects in Dubai"
        )

        # Export to markdown
        report = review.to_markdown()
    """

    def __init__(self, gsc_client: GSCClient):
        self.gsc = gsc_client

    def review_page(
        self,
        page_path: str,
        page_content: str,
        page_title: str,
        meta_description: Optional[str] = None,
        h1_tags: Optional[List[str]] = None,
        h2_tags: Optional[List[str]] = None,
        days: int = 90
    ) -> ContentReview:
        """
        Generate a comprehensive content review for a page.

        Args:
            page_path: URL path (e.g., '/off-plan-projects/')
            page_content: Full text content of the page
            page_title: Page <title> tag content
            meta_description: Meta description content
            h1_tags: List of H1 tag contents
            h2_tags: List of H2 tag contents
            days: Days of GSC data to analyze

        Returns:
            ContentReview with data-driven recommendations
        """
        # Get GSC performance data
        performance = self.gsc.get_page_performance(page_path, days=days)

        # Analyze content gaps
        content_gaps = self._analyze_content_gaps(
            performance, page_content, page_title, h1_tags or [], h2_tags or []
        )

        # Calculate content score
        content_score = self._calculate_content_score(
            performance, page_content, page_title, content_gaps
        )

        # Generate recommendations
        title_recs = self._generate_title_recommendations(performance, page_title)
        meta_recs = self._generate_meta_recommendations(performance, meta_description or "")
        content_recs = self._generate_content_recommendations(performance, page_content, content_gaps)
        keyword_opps = self._identify_keyword_opportunities(performance, page_content)

        # Identify quick wins
        quick_wins = self._identify_quick_wins(
            performance, content_gaps, title_recs, meta_recs
        )

        return ContentReview(
            page_url=performance.page_url,
            page_path=page_path,
            review_date=datetime.now().strftime('%Y-%m-%d'),
            gsc_performance=performance,
            content_score=content_score,
            content_gaps=content_gaps,
            title_recommendations=title_recs,
            meta_recommendations=meta_recs,
            content_recommendations=content_recs,
            keyword_opportunities=keyword_opps,
            quick_wins=quick_wins
        )

    def _analyze_content_gaps(
        self,
        performance: PagePerformance,
        content: str,
        title: str,
        h1_tags: List[str],
        h2_tags: List[str]
    ) -> List[ContentGap]:
        """Identify gaps between search queries and content."""
        gaps = []
        content_lower = content.lower()
        title_lower = title.lower()
        h1_lower = ' '.join(h1_tags).lower()
        h2_lower = ' '.join(h2_tags).lower()

        for query in performance.queries:
            if query.impressions < 5:  # Skip very low volume queries
                continue

            query_words = set(query.query.lower().split())
            query_lower = query.query.lower()

            # Check title alignment
            title_match = sum(1 for w in query_words if w in title_lower) / len(query_words)

            # Check H1/H2 coverage
            h1_match = sum(1 for w in query_words if w in h1_lower) / len(query_words)
            h2_match = sum(1 for w in query_words if w in h2_lower) / len(query_words)

            # Check content coverage
            content_match = sum(1 for w in query_words if w in content_lower) / len(query_words)

            # Calculate potential clicks (if we improved to position 3)
            current_ctr = query.ctr
            potential_ctr = 0.11  # Position 3 average
            potential_clicks = int((potential_ctr - current_ctr) * query.impressions) if query.position > 3 else 0

            # Identify gap type and priority
            if content_match < 0.5:
                # Query words not well represented in content
                gaps.append(ContentGap(
                    query=query.query,
                    impressions=query.impressions,
                    clicks=query.clicks,
                    position=query.position,
                    gap_type='missing_section',
                    recommendation=f"Add dedicated section covering '{query.query}' - currently weak/missing in content",
                    priority='high' if query.impressions >= 50 else 'medium',
                    potential_clicks=potential_clicks
                ))
            elif title_match < 0.3 and query.impressions >= 20:
                # High-volume query not reflected in title
                gaps.append(ContentGap(
                    query=query.query,
                    impressions=query.impressions,
                    clicks=query.clicks,
                    position=query.position,
                    gap_type='title_mismatch',
                    recommendation=f"Consider incorporating '{query.query}' concepts into page title",
                    priority='high' if query.position <= 10 else 'medium',
                    potential_clicks=potential_clicks
                ))
            elif h2_match < 0.3 and content_match >= 0.5:
                # Content covers it but no clear heading
                gaps.append(ContentGap(
                    query=query.query,
                    impressions=query.impressions,
                    clicks=query.clicks,
                    position=query.position,
                    gap_type='weak_coverage',
                    recommendation=f"Add H2 heading for '{query.query}' to improve content structure",
                    priority='medium',
                    potential_clicks=potential_clicks
                ))
            elif query.ctr < query._expected_ctr_for_position(query.position) * 0.6:
                # CTR significantly below expected - meta description issue
                gaps.append(ContentGap(
                    query=query.query,
                    impressions=query.impressions,
                    clicks=query.clicks,
                    position=query.position,
                    gap_type='meta_mismatch',
                    recommendation=f"Improve meta description to better address '{query.query}' search intent",
                    priority='high' if query.impressions >= 30 else 'low',
                    potential_clicks=potential_clicks
                ))

        # Sort by priority and potential
        priority_order = {'high': 0, 'medium': 1, 'low': 2}
        gaps.sort(key=lambda x: (priority_order[x.priority], -x.potential_clicks))

        return gaps[:20]  # Top 20 gaps

    def _calculate_content_score(
        self,
        performance: PagePerformance,
        content: str,
        title: str,
        gaps: List[ContentGap]
    ) -> ContentScore:
        """Calculate content optimization scores."""
        content_lower = content.lower()

        # Query coverage: what % of top queries are well-covered in content
        top_queries = performance.top_queries[:20]
        covered = 0
        for q in top_queries:
            words = q.query.lower().split()
            match_rate = sum(1 for w in words if w in content_lower) / len(words)
            if match_rate >= 0.6:
                covered += 1
        query_coverage = (covered / len(top_queries) * 100) if top_queries else 50

        # Intent alignment: do title/headings match top query themes
        title_lower = title.lower()
        top_5_queries = [q.query.lower() for q in top_queries[:5]]
        title_words = set(title_lower.split())
        query_words = set(' '.join(top_5_queries).split())
        common_words = title_words & query_words - {'in', 'the', 'a', 'for', 'and', 'or', 'to', 'of'}
        intent_alignment = min(100, len(common_words) * 20)

        # CTR efficiency: actual CTR vs expected for positions
        total_expected_clicks = 0
        total_actual_clicks = 0
        for q in performance.queries:
            expected_ctr = q._expected_ctr_for_position(q.position)
            total_expected_clicks += expected_ctr * q.impressions
            total_actual_clicks += q.clicks
        ctr_efficiency = (total_actual_clicks / total_expected_clicks * 100) if total_expected_clicks > 0 else 50
        ctr_efficiency = min(100, ctr_efficiency)  # Cap at 100

        # Opportunity index: how much untapped potential
        high_priority_gaps = len([g for g in gaps if g.priority == 'high'])
        near_ranking = len(performance.near_ranking_queries)
        opportunity_index = min(100, (high_priority_gaps * 5 + near_ranking * 3))

        # Overall score (weighted)
        overall = (
            query_coverage * 0.30 +
            intent_alignment * 0.25 +
            ctr_efficiency * 0.25 +
            (100 - opportunity_index) * 0.20  # Lower opportunity = better current state
        )

        return ContentScore(
            query_coverage=round(query_coverage, 1),
            intent_alignment=round(intent_alignment, 1),
            ctr_efficiency=round(ctr_efficiency, 1),
            opportunity_index=round(opportunity_index, 1),
            overall=round(overall, 1)
        )

    def _generate_title_recommendations(
        self,
        performance: PagePerformance,
        current_title: str
    ) -> List[str]:
        """Generate title tag optimization recommendations."""
        recommendations = []
        top_queries = performance.top_queries[:10]

        if not top_queries:
            return ["Insufficient search data to make title recommendations"]

        # Find top query themes not in title
        title_lower = current_title.lower()
        missing_themes = []
        for q in top_queries[:5]:
            key_words = [w for w in q.query.lower().split()
                        if len(w) > 3 and w not in title_lower]
            if key_words:
                missing_themes.extend(key_words)

        if missing_themes:
            common_missing = list(set(missing_themes))[:3]
            recommendations.append(
                f"Consider incorporating these high-volume search terms: {', '.join(common_missing)}"
            )

        # Check title length
        if len(current_title) > 60:
            recommendations.append(
                f"Title is {len(current_title)} characters - consider shortening to under 60 for full SERP display"
            )
        elif len(current_title) < 30:
            recommendations.append(
                "Title is short - consider expanding with relevant keywords from top queries"
            )

        # Suggest title variations based on top queries
        top_query = top_queries[0].query if top_queries else ""
        if top_query and top_query.lower() not in title_lower:
            recommendations.append(
                f"Top query '{top_query}' ({top_queries[0].impressions} impressions) not reflected in title"
            )

        return recommendations

    def _generate_meta_recommendations(
        self,
        performance: PagePerformance,
        current_meta: str
    ) -> List[str]:
        """Generate meta description recommendations."""
        recommendations = []

        # Find low-CTR high-impression queries
        low_ctr_queries = performance.low_ctr_queries[:5]

        if low_ctr_queries:
            queries_str = ', '.join([f"'{q.query}'" for q in low_ctr_queries[:3]])
            recommendations.append(
                f"CTR is below expected for: {queries_str}. Meta description may not be addressing these search intents."
            )

        # Check meta length
        if len(current_meta) > 160:
            recommendations.append(
                f"Meta description is {len(current_meta)} characters - will be truncated in SERPs"
            )
        elif len(current_meta) < 120:
            recommendations.append(
                "Meta description is short - add compelling details to improve CTR"
            )

        # Suggest including call-to-action
        cta_words = ['discover', 'explore', 'find', 'learn', 'get', 'start', 'view']
        if not any(word in current_meta.lower() for word in cta_words):
            recommendations.append(
                "Consider adding a call-to-action (e.g., 'Explore', 'Discover', 'Find') to improve CTR"
            )

        return recommendations

    def _generate_content_recommendations(
        self,
        performance: PagePerformance,
        content: str,
        gaps: List[ContentGap]
    ) -> List[Dict[str, Any]]:
        """Generate content improvement recommendations."""
        recommendations = []

        # Group gaps by type
        gap_types = {}
        for gap in gaps:
            if gap.gap_type not in gap_types:
                gap_types[gap.gap_type] = []
            gap_types[gap.gap_type].append(gap)

        # Missing section recommendations
        if 'missing_section' in gap_types:
            missing = gap_types['missing_section'][:5]
            total_potential = sum(g.potential_clicks for g in missing)
            recommendations.append({
                'type': 'Add New Sections',
                'priority': 'high',
                'queries': [g.query for g in missing],
                'potential_clicks': total_potential,
                'action': f"Create dedicated sections for these topics: {', '.join([g.query for g in missing[:3]])}"
            })

        # Weak coverage recommendations
        if 'weak_coverage' in gap_types:
            weak = gap_types['weak_coverage'][:5]
            recommendations.append({
                'type': 'Improve Content Structure',
                'priority': 'medium',
                'queries': [g.query for g in weak],
                'action': f"Add H2/H3 headings to better organize content around: {', '.join([g.query for g in weak[:3]])}"
            })

        # Near-ranking opportunities
        near_ranking = performance.near_ranking_queries[:10]
        if near_ranking:
            total_impressions = sum(q.impressions for q in near_ranking)
            recommendations.append({
                'type': 'Push to Page 1',
                'priority': 'high',
                'queries': [q.query for q in near_ranking],
                'current_positions': [(q.query, round(q.position, 1)) for q in near_ranking[:5]],
                'potential_impressions': total_impressions,
                'action': f"Strengthen content for queries ranking 11-30 to push them to page 1"
            })

        # Content freshness
        # Check for year references in content
        year_pattern = r'\b20[12][0-9]\b'
        years_found = re.findall(year_pattern, content)
        if years_found:
            oldest_year = min(int(y) for y in years_found)
            current_year = datetime.now().year
            if oldest_year < current_year - 1:
                recommendations.append({
                    'type': 'Update Outdated Information',
                    'priority': 'high',
                    'action': f"Content references year {oldest_year} - update with current data for {current_year}"
                })

        return recommendations

    def _identify_keyword_opportunities(
        self,
        performance: PagePerformance,
        content: str
    ) -> List[Dict[str, Any]]:
        """Identify keyword optimization opportunities."""
        opportunities = []
        content_lower = content.lower()

        # High-opportunity queries
        for query in performance.opportunity_queries[:15]:
            query_words = query.query.lower().split()
            in_content = sum(1 for w in query_words if w in content_lower)
            coverage = in_content / len(query_words)

            opportunities.append({
                'query': query.query,
                'impressions': query.impressions,
                'clicks': query.clicks,
                'position': round(query.position, 1),
                'ctr': f"{query.ctr*100:.1f}%",
                'opportunity_score': round(query.opportunity_score, 1),
                'current_coverage': f"{coverage*100:.0f}%",
                'action': 'Strengthen' if coverage >= 0.5 else 'Add content for'
            })

        return opportunities

    def _identify_quick_wins(
        self,
        performance: PagePerformance,
        gaps: List[ContentGap],
        title_recs: List[str],
        meta_recs: List[str]
    ) -> List[str]:
        """Identify quick wins with lowest effort and highest impact."""
        quick_wins = []

        # Title/meta changes (low effort, can have big impact)
        if title_recs:
            quick_wins.append(f"Title optimization: {title_recs[0]}")

        low_ctr = performance.low_ctr_queries[:3]
        if low_ctr:
            quick_wins.append(
                f"Improve meta description to boost CTR for '{low_ctr[0].query}' "
                f"(position {low_ctr[0].position:.1f}, CTR {low_ctr[0].ctr*100:.1f}%)"
            )

        # High-impression, slightly off position queries
        for q in performance.queries:
            if 8 <= q.position <= 12 and q.impressions >= 30:
                quick_wins.append(
                    f"Small push needed: '{q.query}' at position {q.position:.1f} "
                    f"with {q.impressions} impressions - add 1-2 paragraphs to push to page 1"
                )
                break

        # Missing obvious heading
        for gap in gaps:
            if gap.gap_type == 'weak_coverage' and gap.impressions >= 30:
                quick_wins.append(
                    f"Add H2 heading for '{gap.query}' - content exists but structure is weak"
                )
                break

        return quick_wins[:5]


class ContentReviewFormatter:
    """Formats content reviews as markdown reports."""

    @staticmethod
    def to_markdown(review: ContentReview) -> str:
        """Convert ContentReview to markdown report."""
        lines = []

        # Header
        lines.append(f"# Content Review: {review.page_path}")
        lines.append(f"\n**Review Date:** {review.review_date}")
        lines.append(f"**Page URL:** {review.page_url}")
        lines.append(f"**Data Period:** {review.gsc_performance.date_range['start']} to {review.gsc_performance.date_range['end']}")

        # Performance Summary
        lines.append("\n---\n")
        lines.append("## Performance Summary\n")
        perf = review.gsc_performance
        lines.append(f"| Metric | Value |")
        lines.append(f"|--------|-------|")
        lines.append(f"| Total Clicks | {perf.total_clicks:,} |")
        lines.append(f"| Total Impressions | {perf.total_impressions:,} |")
        lines.append(f"| Average CTR | {perf.avg_ctr*100:.2f}% |")
        lines.append(f"| Average Position | {perf.avg_position:.1f} |")

        # Content Score
        lines.append("\n## Content Score\n")
        score = review.content_score
        lines.append(f"**Overall Score: {score.overall}/100**\n")
        lines.append(f"| Component | Score | Status |")
        lines.append(f"|-----------|-------|--------|")
        lines.append(f"| Query Coverage | {score.query_coverage}/100 | {ContentReviewFormatter._score_status(score.query_coverage)} |")
        lines.append(f"| Intent Alignment | {score.intent_alignment}/100 | {ContentReviewFormatter._score_status(score.intent_alignment)} |")
        lines.append(f"| CTR Efficiency | {score.ctr_efficiency}/100 | {ContentReviewFormatter._score_status(score.ctr_efficiency)} |")
        lines.append(f"| Opportunity Index | {score.opportunity_index}/100 | {'High Potential' if score.opportunity_index > 50 else 'Optimized'} |")

        # Quick Wins
        if review.quick_wins:
            lines.append("\n## Quick Wins (Do These First)\n")
            for i, win in enumerate(review.quick_wins, 1):
                lines.append(f"{i}. {win}")

        # Top Search Queries
        lines.append("\n## Top Search Queries\n")
        lines.append("| Query | Impressions | Clicks | CTR | Position |")
        lines.append("|-------|-------------|--------|-----|----------|")
        for q in perf.top_queries[:15]:
            lines.append(f"| {q.query} | {q.impressions:,} | {q.clicks} | {q.ctr*100:.1f}% | {q.position:.1f} |")

        # Content Gaps
        if review.content_gaps:
            lines.append("\n## Content Gaps Identified\n")
            high_priority = [g for g in review.content_gaps if g.priority == 'high']
            if high_priority:
                lines.append("### High Priority\n")
                for gap in high_priority[:5]:
                    lines.append(f"- **{gap.query}** ({gap.impressions} impressions, position {gap.position:.1f})")
                    lines.append(f"  - Issue: {gap.gap_type.replace('_', ' ').title()}")
                    lines.append(f"  - Action: {gap.recommendation}")
                    lines.append(f"  - Potential: +{gap.potential_clicks} clicks")

        # Keyword Opportunities
        if review.keyword_opportunities:
            lines.append("\n## Keyword Opportunities\n")
            lines.append("| Query | Position | Impressions | Opportunity Score | Action |")
            lines.append("|-------|----------|-------------|-------------------|--------|")
            for kw in review.keyword_opportunities[:10]:
                lines.append(f"| {kw['query']} | {kw['position']} | {kw['impressions']:,} | {kw['opportunity_score']} | {kw['action']} |")

        # Recommendations
        lines.append("\n## Recommendations\n")

        if review.title_recommendations:
            lines.append("### Title Tag\n")
            for rec in review.title_recommendations:
                lines.append(f"- {rec}")

        if review.meta_recommendations:
            lines.append("\n### Meta Description\n")
            for rec in review.meta_recommendations:
                lines.append(f"- {rec}")

        if review.content_recommendations:
            lines.append("\n### Content Improvements\n")
            for rec in review.content_recommendations:
                lines.append(f"#### {rec['type']} ({rec['priority'].upper()} priority)")
                lines.append(f"- {rec['action']}")
                if 'queries' in rec:
                    lines.append(f"- Target queries: {', '.join(rec['queries'][:5])}")
                if 'potential_clicks' in rec:
                    lines.append(f"- Potential: +{rec['potential_clicks']} clicks")

        # Footer
        lines.append("\n---\n")
        lines.append("*Report generated using GSC data analysis*")

        return '\n'.join(lines)

    @staticmethod
    def _score_status(score: float) -> str:
        """Convert score to status indicator."""
        if score >= 80:
            return "Excellent"
        elif score >= 60:
            return "Good"
        elif score >= 40:
            return "Needs Work"
        else:
            return "Poor"
