# src/analytics/gsc_client.py
"""
Google Search Console API Client

Provides access to GSC data for content optimization analysis.
Requires a service account with Search Console API access.
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
import json

from google.oauth2 import service_account
from googleapiclient.discovery import build


@dataclass
class SearchQuery:
    """Represents a search query with metrics."""
    query: str
    clicks: int
    impressions: int
    ctr: float
    position: float

    @property
    def opportunity_score(self) -> float:
        """
        Calculate opportunity score for content optimization.
        High impressions + low CTR + position 5-20 = high opportunity.
        """
        # Position opportunity: best opportunity is positions 5-20 (page 1-2)
        if 5 <= self.position <= 20:
            position_score = 1.0 - (abs(self.position - 10) / 15)
        elif self.position < 5:
            position_score = 0.3  # Already ranking well
        else:
            position_score = max(0.1, 1.0 - (self.position - 20) / 80)

        # CTR opportunity: low CTR with high impressions = title/meta issue
        expected_ctr = self._expected_ctr_for_position(self.position)
        ctr_gap = max(0, expected_ctr - self.ctr)
        ctr_score = min(1.0, ctr_gap / expected_ctr) if expected_ctr > 0 else 0

        # Volume factor: more impressions = more valuable opportunity
        volume_score = min(1.0, self.impressions / 1000)

        return (position_score * 0.4 + ctr_score * 0.3 + volume_score * 0.3) * 100

    def _expected_ctr_for_position(self, position: float) -> float:
        """Expected CTR based on position (industry benchmarks)."""
        expected = {
            1: 0.28, 2: 0.15, 3: 0.11, 4: 0.08, 5: 0.06,
            6: 0.05, 7: 0.04, 8: 0.03, 9: 0.03, 10: 0.02
        }
        pos = int(min(position, 10))
        return expected.get(pos, 0.01)


@dataclass
class PagePerformance:
    """Aggregated performance data for a specific page."""
    page_url: str
    total_clicks: int
    total_impressions: int
    avg_ctr: float
    avg_position: float
    queries: List[SearchQuery]
    date_range: Dict[str, str]

    @property
    def top_queries(self) -> List[SearchQuery]:
        """Top queries by impressions."""
        return sorted(self.queries, key=lambda x: x.impressions, reverse=True)[:20]

    @property
    def opportunity_queries(self) -> List[SearchQuery]:
        """Queries with highest optimization opportunity."""
        return sorted(self.queries, key=lambda x: x.opportunity_score, reverse=True)[:20]

    @property
    def low_ctr_queries(self) -> List[SearchQuery]:
        """Queries with CTR below expected for position."""
        results = []
        for q in self.queries:
            expected = q._expected_ctr_for_position(q.position)
            if q.ctr < expected * 0.7 and q.impressions >= 10:  # 30%+ below expected
                results.append(q)
        return sorted(results, key=lambda x: x.impressions, reverse=True)[:15]

    @property
    def near_ranking_queries(self) -> List[SearchQuery]:
        """Queries ranking 11-30 that could be pushed to page 1."""
        return [
            q for q in self.queries
            if 11 <= q.position <= 30 and q.impressions >= 5
        ][:15]


@dataclass
class SiteOverview:
    """Site-wide performance overview."""
    site_url: str
    total_clicks: int
    total_impressions: int
    avg_ctr: float
    avg_position: float
    top_pages: List[Dict[str, Any]]
    top_queries: List[SearchQuery]
    date_range: Dict[str, str]


class GSCClient:
    """
    Google Search Console API client for content optimization.

    Usage:
        client = GSCClient(
            credentials_path='path/to/service-account.json',
            site_url='https://your-site.com'
        )

        # Get page-specific data
        page_data = client.get_page_performance('/off-plan-projects/')

        # Get site overview
        overview = client.get_site_overview()
    """

    SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']

    def __init__(
        self,
        credentials_path: str,
        site_url: str = ''
    ):
        """
        Initialize GSC client.

        Args:
            credentials_path: Path to service account JSON file
            site_url: The site URL as registered in Search Console
                     (e.g., 'https://your-site.com' or 'sc-domain:your-site.com')
        """
        self.credentials_path = Path(credentials_path)
        self.site_url = site_url
        self._service = None

    @property
    def service(self):
        """Lazy-load the GSC service."""
        if self._service is None:
            credentials = service_account.Credentials.from_service_account_file(
                str(self.credentials_path),
                scopes=self.SCOPES
            )
            self._service = build('searchconsole', 'v1', credentials=credentials)
        return self._service

    def get_page_performance(
        self,
        page_path: str,
        days: int = 90,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> PagePerformance:
        """
        Get performance data for a specific page.

        Args:
            page_path: URL path (e.g., '/off-plan-projects/' or full URL)
            days: Number of days to look back (default 90)
            start_date: Override start date (YYYY-MM-DD)
            end_date: Override end date (YYYY-MM-DD)

        Returns:
            PagePerformance object with queries and metrics
        """
        if end_date is None:
            # GSC data has ~3 day delay
            end_date = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=days + 3)).strftime('%Y-%m-%d')

        # Build full URL if only path provided
        if page_path.startswith('/'):
            page_url = f"{self.site_url.rstrip('/')}{page_path}"
        else:
            page_url = page_path

        # Query for page-specific data with query dimension
        request = {
            'startDate': start_date,
            'endDate': end_date,
            'dimensions': ['query'],
            'dimensionFilterGroups': [{
                'filters': [{
                    'dimension': 'page',
                    'operator': 'contains',
                    'expression': page_path.strip('/')
                }]
            }],
            'rowLimit': 500
        }

        response = self.service.searchanalytics().query(
            siteUrl=self.site_url,
            body=request
        ).execute()

        queries = []
        total_clicks = 0
        total_impressions = 0

        for row in response.get('rows', []):
            query = SearchQuery(
                query=row['keys'][0],
                clicks=row['clicks'],
                impressions=row['impressions'],
                ctr=row['ctr'],
                position=row['position']
            )
            queries.append(query)
            total_clicks += row['clicks']
            total_impressions += row['impressions']

        avg_ctr = total_clicks / total_impressions if total_impressions > 0 else 0
        avg_position = sum(q.position * q.impressions for q in queries) / total_impressions if total_impressions > 0 else 0

        return PagePerformance(
            page_url=page_url,
            total_clicks=total_clicks,
            total_impressions=total_impressions,
            avg_ctr=avg_ctr,
            avg_position=avg_position,
            queries=queries,
            date_range={'start': start_date, 'end': end_date}
        )

    def get_site_overview(
        self,
        days: int = 90,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> SiteOverview:
        """
        Get site-wide performance overview.

        Args:
            days: Number of days to look back
            start_date: Override start date
            end_date: Override end date

        Returns:
            SiteOverview with top pages and queries
        """
        if end_date is None:
            end_date = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=days + 3)).strftime('%Y-%m-%d')

        # Get top pages
        pages_request = {
            'startDate': start_date,
            'endDate': end_date,
            'dimensions': ['page'],
            'rowLimit': 100
        }

        pages_response = self.service.searchanalytics().query(
            siteUrl=self.site_url,
            body=pages_request
        ).execute()

        top_pages = []
        for row in pages_response.get('rows', []):
            top_pages.append({
                'page': row['keys'][0],
                'clicks': row['clicks'],
                'impressions': row['impressions'],
                'ctr': row['ctr'],
                'position': row['position']
            })

        # Get top queries
        queries_request = {
            'startDate': start_date,
            'endDate': end_date,
            'dimensions': ['query'],
            'rowLimit': 200
        }

        queries_response = self.service.searchanalytics().query(
            siteUrl=self.site_url,
            body=queries_request
        ).execute()

        top_queries = []
        total_clicks = 0
        total_impressions = 0

        for row in queries_response.get('rows', []):
            query = SearchQuery(
                query=row['keys'][0],
                clicks=row['clicks'],
                impressions=row['impressions'],
                ctr=row['ctr'],
                position=row['position']
            )
            top_queries.append(query)
            total_clicks += row['clicks']
            total_impressions += row['impressions']

        avg_ctr = total_clicks / total_impressions if total_impressions > 0 else 0
        avg_position = sum(q.position * q.impressions for q in top_queries) / total_impressions if total_impressions > 0 else 0

        return SiteOverview(
            site_url=self.site_url,
            total_clicks=total_clicks,
            total_impressions=total_impressions,
            avg_ctr=avg_ctr,
            avg_position=avg_position,
            top_pages=top_pages,
            top_queries=top_queries,
            date_range={'start': start_date, 'end': end_date}
        )

    def get_query_pages(
        self,
        query: str,
        days: int = 90
    ) -> List[Dict[str, Any]]:
        """
        Find which pages rank for a specific query.

        Useful for content cannibalization analysis.
        """
        end_date = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days + 3)).strftime('%Y-%m-%d')

        request = {
            'startDate': start_date,
            'endDate': end_date,
            'dimensions': ['page'],
            'dimensionFilterGroups': [{
                'filters': [{
                    'dimension': 'query',
                    'operator': 'contains',
                    'expression': query
                }]
            }],
            'rowLimit': 50
        }

        response = self.service.searchanalytics().query(
            siteUrl=self.site_url,
            body=request
        ).execute()

        return [
            {
                'page': row['keys'][0],
                'clicks': row['clicks'],
                'impressions': row['impressions'],
                'ctr': row['ctr'],
                'position': row['position']
            }
            for row in response.get('rows', [])
        ]

    def list_sites(self) -> List[str]:
        """List all sites accessible with current credentials."""
        response = self.service.sites().list().execute()
        return [site['siteUrl'] for site in response.get('siteEntry', [])]


# Convenience function for quick access
def create_client(
    site_url: str = ''
) -> GSCClient:
    """
    Create GSC client using default credentials path.

    Searches for credentials in standard locations.
    """
    # Check common paths
    paths = [
        Path('credentials/gsc-service-account.json'),
        Path('credentials/gsc-service-account.json'),
        Path.home() / '.config/gsc/credentials.json'
    ]

    for path in paths:
        if path.exists():
            return GSCClient(str(path), site_url)

    raise FileNotFoundError(
        "GSC credentials not found. Please provide credentials_path or place "
        "service account JSON in one of the standard locations."
    )
