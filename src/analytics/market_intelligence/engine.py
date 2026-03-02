# src/analytics/market_intelligence/engine.py
"""
Market Intelligence Engine - Orchestrates all intelligence modules

Provides a single interface to generate comprehensive market intelligence
for content generation. Combines:
- Anomaly detection (records, spikes, new developers)
- Opportunity identification (hotspots, arbitrage, momentum)
- Trend prediction (momentum, seasonality, cycles)
- Comparative analytics (area DNA, profiles)
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import duckdb

from .anomaly_detector import AnomalyDetector, AnomalyResults
from .opportunity_detector import OpportunityDetector, OpportunityResults
from .trend_predictor import TrendPredictor, TrendResults
from .comparative_analytics import ComparativeAnalytics, AreaDNA
from ..supply_intelligence import SupplyIntelligence


@dataclass
class MarketIntelligence:
    """Complete market intelligence for a period/scope"""
    scope: Dict[str, Any]
    anomalies: Optional[AnomalyResults]
    opportunities: Optional[OpportunityResults]
    trends: Optional[TrendResults]
    area_profile: Optional[AreaDNA]
    market_concentration: Optional[Dict[str, Any]]


@dataclass
class ContentTypeIntelligence:
    """Intelligence tailored for specific content types"""
    content_type: str
    primary_insights: str  # Formatted for prompt injection
    supporting_data: Dict[str, Any]  # Raw data for reference


class MarketIntelligenceEngine:
    """
    Central orchestrator for all market intelligence modules.

    Usage:
        engine = MarketIntelligenceEngine(connection)

        # For market reports
        intel = engine.get_market_report_intelligence(2024, 'quarterly', 4)

        # For area guides
        intel = engine.get_area_guide_intelligence('Dubai Marina')

        # For developer profiles
        intel = engine.get_developer_intelligence('Emaar')
    """

    def __init__(self, connection: duckdb.DuckDBPyConnection):
        self.con = connection
        self.anomaly = AnomalyDetector(connection)
        self.opportunity = OpportunityDetector(connection)
        self.trend = TrendPredictor(connection)
        self.comparative = ComparativeAnalytics(connection)

        # Supply intelligence integration (Phase 2)
        # Uses connection-based initialization for consistency
        self.supply_intel = SupplyIntelligence(db_path=connection.path if hasattr(connection, 'path') else 'data/database/property_monitor.db')

    def get_market_report_intelligence(
        self,
        year: int,
        period_type: str,
        period_number: int
    ) -> ContentTypeIntelligence:
        """
        Generate intelligence for market reports.

        Includes all modules for comprehensive market overview.
        """
        # Run all detectors
        anomalies = self.anomaly.detect_anomalies(year, period_type, period_number)
        opportunities = self.opportunity.detect_opportunities(year, period_type, period_number)
        trends = self.trend.analyze_trends()  # Market-wide
        concentration = self.comparative.get_market_concentration()

        # Format for prompt
        sections = []

        # Anomalies section
        anomaly_text = self.anomaly.format_for_prompt(anomalies)
        if anomaly_text and "No significant" not in anomaly_text:
            sections.append("### Notable Findings\n" + anomaly_text)

        # Opportunities section
        opportunity_text = self.opportunity.format_for_prompt(opportunities)
        if opportunity_text and "No significant" not in opportunity_text:
            sections.append("### Market Opportunities\n" + opportunity_text)

        # Trends section
        trend_text = self.trend.format_for_prompt(trends)
        if trend_text and "Insufficient" not in trend_text:
            sections.append("### Trend Analysis\n" + trend_text)

        # Market concentration
        if concentration:
            conc_text = "### Market Concentration\n"
            conc_text += f"- Top 5 areas: {concentration['area_concentration']['top_5_share']:.1f}% of transactions\n"
            conc_text += f"- Top 5 developers: {concentration['developer_concentration']['top_5_share']:.1f}% of transactions\n"
            sections.append(conc_text)

        primary_insights = "\n\n".join(sections) if sections else "Market intelligence analysis completed. No significant anomalies or opportunities detected."

        return ContentTypeIntelligence(
            content_type="market_report",
            primary_insights=primary_insights,
            supporting_data={
                'anomalies': self._serialize_anomalies(anomalies),
                'opportunities': self._serialize_opportunities(opportunities),
                'trends': self._serialize_trends(trends),
                'concentration': concentration
            }
        )

    def get_area_guide_intelligence(
        self,
        area_name: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> ContentTypeIntelligence:
        """
        Generate intelligence for area guides.

        Focuses on area-specific analysis and comparisons.
        """
        # Area DNA profile
        area_dna = self.comparative.get_area_dna(area_name, start_date, end_date)

        # Area-specific trends
        trends = self.trend.analyze_trends(area_name=area_name)

        # Supply intelligence (Phase 2)
        supply_data = self.supply_intel.get_area_intelligence(area_name)

        # Format for prompt
        sections = []

        # Area profile
        dna_text = self.comparative.format_area_dna_for_prompt(area_dna)
        if dna_text:
            sections.append("### Area Profile (Property Monitor Exclusive)\n" + dna_text)

        # Trends for this area
        trend_text = self.trend.format_for_prompt(trends)
        if trend_text and "Insufficient" not in trend_text:
            sections.append("### Area Trends\n" + trend_text)

        # Supply intelligence (Phase 2 - Unique Competitive Advantage)
        if supply_data and 'error' not in supply_data:
            supply_text = self._format_supply_for_area_guide(supply_data)
            if supply_text:
                sections.append(supply_text)

        # Comparable areas context
        if area_dna.comparable_areas:
            comp_text = "### Comparable Areas for Context\n"
            comp_text += f"Similar market characteristics to: {', '.join(area_dna.comparable_areas)}\n"
            comp_text += "Consider these for price/value comparisons in the narrative.\n"
            sections.append(comp_text)

        primary_insights = "\n\n".join(sections) if sections else f"Area profile generated for {area_name}."

        return ContentTypeIntelligence(
            content_type="area_guide",
            primary_insights=primary_insights,
            supporting_data={
                'area_dna': self._serialize_area_dna(area_dna),
                'trends': self._serialize_trends(trends),
                'comparable_areas': area_dna.comparable_areas,
                'supply_intelligence': supply_data if supply_data and 'error' not in supply_data else None
            }
        )

    def get_developer_intelligence(
        self,
        developer_name: str,
        year: Optional[int] = None,
        period_type: Optional[str] = None,
        period_number: Optional[int] = None
    ) -> ContentTypeIntelligence:
        """
        Generate intelligence for developer profiles.

        Focuses on developer positioning and momentum.
        """
        # Developer profile
        dev_profile = self.comparative.get_developer_profile(developer_name)

        # Get momentum data if period specified
        momentum_data = None
        if year and period_type and period_number:
            opportunities = self.opportunity.detect_opportunities(year, period_type, period_number)
            # Find this developer in momentum list
            for dev in opportunities.developer_momentum:
                if dev.developer == developer_name:
                    momentum_data = dev
                    break

        # Format for prompt
        sections = []

        # Developer profile
        profile_text = "### Developer Profile\n"
        profile_text += f"- Market segment focus: {dev_profile.market_segment_focus.value.replace('_', '-').title()}\n"
        profile_text += f"- Primary areas: {', '.join(dev_profile.primary_areas[:5])}\n"
        profile_text += f"- Off-plan focus: {dev_profile.offplan_focus:.1f}%\n"
        profile_text += f"- Market share: {dev_profile.market_share:.2f}%\n"
        profile_text += f"- Total transactions: {dev_profile.total_transactions:,}\n"
        sections.append(profile_text)

        # Property type focus
        if dev_profile.property_type_focus:
            type_text = "### Property Type Focus\n"
            for ptype, pct in sorted(dev_profile.property_type_focus.items(), key=lambda x: x[1], reverse=True)[:3]:
                type_text += f"- {ptype}: {pct:.1f}%\n"
            sections.append(type_text)

        # Momentum if available
        if momentum_data:
            momentum_text = "### Market Momentum\n"
            momentum_text += f"- Market share change: {momentum_data.market_share_change:+.2f}pp\n"
            momentum_text += f"- Trend: {momentum_data.trend.title()}\n"
            sections.append(momentum_text)

        primary_insights = "\n\n".join(sections)

        return ContentTypeIntelligence(
            content_type="developer_profile",
            primary_insights=primary_insights,
            supporting_data={
                'profile': {
                    'segment': dev_profile.market_segment_focus.value,
                    'areas': dev_profile.primary_areas,
                    'offplan_focus': dev_profile.offplan_focus,
                    'market_share': dev_profile.market_share,
                    'transactions': dev_profile.total_transactions,
                    'property_types': dev_profile.property_type_focus
                },
                'momentum': {
                    'share_change': momentum_data.market_share_change if momentum_data else None,
                    'trend': momentum_data.trend if momentum_data else None
                } if momentum_data else None
            }
        )

    def get_luxury_report_intelligence(
        self,
        year: int,
        period_type: str,
        period_number: int
    ) -> ContentTypeIntelligence:
        """
        Generate intelligence for luxury market reports.

        Focuses on high-value transactions and luxury segment analysis.
        """
        # Run anomaly detection (captures record transactions)
        anomalies = self.anomaly.detect_anomalies(year, period_type, period_number)

        # Market concentration for luxury context
        concentration = self.comparative.get_market_concentration()

        # Format for prompt
        sections = []

        # Record transactions (luxury focus)
        if anomalies.record_transactions:
            record_text = "### Record Transactions\n"
            for tx in anomalies.record_transactions[:5]:
                record_text += f"- #{tx.rank}: AED {tx.price:,.0f} - {tx.property_type} in {tx.area}\n"
            sections.append(record_text)

        # Ultra-luxury specific
        if anomalies.ultra_luxury_records:
            ultra_text = "### Ultra-Luxury Segment (AED 10M+)\n"
            for tx in anomalies.ultra_luxury_records[:3]:
                ultra_text += f"- AED {tx.price:,.0f} {tx.property_type} in {tx.area}\n"
            sections.append(ultra_text)

        # Luxury concentration
        if concentration and 'segment_distribution' in concentration:
            luxury_pct = concentration['segment_distribution'].get('luxury', 0)
            ultra_pct = concentration['segment_distribution'].get('ultra_luxury', 0)
            seg_text = "### Luxury Market Share\n"
            seg_text += f"- Luxury (5-10M): {luxury_pct:.1f}% of transactions\n"
            seg_text += f"- Ultra-luxury (10M+): {ultra_pct:.1f}% of transactions\n"
            sections.append(seg_text)

        primary_insights = "\n\n".join(sections) if sections else "Luxury market intelligence generated."

        return ContentTypeIntelligence(
            content_type="luxury_report",
            primary_insights=primary_insights,
            supporting_data={
                'record_transactions': [
                    {'area': tx.area, 'price': tx.price, 'type': tx.property_type}
                    for tx in anomalies.record_transactions[:10]
                ],
                'ultra_luxury': [
                    {'area': tx.area, 'price': tx.price, 'type': tx.property_type}
                    for tx in anomalies.ultra_luxury_records
                ],
                'segment_distribution': concentration.get('segment_distribution', {}) if concentration else {}
            }
        )

    def get_offplan_report_intelligence(
        self,
        year: int,
        period_type: str,
        period_number: int
    ) -> ContentTypeIntelligence:
        """
        Generate intelligence for off-plan market reports.

        Focuses on off-plan specific trends and developer activity.
        """
        # Run detectors
        anomalies = self.anomaly.detect_anomalies(year, period_type, period_number)
        opportunities = self.opportunity.detect_opportunities(year, period_type, period_number)
        trends = self.trend.analyze_trends()

        # Format for prompt
        sections = []

        # New developers (often off-plan focused)
        if anomalies.new_developers:
            new_dev_text = "### New Market Entrants\n"
            for dev in anomalies.new_developers[:5]:
                new_dev_text += f"- {dev.developer}: {dev.transaction_count} sales, AED {dev.total_value:,.0f} total\n"
            sections.append(new_dev_text)

        # Developer surges (launch activity indicator)
        if anomalies.developer_surges:
            surge_text = "### Developer Activity Surges\n"
            for surge in anomalies.developer_surges[:5]:
                surge_text += f"- {surge['developer']}: +{surge['surge_percentage']:.1f}% vs historical\n"
            sections.append(surge_text)

        # Volume spikes (potential new launch areas)
        if anomalies.volume_spikes:
            spike_text = "### Volume Spikes (Potential Launch Activity)\n"
            for spike in anomalies.volume_spikes[:5]:
                spike_text += f"- {spike.area}: +{spike.spike_percentage:.1f}% above historical\n"
            sections.append(spike_text)

        # Emerging hotspots (off-plan interest indicators)
        if opportunities.emerging_hotspots:
            hotspot_text = "### Emerging Off-Plan Hotspots\n"
            for spot in opportunities.emerging_hotspots[:5]:
                hotspot_text += f"- {spot.area}: Volume +{spot.tx_growth_rate:.1f}%, Price +{spot.price_growth_rate:.1f}%\n"
            sections.append(hotspot_text)

        primary_insights = "\n\n".join(sections) if sections else "Off-plan market intelligence generated."

        return ContentTypeIntelligence(
            content_type="offplan_report",
            primary_insights=primary_insights,
            supporting_data={
                'new_developers': [
                    {'name': d.developer, 'transactions': d.transaction_count, 'value': d.total_value}
                    for d in anomalies.new_developers
                ],
                'developer_surges': anomalies.developer_surges,
                'volume_spikes': [
                    {'area': s.area, 'spike_pct': s.spike_percentage}
                    for s in anomalies.volume_spikes
                ],
                'emerging_hotspots': [
                    {'area': h.area, 'tx_growth': h.tx_growth_rate, 'price_growth': h.price_growth_rate}
                    for h in opportunities.emerging_hotspots
                ]
            }
        )

    # Serialization helpers for supporting data
    def _serialize_anomalies(self, anomalies: AnomalyResults) -> Dict[str, Any]:
        """Serialize anomaly results to dict."""
        return {
            'record_transactions': [
                {'area': tx.area, 'price': tx.price, 'type': tx.property_type, 'rank': tx.rank}
                for tx in anomalies.record_transactions
            ],
            'volume_spikes': [
                {'area': s.area, 'spike_pct': s.spike_percentage, 'current': s.current_volume}
                for s in anomalies.volume_spikes
            ],
            'new_developers': [
                {'name': d.developer, 'transactions': d.transaction_count, 'value': d.total_value}
                for d in anomalies.new_developers
            ]
        }

    def _serialize_opportunities(self, opportunities: OpportunityResults) -> Dict[str, Any]:
        """Serialize opportunity results to dict."""
        return {
            'emerging_hotspots': [
                {'area': h.area, 'tx_growth': h.tx_growth_rate, 'price_growth': h.price_growth_rate}
                for h in opportunities.emerging_hotspots
            ],
            'undervalued_areas': [
                {'area': a.area, 'discount': a.price_discount, 'price_sqm': a.avg_price_sqm}
                for a in opportunities.undervalued_areas
            ],
            'developer_momentum': [
                {'developer': d.developer, 'change': d.market_share_change, 'trend': d.trend}
                for d in opportunities.developer_momentum
            ]
        }

    def _serialize_trends(self, trends: TrendResults) -> Dict[str, Any]:
        """Serialize trend results to dict."""
        pm = trends.price_momentum
        return {
            'price_momentum': {
                '3m': pm.momentum_3m,
                '6m': pm.momentum_6m,
                '12m': pm.momentum_12m,
                'trajectory': pm.trajectory.value
            },
            'volume_momentum': trends.volume_momentum,
            'cycle_position': {
                'phase': trends.cycle_position.phase.value,
                'confidence': trends.cycle_position.phase_confidence
            }
        }

    def _serialize_area_dna(self, dna: AreaDNA) -> Dict[str, Any]:
        """Serialize area DNA to dict."""
        return {
            'area': dna.area_name,
            'segment': dna.market_segment.value,
            'avg_price': dna.avg_price,
            'avg_price_sqm': dna.avg_price_sqm,
            'transactions': dna.total_transactions,
            'offplan_pct': dna.offplan_preference,
            'luxury_pct': dna.luxury_penetration,
            'comparable_areas': dna.comparable_areas
        }

    # ========== SUPPLY INTELLIGENCE INTEGRATION (Phase 2) ==========

    def _format_supply_for_area_guide(self, supply_data: Dict) -> Optional[str]:
        """
        Format supply intelligence for area guide prompts.

        Returns formatted markdown section with supply insights.
        """
        if not supply_data or 'supply_demand_metrics' not in supply_data:
            return None

        metrics = supply_data['supply_demand_metrics']
        opportunity = supply_data.get('opportunity', {})

        sections = []
        sections.append("### Project Supply Pipeline (Exclusive Data)")

        # Market balance and supply-demand ratio
        balance = metrics.get('market_balance', 'Unknown')
        sd_ratio = metrics.get('supply_demand_ratio')
        if sd_ratio:
            sections.append(f"- **Market Balance:** {balance} (Supply-Demand Ratio: {sd_ratio:.2f})")
        else:
            sections.append(f"- **Market Balance:** {balance}")

        # Off-plan pipeline
        offplan_units = metrics.get('supply_offplan_units', 0)
        offplan_projects = metrics.get('supply_offplan_projects', 0)
        if offplan_units > 0:
            sections.append(f"- **Off-Plan Pipeline:** {int(offplan_units):,} units across {int(offplan_projects)} projects")

        # Near-term delivery forecast
        near_term = metrics.get('near_term_supply', 0)
        if near_term > 0:
            sections.append(f"- **Near-Term Supply (2026-2027):** {int(near_term):,} units delivering")

        # Developer activity
        developers = metrics.get('supply_developers', 0)
        if developers > 0:
            sections.append(f"- **Developer Activity:** {int(developers)} active developers")

        # Investment opportunity
        opp_class = opportunity.get('classification')
        if opp_class and opp_class != 'Standard':
            timing = opportunity.get('timing', 'Monitor')
            sections.append(f"- **Investment Outlook:** {opp_class} - {timing}")

        # Risk indicators
        oversupply_risk = metrics.get('oversupply_risk', 'Low')
        if oversupply_risk in ['High', 'Moderate']:
            sections.append(f"- **Supply Risk:** {oversupply_risk} oversupply risk")

        return "\n".join(sections) if len(sections) > 1 else None

    def get_supply_forecast_intelligence(
        self,
        start_quarter: str = 'Q1 2026',
        quarters_ahead: int = 8
    ) -> ContentTypeIntelligence:
        """
        Generate intelligence for supply forecast reports (Phase 2).

        Focuses on delivery pipeline, saturation risks, and emerging hotspots.
        """
        # Get delivery forecast
        delivery_forecast = self.supply_intel.forecast_delivery_waves(
            start_quarter=start_quarter,
            quarters_ahead=quarters_ahead
        )

        # Get oversupplied areas
        oversupplied = self.supply_intel.detect_supply_saturation(threshold=3.0)

        # Get emerging hotspots
        hotspots = self.supply_intel.identify_emerging_hotspots()

        # Get market alerts
        alerts = self.supply_intel.generate_market_alerts()

        # Format for prompt
        sections = []

        # Delivery forecast
        if not delivery_forecast.empty:
            forecast_text = "### Upcoming Delivery Waves\n"
            for _, row in delivery_forecast.head(4).iterrows():
                forecast_text += f"- **{row['delivery_quarter']}:** {int(row['total_units_delivering']):,} units from {int(row['projects_delivering'])} projects "
                forecast_text += f"(Wave Intensity: {row['wave_intensity_pct']:.0f}% of average)\n"
            sections.append(forecast_text)

        # Oversupply alerts
        if not oversupplied.empty and len(oversupplied) > 0:
            oversupply_text = "### Oversupply Risk Areas\n"
            for _, row in oversupplied.head(5).iterrows():
                oversupply_text += f"- **{row['area']}:** {int(row['supply_offplan_units'])} units vs {int(row['demand_offplan_tx'])} transactions "
                oversupply_text += f"(Ratio: {row['supply_demand_ratio']:.1f}, Risk: {row['saturation_severity']})\n"
            sections.append(oversupply_text)

        # Emerging hotspots
        if not hotspots.empty and len(hotspots) > 0:
            hotspot_text = "### Emerging Supply Hotspots\n"
            for _, row in hotspots.head(5).iterrows():
                hotspot_text += f"- **{row['area']}:** {int(row['supply_offplan_projects'])} projects, "
                hotspot_text += f"Demand +{row['tx_yoy_change_pct']:.0f}%, Price +{row['price_yoy_change_pct']:.0f}% YoY\n"
            sections.append(hotspot_text)

        # Critical alerts
        if alerts:
            critical_alerts = [a for a in alerts if a.severity == 'HIGH']
            if critical_alerts:
                alert_text = "### Critical Market Alerts\n"
                for alert in critical_alerts[:3]:
                    alert_text += f"- [{alert.alert_type}] {alert.message}\n"
                sections.append(alert_text)

        primary_insights = "\n\n".join(sections) if sections else "Supply forecast generated."

        return ContentTypeIntelligence(
            content_type="supply_forecast",
            primary_insights=primary_insights,
            supporting_data={
                'delivery_forecast': delivery_forecast.to_dict('records') if not delivery_forecast.empty else [],
                'oversupplied_areas': oversupplied[['area', 'supply_demand_ratio', 'saturation_severity']].head(10).to_dict('records') if not oversupplied.empty else [],
                'emerging_hotspots': hotspots[['area', 'tx_yoy_change_pct', 'price_yoy_change_pct', 'momentum_score']].head(10).to_dict('records') if not hotspots.empty else [],
                'alerts': [
                    {'type': a.alert_type, 'severity': a.severity, 'message': a.message, 'metrics': a.metrics}
                    for a in alerts
                ]
            }
        )

    def get_project_profile_intelligence(self, area: str) -> ContentTypeIntelligence:
        """
        Generate intelligence for project-specific content (Phase 2).

        Provides area context, supply pipeline, and developer insights.
        """
        # Get comprehensive area intelligence
        area_intel = self.supply_intel.get_area_intelligence(area)

        if 'error' in area_intel:
            return ContentTypeIntelligence(
                content_type="project_profile",
                primary_insights=f"Area '{area}' not found in database.",
                supporting_data={}
            )

        metrics = area_intel['supply_demand_metrics']
        opportunity = area_intel['opportunity']
        top_devs = area_intel['top_developers']

        # Format for prompt
        sections = []

        # Area market context
        context_text = "### Market Context\n"
        context_text += f"- **Location:** {area}\n"
        context_text += f"- **Market Balance:** {metrics.get('market_balance', 'Unknown')}\n"
        if metrics.get('supply_demand_ratio'):
            context_text += f"- **Supply-Demand Ratio:** {metrics['supply_demand_ratio']:.2f}\n"
        if metrics.get('demand_avg_price'):
            context_text += f"- **Average Price:** AED {metrics['demand_avg_price']:,.0f}\n"
        if metrics.get('price_yoy_change_pct'):
            context_text += f"- **Price Change YoY:** {metrics['price_yoy_change_pct']:+.1f}%\n"
        sections.append(context_text)

        # Supply pipeline
        if metrics.get('supply_offplan_units', 0) > 0:
            pipeline_text = "### Area Supply Pipeline\n"
            pipeline_text += f"- **Total Off-Plan Units:** {int(metrics['supply_offplan_units']):,}\n"
            pipeline_text += f"- **Active Projects:** {int(metrics['supply_offplan_projects'])}\n"
            pipeline_text += f"- **Active Developers:** {int(metrics['supply_developers'])}\n"
            if metrics.get('near_term_supply', 0) > 0:
                pipeline_text += f"- **Delivering 2026-2027:** {int(metrics['near_term_supply']):,} units\n"
            sections.append(pipeline_text)

        # Top developers
        if top_devs:
            dev_text = "### Leading Developers in Area\n"
            for dev in top_devs[:3]:
                dev_text += f"- **{dev['developer']}:** {int(dev['units'])} units across {int(dev['projects'])} projects\n"
            sections.append(dev_text)

        # Investment timing
        if opportunity.get('classification'):
            inv_text = "### Investment Outlook\n"
            inv_text += f"- **Classification:** {opportunity['classification']}\n"
            if opportunity.get('score'):
                inv_text += f"- **Opportunity Score:** {opportunity['score']:.0f}/100\n"
            if opportunity.get('timing'):
                inv_text += f"- **Timing:** {opportunity['timing']}\n"
            sections.append(inv_text)

        primary_insights = "\n\n".join(sections)

        return ContentTypeIntelligence(
            content_type="project_profile",
            primary_insights=primary_insights,
            supporting_data={
                'area_metrics': metrics,
                'opportunity': opportunity,
                'top_developers': top_devs,
                'delivery_timeline': area_intel.get('delivery_timeline', [])
            }
        )
