"""
Create interactive visualization for Off-Plan vs Existing breakdown
in emerging hotspots analysis.
"""

import json
from pathlib import Path

# Load the data
with open('data/generated_content/emerging_hotspots_offplan_breakdown.json', 'r') as f:
    data = json.load(f)

# Filter to emerging areas with significant growth and volume
emerging_areas = []
for area in data['areas']:
    growth = area.get('growth_2025_vs_2024')
    if growth and growth != 'N/A' and float(growth) > 50 and area['2025_total'] >= 100:
        emerging_areas.append(area)

# Sort by growth
emerging_areas.sort(key=lambda x: float(x.get('growth_2025_vs_2024', 0) or 0), reverse=True)
top_20 = emerging_areas[:20]

# Create HTML
html = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Emerging Hotspots: Off-Plan vs Existing Analysis | Property Monitor</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2.2.0/dist/chartjs-plugin-datalabels.min.js"></script>
    <style>
        :root {
            --pm-navy: #1a365d;
            --pm-gold: #d69e2e;
            --pm-green: #38a169;
            --pm-blue: #3182ce;
            --pm-red: #e53e3e;
            --pm-orange: #dd6b20;
            --pm-light: #f7fafc;
        }

        * { margin: 0; padding: 0; box-sizing: border-box; }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: var(--pm-light);
            color: var(--pm-navy);
            line-height: 1.6;
        }

        .header {
            background: linear-gradient(135deg, var(--pm-navy) 0%, #2d3748 100%);
            color: white;
            padding: 2rem;
            text-align: center;
        }

        .header h1 { font-size: 1.8rem; margin-bottom: 0.5rem; }
        .header p { opacity: 0.9; }

        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 2rem;
        }

        .insight-box {
            background: white;
            border-radius: 8px;
            padding: 1.5rem;
            margin-bottom: 2rem;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            border-left: 4px solid var(--pm-gold);
        }

        .insight-box h3 { color: var(--pm-navy); margin-bottom: 0.5rem; }

        .chart-section {
            background: white;
            border-radius: 8px;
            padding: 1.5rem;
            margin-bottom: 2rem;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }

        .chart-section h2 {
            color: var(--pm-navy);
            margin-bottom: 1rem;
            padding-bottom: 0.5rem;
            border-bottom: 2px solid var(--pm-light);
        }

        .chart-container {
            position: relative;
            height: 500px;
        }

        .chart-container.tall {
            height: 700px;
        }

        .legend-box {
            display: flex;
            gap: 2rem;
            justify-content: center;
            margin: 1rem 0;
            flex-wrap: wrap;
        }

        .legend-item {
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }

        .legend-color {
            width: 20px;
            height: 20px;
            border-radius: 4px;
        }

        .data-table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 1rem;
            font-size: 0.9rem;
        }

        .data-table th {
            background: var(--pm-navy);
            color: white;
            padding: 12px 8px;
            text-align: left;
        }

        .data-table td {
            padding: 10px 8px;
            border-bottom: 1px solid #e2e8f0;
        }

        .data-table tr:hover {
            background: #f7fafc;
        }

        .shift-indicator {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 0.8rem;
            font-weight: 600;
        }

        .shift-up { background: #c6f6d5; color: #22543d; }
        .shift-down { background: #fed7d7; color: #822727; }

        .methodology {
            background: #edf2f7;
            padding: 1.5rem;
            border-radius: 8px;
            margin-top: 2rem;
            font-size: 0.85rem;
        }

        .methodology h4 { margin-bottom: 0.5rem; }

        @media print {
            .chart-container { height: 400px !important; }
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>Emerging Hotspots: Off-Plan vs Secondary Analysis</h1>
        <p>Understanding the drivers behind Dubai's fastest-growing areas (2024-2025)</p>
    </div>

    <div class="container">
        <div class="insight-box">
            <h3>Key Finding</h3>
            <p>The surge in emerging areas is predominantly driven by <strong>new off-plan developments</strong>.
            For example, <strong>Jumeirah Heights</strong> saw a dramatic shift from just 3% off-plan in 2024
            to 96% off-plan in 2025, explaining its +1,703% growth. This indicates new project launches
            rather than organic secondary market growth.</p>
        </div>

        <div class="chart-section">
            <h2>Chart 1: Off-Plan Mix Shift (2024 vs 2025)</h2>
            <p style="color: #718096; margin-bottom: 1rem;">
                Shows how the off-plan percentage changed year-over-year for each emerging area
            </p>
            <div class="legend-box">
                <div class="legend-item">
                    <div class="legend-color" style="background: var(--pm-blue);"></div>
                    <span>2024 Off-Plan %</span>
                </div>
                <div class="legend-item">
                    <div class="legend-color" style="background: var(--pm-green);"></div>
                    <span>2025 Off-Plan %</span>
                </div>
            </div>
            <div class="chart-container tall">
                <canvas id="offplanShiftChart"></canvas>
            </div>
        </div>

        <div class="chart-section">
            <h2>Chart 2: Transaction Composition by Area (2025)</h2>
            <p style="color: #718096; margin-bottom: 1rem;">
                Stacked bar showing off-plan vs existing transactions for each emerging area
            </p>
            <div class="legend-box">
                <div class="legend-item">
                    <div class="legend-color" style="background: var(--pm-navy);"></div>
                    <span>Off-Plan</span>
                </div>
                <div class="legend-item">
                    <div class="legend-color" style="background: var(--pm-orange);"></div>
                    <span>Existing/Secondary</span>
                </div>
            </div>
            <div class="chart-container tall">
                <canvas id="stackedChart"></canvas>
            </div>
        </div>

        <div class="chart-section">
            <h2>Chart 3: Growth vs Off-Plan Dependency</h2>
            <p style="color: #718096; margin-bottom: 1rem;">
                Bubble chart: X = Off-Plan %, Y = Growth %, Size = Total Transactions
            </p>
            <div class="chart-container">
                <canvas id="bubbleChart"></canvas>
            </div>
        </div>

        <div class="chart-section">
            <h2>Detailed Data Table</h2>
            <table class="data-table" id="dataTable">
                <thead>
                    <tr>
                        <th>Area</th>
                        <th>Growth %</th>
                        <th>2024 Total</th>
                        <th>2025 Total</th>
                        <th>2024 Off-Plan %</th>
                        <th>2025 Off-Plan %</th>
                        <th>Shift</th>
                    </tr>
                </thead>
                <tbody></tbody>
            </table>
        </div>

        <div class="methodology">
            <h4>Methodology Notes</h4>
            <ul>
                <li><strong>Off-Plan:</strong> Properties sold before or during construction (reg_type_en = 'Off-Plan')</li>
                <li><strong>Existing/Secondary:</strong> Completed properties being resold (reg_type_en = 'Existing')</li>
                <li><strong>Growth %:</strong> Year-over-year change in total transaction volume</li>
                <li><strong>Shift:</strong> Change in off-plan percentage from 2024 to 2025</li>
                <li>Areas shown have >50% growth and at least 100 transactions in 2025</li>
            </ul>
        </div>
    </div>

    <script>
        // Embedded data
        const areaData = ''' + json.dumps(top_20) + ''';

        // Prepare data for charts
        const labels = areaData.map(d => d.area);
        const offplan2024 = areaData.map(d => d['2024_offplan_pct'] || 0);
        const offplan2025 = areaData.map(d => d['2025_offplan_pct'] || 0);
        const offplanCount2025 = areaData.map(d => d['2025_offplan'] || 0);
        const existingCount2025 = areaData.map(d => d['2025_existing'] || 0);
        const growth = areaData.map(d => d['growth_2025_vs_2024'] || 0);
        const total2025 = areaData.map(d => d['2025_total'] || 0);

        // Chart 1: Off-Plan Shift (Grouped Bar)
        const ctx1 = document.getElementById('offplanShiftChart').getContext('2d');
        new Chart(ctx1, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [
                    {
                        label: '2024 Off-Plan %',
                        data: offplan2024,
                        backgroundColor: '#3182ce',
                        borderRadius: 4
                    },
                    {
                        label: '2025 Off-Plan %',
                        data: offplan2025,
                        backgroundColor: '#38a169',
                        borderRadius: 4
                    }
                ]
            },
            options: {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            afterLabel: function(ctx) {
                                const idx = ctx.dataIndex;
                                const shift = offplan2025[idx] - offplan2024[idx];
                                return 'Shift: ' + (shift > 0 ? '+' : '') + shift.toFixed(1) + '%';
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        max: 100,
                        title: { display: true, text: 'Off-Plan Percentage (%)' }
                    }
                }
            }
        });

        // Chart 2: Stacked Bar - ABSOLUTE COUNTS
        const ctx2 = document.getElementById('stackedChart').getContext('2d');

        // Debug: Log the data to console
        console.log('Off-Plan Counts:', offplanCount2025);
        console.log('Existing Counts:', existingCount2025);

        new Chart(ctx2, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [
                    {
                        label: 'Off-Plan',
                        data: offplanCount2025,
                        backgroundColor: '#1a365d',
                        borderRadius: 4
                    },
                    {
                        label: 'Existing',
                        data: existingCount2025,
                        backgroundColor: '#dd6b20',
                        borderRadius: 4
                    }
                ]
            },
            options: {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    datalabels: {
                        display: true,
                        color: 'white',
                        font: { weight: 'bold', size: 10 },
                        formatter: function(value) {
                            return value > 100 ? value.toLocaleString() : '';
                        }
                    },
                    tooltip: {
                        callbacks: {
                            label: function(ctx) {
                                return ctx.dataset.label + ': ' + ctx.raw.toLocaleString() + ' transactions';
                            },
                            afterBody: function(ctx) {
                                const idx = ctx[0].dataIndex;
                                const total = offplanCount2025[idx] + existingCount2025[idx];
                                const pct = (offplanCount2025[idx] / total * 100).toFixed(0);
                                return 'Total: ' + total.toLocaleString() + ' (' + pct + '% Off-Plan)';
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        stacked: true,
                        title: { display: true, text: 'Number of Transactions (2025)' },
                        ticks: {
                            callback: function(value) {
                                return value.toLocaleString();
                            }
                        }
                    },
                    y: { stacked: true }
                }
            }
        });

        // Chart 3: Bubble Chart
        const ctx3 = document.getElementById('bubbleChart').getContext('2d');
        const bubbleData = areaData.map((d, i) => ({
            x: d['2025_offplan_pct'] || 0,
            y: d['growth_2025_vs_2024'] || 0,
            r: Math.sqrt(d['2025_total'] || 0) / 3,
            label: d.area
        }));

        new Chart(ctx3, {
            type: 'bubble',
            data: {
                datasets: [{
                    label: 'Areas',
                    data: bubbleData,
                    backgroundColor: 'rgba(26, 54, 93, 0.6)',
                    borderColor: '#1a365d',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: function(ctx) {
                                const d = ctx.raw;
                                return [
                                    d.label,
                                    'Off-Plan: ' + d.x.toFixed(0) + '%',
                                    'Growth: +' + d.y.toFixed(0) + '%',
                                    'Transactions: ' + (d.r * 3) ** 2
                                ];
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        min: 0,
                        max: 105,
                        title: { display: true, text: '2025 Off-Plan Percentage (%)' }
                    },
                    y: {
                        title: { display: true, text: 'YoY Transaction Growth (%)' }
                    }
                }
            }
        });

        // Populate data table
        const tbody = document.querySelector('#dataTable tbody');
        areaData.forEach(d => {
            const shift = (d['2025_offplan_pct'] || 0) - (d['2024_offplan_pct'] || 0);
            const shiftClass = shift > 10 ? 'shift-up' : (shift < -10 ? 'shift-down' : '');
            const shiftText = (shift > 0 ? '+' : '') + shift.toFixed(0) + '%';

            const row = document.createElement('tr');
            row.innerHTML = `
                <td><strong>${d.area}</strong></td>
                <td style="color: var(--pm-green); font-weight: 600;">+${(d['growth_2025_vs_2024'] || 0).toFixed(0)}%</td>
                <td>${(d['2024_total'] || 0).toLocaleString()}</td>
                <td>${(d['2025_total'] || 0).toLocaleString()}</td>
                <td>${(d['2024_offplan_pct'] || 0).toFixed(0)}%</td>
                <td>${(d['2025_offplan_pct'] || 0).toFixed(0)}%</td>
                <td><span class="shift-indicator ${shiftClass}">${shiftText}</span></td>
            `;
            tbody.appendChild(row);
        });
    </script>
</body>
</html>'''

# Save the HTML
output_path = Path('data/generated_content/emerging_hotspots_offplan_analysis.html')
with open(output_path, 'w', encoding='utf-8') as f:
    f.write(html)

print(f"Created: {output_path}")
print(f"File size: {len(html):,} bytes")
