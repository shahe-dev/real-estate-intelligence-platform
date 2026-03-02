// Dubai Real Estate Intelligence - Frontend Application
// API Configuration
const API_BASE_DLD = 'http://localhost:8000';
const API_BASE_PM = 'http://localhost:8001';

// State
let allAreas = [];
let allDevelopers = [];
let luxuryChart = null;
let currentDataSource = 'pm'; // Default to Property Monitor

// Get current API base URL based on data source selection
function getApiBase() {
    return currentDataSource === 'pm' ? API_BASE_PM : API_BASE_DLD;
}

// Initialize app when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    console.log('Dubai RE Intel App Initializing...');

    // Get saved data source preference or default to PM
    const savedSource = localStorage.getItem('dataSource') || 'pm';
    currentDataSource = savedSource;
    const sourceSelect = document.getElementById('global-data-source');
    if (sourceSelect) {
        sourceSelect.value = savedSource;
    }

    // Check API status
    checkApiStatus();

    // Load initial data
    loadAllData();

    // Load supply intelligence if PM data source
    if (currentDataSource === 'pm') {
        loadSupplyIntelligence();
    }

    // Setup event listeners
    setupEventListeners();
});

// Load all data based on current data source
function loadAllData() {
    loadOverviewStats();
    loadAreas();
    loadSampleContent();
    loadContentList();
    loadLuxuryMarket();
    loadProjects();
    if (currentDataSource === 'pm') {
        loadDevelopers();
    }
}

// Check if API is online
async function checkApiStatus() {
    const statusEl = document.getElementById('api-status');
    const API_BASE = getApiBase();

    try {
        const response = await fetch(`${API_BASE}/`);
        const data = await response.json();

        if (data.status === 'online') {
            statusEl.textContent = currentDataSource === 'pm' ? 'PM API Online' : 'DLD API Online';
            statusEl.classList.remove('offline');
            statusEl.classList.add('online');
        } else {
            statusEl.textContent = 'API Offline';
            statusEl.classList.remove('online');
            statusEl.classList.add('offline');
        }
    } catch (error) {
        statusEl.textContent = 'API Offline';
        statusEl.classList.remove('online');
        statusEl.classList.add('offline');
        console.error('API connection failed:', error);
    }
}

// Load overview statistics
async function loadOverviewStats() {
    const API_BASE = getApiBase();
    try {
        // PM uses /api/overview, DLD uses /api/stats/overview
        const endpoint = currentDataSource === 'pm' ? '/api/overview' : '/api/stats/overview';
        const response = await fetch(`${API_BASE}${endpoint}`);
        const data = await response.json();

        // Handle different response structures
        let stats;
        if (currentDataSource === 'pm') {
            stats = {
                total_transactions: data.summary.total_transactions,
                total_areas: data.summary.unique_areas,
                luxury_transactions: data.summary.luxury_transactions,
                overall_avg_price: data.summary.avg_price,
                offplan_pct: data.summary.offplan_percentage
            };
        } else {
            stats = {
                total_transactions: data.total_transactions,
                total_areas: data.total_areas,
                luxury_transactions: data.luxury_transactions,
                overall_avg_price: data.overall_avg_price
            };
        }

        // Update stat cards
        const statsContainer = document.getElementById('overview-stats');
        statsContainer.innerHTML = `
            <div class="stat-card">
                <div class="stat-icon">📈</div>
                <div class="stat-value">${formatNumber(stats.total_transactions)}</div>
                <div class="stat-label">Total Transactions</div>
            </div>
            <div class="stat-card">
                <div class="stat-icon">🏘️</div>
                <div class="stat-value">${stats.total_areas}</div>
                <div class="stat-label">Unique Areas</div>
            </div>
            <div class="stat-card">
                <div class="stat-icon">💎</div>
                <div class="stat-value">${formatNumber(stats.luxury_transactions)}</div>
                <div class="stat-label">Luxury Properties</div>
            </div>
            <div class="stat-card">
                <div class="stat-icon">💰</div>
                <div class="stat-value">${formatCurrency(stats.overall_avg_price)}</div>
                <div class="stat-label">Avg Price (AED)</div>
            </div>
        `;
    } catch (error) {
        console.error('Error loading overview stats:', error);
    }
}

// Load areas list
async function loadAreas(luxuryOnly = false) {
    const API_BASE = getApiBase();
    try {
        const url = `${API_BASE}/api/areas?min_transactions=50`;
        console.log('Fetching areas from:', url);

        const response = await fetch(url);
        const areas = await response.json();

        console.log('Areas loaded:', areas.length, 'areas');
        console.log('First area:', areas[0]);

        allAreas = areas;
        displayAreas(areas);
        populateAreaSelects(areas);

    } catch (error) {
        console.error('Error loading areas:', error);
        alert('Failed to load areas. Make sure API is running and metrics are built.');
    }
}

// Display areas grid
function displayAreas(areas) {
    const container = document.getElementById('areas-list');

    if (areas.length === 0) {
        container.innerHTML = '<p class="text-center">No areas found</p>';
        return;
    }

    container.innerHTML = areas.slice(0, 12).map(area => `
        <div class="area-card" onclick="showAreaDetails('${area.area}')">
            <h3>${area.area}${area.luxury_count > 0 ? '<span class="luxury-badge">Luxury</span>' : ''}</h3>
            <div class="metric">
                <span class="metric-label">Transactions</span>
                <span class="metric-value">${formatNumber(area.total_transactions)}</span>
            </div>
            <div class="metric">
                <span class="metric-label">Avg Price</span>
                <span class="metric-value">${formatCurrency(area.avg_price)}</span>
            </div>
            <div class="metric">
                <span class="metric-label">Price/sqm</span>
                <span class="metric-value">${formatCurrency(area.avg_price_sqm)}</span>
            </div>
        </div>
    `).join('');
}

// Populate area dropdowns
function populateAreaSelects(areas) {
    const selects = [
        'compare-area-1',
        'compare-area-2',
        'compare-area-3',
        'gen-area-select',
        'gen-project-area-select'
    ];

    console.log('Populating', selects.length, 'dropdowns with', areas.length, 'areas');

    if (areas.length === 0) {
        console.error('No areas to populate!');
        return;
    }

    selects.forEach(selectId => {
        const select = document.getElementById(selectId);
        if (!select) {
            console.warn('Select not found:', selectId);
            return;
        }

        console.log(`Found select: ${selectId}, current options:`, select.options.length);

        // Clear all options first
        select.innerHTML = '<option value="">Select an area...</option>';

        // Add all areas
        areas.forEach((area, index) => {
            const option = document.createElement('option');
            option.value = area.area;
            option.textContent = area.area;
            select.appendChild(option);

            if (index === 0) {
                console.log('First area added:', area.area);
            }
        });

        console.log(`Populated ${selectId} with ${select.options.length} total options (including default)`);
    });
}

// Show area details
async function showAreaDetails(areaName) {
    const API_BASE = getApiBase();
    try {
        const response = await fetch(`${API_BASE}/api/area/${encodeURIComponent(areaName)}`);
        const data = await response.json();

        const metrics = data.metrics;
        const detailsContainer = document.getElementById('area-details');

        detailsContainer.innerHTML = `
            <h3>${areaName}</h3>
            <div class="metrics-row">
                <div class="metric-box">
                    <div class="label">Total Transactions</div>
                    <div class="value">${formatNumber(metrics.total_transactions)}</div>
                </div>
                <div class="metric-box">
                    <div class="label">Average Price</div>
                    <div class="value">${formatCurrency(metrics.avg_price)}</div>
                </div>
                <div class="metric-box">
                    <div class="label">Median Price</div>
                    <div class="value">${formatCurrency(metrics.median_price)}</div>
                </div>
                <div class="metric-box">
                    <div class="label">Price per Sqm</div>
                    <div class="value">${formatCurrency(metrics.avg_price_sqm)}</div>
                </div>
                <div class="metric-box">
                    <div class="label">Luxury Properties</div>
                    <div class="value">${formatNumber(metrics.luxury_count)}</div>
                </div>
                <div class="metric-box">
                    <div class="label">Avg Size</div>
                    <div class="value">${Math.round(metrics.avg_size_sqm)} sqm</div>
                </div>
            </div>

            ${data.property_types.length > 0 ? `
                <h4>Property Types</h4>
                <table class="data-table">
                    <thead>
                        <tr>
                            <th>Type</th>
                            <th>Transactions</th>
                            <th>Avg Price</th>
                            <th>Price/Sqm</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${data.property_types.map(pt => `
                            <tr>
                                <td>${pt.rooms_en}</td>
                                <td>${formatNumber(pt.tx_count)}</td>
                                <td>${formatCurrency(pt.avg_price)}</td>
                                <td>${formatCurrency(pt.avg_price_sqm)}</td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            ` : ''}
        `;

        detailsContainer.classList.remove('hidden');
        detailsContainer.scrollIntoView({ behavior: 'smooth' });

    } catch (error) {
        console.error('Error loading area details:', error);
        alert('Failed to load area details');
    }
}

// Load sample generated content
async function loadSampleContent() {
    const API_BASE = getApiBase();
    try {
        const response = await fetch(`${API_BASE}/api/content/sample`);
        const data = await response.json();

        const container = document.getElementById('sample-content');

        if (data.status === 'no_content') {
            container.innerHTML = `
                <div class="text-center" style="padding: 2rem;">
                    <p>${data.message}</p>
                    <p style="margin-top: 1rem; color: var(--text-secondary);">
                        Use the generator above to create your first content piece!
                    </p>
                </div>
            `;
        } else {
            // Convert markdown to HTML (basic)
            const htmlContent = markdownToHtml(data.preview);
            container.innerHTML = htmlContent;
        }

        container.classList.remove('loading');

    } catch (error) {
        console.error('Error loading sample content:', error);
        document.getElementById('sample-content').innerHTML = '<p class="text-center">Error loading sample content</p>';
    }
}

// Load list of generated content files
async function loadContentList() {
    const API_BASE = getApiBase();
    try {
        const response = await fetch(`${API_BASE}/api/content/list`);
        const data = await response.json();

        if (data.files && data.files.length > 0) {
            const container = document.getElementById('content-list');

            // Format filename for display
            const formatFilename = (filename) => {
                return filename
                    .replace('.md', '')
                    .replace(/_/g, ' ')
                    .replace(/\b\w/g, c => c.toUpperCase());
            };

            container.innerHTML = `
                <div class="content-list">
                    <h4>All Generated Content (${data.count} files)</h4>
                    ${data.files.slice(0, 10).map(file => `
                        <div class="content-file-card" onclick="viewContent('${file.filename}')">
                            <span class="file-name">${formatFilename(file.filename)}</span>
                            <span class="file-meta">${formatFileSize(file.size)} · ${formatDate(file.modified)}</span>
                        </div>
                    `).join('')}
                </div>
            `;
            container.classList.remove('hidden');
        }
    } catch (error) {
        console.error('Error loading content list:', error);
    }
}

// View full content file with visualization support
async function viewContent(filename) {
    const API_BASE = getApiBase();
    try {
        const response = await fetch(`${API_BASE}/api/content/${encodeURIComponent(filename)}`);
        const data = await response.json();

        const container = document.getElementById('sample-content');

        // Convert markdown to HTML (this also sets up chart containers)
        const htmlContent = markdownToHtml(data.content);
        container.innerHTML = htmlContent;

        // Render Chart.js visualizations if available
        if (data.visualizations?.has_charts) {
            console.log('Content has charts, rendering...');
            renderChartJSVisualizations(data.visualizations.chartjs_configs);
        }

        container.scrollIntoView({ behavior: 'smooth' });

    } catch (error) {
        console.error('Error viewing content:', error);
        alert('Failed to load content file');
    }
}

// Handle content type selection change
function handleContentTypeChange() {
    const contentType = document.getElementById('gen-content-type').value;

    // Hide all option sections
    document.getElementById('area-guide-options').classList.add('hidden');
    document.getElementById('market-report-options').classList.add('hidden');
    document.getElementById('date-range-options').classList.add('hidden');

    // Hide PM-specific options
    const developerOptions = document.getElementById('developer-options');
    const offplanOptions = document.getElementById('offplan-options');
    const supplyForecastOptions = document.getElementById('supply-forecast-options');
    const projectProfileOptions = document.getElementById('project-profile-options');
    const verificationOptions = document.getElementById('verification-options');
    if (developerOptions) developerOptions.classList.add('hidden');
    if (offplanOptions) offplanOptions.classList.add('hidden');
    if (supplyForecastOptions) supplyForecastOptions.classList.add('hidden');
    if (projectProfileOptions) projectProfileOptions.classList.add('hidden');

    // Show verification options for ALL content types (PM data verification)
    if (verificationOptions) verificationOptions.classList.remove('hidden');

    // Show relevant sections based on content type
    switch (contentType) {
        case 'area-guide':
            document.getElementById('area-guide-options').classList.remove('hidden');
            document.getElementById('date-range-options').classList.remove('hidden');
            break;
        case 'batch-guides':
            document.getElementById('date-range-options').classList.remove('hidden');
            break;
        case 'market-report':
            document.getElementById('market-report-options').classList.remove('hidden');
            break;
        case 'developer-profile':
            if (developerOptions) developerOptions.classList.remove('hidden');
            break;
        case 'offplan-report':
            if (offplanOptions) offplanOptions.classList.remove('hidden');
            break;
        case 'luxury-report':
            document.getElementById('market-report-options').classList.remove('hidden');
            break;
        case 'supply-forecast':
            if (supplyForecastOptions) supplyForecastOptions.classList.remove('hidden');
            break;
        case 'project-profile':
            if (projectProfileOptions) projectProfileOptions.classList.remove('hidden');
            break;
    }
}

// Period type options for dynamic dropdown population
const PERIOD_OPTIONS = {
    monthly: [
        { value: '1', label: 'January' },
        { value: '2', label: 'February' },
        { value: '3', label: 'March' },
        { value: '4', label: 'April' },
        { value: '5', label: 'May' },
        { value: '6', label: 'June' },
        { value: '7', label: 'July' },
        { value: '8', label: 'August' },
        { value: '9', label: 'September' },
        { value: '10', label: 'October' },
        { value: '11', label: 'November' },
        { value: '12', label: 'December' }
    ],
    quarterly: [
        { value: '1', label: 'Q1 (Jan-Mar)' },
        { value: '2', label: 'Q2 (Apr-Jun)' },
        { value: '3', label: 'Q3 (Jul-Sep)' },
        { value: '4', label: 'Q4 (Oct-Dec)' }
    ],
    semi_annual: [
        { value: '1', label: 'H1 (First Half)' },
        { value: '2', label: 'H2 (Second Half)' }
    ],
    annual: [
        { value: '1', label: 'Full Year' }
    ]
};

const PERIOD_LABELS = {
    monthly: 'Month:',
    quarterly: 'Quarter:',
    semi_annual: 'Half:',
    annual: 'Period:'
};

// Handle period type change for market reports
function handlePeriodTypeChange(periodTypeSelectId, periodNumberSelectId, periodLabelId) {
    const periodTypeSelect = document.getElementById(periodTypeSelectId);
    const periodNumberSelect = document.getElementById(periodNumberSelectId);
    const periodLabel = document.getElementById(periodLabelId);

    if (!periodTypeSelect || !periodNumberSelect) return;

    const periodType = periodTypeSelect.value;
    const options = PERIOD_OPTIONS[periodType] || PERIOD_OPTIONS.monthly;
    const label = PERIOD_LABELS[periodType] || 'Period:';

    // Update label
    if (periodLabel) {
        periodLabel.textContent = label;
    }

    // Clear and repopulate options
    periodNumberSelect.innerHTML = '';
    options.forEach(opt => {
        const option = document.createElement('option');
        option.value = opt.value;
        option.textContent = opt.label;
        periodNumberSelect.appendChild(option);
    });

    // Hide period number for annual (since there's only one option)
    if (periodType === 'annual') {
        periodNumberSelect.style.display = 'none';
        if (periodLabel) periodLabel.style.display = 'none';
    } else {
        periodNumberSelect.style.display = '';
        if (periodLabel) periodLabel.style.display = '';
    }
}

// Generate new content
async function generateContent() {
    const API_BASE = getApiBase();
    const contentType = document.getElementById('gen-content-type').value;
    const statusDiv = document.getElementById('generation-status');
    const generateBtn = document.getElementById('generate-btn');

    // Show loading state
    statusDiv.className = 'generation-status loading';
    statusDiv.textContent = '🤖 Generating content... This may take 10-60 seconds.';
    statusDiv.classList.remove('hidden');
    generateBtn.disabled = true;
    generateBtn.textContent = 'Generating...';

    try {
        let url;
        let response;

        // Get verification checkbox state (applies to all content types)
        const withVerification = document.getElementById('gen-with-verification')?.checked || false;

        switch (contentType) {
            case 'area-guide':
                const areaName = document.getElementById('gen-area-select').value;
                if (!areaName) {
                    throw new Error('Please select an area');
                }
                const dateRange = document.getElementById('gen-date-range').value;
                let yearFrom = null;
                let yearTo = null;
                if (dateRange === 'recent') {
                    yearFrom = 2024;
                    yearTo = 2025;
                }
                url = `${API_BASE}/api/content/generate/area?area_name=${encodeURIComponent(areaName)}${yearFrom ? `&year_from=${yearFrom}` : ''}${yearTo ? `&year_to=${yearTo}` : ''}&with_verification=${withVerification}`;
                response = await fetch(url, { method: 'POST' });
                break;

            case 'batch-guides':
                statusDiv.textContent = '🤖 Generating batch content for top 10 areas... This may take several minutes.';
                url = `${API_BASE}/api/content/generate/batch?with_verification=${withVerification}`;
                response = await fetch(url, { method: 'POST' });
                break;

            case 'market-report':
                const year = document.getElementById('gen-year').value;
                const periodType = document.getElementById('gen-period-type').value;
                const periodNumber = document.getElementById('gen-period-number').value;
                url = `${API_BASE}/api/content/generate/market-report?year=${year}&period_type=${periodType}&period_number=${periodNumber}&with_verification=${withVerification}`;
                response = await fetch(url, { method: 'POST' });
                break;

            case 'developer-profile':
                const developerName = document.getElementById('gen-developer-select').value;
                if (!developerName) {
                    throw new Error('Please select a developer');
                }
                url = `${API_BASE}/api/content/generate/developer?developer_name=${encodeURIComponent(developerName)}&with_verification=${withVerification}`;
                response = await fetch(url, { method: 'POST' });
                break;

            case 'offplan-report':
                const offplanYear = document.getElementById('gen-offplan-year').value;
                const offplanPeriodType = document.getElementById('gen-offplan-period-type').value;
                const offplanPeriodNumber = document.getElementById('gen-offplan-period-number').value;
                url = `${API_BASE}/api/content/generate/offplan?year=${offplanYear}&period_type=${offplanPeriodType}&period_number=${offplanPeriodNumber}&with_verification=${withVerification}`;
                response = await fetch(url, { method: 'POST' });
                break;

            case 'luxury-report':
                const luxuryYear = document.getElementById('gen-year')?.value || '2025';
                url = `${API_BASE}/api/content/generate/luxury-report?year=${luxuryYear}&with_verification=${withVerification}`;
                response = await fetch(url, { method: 'POST' });
                break;

            case 'supply-forecast':
                const startQuarter = document.getElementById('gen-start-quarter').value;
                const quartersAhead = document.getElementById('gen-quarters-ahead').value;
                url = `${API_BASE}/api/content/generate/supply-forecast?start_quarter=${encodeURIComponent(startQuarter)}&quarters_ahead=${quartersAhead}`;
                response = await fetch(url, { method: 'POST' });
                break;

            case 'project-profile':
                const profileArea = document.getElementById('gen-project-area-select').value;
                if (!profileArea) {
                    throw new Error('Please select an area for the project profile');
                }
                url = `${API_BASE}/api/content/generate/project-profile?area_name=${encodeURIComponent(profileArea)}`;
                response = await fetch(url, { method: 'POST' });
                break;

            default:
                throw new Error('Unknown content type');
        }

        const data = await response.json();

        if (data.status === 'success') {
            statusDiv.className = 'generation-status success';
            let successMessage = `✅ ${data.message}`;
            if (data.filename) {
                successMessage += ` - File: ${data.filename}`;
            }
            if (data.verification_file) {
                successMessage += `\n📊 Verification Excel: ${data.verification_file}`;
            }
            statusDiv.innerHTML = successMessage.replace('\n', '<br>');

            // Reload content displays
            setTimeout(() => {
                loadSampleContent();
                loadContentList();
            }, 1000);
        } else {
            throw new Error(data.detail || 'Generation failed');
        }

    } catch (error) {
        statusDiv.className = 'generation-status error';
        statusDiv.textContent = `❌ Error: ${error.message}`;
    } finally {
        generateBtn.disabled = false;
        generateBtn.textContent = 'Generate Content';
    }
}

// Compare areas
async function compareAreas() {
    const API_BASE = getApiBase();
    const area1 = document.getElementById('compare-area-1').value;
    const area2 = document.getElementById('compare-area-2').value;
    const area3 = document.getElementById('compare-area-3').value;

    const areas = [area1, area2, area3].filter(a => a);

    if (areas.length < 2) {
        alert('Please select at least 2 areas to compare');
        return;
    }

    try {
        const url = `${API_BASE}/api/compare?${areas.map(a => `areas=${encodeURIComponent(a)}`).join('&')}`;
        const response = await fetch(url);
        const data = await response.json();

        const container = document.getElementById('comparison-results');
        container.innerHTML = data.map(area => `
            <div class="comparison-card">
                <h4>${area.area_name_en}</h4>
                <div class="metric">
                    <span class="metric-label">Transactions</span>
                    <span class="metric-value">${formatNumber(area.total_transactions)}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Avg Price</span>
                    <span class="metric-value">${formatCurrency(area.avg_price)}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Median Price</span>
                    <span class="metric-value">${formatCurrency(area.median_price)}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Price/sqm</span>
                    <span class="metric-value">${formatCurrency(area.avg_price_sqm)}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Luxury Units</span>
                    <span class="metric-value">${formatNumber(area.luxury_count)}</span>
                </div>
            </div>
        `).join('');

        container.classList.remove('hidden');
        container.scrollIntoView({ behavior: 'smooth' });

    } catch (error) {
        console.error('Error comparing areas:', error);
        alert('Failed to compare areas');
    }
}

// Load luxury market data
async function loadLuxuryMarket() {
    const API_BASE = getApiBase();
    const year = currentDataSource === 'pm' ? 2025 : 2024;
    try {
        const response = await fetch(`${API_BASE}/api/luxury?year=${year}`);
        const data = await response.json();

        if (data.length === 0) return;

        // Display chart
        const ctx = document.getElementById('luxury-chart');
        const top10 = data.slice(0, 10);

        if (luxuryChart) {
            luxuryChart.destroy();
        }

        luxuryChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: top10.map(d => d.area_name_en),
                datasets: [{
                    label: 'Luxury Transactions',
                    data: top10.map(d => d.luxury_tx_count),
                    backgroundColor: 'rgba(37, 99, 235, 0.8)',
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                plugins: {
                    legend: { display: false },
                    title: {
                        display: true,
                        text: 'Top 10 Luxury Markets (2024)'
                    }
                }
            }
        });

        // Display table
        const tableContainer = document.getElementById('luxury-table');
        tableContainer.innerHTML = `
            <table class="data-table">
                <thead>
                    <tr>
                        <th>Area</th>
                        <th>Luxury Transactions</th>
                        <th>Avg Price</th>
                        <th>Max Price</th>
                    </tr>
                </thead>
                <tbody>
                    ${top10.map(area => `
                        <tr>
                            <td><strong>${area.area_name_en}</strong></td>
                            <td>${formatNumber(area.luxury_tx_count)}</td>
                            <td>${formatCurrency(area.avg_luxury_price)}</td>
                            <td>${formatCurrency(area.max_luxury_price)}</td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        `;

    } catch (error) {
        console.error('Error loading luxury market:', error);
    }
}

// Load projects
async function loadProjects(offPlanOnly = false) {
    const API_BASE = getApiBase();
    try {
        const url = `${API_BASE}/api/projects${offPlanOnly ? '?offplan=true' : ''}`;
        console.log('Fetching projects from:', url);
        const response = await fetch(url);
        const projects = await response.json();

        const container = document.getElementById('projects-list');

        if (projects.length === 0) {
            container.innerHTML = '<p class="text-center">No projects found</p>';
            return;
        }

        container.innerHTML = `
            <table class="data-table">
                <thead>
                    <tr>
                        <th>Project</th>
                        <th>Area</th>
                        <th>Developer</th>
                        <th>Transactions</th>
                        <th>Avg Price</th>
                        <th>Luxury Units</th>
                    </tr>
                </thead>
                <tbody>
                    ${projects.slice(0, 20).map(project => `
                        <tr>
                            <td><strong>${project.project || project.project_name_en || 'N/A'}</strong></td>
                            <td>${project.area || project.area_name_en || 'N/A'}</td>
                            <td>${project.developer || project.reg_type_en || 'N/A'}</td>
                            <td>${formatNumber(project.transactions || project.tx_count || 0)}</td>
                            <td>${formatCurrency(project.avg_price || 0)}</td>
                            <td>${formatNumber(project.luxury_units || 0)}</td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        `;

    } catch (error) {
        console.error('Error loading projects:', error);
    }
}

// Load developers list (PM only)
async function loadDevelopers() {
    if (currentDataSource !== 'pm') return;

    const API_BASE = getApiBase();
    try {
        const response = await fetch(`${API_BASE}/api/developers?min_transactions=100`);
        const developers = await response.json();

        allDevelopers = developers;
        populateDeveloperSelect(developers);
    } catch (error) {
        console.error('Error loading developers:', error);
    }
}

// Populate developer dropdown
function populateDeveloperSelect(developers) {
    const select = document.getElementById('gen-developer-select');
    if (!select) return;

    select.innerHTML = '<option value="">Select a developer...</option>';
    developers.forEach(dev => {
        const option = document.createElement('option');
        option.value = dev.developer;
        option.textContent = `${dev.developer} (${formatNumber(dev.total_transactions)} sales)`;
        select.appendChild(option);
    });
}

// Handle data source change
function handleDataSourceChange(source) {
    currentDataSource = source;
    localStorage.setItem('dataSource', source);

    // Update UI elements
    const tagline = document.querySelector('.tagline');
    if (tagline) {
        tagline.textContent = source === 'pm'
            ? 'AI-Powered Analytics | Property Monitor Data | 2023-2025'
            : 'AI-Powered Analytics | DLD Legacy Data | 2010-2024';
    }

    // Reload all data
    checkApiStatus();
    loadAllData();

    // Load supply intelligence if PM data source
    if (source === 'pm') {
        loadSupplyIntelligence();
    }
}

// Setup event listeners
function setupEventListeners() {
    // Data source selector
    document.getElementById('global-data-source')?.addEventListener('change', (e) => {
        handleDataSourceChange(e.target.value);
    });

    // Luxury filter
    document.getElementById('luxury-filter')?.addEventListener('change', (e) => {
        loadAreas(e.target.checked);
    });

    // Off-plan filter
    document.getElementById('offplan-filter')?.addEventListener('change', (e) => {
        loadProjects(e.target.checked);
    });

    // Area search
    document.getElementById('search-btn')?.addEventListener('click', () => {
        const areaName = document.getElementById('area-search').value;
        if (areaName) {
            showAreaDetails(areaName);
        }
    });

    // Compare button
    document.getElementById('compare-btn')?.addEventListener('click', compareAreas);

    // Generate content button
    document.getElementById('generate-btn')?.addEventListener('click', generateContent);

    // Content type selector
    document.getElementById('gen-content-type')?.addEventListener('change', handleContentTypeChange);

    // Period type selectors for market report
    document.getElementById('gen-period-type')?.addEventListener('change', () => {
        handlePeriodTypeChange('gen-period-type', 'gen-period-number', 'period-number-label');
    });

    // Period type selector for offplan report
    document.getElementById('gen-offplan-period-type')?.addEventListener('change', () => {
        handlePeriodTypeChange('gen-offplan-period-type', 'gen-offplan-period-number', 'offplan-period-number-label');
    });

    // Initialize content type options visibility
    handleContentTypeChange();

    // Initialize period type options
    handlePeriodTypeChange('gen-period-type', 'gen-period-number', 'period-number-label');
    handlePeriodTypeChange('gen-offplan-period-type', 'gen-offplan-period-number', 'offplan-period-number-label');
}

// =====================================================
// SUPPLY INTELLIGENCE FUNCTIONS (Phase 2)
// =====================================================

// Main supply intelligence loader
function loadSupplyIntelligence() {
    console.log('Loading supply intelligence...');
    loadSupplyOverview();
    setupSupplyTabs();
}

// Load supply overview tab
async function loadSupplyOverview() {
    const API_BASE = getApiBase();
    try {
        const response = await fetch(`${API_BASE}/api/supply/overview`);
        const data = await response.json();

        // Update stat cards
        document.getElementById('supply-total-areas').textContent = formatNumber(data.total_areas);
        document.getElementById('supply-total-units').textContent = formatNumber(data.total_offplan_units);
        document.getElementById('supply-oversupplied').textContent = formatNumber(data.oversupplied_areas);
        document.getElementById('supply-avg-ratio').textContent = data.avg_supply_demand_ratio?.toFixed(2) || 'N/A';

        // Render market balance chart with click handler
        const ctx = document.getElementById('supply-balance-chart');
        if (ctx) {
            const balanceData = data.market_balance_distribution;
            const balanceChart = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: balanceData.map(b => b.market_balance),
                    datasets: [{
                        label: 'Number of Areas',
                        data: balanceData.map(b => b.areas),
                        backgroundColor: balanceData.map(b => {
                            if (b.market_balance.includes('Oversupplied')) return 'rgba(239, 68, 68, 0.8)';
                            if (b.market_balance.includes('Undersupplied')) return 'rgba(16, 185, 129, 0.8)';
                            if (b.market_balance === 'Balanced') return 'rgba(59, 130, 246, 0.8)';
                            return 'rgba(156, 163, 175, 0.8)';
                        })
                    }]
                },
                options: {
                    responsive: true,
                    onClick: (event, elements) => {
                        if (elements.length > 0) {
                            const index = elements[0].index;
                            const category = balanceData[index].market_balance;
                            openBalanceModal(category);
                        }
                    },
                    plugins: {
                        legend: { display: false },
                        title: {
                            display: true,
                            text: 'Market Balance Distribution Across Dubai (Click bars to explore)'
                        },
                        tooltip: {
                            callbacks: {
                                afterLabel: function() {
                                    return 'Click to see areas';
                                }
                            }
                        }
                    }
                }
            });

            // Change cursor to pointer on hover
            ctx.style.cursor = 'pointer';
        }

    } catch (error) {
        console.error('Error loading supply overview:', error);
    }
}

// Load supply opportunities tab
async function loadSupplyOpportunities() {
    const API_BASE = getApiBase();
    try {
        const response = await fetch(`${API_BASE}/api/supply/opportunities?limit=10`);
        const data = await response.json();

        const container = document.getElementById('opportunities-list');

        if (!data.opportunities || data.opportunities.length === 0) {
            container.innerHTML = '<p class="text-center">No opportunities found</p>';
            return;
        }

        container.innerHTML = data.opportunities.map(opp => `
            <div class="opportunity-card">
                <h4>${opp.area}</h4>
                <div class="opportunity-score">
                    <span class="score-label">Opportunity Score:</span>
                    <span class="score-value">${opp.opportunity_score.toFixed(0)}/100</span>
                </div>
                <div class="opportunity-badges">
                    <span class="balance-indicator ${getBalanceClass(opp.market_balance)}">${opp.market_balance}</span>
                    <span class="timing-badge ${getTimingClass(opp.investment_timing)}">${opp.investment_timing}</span>
                </div>
                <div class="opportunity-metrics">
                    <div class="metric-row">
                        <span>Supply:</span>
                        <strong>${formatNumber(opp.supply_offplan_units)} units</strong>
                    </div>
                    <div class="metric-row">
                        <span>Demand:</span>
                        <strong>${formatNumber(opp.demand_offplan_tx)} transactions</strong>
                    </div>
                    <div class="metric-row">
                        <span>Price Growth:</span>
                        <strong>${opp.price_yoy_change_pct?.toFixed(1) || 'N/A'}% YoY</strong>
                    </div>
                </div>
            </div>
        `).join('');

    } catch (error) {
        console.error('Error loading supply opportunities:', error);
    }
}

// Load developer reliability tab
async function loadDeveloperReliability() {
    const API_BASE = getApiBase();
    try {
        const response = await fetch(`${API_BASE}/api/supply/developers/reliability?limit=20`);
        const data = await response.json();

        const container = document.getElementById('developers-reliability');

        if (!data.developers || data.developers.length === 0) {
            container.innerHTML = '<p class="text-center">No developer data available</p>';
            return;
        }

        container.innerHTML = `
            <table class="data-table">
                <thead>
                    <tr>
                        <th>Developer</th>
                        <th>Track Record</th>
                        <th>Reliability Score</th>
                        <th>Active Projects</th>
                        <th>Offplan Units</th>
                        <th>Market Segment</th>
                    </tr>
                </thead>
                <tbody>
                    ${data.developers.map(dev => `
                        <tr>
                            <td><strong>${dev.developer}</strong></td>
                            <td>
                                <span class="reliability-badge ${getReliabilityClass(dev.delivery_track_record)}">
                                    ${dev.delivery_track_record}
                                </span>
                            </td>
                            <td>${dev.reliability_score?.toFixed(0) || 'N/A'}</td>
                            <td>${formatNumber(dev.supply_active_projects)}</td>
                            <td>${formatNumber(dev.supply_offplan_units)}</td>
                            <td>${dev.market_segment}</td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        `;

    } catch (error) {
        console.error('Error loading developer reliability:', error);
    }
}

// Load delivery forecast tab
async function loadDeliveryForecast() {
    const API_BASE = getApiBase();
    try {
        const response = await fetch(`${API_BASE}/api/supply/forecast?start_quarter=Q1%202026&quarters=8`);
        const data = await response.json();
        const forecast = data.forecast || [];

        // Render forecast chart
        const ctx = document.getElementById('delivery-forecast-chart');
        if (ctx) {
            new Chart(ctx, {
                type: 'line',
                data: {
                    labels: forecast.map(f => f.delivery_quarter),
                    datasets: [
                        {
                            label: 'Units Delivering',
                            data: forecast.map(f => f.total_units_delivering),
                            borderColor: 'rgba(37, 99, 235, 1)',
                            backgroundColor: 'rgba(37, 99, 235, 0.1)',
                            fill: true
                        },
                        {
                            label: 'Projects Delivering',
                            data: forecast.map(f => f.projects_delivering),
                            borderColor: 'rgba(16, 185, 129, 1)',
                            backgroundColor: 'rgba(16, 185, 129, 0.1)',
                            fill: true,
                            yAxisID: 'y2'
                        }
                    ]
                },
                options: {
                    responsive: true,
                    interaction: {
                        mode: 'index',
                        intersect: false,
                    },
                    plugins: {
                        title: {
                            display: true,
                            text: 'Quarterly Delivery Forecast'
                        }
                    },
                    scales: {
                        y: {
                            type: 'linear',
                            display: true,
                            position: 'left',
                            title: {
                                display: true,
                                text: 'Units'
                            }
                        },
                        y2: {
                            type: 'linear',
                            display: true,
                            position: 'right',
                            title: {
                                display: true,
                                text: 'Projects'
                            },
                            grid: {
                                drawOnChartArea: false,
                            }
                        }
                    }
                }
            });
        }

        // Render forecast table
        const container = document.getElementById('forecast-details');
        container.innerHTML = `
            <table class="data-table">
                <thead>
                    <tr>
                        <th>Quarter</th>
                        <th>Projects</th>
                        <th>Total Units</th>
                        <th>Residential</th>
                        <th>Commercial</th>
                        <th>Areas</th>
                    </tr>
                </thead>
                <tbody>
                    ${forecast.map(f => `
                        <tr>
                            <td><strong>${f.delivery_quarter}</strong></td>
                            <td>${formatNumber(f.projects_delivering)}</td>
                            <td>${formatNumber(f.total_units_delivering)}</td>
                            <td>${formatNumber(f.total_residential_units)}</td>
                            <td>${formatNumber(f.total_commercial_units)}</td>
                            <td>${f.areas_delivering}</td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        `;

    } catch (error) {
        console.error('Error loading delivery forecast:', error);
    }
}

// Load market alerts tab
async function loadMarketAlerts() {
    const API_BASE = getApiBase();
    try {
        const response = await fetch(`${API_BASE}/api/supply/alerts`);
        const data = await response.json();

        const container = document.getElementById('market-alerts');

        let html = '';

        // Oversupply alerts
        if (data.oversupply && data.oversupply.length > 0) {
            html += '<h4>⚠️ Oversupply Alerts</h4>';
            html += data.oversupply.map(alert => `
                <div class="alert-card severity-${alert.severity.toLowerCase()}">
                    <div class="alert-header">
                        <strong>${alert.area}</strong>
                        <span class="alert-severity">${alert.severity}</span>
                    </div>
                    <p class="alert-message">${alert.message}</p>
                </div>
            `).join('');
        }

        // Opportunity alerts
        if (data.opportunities && data.opportunities.length > 0) {
            html += '<h4 style="margin-top: 2rem;">🎯 High Opportunity Areas</h4>';
            html += data.opportunities.map(alert => `
                <div class="alert-card severity-${alert.severity.toLowerCase()}">
                    <div class="alert-header">
                        <strong>${alert.area}</strong>
                        <span class="alert-severity">${alert.severity}</span>
                    </div>
                    <p class="alert-message">${alert.message}</p>
                </div>
            `).join('');
        }

        // Risk alerts
        if (data.risks && data.risks.length > 0) {
            html += '<h4 style="margin-top: 2rem;">🚨 Price Decline Risks</h4>';
            html += data.risks.map(alert => `
                <div class="alert-card severity-high">
                    <div class="alert-header">
                        <strong>${alert.area}</strong>
                        <span class="alert-severity">HIGH RISK</span>
                    </div>
                    <p class="alert-message">${alert.message}</p>
                </div>
            `).join('');
        }

        container.innerHTML = html || '<p class="text-center">No alerts at this time</p>';

    } catch (error) {
        console.error('Error loading market alerts:', error);
    }
}

// Setup supply intelligence tabs
function setupSupplyTabs() {
    const tabs = document.querySelectorAll('.supply-tab');
    const contents = document.querySelectorAll('.supply-tab-content');

    tabs.forEach(tab => {
        tab.addEventListener('click', function() {
            const tabName = this.dataset.tab;

            // Update active states
            tabs.forEach(t => t.classList.remove('active'));
            contents.forEach(c => c.classList.remove('active'));

            this.classList.add('active');
            document.getElementById(`supply-tab-${tabName}`).classList.add('active');

            // Load data for the selected tab
            switch(tabName) {
                case 'overview':
                    // Already loaded on page load
                    break;
                case 'opportunities':
                    loadSupplyOpportunities();
                    break;
                case 'developers':
                    loadDeveloperReliability();
                    break;
                case 'forecast':
                    loadDeliveryForecast();
                    break;
                case 'alerts':
                    loadMarketAlerts();
                    break;
            }
        });
    });
}

// Helper functions for CSS classes
function getBalanceClass(balance) {
    if (!balance) return '';
    if (balance.includes('Oversupplied')) return 'oversupplied';
    if (balance.includes('Undersupplied')) return 'undersupplied';
    if (balance === 'Balanced') return 'balanced';
    return '';
}

function getTimingClass(timing) {
    if (!timing) return '';
    const lower = timing.toLowerCase();
    if (lower.includes('buy now')) return 'buy-now';
    if (lower.includes('good entry')) return 'good-entry';
    if (lower.includes('monitor')) return 'monitor';
    if (lower.includes('wait')) return 'wait';
    if (lower.includes('avoid')) return 'avoid';
    return '';
}

function getReliabilityClass(trackRecord) {
    if (!trackRecord) return '';
    const lower = trackRecord.toLowerCase();
    if (lower.includes('highly reliable')) return 'highly-reliable';
    if (lower.includes('reliable')) return 'reliable';
    if (lower.includes('moderate')) return 'moderate';
    if (lower.includes('unproven')) return 'unproven';
    return '';
}

// =====================================================
// UTILITY FUNCTIONS
// =====================================================

// Utility Functions
function formatNumber(num) {
    if (!num) return '0';
    return Math.round(num).toLocaleString();
}

function formatCurrency(amount) {
    if (!amount) return 'AED 0';
    return 'AED ' + Math.round(amount).toLocaleString();
}

function formatFileSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}

function formatDate(timestamp) {
    const date = new Date(timestamp * 1000);
    return date.toLocaleDateString();
}

// Enhanced markdown to HTML converter with visualization support
function markdownToHtml(markdown, chartConfigs = null) {
    if (!markdown) return '';

    // Generate unique IDs for chart containers
    let chartIndex = 0;
    const chartContainerIds = [];

    let html = markdown
        // Remove YAML frontmatter
        .replace(/^---[\s\S]*?---\n*/m, '')
        // Headers
        .replace(/^### (.*$)/gim, '<h3>$1</h3>')
        .replace(/^## (.*$)/gim, '<h2>$1</h2>')
        .replace(/^# (.*$)/gim, '<h1>$1</h1>')
        // Bold
        .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
        // Images (including base64 embedded images) - handle separately due to length
        .replace(/!\[([^\]]*)\]\((data:image\/png;base64,[A-Za-z0-9+/=]+)\)/g, '<figure class="chart-figure"><img src="$2" alt="$1" class="chart-image" loading="lazy"><figcaption>$1</figcaption></figure>')
        .replace(/!\[([^\]]*)\]\(([^)\s]+)\)/g, '<figure class="chart-figure"><img src="$2" alt="$1" class="chart-image"><figcaption>$1</figcaption></figure>')
        // Chart.js config comments - replace with container divs
        .replace(/<!-- CHARTJS_CONFIG:(\w+):[\s\S]*? -->/g, (match, chartName) => {
            const containerId = `chart-container-${chartName}-${chartIndex++}`;
            chartContainerIds.push({ id: containerId, name: chartName });
            return `<div class="interactive-chart-wrapper"><canvas id="${containerId}" class="interactive-chart"></canvas></div>`;
        })
        // Tables (basic support)
        .replace(/\|(.+)\|/g, (match, content) => {
            const cells = content.split('|').map(cell => cell.trim());
            return '<tr>' + cells.map(cell => `<td>${cell}</td>`).join('') + '</tr>';
        })
        // Lists
        .replace(/^\- (.*$)/gim, '<li>$1</li>')
        // Wrap consecutive list items
        .replace(/(<li>.*<\/li>\n?)+/g, '<ul>$&</ul>')
        // Line breaks
        .replace(/\n\n/g, '</p><p>')
        // Links
        .replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" target="_blank">$1</a>');

    // Store chart container info for later rendering
    window._pendingChartContainers = chartContainerIds;

    return '<div class="content-wrapper">' + html + '</div>';
}

// Render Chart.js charts from API response
function renderChartJSVisualizations(chartConfigs) {
    if (!chartConfigs || Object.keys(chartConfigs).length === 0) {
        console.log('No Chart.js configs to render');
        return;
    }

    // Wait for DOM to be updated
    setTimeout(() => {
        const containers = window._pendingChartContainers || [];
        console.log(`Rendering ${Object.keys(chartConfigs).length} Chart.js charts`);

        containers.forEach(({ id, name }) => {
            const canvas = document.getElementById(id);
            if (!canvas) {
                console.warn(`Canvas not found: ${id}`);
                return;
            }

            const config = chartConfigs[name];
            if (!config) {
                console.warn(`Config not found for chart: ${name}`);
                return;
            }

            try {
                // Clean up existing chart if any
                const existingChart = Chart.getChart(canvas);
                if (existingChart) {
                    existingChart.destroy();
                }

                // Process tooltip callbacks (they come as strings from JSON)
                if (config.options?.plugins?.tooltip?.callbacks?.label) {
                    // Remove string callback, use default
                    delete config.options.plugins.tooltip.callbacks;
                }

                // Create new chart
                new Chart(canvas.getContext('2d'), config);
                console.log(`Rendered chart: ${name}`);
            } catch (error) {
                console.error(`Error rendering chart ${name}:`, error);
            }
        });
    }, 100);
}

// =====================================================
// MARKET BALANCE MODAL (Interactive Chart Drill-Down)
// =====================================================

let currentModalData = [];

async function openBalanceModal(balanceCategory) {
    const API_BASE = getApiBase();
    const modal = document.getElementById('balance-modal');
    const title = document.getElementById('modal-title');
    const areasList = document.getElementById('modal-areas-list');

    // Show modal
    modal.classList.remove('hidden');
    title.textContent = `${balanceCategory} Areas`;
    areasList.innerHTML = '<div class="loading-spinner">Loading areas...</div>';

    try {
        const response = await fetch(`${API_BASE}/api/supply/areas-by-balance?balance=${encodeURIComponent(balanceCategory)}`);
        const data = await response.json();

        currentModalData = data.areas;
        title.textContent = `${balanceCategory} (${data.total_areas} areas)`;

        renderModalAreas(currentModalData);
    } catch (error) {
        console.error('Error loading balance areas:', error);
        areasList.innerHTML = '<p class="text-center">Error loading areas. Please try again.</p>';
    }
}

function closeBalanceModal() {
    const modal = document.getElementById('balance-modal');
    modal.classList.add('hidden');
    currentModalData = [];
}

function renderModalAreas(areas) {
    const areasList = document.getElementById('modal-areas-list');

    if (areas.length === 0) {
        areasList.innerHTML = '<p class="text-center">No areas found in this category.</p>';
        return;
    }

    areasList.innerHTML = areas.map(area => `
        <div class="modal-area-card" onclick="showAreaDetails('${area.area}')">
            <div class="modal-area-name">${area.area}</div>
            <div class="modal-area-metrics">
                <div class="modal-metric">
                    <div class="modal-metric-label">SD Ratio</div>
                    <div class="modal-metric-value">${area.supply_demand_ratio?.toFixed(2) || 'N/A'}</div>
                </div>
                <div class="modal-metric">
                    <div class="modal-metric-label">Supply</div>
                    <div class="modal-metric-value">${formatNumber(area.supply_offplan_units || 0)} units</div>
                </div>
                <div class="modal-metric">
                    <div class="modal-metric-label">Demand</div>
                    <div class="modal-metric-value">${formatNumber(area.demand_offplan_tx || 0)} tx</div>
                </div>
                <div class="modal-metric">
                    <div class="modal-metric-label">Price Growth</div>
                    <div class="modal-metric-value">${area.price_yoy_change_pct?.toFixed(1) || 'N/A'}% YoY</div>
                </div>
                <div class="modal-metric">
                    <div class="modal-metric-label">Developers</div>
                    <div class="modal-metric-value">${area.supply_developers || 0}</div>
                </div>
                ${area.opportunity_score ? `
                <div class="modal-metric">
                    <div class="modal-metric-label">Opportunity</div>
                    <div class="modal-metric-value">${area.opportunity_score.toFixed(0)}/100</div>
                </div>
                ` : ''}
            </div>
            ${area.investment_timing || area.opportunity_score ? `
            <div class="modal-area-badges">
                ${area.investment_timing ? `
                <span class="modal-badge timing ${getTimingClass(area.investment_timing)}">${area.investment_timing}</span>
                ` : ''}
                ${area.opportunity_score ? `
                <span class="modal-badge score">Score: ${area.opportunity_score.toFixed(0)}</span>
                ` : ''}
            </div>
            ` : ''}
        </div>
    `).join('');
}

function sortModalAreas() {
    const sortBy = document.getElementById('modal-sort').value;

    let sorted = [...currentModalData];

    switch(sortBy) {
        case 'opportunity':
            sorted.sort((a, b) => (b.opportunity_score || 0) - (a.opportunity_score || 0));
            break;
        case 'sd_ratio':
            sorted.sort((a, b) => (a.supply_demand_ratio || 999) - (b.supply_demand_ratio || 999));
            break;
        case 'price_growth':
            sorted.sort((a, b) => (b.price_yoy_change_pct || 0) - (a.price_yoy_change_pct || 0));
            break;
        case 'name':
            sorted.sort((a, b) => a.area.localeCompare(b.area));
            break;
    }

    renderModalAreas(sorted);
}

// Close modal on Escape key
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        closeBalanceModal();
    }
});
