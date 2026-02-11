const API_ENDPOINT = 'http://localhost:8011/api/dashboard';

// Global Chart Instances
let charts = {
    status: null,
    endpoint: null,
    tiktokApi: null,
    fbUsage: null
};

let rawData = null;
let currentTab = "overview";

// ================================================================
// INIT & EVENT LISTENERS
// ================================================================

function formatDateSafe(dateString) {
    if (!dateString) return 'N/A';
    const date = new Date(dateString);
    if (isNaN(date.getTime())) return 'N/A';
    return date.toLocaleString();
}
document.addEventListener('DOMContentLoaded', () => {
    fetchAndRender();

    // Auto refresh 30s
    setInterval(fetchAndRender, 30000);

    // Date Range Filter Event
    document.getElementById('date-range').addEventListener('change', () => {
        if (rawData) applyFilterAndRender();
    });

    // Refresh Button
    document.getElementById('refresh-btn').addEventListener('click', () => {
        fetchAndRender();
        const btn = document.getElementById('refresh-btn');
        btn.style.transform = 'rotate(360deg)';
        setTimeout(() => btn.style.transform = 'none', 500);
    });

    // Tab Switching Logic
    const tabLinks = document.querySelectorAll('.tab-link');
    tabLinks.forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();

            // UI Update
            document.querySelectorAll('.tab-link').forEach(l => l.classList.remove('active'));
            link.classList.add('active');

            const tabId = link.getAttribute('data-tab');
            currentTab = tabId;

            document.querySelectorAll('.tab-content').forEach(c => c.style.display = 'none');
            document.getElementById(`tab-${tabId}`).style.display = 'block';

            // Update Title
            const titleMap = {
                'overview': 'Dashboard Overview',
                'tiktok': 'TikTok Dashboard',
                'facebook': 'Facebook Dashboard'
            };
            document.getElementById('page-title').textContent = titleMap[tabId];

            // Re-render charts for active tab (to fix sizing issues)
            if (rawData) applyFilterAndRender();
        });
    });
});

async function fetchAndRender() {
    try {
        const response = await fetch(API_ENDPOINT);
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);

        rawData = await response.json();
        applyFilterAndRender();

    } catch (error) {
        console.error('Error:', error);
    }
}

// ================================================================
// FILTER LOGIC
// ================================================================
function applyFilterAndRender() {
    if (!rawData) return;

    const rangeType = document.getElementById('date-range').value;
    const now = new Date();
    let cutoffDate = null;

    if (rangeType === '24h') cutoffDate = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    else if (rangeType === '7d') cutoffDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
    else if (rangeType === '30d') cutoffDate = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);

    // 1. Filter Tasks
    let filteredTasks = rawData.task_logs || [];
    if (cutoffDate) {
        filteredTasks = filteredTasks.filter(t => {
            const tDate = t.start_time ? new Date(t.start_time.replace('+00:00', '')) : new Date();
            return tDate >= cutoffDate;
        });
    }

    // 2. Render based on active tab
    if (currentTab === 'overview') {
        renderOverview(filteredTasks);
    } else if (currentTab === 'tiktok') {
        renderTikTokDashboard(filteredTasks, rawData.api_timeseries);
    } else if (currentTab === 'facebook') {
        renderFacebookDashboard(filteredTasks);
    }
}

// ================================================================
// TAB 1: OVERVIEW RENDERER
// ================================================================
function renderOverview(tasks) {
    // 1. Summary Cards
    const totalTasks = tasks.length;
    const successfulTasks = tasks.filter(t => t.status === 'SUCCESS').length;
    const successRate = totalTasks > 0 ? ((successfulTasks / totalTasks) * 100).toFixed(1) : 0;

    // Avg Duration
    const completedTasks = tasks.filter(t => t.duration_seconds && t.duration_seconds > 0);
    const avgDuration = completedTasks.length > 0
        ? (completedTasks.reduce((sum, t) => sum + t.duration_seconds, 0) / completedTasks.length).toFixed(1)
        : 0;

    // Emails
    const uniqueEmails = new Set(tasks.map(t => t.user_email).filter(e => e && e.includes('@')));

    document.getElementById('total-tasks').textContent = totalTasks;
    document.getElementById('successful-tasks').textContent = successfulTasks;
    document.getElementById('success-rate').textContent = `${successRate}%`;
    document.getElementById('avg-duration').textContent = `${avgDuration}s`;
    document.getElementById('total-emails').textContent = uniqueEmails.size;

    // 2. Status Chart
    renderStatusChart(tasks);

    // 3. Task Type Chart
    renderEndpointChart(tasks);

    // 4. Top Users Chart & Table
    renderUserStats(tasks);

    // 5. Top Failed Users Table (NEW)
    const failedUsers = {};
    tasks.forEach(t => {
        if (t.status === 'FAILED' || t.status === 'FAILURE') {
            const email = t.user_email || 'Unknown';
            failedUsers[email] = (failedUsers[email] || 0) + 1;
        }
    });

    const sortedFailedUsers = Object.entries(failedUsers)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 10); // Top 10

    const failedBody = document.getElementById('top-failed-users-body');
    if (failedBody) {
        if (sortedFailedUsers.length === 0) {
            failedBody.innerHTML = '<tr><td colspan="2" style="text-align:center; color:#999;">No failed tasks</td></tr>';
        } else {
            failedBody.innerHTML = sortedFailedUsers.map(([email, count]) => `
                <tr>
                    <td>${email}</td>
                    <td><strong style="color: #e74c3c;">${count}</strong></td>
                </tr>
            `).join('');
        }
    }

    // 6. Recent Tasks Table
    renderTasksTable(tasks, 'tasks-table-body');
}

// ================================================================
// TAB 2: TIKTOK RENDERER
// ================================================================
function renderTikTokDashboard(tasks, apiTimeseries) {
    // Filter TikTok Tasks
    const tkTasks = tasks.filter(t =>
        ['product', 'creative', 'tiktok_product', 'tiktok_creative'].includes(t.task_type)
        || (t.api_total_counts && JSON.stringify(t.api_total_counts).includes("tiktok.com"))
    );

    const productTasks = tkTasks.filter(t => t.task_type.includes('product')).length;
    const creativeTasks = tkTasks.filter(t => t.task_type.includes('creative')).length;

    document.getElementById('tiktok-total-tasks').textContent = tkTasks.length;
    document.getElementById('tiktok-product-tasks').textContent = productTasks;
    document.getElementById('tiktok-creative-tasks').textContent = creativeTasks;

    // --- NEW CHARTS for TikTok ---

    // 1. Task Type Breakdown (Product vs Creative)
    const typeCounts = { 'Product': 0, 'Creative': 0, 'Other': 0 };
    tkTasks.forEach(t => {
        const type = (t.task_type || '').toLowerCase();
        if (type.includes('product')) typeCounts['Product']++;
        else if (type.includes('creative')) typeCounts['Creative']++;
        else typeCounts['Other']++;
    });

    const ctxTkType = document.getElementById('tiktokTypeChart').getContext('2d');
    if (charts.tiktokType) charts.tiktokType.destroy();

    charts.tiktokType = new Chart(ctxTkType, {
        type: 'pie',
        data: {
            labels: Object.keys(typeCounts),
            datasets: [{
                data: Object.values(typeCounts),
                backgroundColor: ['#3498db', '#e67e22', '#95a5a6']
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { position: 'bottom' } }
        }
    });

    // 2. Success Rate Breakdown
    const statusCounts = { 'Success': 0, 'Failed': 0, 'Cancelled': 0, 'Started': 0 };
    tkTasks.forEach(t => {
        const s = (t.status || '').toUpperCase();
        if (s === 'SUCCESS' || s === 'COMPLETED') statusCounts['Success']++;
        else if (s === 'CANCELLED' || s === 'REVOKED') statusCounts['Cancelled']++;
        else if (s === 'STARTED' || s === 'RUNNING') statusCounts['Started']++;
        else statusCounts['Failed']++;
    });

    const ctxTkStatus = document.getElementById('tiktokStatusChart').getContext('2d');
    if (charts.tiktokStatus) charts.tiktokStatus.destroy();

    charts.tiktokStatus = new Chart(ctxTkStatus, {
        type: 'doughnut',
        data: {
            labels: ['Success', 'Failed', 'Cancelled', 'Started'],
            datasets: [{
                data: Object.values(statusCounts),
                backgroundColor: ['#2ecc71', '#e74c3c', '#95a5a6', '#3498db'],
                borderWidth: 0
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { position: 'bottom' } }
        }
    });

    // Chart: TikTok API Timeline -> Filter only tiktok endpoints
    const tiktokEndpoints = {};
    if (apiTimeseries) {
        for (const [url, data] of Object.entries(apiTimeseries)) {
            if (url.includes('tiktok.com')) {
                // Shorten name
                let name = url.split('/v1.3/')[1] || url;
                name = name.replace(/\/$/, "");
                tiktokEndpoints[name] = data;
            }
        }
    }

    renderLineChart('tiktokApiChart', tiktokEndpoints, charts.tiktokApi, (c) => charts.tiktokApi = c);

    // --- API Totals Table ---
    const endpointCounts = {};
    if (tiktokEndpoints) { // tiktokEndpoints is already filtered from apiTimeseries
        Object.values(tiktokEndpoints).forEach(data => {
            if (Array.isArray(data)) {
                data.forEach(p => {
                    // Check 'breakdown' for specific endpoints if available, otherwise just use Count
                    // The requirement is "Endpoint / Task Type" and "Count"
                    // 'tiktokEndpoints' keys ARE the endpoint names (from url)
                });
            }
        });

        // Actually tiktokEndpoints map is: { "business/get/": [{timestamp, count}, ...], ... }
        // We need to sum up count for each key.

        for (const [endpoint, data] of Object.entries(tiktokEndpoints)) {
            const total = data.reduce((sum, item) => sum + (item.count || 0), 0);
            endpointCounts[endpoint] = total;
        }
    }

    const sortedEndpoints = Object.entries(endpointCounts)
        .sort((a, b) => b[1] - a[1]);

    const totalsBody = document.getElementById('tiktok-api-totals-body');
    if (totalsBody) {
        totalsBody.innerHTML = sortedEndpoints.map(([ep, count]) => `
            <tr>
                <td>${ep}</td>
                <td><strong>${count}</strong></td>
            </tr>
        `).join('');
    }

    // Table
    renderTasksTable(tkTasks, 'tiktok-tasks-body', ['job_id', 'task_type', 'user_email', 'status', 'duration_seconds', 'message']);
}

// ================================================================
// TAB 3: FACEBOOK RENDERER
// ================================================================
function renderFacebookDashboard(tasks) {
    const fbTasks = tasks.filter(t =>
        t.task_type && (t.task_type.includes('facebook') || t.task_type.includes('fb'))
    );

    // Calculate aggregated stats
    let totalBatches = 0;
    let successBatches = 0;
    let totalBackoff = 0;

    // Aggregated Usage for Chart
    // Usage: app_usage_pct, insights_usage, etc.
    // Since these are per-request snapshots, displaying them as a timeline or distribution is tricky.
    // User asked for "visualize detailed API usage".
    // We can parse 'api_total_counts' which contains 'summaries' for Facebook tasks.

    // Let's count success vs error batches from summaries

    fbTasks.forEach(t => {
        if (t.api_total_counts) {
            totalBackoff += (t.api_total_counts.total_backoff_sec || 0);

            const summaries = t.api_total_counts.summaries || [];
            if (Array.isArray(summaries)) {
                summaries.forEach(s => {
                    totalBatches++;
                    // Assumes summary has success_count/error_count
                    if (s.success_count > 0 && s.error_count === 0) successBatches++;
                });
            }
        }
    });

    document.getElementById('fb-total-tasks').textContent = fbTasks.length;
    document.getElementById('fb-success-batches').textContent = successBatches; // Display batches count
    document.getElementById('fb-total-backoff').textContent = `${totalBackoff}s`;

    // Chart: API Usage Breakdown (Batches vs Single Calls etc? Or Request Counts)
    // Let's graph Request Breakdown based on "Business Use Cases" if available in log
    // Or simpler: Success vs Failed batches over tasks?

    // Let's try to visualize: Call Counts by Type (Campaign, AdSet, Ad, Insights) if detectable.
    // Based on user log: `api_total_counts.summaries` has rate limits.
    // Let's do a Pie Chart of "App Usage Pct" bucket distribution (Safe, Warning, Critical)

    const usageBuckets = { 'Safe (<75%)': 0, 'Warning (75-95%)': 0, 'Critical (>95%)': 0 };

    fbTasks.forEach(t => {
        const summaries = t.api_total_counts?.summaries || [];
        summaries.forEach(s => {
            const appUsage = (s.rate_limits?.app_usage_pct || 0) * 100;
            if (appUsage >= 95) usageBuckets['Critical (>95%)']++;
            else if (appUsage >= 75) usageBuckets['Warning (75-95%)']++;
            else usageBuckets['Safe (<75%)']++;
        });
    });

    const ctx = document.getElementById('fbUsageChart').getContext('2d');
    if (charts.fbUsage) charts.fbUsage.destroy();

    charts.fbUsage = new Chart(ctx, {
        type: 'bar', // Using Bar to show volume
        data: {
            labels: Object.keys(usageBuckets),
            datasets: [{
                label: 'Batch Requests by Usage Load',
                data: Object.values(usageBuckets),
                backgroundColor: ['#2ecc71', '#f1c40f', '#e74c3c']
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: { y: { beginAtZero: true } }
        }
    });

    // --- NEW CHARTS: Type & Status ---

    // 1. Task Type Breakdown
    const typeCounts = { 'Daily': 0, 'Performance': 0, 'Breakdown': 0, 'Other': 0 };
    fbTasks.forEach(t => {
        let name = (t.task_type || '').toLowerCase(); // Use task_type which contains 'facebook_daily' etc.
        if (name.includes('daily')) typeCounts['Daily']++;
        else if (name.includes('performance')) typeCounts['Performance']++;
        else if (name.includes('breakdown')) typeCounts['Breakdown']++;
        else typeCounts['Other']++;
    });

    const ctxType = document.getElementById('fbTypeChart').getContext('2d');
    if (charts.fbType) charts.fbType.destroy();

    charts.fbType = new Chart(ctxType, {
        type: 'pie',
        data: {
            labels: Object.keys(typeCounts),
            datasets: [{
                data: Object.values(typeCounts),
                backgroundColor: ['#3498db', '#9b59b6', '#f1c40f', '#95a5a6']
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { position: 'bottom' } }
        }
    });

    // 2. Success Rate Breakdown
    const statusCounts = { 'Success': 0, 'Failed': 0, 'Cancelled': 0, 'Started': 0 };
    fbTasks.forEach(t => {
        const s = (t.status || '').toUpperCase();
        if (s === 'SUCCESS' || s === 'COMPLETED') statusCounts['Success']++;
        else if (s === 'CANCELLED' || s === 'REVOKED') statusCounts['Cancelled']++;
        else if (s === 'STARTED' || s === 'RUNNING') statusCounts['Started']++;
        else statusCounts['Failed']++;
    });

    const ctxStatus = document.getElementById('fbStatusChart').getContext('2d');
    if (charts.fbStatus) charts.fbStatus.destroy();

    charts.fbStatus = new Chart(ctxStatus, {
        type: 'doughnut',
        data: {
            labels: ['Success', 'Failed', 'Cancelled', 'Started'],
            datasets: [{
                data: [
                    statusCounts['Success'],
                    statusCounts['Failed'],
                    statusCounts['Cancelled'],
                    statusCounts['Started']
                ],
                backgroundColor: [
                    '#2ecc71', // Green
                    '#e74c3c', // Red
                    '#95a5a6', // Grey
                    '#3498db'  // Blue
                ],
                borderWidth: 0
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { position: 'bottom' } }
        }
    });

    // Table specialized for Facebook
    const tbody = document.getElementById('fb-tasks-body');
    const displayTasks = fbTasks.slice(0, 1000);

    tbody.innerHTML = displayTasks.map(t => {
        const template = t.template_name || 'N/A';
        const batches = t.api_total_counts?.batch_count || 0;
        const backoff = t.api_total_counts?.total_backoff_sec || 0;

        return `
            <tr onclick="openTaskDetail('${t.job_id}')" title="Click to view details">
                <td><span class="job-id">${(t.job_id || '').substring(0, 8)}...</span></td>
                <td>${template}</td>
                <td>${t.user_email}</td>
                <td class="status-${t.status}">${t.status}</td>
                <td>${backoff}s</td>
                <td>${batches}</td>
                <td class="task-message" style="max-width: 300px; white-space: normal; word-wrap: break-word;">${t.message || ''}</td>
            </tr>
        `;
    }).join('');
}


// ================================================================
// ================================================================
// TASK DETAIL MODAL
// ================================================================

let currentTaskData = null; // Store parsed data for dynamic charting

function openTaskDetail(jobId) {
    if (!rawData || !rawData.task_logs) return;
    const task = rawData.task_logs.find(t => t.job_id === jobId);
    if (!task) return;

    // 1. Basic Info
    document.getElementById('modal-task-id').textContent = `Task: ${jobId}`;
    document.getElementById('modal-task-meta').innerHTML = `
        <span class="status-${task.status}">${task.status}</span> | 
        ${task.user_email} | 
        ${formatDateSafe(task.start_time)}
    `;

    // 2. Parse Data
    const summaries = task.api_total_counts?.summaries || [];
    currentTaskData = {
        timestamps: [],
        appUsage: [],
        globalMaxCpu: [],      // NEW
        globalMaxTotalTime: [], // NEW
        accounts: {}
    };

    summaries.forEach((s, index) => {
        const label = s.timestamp ? formatDateSafe(s.timestamp) : `Batch ${index + 1}`;
        currentTaskData.timestamps.push(label);
        currentTaskData.appUsage.push((s.rate_limits?.app_usage_pct || 0) * 100);

        let maxCpuInBatch = 0;
        let maxTotalTimeInBatch = 0;

        if (s.rate_limits?.account_details) {
            s.rate_limits.account_details.forEach(acc => {
                const accId = acc.account_id;

                // Initialize account data if new
                if (!currentTaskData.accounts[accId]) {
                    currentTaskData.accounts[accId] = {
                        insightsUsage: [],
                        eta: [],
                        cpuTime: [],
                        totalTime: [],
                        tier: 'N/A',
                        call_count: 0,
                        max_insights: 0,
                        max_eta: 0
                    };
                    // Backfill zeros
                    for (let i = 0; i < currentTaskData.timestamps.length - 1; i++) {
                        currentTaskData.accounts[accId].insightsUsage.push(0);
                        currentTaskData.accounts[accId].eta.push(0);
                        currentTaskData.accounts[accId].cpuTime.push(0);
                        currentTaskData.accounts[accId].totalTime.push(0);
                    }
                }

                currentTaskData.accounts[accId].insightsUsage.push(acc.insights_usage_pct || 0);
                currentTaskData.accounts[accId].eta.push(acc.eta_seconds || 0);

                // CPU & Time
                let cpu = 0;
                let t_time = 0;
                let tier = currentTaskData.accounts[accId].tier;

                if (acc.business_use_cases) {
                    acc.business_use_cases.forEach(uc => {
                        cpu += (uc.total_cputime || 0);
                        t_time += (uc.total_time || 0);
                        if (uc.type === 'ads_insights') tier = uc.ads_api_access_tier;
                    });
                }
                currentTaskData.accounts[accId].cpuTime.push(cpu);
                currentTaskData.accounts[accId].totalTime.push(t_time);
                currentTaskData.accounts[accId].tier = tier;

                // Aggregates
                currentTaskData.accounts[accId].max_insights = Math.max(currentTaskData.accounts[accId].max_insights, acc.insights_usage_pct || 0);
                currentTaskData.accounts[accId].max_eta = Math.max(currentTaskData.accounts[accId].max_eta, acc.eta_seconds || 0);
                currentTaskData.accounts[accId].call_count++;

                // Track global max for this batch
                maxCpuInBatch = Math.max(maxCpuInBatch, cpu);
                maxTotalTimeInBatch = Math.max(maxTotalTimeInBatch, t_time);
            });
        }

        // Push global maxes for this timestamp
        currentTaskData.globalMaxCpu.push(maxCpuInBatch);
        currentTaskData.globalMaxTotalTime.push(maxTotalTimeInBatch);

        // Fill gaps for accounts not present in this summary
        Object.keys(currentTaskData.accounts).forEach(accId => {
            const acc = currentTaskData.accounts[accId];
            if (acc.insightsUsage.length < currentTaskData.timestamps.length) {
                acc.insightsUsage.push(0);
                acc.eta.push(0);
                acc.cpuTime.push(0);
                acc.totalTime.push(0);
            }
        });
    });

    // 3. Populate Selectors
    const accSelect = document.getElementById('account-selector');
    const metricSelect = document.getElementById('metric-selector');

    // Enable metric selector (it starts disabled in HTML)
    metricSelect.disabled = false;

    accSelect.innerHTML = '<option value="all">All Accounts</option>';
    Object.keys(currentTaskData.accounts).forEach(accId => {
        accSelect.innerHTML += `<option value="${accId}">${accId}</option>`;
    });

    accSelect.onchange = () => {
        const isAll = accSelect.value === 'all';
        // Allow metric selection even for 'all' to show global max charts
        // If 'all' is selected, default to 'app_usage' IF the current metric is account-specific (like eta/insights)
        // But if user wants to see global time stats, let them.

        if (isAll && (metricSelect.value === 'insights_usage' || metricSelect.value === 'eta')) {
            metricSelect.value = 'app_usage';
        }

        updateDetailChart();
    };

    metricSelect.onchange = updateDetailChart;

    // 4. Render Initial View
    updateDetailChart();

    // 5. Render Account List (SORTED BY CALL COUNT DESC)
    const sortedAccounts = Object.entries(currentTaskData.accounts)
        .sort((a, b) => b[1].call_count - a[1].call_count); // Sort by call_count DESC

    const tbody = document.getElementById('modal-account-list');
    tbody.innerHTML = sortedAccounts.map(([id, data]) => `
        <tr onclick="selectAccount('${id}')" style="cursor: pointer;" title="Click to view chart for this account">
            <td>${id}</td>
            <td>${data.max_insights}%</td>
            <td>${data.max_eta > 0 ? `<strong style="color:red">${data.max_eta}</strong>` : '0'}</td>
            <td>${data.tier}</td>
            <td><strong>${data.call_count}</strong></td>
        </tr>
    `).join('');

    // 6. Show Modal
    const modal = document.getElementById('task-detail-modal');
    modal.style.display = "block";

    // ... (Log Viewer logic remains same) ...
    // Reset Log View
    const logContainer = document.getElementById('log-container');
    const logContent = document.getElementById('log-content');
    const btnViewLogs = document.getElementById('btn-view-logs');

    if (logContainer) logContainer.style.display = 'none';
    if (logContent) logContent.textContent = '';
    if (btnViewLogs) {
        btnViewLogs.textContent = 'View Full Logs';
        // Remove old listeners by cloning
        const newBtn = btnViewLogs.cloneNode(true);
        btnViewLogs.parentNode.replaceChild(newBtn, btnViewLogs);

        newBtn.addEventListener('click', () => {
            if (logContainer.style.display === 'none') {
                fetchTaskLogs(jobId);
                logContainer.style.display = 'block';
                newBtn.textContent = 'Hide Logs';
            } else {
                logContainer.style.display = 'none';
                newBtn.textContent = 'View Full Logs';
            }
        });
    }

    const span = modal.querySelector(".close-modal");
    span.onclick = function () { modal.style.display = "none"; }
    window.onclick = function (event) { if (event.target == modal) modal.style.display = "none"; }
}

async function fetchTaskLogs(jobId) {
    const logContent = document.getElementById('log-content');
    logContent.textContent = 'Loading logs...';

    try {
        const response = await fetch(`/api/dashboard/logs/${jobId}`);
        if (!response.ok) throw new Error('Failed to fetch logs');

        const data = await response.json();
        logContent.textContent = data.logs || 'No logs found.';
    } catch (error) {
        console.error('Error fetching logs:', error);
        logContent.textContent = `Error loading logs: ${error.message}`;
    }
}


function selectAccount(accountId) {
    const accSelect = document.getElementById('account-selector');
    if (accSelect) {
        accSelect.value = accountId;
        // Trigger change event to update chart
        const event = new Event('change');
        accSelect.dispatchEvent(event);

        // Update metric selector to time_stats or something specific
        const metricSelect = document.getElementById('metric-selector');
        // If we were on global app usage, maybe switch to something else? 
        // For now keep user selection unless invalid.
    }
}

function updateDetailChart() {
    if (!currentTaskData) return;

    const accSelect = document.getElementById('account-selector');
    const metricSelect = document.getElementById('metric-selector');
    const accountId = accSelect.value;
    const metric = metricSelect.value;

    // Disable irrelevant metrics for 'all'
    // insights_usage and eta are strictly per-account (or maybe avg/max global? user asked for cpu/time specifically)
    // We'll hide/disable options that don't make sense if needed, but let's just handle rendering.

    // Enable all options in logic, handle 'all' case in switch

    Array.from(metricSelect.options).forEach(opt => {
        if (accountId === 'all') {
            if (opt.value === 'insights_usage' || opt.value === 'eta') opt.disabled = true;
            else opt.disabled = false;
        } else {
            opt.disabled = false;
        }
    });


    let labels = currentTaskData.timestamps;
    let datasets = [];
    let yMax = 100;

    if (accountId === 'all') {
        if (metric === 'app_usage') {
            datasets.push({
                label: 'App Usage PCT (Global)',
                data: currentTaskData.appUsage,
                borderColor: '#e74c3c',
                backgroundColor: hexToRgba('#e74c3c', 0.1),
                fill: true,
                tension: 0.3,
                pointRadius: 2
            });
        } else if (metric === 'time_stats') {
            // SHOW MAX GLOBAL CPU/TIME
            datasets.push({
                label: 'Max CPU Time (Across All Accounts)',
                data: currentTaskData.globalMaxCpu,
                borderColor: '#9b59b6', // Purple
                backgroundColor: hexToRgba('#9b59b6', 0.1),
                fill: false,
                tension: 0.3,
                pointRadius: 2
            });
            datasets.push({
                label: 'Max Process Time (Across All Accounts)',
                data: currentTaskData.globalMaxTotalTime,
                borderColor: '#2c3e50', // Dark Blue
                backgroundColor: hexToRgba('#2c3e50', 0.1),
                fill: false,
                tension: 0.3,
                pointRadius: 2
            });
            yMax = null; // Auto scale
        }
    } else {
        const accData = currentTaskData.accounts[accountId];
        if (!accData) return;

        switch (metric) {
            case 'app_usage':
                // Even for single account, showing Global App Usage as reference is often useful, 
                // or we show nothing? The prompt didn't specify. 
                // Let's show Global App Usage as context.
                datasets.push({
                    label: 'App Usage PCT (Global)',
                    data: currentTaskData.appUsage,
                    borderColor: '#e74c3c',
                    backgroundColor: hexToRgba('#e74c3c', 0.1),
                    fill: true,
                    tension: 0.3,
                    pointRadius: 2
                });
                break;
            case 'insights_usage':
                datasets.push({
                    label: `Insights Usage PCT (${accountId})`,
                    data: accData.insightsUsage,
                    borderColor: '#3498db',
                    backgroundColor: hexToRgba('#3498db', 0.1),
                    fill: true,
                    tension: 0.3,
                    pointRadius: 2
                });
                break;
            case 'eta':
                datasets.push({
                    label: `ETA Seconds (${accountId})`,
                    data: accData.eta,
                    borderColor: '#f39c12',
                    backgroundColor: hexToRgba('#f39c12', 0.1),
                    fill: true,
                    tension: 0.3,
                    pointRadius: 2
                });
                yMax = null; // Auto scale
                break;
            case 'time_stats':
                // Multiple datasets for Time Stats
                datasets.push({
                    label: `Total CPU Time (${accountId})`,
                    data: accData.cpuTime,
                    borderColor: '#9b59b6', // Purple
                    backgroundColor: hexToRgba('#9b59b6', 0.1),
                    fill: false,
                    tension: 0.3,
                    pointRadius: 2
                });
                datasets.push({
                    label: `Total Process Time (${accountId})`,
                    data: accData.totalTime,
                    borderColor: '#2c3e50', // Dark Blue
                    backgroundColor: hexToRgba('#2c3e50', 0.1),
                    fill: false,
                    tension: 0.3,
                    pointRadius: 2
                });
                yMax = null;
                break;
            default:
                data = [];
        }
    }

    const ctx = document.getElementById('detailUsageChart').getContext('2d');
    if (charts.detailUsage) charts.detailUsage.destroy();

    charts.detailUsage = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: datasets
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    max: yMax,
                    title: { display: true, text: 'Value' }
                }
            },
            plugins: {
                annotation: (metric === 'insights_usage') ? {
                    annotations: {
                        line1: {
                            type: 'line',
                            yMin: 95,
                            yMax: 95,
                            borderColor: 'red',
                            borderWidth: 1,
                            borderDash: [5, 5],
                            label: { content: 'Limit', enabled: true }
                        }
                    }
                } : {}
            }
        }
    });
}

function hexToRgba(hex, alpha) {
    const r = parseInt(hex.slice(1, 3), 16);
    const g = parseInt(hex.slice(3, 5), 16);
    const b = parseInt(hex.slice(5, 7), 16);
    return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}


// ================================================================
// CHART HELPERS
// ================================================================

function renderUserStats(tasks) {
    // 1. Prepare Data
    const userCounts = {};
    tasks.forEach(t => {
        if (t.user_email) userCounts[t.user_email] = (userCounts[t.user_email] || 0) + 1;
    });

    const sortedUsers = Object.entries(userCounts)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 5); // Top 5

    // 2. Render Table
    const tbody = document.getElementById('top-users-body');
    if (tbody) {
        tbody.innerHTML = sortedUsers.map(([email, count]) => `
            <tr>
                <td>${email}</td>
                <td><strong>${count}</strong></td>
            </tr>
        `).join('');
    }

    // 3. Render Chart
    const ctx = document.getElementById('userChart').getContext('2d');

    // Check global chart instance map, create one for 'user' if not exists in init
    if (typeof charts.user === 'undefined') charts.user = null;
    if (charts.user) charts.user.destroy();

    charts.user = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: sortedUsers.map(u => u[0].split('@')[0]), // Shorten email
            datasets: [{
                label: 'Tasks Count',
                data: sortedUsers.map(u => u[1]),
                backgroundColor: '#3498db',
                borderRadius: 4
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: { x: { beginAtZero: true } }
        }
    });
}

function renderStatusChart(tasks) {
    const counts = tasks.reduce((acc, t) => {
        acc[t.status] = (acc[t.status] || 0) + 1;
        return acc;
    }, {});

    // Define Color Map
    const colorMap = {
        'SUCCESS': '#2ecc71',   // Green
        'COMPLETED': '#2ecc71',
        'FAILED': '#e74c3c',    // Red
        'FAILURE': '#e74c3c',
        'STARTED': '#3498db',   // Blue
        'RUNNING': '#3498db',
        'PENDING': '#f1c40f',   // Yellow
        'REVOKED': '#95a5a6',   // Grey
        'CANCELLED': '#7f8c8d',  // Dark Grey
        'TIMED_OUT': '#e67e22'  // Orange
    };

    const labels = Object.keys(counts);
    const data = Object.values(counts);
    const backgroundColors = labels.map(status => colorMap[status] || '#bdc3c7'); // Default light grey

    const ctx = document.getElementById('statusChart').getContext('2d');
    if (charts.status) charts.status.destroy();

    charts.status = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: labels,
            datasets: [{
                data: data,
                backgroundColor: backgroundColors,
                borderWidth: 0
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { position: 'right' } }
        }
    });
}

function renderEndpointChart(tasks) {
    const counts = tasks.reduce((acc, t) => {
        const type = t.task_type || 'Unknown';
        acc[type] = (acc[type] || 0) + 1;
        return acc;
    }, {});

    const ctx = document.getElementById('endpointChart').getContext('2d');
    if (charts.endpoint) charts.endpoint.destroy();

    charts.endpoint = new Chart(ctx, {
        type: 'pie',
        data: {
            labels: Object.keys(counts),
            datasets: [{
                data: Object.values(counts),
                backgroundColor: ['#3498db', '#9b59b6', '#1abc9c', '#e67e22', '#34495e'],
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { position: 'right' } }
        }
    });
}

function renderLineChart(canvasId, datasetMap, chartInstance, setChartInstance) {
    const ctx = document.getElementById(canvasId).getContext('2d');
    if (chartInstance) chartInstance.destroy();

    if (!datasetMap || Object.keys(datasetMap).length === 0) return;

    const colors = ['#1abc9c', '#e74c3c', '#3498db', '#f1c40f', '#9b59b6'];
    const datasets = [];
    let i = 0;

    for (const [label, dataPoints] of Object.entries(datasetMap)) {
        datasets.push({
            label: label,
            data: dataPoints.map(d => d.count),
            borderColor: colors[i % colors.length],
            tension: 0.3,
            fill: false
        });
        i++;
    }

    // Labels based on first dataset
    const firstData = Object.values(datasetMap)[0];
    const labels = firstData.map(d =>
        new Date(d.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
    );

    const newChart = new Chart(ctx, {
        type: 'line',
        data: { labels, datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: { legend: { position: 'bottom' } },
            scales: { y: { beginAtZero: true } }
        }
    });

    setChartInstance(newChart);
}

function formatDateSafe(dateString) {
    if (!dateString) return '';
    try {
        const cleanedDateString = dateString.replace('+00:00', '');
        const date = new Date(cleanedDateString);
        if (isNaN(date.getTime())) {
            return '';
        }
        return date.toLocaleString();
    } catch (e) {
        console.error("Error formatting date:", e);
        return '';
    }
}

function renderTasksTable(tasks, containerId, columns = null) {
    const tbody = document.getElementById(containerId);
    if (!tbody) return;

    // Default columns for Overview
    if (!columns) {
        columns = ['job_id', 'task_type', 'user_email', 'status', 'start_time', 'end_time', 'duration_seconds', 'message'];
    }

    const displayTasks = tasks.slice(0, 1000); // Limit to 1000 rows

    tbody.innerHTML = displayTasks.map(t => {
        const cells = columns.map(col => {
            let content = t[col];

            // Special formatting
            if (col === 'job_id') {
                return `<td><span class="job-id">${(content || '').substring(0, 8)}...</span></td>`;
            }
            if (col === 'status') {
                return `<td class="status-${content}">${content}</td>`;
            }
            if (col === 'start_time' || col === 'end_time') {
                return `<td>${formatDateSafe(content)}</td>`;
            }
            if (col === 'duration_seconds') {
                return `<td>${content ? parseFloat(content).toFixed(2) + 's' : ''}</td>`;
            }
            if (col === 'message') {
                return `<td class="task-message" style="max-width: 300px; white-space: normal; word-wrap: break-word;">${content || ''}</td>`;
            }

            return `<td>${content || ''}</td>`;
        }).join('');

        return `<tr onclick="openTaskDetail('${t.job_id}')" title="Click to view details">${cells}</tr>`;
    }).join('');
}