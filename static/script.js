// script.js

const API_ENDPOINT = 'http://103.102.131.30:8011/api/dashboard';

let statusChartInstance = null;
let apiUsageChartInstance = null;

// ================================================================
// HÀM LẤY VÀ PHÂN TÁCH DỮ LIỆU
// ================================================================
async function fetchData() {
    try {
        const response = await fetch(API_ENDPOINT);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        return await response.json();
    } catch (error) {
        console.error('Error fetching dashboard data:', error);
        document.body.innerHTML = `<div style="color: red; text-align: center; padding: 50px;"><h1>Could not connect to API</h1><p>Please ensure the API server is running at: <strong>${API_ENDPOINT}</strong></p><p>Error: ${error.message}</p></div>`;
        return null;
    }
}

// ================================================================
// CÁC HÀM RENDER RIÊNG BIỆT
// ================================================================
function renderSummaryCards(tasks) {
    const totalTasks = tasks.length;
    const successfulTasks = tasks.filter(t => t.status === 'SUCCESS').length;
    const successRate = totalTasks > 0 ? ((successfulTasks / totalTasks) * 100).toFixed(2) : 0;
    const totalDuration = tasks.reduce((sum, t) => sum + (t.duration_seconds || 0), 0);
    const avgDuration = totalTasks > 0 ? (totalDuration / totalTasks).toFixed(2) : 0;

    document.getElementById('total-tasks').textContent = totalTasks;
    document.getElementById('successful-tasks').textContent = successfulTasks;
    document.getElementById('success-rate').textContent = `${successRate}%`;
    document.getElementById('avg-duration').textContent = `${avgDuration}s`;
}

function renderStatusChart(tasks) {
    const statusCounts = tasks.reduce((acc, task) => {
        acc[task.status] = (acc[task.status] || 0) + 1;
        return acc;
    }, {});

    const chartLabels = Object.keys(statusCounts);
    const chartData = Object.values(statusCounts);
    const backgroundColors = chartLabels.map(status => {
        switch (status) {
            case 'SUCCESS': return '#28a745';
            case 'FAILED': return '#dc3545';
            case 'CANCELLED': case 'STOPPED': return '#ffc107';
            case 'TIMED_OUT': return '#6c757d';
            case 'STARTED': return '#007bff';
            default: return '#cccccc';
        }
    });

    const ctx = document.getElementById('statusChart').getContext('2d');
    if (statusChartInstance) statusChartInstance.destroy();
    statusChartInstance = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: chartLabels,
            datasets: [{
                data: chartData,
                backgroundColor: backgroundColors,
                hoverOffset: 4
            }]
        },
        options: {
            responsive: true,
            plugins: {
                legend: { position: 'top' },
                title: { display: false }
            }
        }
    });
}

function renderApiTotals(apiTotals) {
    const list = document.getElementById('api-totals-list');
    list.innerHTML = '';
    const sortedTotals = Object.entries(apiTotals).sort(([, a], [, b]) => b - a);

    for (const [endpoint, count] of sortedTotals) {
        const shortName = endpoint.replace('https://business-api.tiktok.com/open_api/v1.3/', '');
        const li = document.createElement('li');
        li.innerHTML = `${shortName} <span>${count}</span>`;
        list.appendChild(li);
    }
}

function renderApiTimeseriesChart(apiTimeseries) {
    const datasets = [];
    const colors = ['rgb(75, 192, 192)', 'rgb(255, 99, 132)', 'rgb(54, 162, 235)', 'rgb(255, 205, 86)'];
    let colorIndex = 0;

    for (const endpoint in apiTimeseries) {
        datasets.push({
            label: endpoint.replace('https://business-api.tiktok.com/open_api/v1.3/', ''),
            data: apiTimeseries[endpoint].map(item => item.count),
            borderColor: colors[colorIndex % colors.length],
            tension: 0.1,
            fill: false
        });
        colorIndex++;
    }
    
    const firstEndpointKey = Object.keys(apiTimeseries)[0];
    const labels = firstEndpointKey ? apiTimeseries[firstEndpointKey].map(item => new Date(item.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })) : [];

    const ctx = document.getElementById('apiUsageChart').getContext('2d');
    if (apiUsageChartInstance) apiUsageChartInstance.destroy();
    apiUsageChartInstance = new Chart(ctx, {
        type: 'line',
        data: { labels: labels, datasets: datasets },
        options: {
            scales: { y: { beginAtZero: true } },
            plugins: { legend: { position: 'bottom' } }
        }
    });
}

function renderTasksTable(tasks) {
    const tableBody = document.getElementById('tasks-table-body');
    tableBody.innerHTML = '';
    
    tasks.forEach(task => {
        const row = tableBody.insertRow();
        
        const jobIdCell = row.insertCell();
        jobIdCell.textContent = (task.job_id || '').substring(0, 8) + '...';
        jobIdCell.title = task.job_id;
        
        row.insertCell().textContent = task.task_type;
        row.insertCell().textContent = task.user_email;
        
        const statusCell = row.insertCell();
        statusCell.textContent = task.status;
        statusCell.className = `status-${task.status}`;
        
        const startTimeString = task.start_time ? task.start_time.replace('+00:00', '') : null;
        const endTimeString = task.end_time ? task.end_time.replace('+00:00', '') : null;
        
        row.insertCell().textContent = startTimeString ? new Date(startTimeString).toLocaleString() : 'N/A';
        row.insertCell().textContent = endTimeString ? new Date(endTimeString).toLocaleString() : 'N/A';
        row.insertCell().textContent = (task.duration_seconds || 0).toFixed(2);
        
        const errorMessage = task.error_message || '';
        const errorCell = row.insertCell();
        errorCell.textContent = errorMessage;
        errorCell.title = errorMessage;
        if (errorMessage) {
            errorCell.style.color = '#dc3545';
        }
    });
}

// ================================================================
// HÀM ĐIỀU PHỐI CHÍNH
// ================================================================
async function renderDashboard() {
    console.log("Refreshing data...");
    const data = await fetchData();
    if (!data) return;

    renderSummaryCards(data.task_logs || []);
    renderStatusChart(data.task_logs || []);
    renderTasksTable(data.task_logs || []);
    
    if (data.api_total_counts) {
        renderApiTotals(data.api_total_counts);
    }
    if (data.api_timeseries && Object.keys(data.api_timeseries).length > 0) {
        renderApiTimeseriesChart(data.api_timeseries);
    }
}

// Chạy và thiết lập tự động reload
document.addEventListener('DOMContentLoaded', () => {
    renderDashboard();
    setInterval(renderDashboard, 10000);
});