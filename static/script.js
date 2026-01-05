const API_ENDPOINT = 'http://localhost:8011/api/dashboard';

// Global Chart Instances để destroy khi render lại
let charts = {
    status: null,
    apiUsage: null,
    user: null,
    endpoint: null
};

// Lưu dữ liệu gốc để lọc
let rawData = null;

// ================================================================
// INIT & EVENT LISTENERS
// ================================================================
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
});

async function fetchAndRender() {
    try {
        const response = await fetch(API_ENDPOINT);
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        
        rawData = await response.json();
        applyFilterAndRender();
        
    } catch (error) {
        console.error('Error:', error);
        // Có thể hiện thông báo lỗi lên UI nếu cần
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
    // 'all' -> cutoffDate = null

    // 1. Lọc Tasks List
    let filteredTasks = rawData.task_logs || [];
    if (cutoffDate) {
        filteredTasks = filteredTasks.filter(t => {
            const tDate = t.start_time ? new Date(t.start_time.replace('+00:00', '')) : new Date();
            return tDate >= cutoffDate;
        });
    }

    // 2. Render UI
    renderSummaryCards(filteredTasks);
    renderStatusChart(filteredTasks);
    renderUserStats(filteredTasks);    // New: User Stats
    renderEndpointChart(filteredTasks); // New: Endpoint Pie Chart
    renderTasksTable(filteredTasks);
    renderApiTotalsList(filteredTasks);

    renderApiTimeseriesChart(rawData.api_timeseries || {}); 
}

// ================================================================
// RENDER FUNCTIONS
// ================================================================

function renderSummaryCards(tasks) {
    const totalTasks = tasks.length;
    const successfulTasks = tasks.filter(t => t.status === 'SUCCESS').length;
    const successRate = totalTasks > 0 ? ((successfulTasks / totalTasks) * 100).toFixed(1) : 0;
    
    const totalDuration = tasks.reduce((sum, t) => sum + (t.duration_seconds || 0), 0);
    const avgDuration = totalTasks > 0 ? (totalDuration / totalTasks).toFixed(1) : 0;

    // Tính tổng email (Số task có user_email hợp lệ)
    const tasksWithEmail = new Set(
        tasks
            .filter(t => t.user_email && t.user_email.includes('@'))
            .map(t => t.user_email)
    ).size;

    document.getElementById('total-tasks').textContent = totalTasks;
    document.getElementById('successful-tasks').textContent = successfulTasks;
    document.getElementById('success-rate').textContent = `${successRate}%`;
    document.getElementById('avg-duration').textContent = `${avgDuration}s`;
    document.getElementById('total-emails').textContent = tasksWithEmail; // New Metric
}

// --- 1. Status Doughnut Chart ---
function renderStatusChart(tasks) {
    const counts = tasks.reduce((acc, t) => {
        acc[t.status] = (acc[t.status] || 0) + 1;
        return acc;
    }, {});
    
    const labels = Object.keys(counts);
    const data = Object.values(counts);

    // 2. Định nghĩa bảng màu cố định (Color Map)
    const colorMap = {
        'SUCCESS': '#2ecc71',   // Xanh lá
        'FAILED': '#e74c3c',    // Đỏ
        'STARTED': '#3498db',   // Xanh dương (đang chạy)
        'PENDING': '#f1c40f',   // Vàng (chờ)
        'TIMED_OUT': '#95a5a6', // Xám
        'REVOKED': '#7f8c8d'    // Xám đậm
    };

    // 3. Tạo mảng màu dựa trên label thực tế
    const backgroundColors = labels.map(status => {
        return colorMap[status] || '#bdc3c7'; 
    });

    const ctx = document.getElementById('statusChart').getContext('2d');
    if (charts.status) charts.status.destroy();

    charts.status = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: labels,
            datasets: [{
                data: data,
                backgroundColor: backgroundColors, // Sử dụng mảng màu đã map đúng
                borderWidth: 0
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { 
                legend: { position: 'right' } 
            }
        }
    });
}

function renderApiTotalsList(tasks) {
    const list = document.getElementById('api-totals-list');
    if (!list) return;

    // Tính tổng số liệu thực tế
    const counts = aggregateApiCounts(tasks);

    // Sắp xếp giảm dần (API nào gọi nhiều nhất lên đầu)
    const sorted = Object.entries(counts).sort(([, a], [, b]) => b - a);

    if (sorted.length === 0) {
        list.innerHTML = '<li style="text-align:center; padding:10px; border:none; background:none;">No API calls recorded</li>';
        return;
    }

    // Render HTML
    list.innerHTML = sorted.map(([fullUrl, count]) => {
        // Logic rút gọn link: Xóa domain, giữ lại phần đuôi
        const shortName = fullUrl.replace('https://business-api.tiktok.com/open_api/v1.3/', '');

        return `
            <li>
                <span class="api-name" title="${fullUrl}">${shortName}</span>
                <span class="api-count">${count}</span>
            </li>
        `;
    }).join('');
}

// --- 2. Endpoint Pie Chart (New) ---
function renderEndpointChart(tasks) {
    // Nhóm theo Endpoint (dựa vào task type hoặc trích xuất từ đâu đó)
    // Giả sử dùng 'task_type' hoặc logic map từ API Total
    // Nếu muốn chính xác endpoint url, cần lấy từ api_total_counts hoặc task detail
    
    // Ở đây ta đếm từ filteredTasks theo task_type
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

// --- 3. User Stats (Chart + Table) (New) ---
function renderUserStats(tasks) {
    // Đếm số task theo email
    const userCounts = tasks.reduce((acc, t) => {
        if(t.user_email) {
            acc[t.user_email] = (acc[t.user_email] || 0) + 1;
        }
        return acc;
    }, {});

    // Chuyển sang mảng và sort giảm dần
    const sortedUsers = Object.entries(userCounts)
        .sort(([,a], [,b]) => b - a)
        .slice(0, 10); // Lấy top 10

    // Render Table
    const tbody = document.getElementById('top-users-body');
    tbody.innerHTML = sortedUsers.map(([email, count]) => `
        <tr>
            <td>${email}</td>
            <td><strong>${count}</strong></td>
        </tr>
    `).join('');

    // Render Horizontal Bar Chart
    const ctx = document.getElementById('userChart').getContext('2d');
    if (charts.user) charts.user.destroy();

    charts.user = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: sortedUsers.map(u => u[0].split('@')[0]), // Lấy tên trước @ cho ngắn
            datasets: [{
                label: 'Tasks Count',
                data: sortedUsers.map(u => u[1]),
                backgroundColor: '#3498db',
                borderRadius: 4
            }]
        },
        options: {
            indexAxis: 'y', // Biểu đồ ngang
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: { x: { beginAtZero: true } }
        }
    });
}

// --- 4. Timeline Chart (Giữ nguyên logic cũ nhưng chỉnh style) ---
function renderApiTimeseriesChart(apiTimeseries) {
    if(!apiTimeseries) return;
    const datasets = [];
    const colors = ['#1abc9c', '#e74c3c', '#3498db', '#f1c40f'];
    let colorIndex = 0;

    for (const endpoint in apiTimeseries) {
        datasets.push({
            label: endpoint.replace('https://business-api.tiktok.com/open_api/v1.3/', ''),
            data: apiTimeseries[endpoint].map(item => item.count),
            borderColor: colors[colorIndex % colors.length],
            tension: 0.3,
            fill: false,
            pointRadius: 2
        });
        colorIndex++;
    }
    
    // Lấy labels từ endpoint đầu tiên
    const firstKey = Object.keys(apiTimeseries)[0];
    const labels = firstKey ? apiTimeseries[firstKey].map(item => 
        new Date(item.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
    ) : [];

    const ctx = document.getElementById('apiUsageChart').getContext('2d');
    if (charts.apiUsage) charts.apiUsage.destroy();
    
    charts.apiUsage = new Chart(ctx, {
        type: 'line',
        data: { labels, datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: { legend: { position: 'bottom' } },
            scales: { y: { beginAtZero: true, grid: { borderDash: [2, 4] } } }
        }
    });
}

// --- 5. Task Table ---
function renderTasksTable(tasks) {
    const tbody = document.getElementById('tasks-table-body');
    // Chỉ hiện tối đa 100 task mới nhất để tránh lag browser
    const displayTasks = tasks.slice(0, 100); 
    
    tbody.innerHTML = displayTasks.map(t => {
        const startTime = t.start_time ? new Date(t.start_time.replace('+00:00', '')).toLocaleString() : '';
        const endTime = t.end_time ? new Date(t.end_time.replace('+00:00', '')).toLocaleString() : '';
        
        return `
            <tr>
                <td><span title="${t.job_id}" style="font-family:monospace; background:#eee; padding:2px 4px; border-radius:3px;">${(t.job_id || '').substring(0, 8)}...</span></td>
                <td>${t.task_type}</td>
                <td>${t.user_email}</td>
                <td class="status-${t.status}">${t.status}</td>
                <td>${startTime}</td>
                <td>${endTime}</td>
                <td>${(t.duration_seconds || 0).toFixed(2)}</td>
                <td style="color: #c0392b; max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${t.error_message || ''}">${t.error_message || ''}</td>
            </tr>
        `;
    }).join('');
}

function aggregateApiCounts(tasks) {
    const globalCounts = {};

    tasks.forEach(task => {
        // Kiểm tra xem task có dữ liệu api_total_counts không
        if (task.api_total_counts && typeof task.api_total_counts === 'object') {
            
            // Duyệt qua từng endpoint trong task đó
            Object.entries(task.api_total_counts).forEach(([url, count]) => {
                // Cộng dồn vào biến tổng global
                globalCounts[url] = (globalCounts[url] || 0) + count;
            });
        }
    });

    return globalCounts;
}