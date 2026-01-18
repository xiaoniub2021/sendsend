
//#region 超级管理员面板

//#region 超级管理员密码验证与面板逻辑
let currentSuperAdminServerId = null;
let superAdminServers = [];
// 检查变量是否已声明，避免重复声明错误（这些变量可能在 server_page.js 或 admin_page.js 中已声明）
// 使用 window 对象来避免重复声明错误
if (typeof window.currentManagerId === 'undefined') {
    window.currentManagerId = null;
}
if (typeof window.managerUsers === 'undefined') {
    window.managerUsers = [];
}
if (typeof window.managerUserGroups === 'undefined') {
    window.managerUserGroups = [];
}
if (typeof window.adminAccounts === 'undefined') {
    window.adminAccounts = [];
}

function showSuperAdminPasswordModal() {
    const modal = document.getElementById('superAdminPasswordModal');
    if (!modal) return;

    const passwordInput = document.getElementById('superAdminPasswordInput');
    if (passwordInput) {
        passwordInput.value = '';
    }

    modal.style.display = 'flex';
    requestAnimationFrame(() => {
        modal.classList.add('show');
        setTimeout(() => {
            if (passwordInput) {
                passwordInput.focus();
            }
        }, 50);
    });
}

function closeSuperAdminPasswordModal() {
    const modal = document.getElementById('superAdminPasswordModal');
    if (!modal) return;

    modal.classList.remove('show');
    setTimeout(() => {
        modal.style.display = 'none';
        const passwordInput = document.getElementById('superAdminPasswordInput');
        if (passwordInput) {
            passwordInput.value = '';
        }
    }, 200);
}

async function verifySuperAdminPassword() {
    const password = document.getElementById('superAdminPasswordInput').value.trim();
    
    if (!password) {
        await customAlert('请输入密码');
        return;
    }
    
    try {
        // 调用后端API验证密码并获取token
        const response = await fetch(`${API_BASE_URL}/server-manager/login`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ password: password })
        });
        
        const data = await response.json();
        
        if (response.ok && data.success && data.token) {
            // 🔒 超级管理员页面：使用sessionStorage，关闭页面就清除
            // 密码验证通过后，保存token到sessionStorage用于本次会话的API调用
            if (data.token) {
                sessionStorage.setItem('super_admin_token', data.token);
            }
            
            closeSuperAdminPasswordModal();
            showSuperAdminPanel();
        } else {
            await customAlert(data.message || '密码错误');
        }
    } catch (e) {
        console.error('超级管理员登录失败:', e);
        await customAlert('登录失败，请检查网络连接');
    }
}

function showSuperAdminPanel() {
    const panel = document.getElementById('superAdminPanel');
    if (!panel) return;

    // ✅ 样式扁平化：移除 3/4/5 面板的“板上板”背景块（不改变布局/位置）
    // 只影响：充值(3) / 费率(4) / 日志(5)
    saInjectFlatPanels345Styles();

    panel.style.display = 'flex';
    requestAnimationFrame(() => {
        panel.classList.add('show');
        loadSuperAdminServers();
        setupSuperAdminLogControls();
    });
}

/**
 * 移除 3/4/5 面板内的多余底板（背景/边框/圆角）。
 * 注意：只扁平化“容器底板”，不改按钮/输入框等控件本身风格，元素位置不变。
 */
function saInjectFlatPanels345Styles() {
    if (document.getElementById('saFlatPanels345Style')) return;

    const style = document.createElement('style');
    style.id = 'saFlatPanels345Style';
    style.textContent = `
        /* ================================
           超级管理员面板：3/4/5 扁平化
           3=充值 superAdminRechargeSection
           4=费率 superAdminRatesSection
           5=日志 superAdminLogsSection
           ================================ */

        /* 3) 充值：用户信息块/记录块去底板 */
        #superAdminRechargeSection #saRechargeUserInfoPanel > div {
            background: transparent !important;
            background-color: transparent !important;
            border: none !important;
            box-shadow: none !important;
            border-radius: 0 !important;
        }
        #superAdminRechargeSection #saRechargeRecordsList {
            background: transparent !important;
            background-color: transparent !important;
            border: none !important;
            box-shadow: none !important;
            border-radius: 0 !important;
        }

        /* 4) 费率：列表容器/头/体去底板（保留行分隔与控件本身样式） */
        #superAdminRatesSection .rate-list-container {
            background: transparent !important;
            background-color: transparent !important;
            border: none !important;
            box-shadow: none !important;
            border-radius: 0 !important;
        }
        #superAdminRatesSection .rate-list-header,
        #superAdminRatesSection .rate-list-body {
            background: transparent !important;
            background-color: transparent !important;
            border: none !important;
            box-shadow: none !important;
        }

        /* 5) 日志：日志内容框去底板 */
        #superAdminLogsSection #superAdminLogContent {
            background: transparent !important;
            background-color: transparent !important;
            border: none !important;
            box-shadow: none !important;
            border-radius: 0 !important;
        }
    `;

    document.head.appendChild(style);
}

function closeSuperAdminPanel() {
    const panel = document.getElementById('superAdminPanel');
    if (!panel) return;

    panel.classList.remove('show');
    setTimeout(() => {
        panel.style.display = 'none';
        currentSuperAdminServerId = null;
        const detailSection = document.getElementById('superAdminDetailSection');
        if (detailSection) {
            detailSection.style.display = 'none';
        }
    }, 200);
}
//#endregion
//#region 超级管理员服务器列表管理
async function loadSuperAdminServers() {
    try {
        const response = await fetch(`${API_BASE_URL}/servers?t=${Date.now()}`, {
            method: 'GET',
            headers: { 'Content-Type': 'application/json' }
        });

        if (!response.ok) {
            throw new Error(`API响应错误: ${response.status}`);
        }

        const data = await response.json();
        if (data.success && data.servers) {
            // 只显示在线服务器
            superAdminServers = data.servers.filter(s => {
                const status = (s.status || '').toLowerCase();
                return status === 'connected' || status === 'available';
            });

            renderSuperAdminServers();
        }
    } catch (error) {
        appendSuperAdminLog(`加载服务器列表失败: ${error.message}`, 'error');
    }
}

function switchSuperAdminTab(tab) {
    // 1. Update Sidebar Buttons
    document.querySelectorAll('.super-admin-sidebar .sidebar-btn').forEach(btn => btn.classList.remove('active'));
    const activeBtn = document.querySelector(`.super-admin-sidebar .sidebar-btn[onclick*="'${tab}'"]`);
    if (activeBtn) activeBtn.classList.add('active');

    // 2. Hide All Main Sections
    const sections = [
        'superAdminServersSection',
        'superAdminUserSection',
        'superAdminRechargeSection',
        'superAdminDetailSection',
        'superAdminRatesSection',
        'superAdminLogsSection'
    ];
    sections.forEach(id => {
        const el = document.getElementById(id);
        if (el) el.style.display = 'none';
    });

    // 3. Show Target Section & logic
    if (tab === 'servers' || tab === 'default') {
        const el = document.getElementById('superAdminServersSection');
        if (el) el.style.display = 'block';
        if (typeof loadSuperAdminServers === 'function') loadSuperAdminServers();
        // Also show radar
        const radar = document.querySelector('.servers-radar-section');
        if (radar) radar.style.display = 'block';
    }
    else if (tab === 'users') {
        const el = document.getElementById('superAdminUserSection');
        if (el) el.style.display = 'block';
        // 不自动加载数据，等待用户点击按钮
    }
    else if (tab === 'recharge') {
        const el = document.getElementById('superAdminRechargeSection');
        if (el) el.style.display = 'block';
    }
    else if (tab === 'rates') {
        const el = document.getElementById('superAdminRatesSection');
        if (el) el.style.display = 'block';
        // 加载全局费率
        saLoadGlobalRates();
    }
    else if (tab === 'logs') {
        const el = document.getElementById('superAdminLogsSection');
        if (el) el.style.display = 'block';
        const logContent = document.getElementById('superAdminLogContent');
        if (logContent) logContent.innerHTML = '';
        document.querySelectorAll('.log-type-btn').forEach(btn => btn.style.opacity = '0.6');
        window.currentLogType = null;
    }
}

function renderSuperAdminServers() {
    const container = document.getElementById('superAdminServersList');
    if (!container) return;

    container.innerHTML = '';

    if (superAdminServers.length === 0) {
        container.innerHTML = '<div style="padding: 20px; text-align: center; color: #999; grid-column: 1 / -1;">暂无在线服务器</div>';
        return;
    }

    superAdminServers.forEach(server => {
        const btn = document.createElement('button');
        // Use reuse 'server-button' class if available, or 'super-admin-server-btn' with updated styles?
        // The prompt asked for "radar-bot style". Existing 'server-button' class (lines 8000+) has all the radar CSS.
        // Let's us 'server-button' and override size if needed, or rely on grid.
        btn.className = 'server-button connected super-admin-btn';

        const serverId = server.server_id || server.server_name || 'Unknown';
        const portMatch = (server.url || '').match(/:(\d+)/);
        const port = portMatch ? portMatch[1] : (server.port || serverId.match(/\d+/)?.[0] || '?');

        // Add 'selected' class if matched
        if (currentSuperAdminServerId === serverId) {
            btn.classList.add('selected');
        }

        // Radar Bot HTML
        const botHTML = SERVER_BOT_HTML;


        btn.innerHTML = `
            ${botHTML}
            <div class="server-button-name" style="position: absolute; bottom: -15px; left: 50%; transform: translateX(-50%); font-size: 11px; color: #2d3436; white-space: nowrap; pointer-events: none; z-index: 100;">${serverId}</div>
            <div class="server-tooltip">
                <div style="font-weight: bold; margin-bottom: 4px;">${serverId}</div>
                <div style="font-size: 11px; opacity: 0.9;">${server.url || ''}</div>
                <div style="font-size: 11px; color: #00ff88; margin-top: 4px;">ID: ${serverId}</div>
                ${server.bound_manager ? `<div style="font-size: 11px; color: #ff9800; margin-top: 2px;">Assigned to: ${server.bound_manager}</div>` : ''}
            </div>
        `;

        btn.onclick = () => {
            // Update selection UI
            document.querySelectorAll('.super-admin-btn').forEach(b => b.classList.remove('selected'));
            btn.classList.add('selected');
            selectSuperAdminServer(serverId);
        };
        container.appendChild(btn);
    });

    // Init animations if needed (initRadarBots usually runs global loop, checking dom? or needs call?)
    // The existing code called `setTimeout(initRadarBots, 50);` so we keep that.
    if (typeof initRadarBots === 'function') {
        setTimeout(initRadarBots, 50);
    }
}

async function selectSuperAdminServer(serverId) {
    currentSuperAdminServerId = serverId;

    // 更新按钮状态
    document.querySelectorAll('.super-admin-server-btn').forEach(btn => {
        btn.classList.remove('active');
        if (btn.textContent === serverId) {
            btn.classList.add('active');
        }
    });

    // 获取服务器详细信息
    try {
        const response = await fetch(`${API_BASE_URL}/super-admin/worker/${encodeURIComponent(serverId)}/info`, {
            method: 'GET',
            headers: { 'Content-Type': 'application/json' }
        });

        if (response.ok) {
            const data = await response.json();
            if (data.success) {
                displaySuperAdminServerInfo(data.info);
                const detailSection = document.getElementById('superAdminDetailSection');
                if (detailSection) {
                    detailSection.style.display = 'flex';
                }
            } else {
                appendSuperAdminLog(`获取服务器信息失败: ${data.message}`, 'error');
            }
        } else {
            // 如果API接口不存在，从本地数据获取
            const server = superAdminServers.find(s => (s.server_id || s.server_name) === serverId);
            if (server) {
                displaySuperAdminServerInfoFromData(server);
                const detailSection = document.getElementById('superAdminDetailSection');
                if (detailSection) {
                    detailSection.style.display = 'flex';
                }
            }
        }
    } catch (error) {
        // 如果API接口不存在，从本地数据获取
        const server = superAdminServers.find(s => (s.server_id || s.server_name) === serverId);
        if (server) {
            displaySuperAdminServerInfoFromData(server);
            const detailSection = document.getElementById('superAdminDetailSection');
            if (detailSection) {
                detailSection.style.display = 'flex';
            }
        } else {
            appendSuperAdminLog(`获取服务器信息失败: ${error.message}`, 'error');
        }
    }
}
//#endregion
//#region 超级管理员服务器详情展示
function displaySuperAdminServerInfo(info) {
    const serverIdEl = document.getElementById('opPanelServerId');
    const numberEl = document.getElementById('superAdminNumber');
    const emailEl = document.getElementById('superAdminEmail');
    const portEl = document.getElementById('superAdminPort');
    const apiEl = document.getElementById('superAdminApi');
    const statusBtn = document.getElementById('superAdminServerStatusBtn');

    if (serverIdEl) serverIdEl.textContent = 'ID: ' + (info.server_id || info.server_name || '-');
    if (numberEl) numberEl.textContent = (info.meta && info.meta.phone) || '-';
    if (emailEl) emailEl.textContent = (info.meta && info.meta.email) || '-';
    if (portEl) portEl.textContent = info.port || '-';
    if (apiEl) apiEl.textContent = info.api_url || '-';

    if (statusBtn) {
        if (info.status === 'connected' || info.status === 'available') {
            statusBtn.textContent = 'Stop Server';
            statusBtn.classList.remove('primary');
            statusBtn.classList.add('danger', 'running');
        } else {
            statusBtn.textContent = 'Start Server';
            statusBtn.classList.remove('danger', 'running');
            statusBtn.classList.add('primary');
        }
    }
}

function displaySuperAdminServerInfoFromData(server) {
    const meta = server.meta || {};
    const serverIdEl = document.getElementById('opPanelServerId');
    const numberEl = document.getElementById('superAdminNumber');
    const emailEl = document.getElementById('superAdminEmail');
    const portEl = document.getElementById('superAdminPort');
    const apiEl = document.getElementById('superAdminApi');
    const statusBtn = document.getElementById('superAdminServerStatusBtn');

    if (serverIdEl) serverIdEl.textContent = 'ID: ' + (server.server_id || server.server_name || '-');
    if (numberEl) numberEl.textContent = meta.phone || '-';
    if (emailEl) emailEl.textContent = meta.email || '-';
    if (portEl) portEl.textContent = server.port || '-';
    if (apiEl) apiEl.textContent = server.server_url || '-';

    if (statusBtn) {
        const status = (server.status || '').toLowerCase();
        if (status === 'connected' || status === 'available') {
            statusBtn.textContent = 'Stop Server';
            statusBtn.classList.remove('primary');
            statusBtn.classList.add('danger', 'running');
        } else {
            statusBtn.textContent = 'Start Server';
            statusBtn.classList.remove('danger', 'running');
            statusBtn.classList.add('primary');
        }
    }
}
//#endregion
//#region 超级管理员远程指令控制
async function toggleSuperAdminServer() {
    if (!currentSuperAdminServerId) {
        appendSuperAdminLog('请先选择服务器', 'warning');
        return;
    }

    const statusBtn = document.getElementById('superAdminServerStatusBtn');
    const isRunning = statusBtn && statusBtn.classList.contains('running');
    const action = isRunning ? 'stop_server' : 'start_server';

    await sendSuperAdminCommand(action);
}

async function sendSuperAdminCommand(action, params = {}) {
    if (!currentSuperAdminServerId) {
        appendSuperAdminLog('请先选择服务器', 'warning');
        return;
    }

    appendSuperAdminLog(`执行命令: ${action}...`, 'info');

    try {
        const response = await fetch(`${API_BASE_URL}/super-admin/worker/${encodeURIComponent(currentSuperAdminServerId)}/control`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action, params })
        });

        const data = await response.json();
        if (data.success) {
            appendSuperAdminLog(`命令执行成功: ${action}`, 'success');
            if (data.logs && Array.isArray(data.logs)) {
                data.logs.forEach(log => {
                    appendSuperAdminLog(log.message || log, log.type || 'info');
                });
            }
        } else {
            appendSuperAdminLog(`命令执行失败: ${data.message || '未知错误'}`, 'error');
        }
    } catch (error) {
        appendSuperAdminLog(`发送命令失败: ${error.message}`, 'error');
    }
}
//#endregion
//#region 超级管理员日志系统
async function loadHistoryLogs() {
    const logContent = document.getElementById('superAdminLogContent');
    if (!logContent) {
        appendSuperAdminLog('日志容器未找到', 'error');
        return;
    }
    
    // 清空现有日志
    logContent.innerHTML = '';
    appendSuperAdminLog('正在加载历史日志...', 'info');

    try {
        // 🔒 超级管理员页面：只使用super_admin_token，不允许降级到低权限token
        const token = sessionStorage.getItem('super_admin_token') || '';
        if (!token) {
            appendSuperAdminLog('未登录或会话已过期，请重新输入密码', 'error');
            return;
        }
        const response = await fetch(`${API_BASE_URL}/admin/logs?limit=100`, {
            headers: { 'Authorization': 'Bearer ' + token }
        });
        const data = await response.json();
        if (data.ok && data.logs) {
            // 清空并加载历史日志
            logContent.innerHTML = '';
            data.logs.reverse().forEach(log => {
                let type = 'info';
                if (log.level === 'WARN') type = 'warning';
                if (log.level === 'ERROR') type = 'error';
                const ts = log.ts ? new Date(log.ts).toLocaleTimeString('zh-CN') : '';

                const logEntry = document.createElement('div');
                logEntry.className = `log-line ${type}`;
                logEntry.textContent = `[${ts}] [${log.module || 'SYSTEM'}] ${log.message || ''}`;
                logContent.appendChild(logEntry);
            });
            logContent.scrollTop = logContent.scrollHeight;
            appendSuperAdminLog(`历史日志加载完毕 (共 ${data.logs.length} 条)`, 'success');
        } else {
            appendSuperAdminLog('获取历史日志失败: ' + (data.message || data.error || 'unknown'), 'error');
        }
    } catch (e) {
        appendSuperAdminLog('网络错误: ' + e.message, 'error');
        console.error('加载历史日志失败:', e);
    }
}

let currentLogType = null;

// 确保函数在全局作用域中可用
async function switchLogType(type) {
    currentLogType = type;
    const logContent = document.getElementById('superAdminLogContent');
    if (!logContent) return;
    
    document.querySelectorAll('.log-type-btn').forEach(btn => {
        btn.style.opacity = '0.6';
        btn.style.fontWeight = 'normal';
    });
    
    const btnMap = {
        'html': 'btnLogHTML',
        'api': 'btnLogAPI',
        'worker': 'btnLogWorker',
        'record': 'btnLogRecord'
    };
    
    const activeBtn = document.getElementById(btnMap[type]);
    if (activeBtn) {
        activeBtn.style.opacity = '1';
        activeBtn.style.fontWeight = 'bold';
    }
    
    logContent.innerHTML = '';
    await loadLogs(type);
}

// 确保函数暴露到全局作用域
if (typeof window !== 'undefined') {
    window.switchLogType = switchLogType;
}

async function loadLogs(type) {
    const logContent = document.getElementById('superAdminLogContent');
    if (!logContent) return;
    
    // Record按钮暂时留空
    if (type === 'record') {
        logContent.innerHTML = '<div style="padding: 20px; text-align: center; color: #999;">Record日志功能待开发</div>';
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE_URL}/admin/logs/get?type=${type}&limit=1000`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json'
            }
        });
        
        const data = await response.json();
        if (data.ok && data.logs) {
            logContent.innerHTML = '';
            data.logs.forEach(log => {
                const logEntry = document.createElement('div');
                logEntry.className = `log-line ${log.level.toLowerCase()}`;
                const timestamp = new Date(log.ts).toLocaleString('zh-CN');
                const serverInfo = log.server_id ? ` [${log.server_id}]` : '';
                logEntry.textContent = `[${timestamp}]${serverInfo} ${log.message}`;
                logContent.appendChild(logEntry);
            });
            logContent.scrollTop = logContent.scrollHeight;
        }
    } catch (e) {
        console.error('加载日志失败:', e);
    }
}

function setupSuperAdminLogControls() {
    // 已由HTML直接定义按钮，这里不需要额外设置
}

function appendSuperAdminLog(message, type = 'info') {
    // 优先使用日志面板的日志容器
    let logContent = document.getElementById('superAdminLogContent');
    // 如果日志面板不存在，使用详情面板的日志容器
    if (!logContent) {
        logContent = document.getElementById('superAdminDetailLogContent');
    }
    if (!logContent) return;

    const timestamp = new Date().toLocaleTimeString('zh-CN');
    const logEntry = document.createElement('div');
    logEntry.className = `log-line ${type}`;
    logEntry.textContent = `[${timestamp}] ${message}`;

    logContent.appendChild(logEntry);
    logContent.scrollTop = logContent.scrollHeight;
}

function handleSuperAdminResponse(msg) {
    // 检查是否是当前选中的服务器
    if (msg.server_id && msg.server_id !== currentSuperAdminServerId) {
        return; // 不是当前服务器的响应，忽略
    }

    // 显示响应消息
    if (msg.message) {
        appendSuperAdminLog(msg.message, msg.success ? 'success' : 'error');
    }

    // 显示日志
    if (msg.logs && Array.isArray(msg.logs)) {
        msg.logs.forEach(log => {
            if (typeof log === 'string') {
                appendSuperAdminLog(log, 'info');
            } else if (log.message) {
                appendSuperAdminLog(log.message, log.type || 'info');
            }
        });
    }
}
//#endregion
//#region 系统页面路由与视图切换
function handleLogout() {
    // 🔒 清除所有登录信息，包括所有token
    // 🔒 用户登录：1小时内自动登录，超过1小时需要重新输入密码
    localStorage.removeItem('user_id');
    localStorage.removeItem('auth_token');
    localStorage.removeItem('username');
    localStorage.removeItem('login_time');  // 用户登录时间
    localStorage.removeItem('admin_id');
    localStorage.removeItem('admin_token');  // Admin ID的token
    localStorage.removeItem('server_manager_token');  // 服务器管理的token
    localStorage.removeItem('super_admin_token');  // 超级管理员的token
    localStorage.removeItem('server_manager_logged_in');
    localStorage.removeItem('admin_username');

    showLoginPage();
}

function showMainApp() {
    const loginPage = document.getElementById('loginPage');
    const adminPage = document.getElementById('adminPage');
    const managerPage = document.getElementById('managerPage');
    const contentWrapper = document.querySelector('.content-wrapper');
    const mainContainer = document.querySelector('.main-container');
    const panelA = document.getElementById('panelA');
    const panelB = document.getElementById('panelB');
    const panelC = document.getElementById('panelC');
    const panelD = document.getElementById('panelD');
    const panelE = document.getElementById('panelE');

    if (loginPage) {
        loginPage.style.display = 'none';
        document.body.classList.remove('login-mode');
    }
    if (adminPage) {
        adminPage.classList.remove('show');
        adminPage.style.display = 'none';
    }
    if (managerPage) {
        managerPage.style.display = 'none';
    }

    if (contentWrapper) {
        contentWrapper.style.display = 'flex';
    }
    if (mainContainer) {
        mainContainer.style.display = 'flex';
    }

    if (panelA) {
        panelA.style.display = 'flex';
    }
    if (panelB) {
        panelB.style.display = 'none';
        panelB.classList.remove('mobile-show');
    }
    if (panelC) {
        panelC.style.display = 'none';
    }
    if (panelD) {
        panelD.style.display = 'none';
    }
    if (panelE) {
        panelE.style.display = 'none';
        panelE.classList.remove('mobile-show');
    }

    const navHomeBtn = document.getElementById('navHomeBtn');
    const navAccountBtn = document.getElementById('navAccountBtn');
    const navSendBtn = document.getElementById('navSendBtn');
    const navInboxBtn = document.getElementById('navInboxBtn');
    if (navHomeBtn) navHomeBtn.classList.add('active');
    if (navAccountBtn) navAccountBtn.classList.remove('active');
    if (navSendBtn) navSendBtn.classList.remove('active');
    if (navInboxBtn) navInboxBtn.classList.remove('active');

}

function showLoginPage() {
    const loginPage = document.getElementById('loginPage');
    const contentWrapper = document.querySelector('.content-wrapper');
    const mainContainer = document.querySelector('.main-container');
    const adminPage = document.getElementById('adminPage');
    const managerPage = document.getElementById('managerPage');

    if (loginPage) {
        loginPage.style.display = 'flex';
        document.body.classList.add('login-mode');
    }

    if (contentWrapper) contentWrapper.style.display = 'none';
    if (mainContainer) mainContainer.style.display = 'none';

    if (adminPage) {
        adminPage.classList.remove('show');
        adminPage.style.display = 'none';
    }
    if (managerPage) {
        managerPage.style.display = 'none';
    }
}

function showAdminPage() {
    document.getElementById('adminPage').classList.add('show');
    document.getElementById('loginPage').style.display = 'none';
    // 🔥 确保加载并显示服务器（立即加载，不等待）
    loadServersFromAPI().then(() => {
        // 延迟一下确保DOM已渲染
        setTimeout(() => {
            updateServerDisplay();
            // 初始化雷达机器人
            setTimeout(initRadarBots, 100);
        }, 100);
    }).catch(err => {
        console.error('加载服务器失败:', err);
        // 即使加载失败，也尝试更新显示（可能使用本地数据）
        setTimeout(() => {
            updateServerDisplay();
            setTimeout(initRadarBots, 100);
        }, 100);
    });

    // 🔥 确保 WebSocket 连接已建立（用于接收实时服务器状态更新）
    if (!activeWs || activeWs.readyState !== WebSocket.OPEN) {
        // 如果没有 WebSocket 连接，尝试连接（服务器管理页面也需要实时更新）
        // 注意：这里不传 user_id，因为服务器管理页面可能不需要用户订阅
        setTimeout(() => {
            if (typeof connectToBackendWS === 'function') {
                connectToBackendWS(true); // 传入 true 表示忽略用户订阅
            }
        }, 500);
    }

    // 🔥 定期刷新服务器列表（每30秒）
    if (window.adminPageRefreshTimer) {
        clearInterval(window.adminPageRefreshTimer);
    }
    window.adminPageRefreshTimer = setInterval(() => {
        loadServersFromAPI().then(() => {
            updateServerDisplay();
        }).catch(() => { });
    }, 30000);
}

function showLoading() {
    document.getElementById('loadingOverlay').classList.add('show');
}

function hideLoading() {
    document.getElementById('loadingOverlay').classList.remove('show');
}

//#endregion
//#region 系统初始化入口
// 🔥 防止重复初始化
let _initPageExecuted = false;

async function initPage() {
    // 🔥 防止重复执行
    if (_initPageExecuted) {
        return;
    }
    _initPageExecuted = true;

    const loginPage = document.getElementById('loginPage');
    if (loginPage && loginPage.style.display !== 'none') {
        document.body.classList.add('login-mode');
    }

    // 🔒 只处理普通用户“1小时后需重新输入账号密码”的前端门禁
    // 管理员/服务器管理/超级管理员：每次点击入口都必须弹密码框（与 token 无关），这里绝不自动放行
    const userId = localStorage.getItem('user_id');
    const authToken = localStorage.getItem('auth_token');
    const loginTime = localStorage.getItem('login_time');
    
    // 🔥 普通用户：检查登录时间是否超过1小时
    const SESSION_TIMEOUT = 60 * 60 * 1000; // 1小时
    if (loginTime) {
        const timeSinceLogin = Date.now() - parseInt(loginTime);
        if (timeSinceLogin > SESSION_TIMEOUT) {
            // 超过1小时：只清“登录时间”，强制重新输入账号密码；token 不删除
            localStorage.removeItem('login_time');
            showLoginPage();
            return;
        }
    }
    
    // 🔥 普通用户：1小时内自动登录
    if (userId && authToken) {
        // 🔥 安全检查：确保 API_BASE_URL 已定义
        if (typeof API_BASE_URL === 'undefined' || !API_BASE_URL) {
            // API_BASE_URL未定义，可能是脚本加载顺序问题，显示登录页
            showLoginPage();
            return;
        }

        // 🔥 普通用户：1小时内直接进入（不校验 /verify，不删除 token）
        showMainApp();
        if (typeof window.init === 'function') {
            window.init();
        }
    } else {
        // 没有登录信息，显示登录页
        showLoginPage();
    }
}
//#endregion
//#region 雷达机器人动画逻辑
// 初始化雷达机器人到服务器按钮
function initRadarBots() {
    const buttons = document.querySelectorAll('.server-buttons-grid-new > button, .server-buttons-grid > button, .server-buttons-grid .server-button, .super-admin-servers-grid .super-admin-server-btn');
    const botHTML = SERVER_BOT_HTML;

    buttons.forEach(button => {
        if (!button.querySelector('.bot-container')) {
            button.innerHTML = botHTML + button.innerHTML;
        }
    });
}

// 使用MutationObserver监听按钮添加
function observeServerButtons() {
    const observer = new MutationObserver((mutations) => {
        mutations.forEach((mutation) => {
            mutation.addedNodes.forEach((node) => {
                if (node.nodeType === 1) {
                    if (node.classList && (node.classList.contains('server-buttons-grid-new') || node.classList.contains('server-buttons-grid') || node.classList.contains('super-admin-servers-grid'))) {
                        initRadarBots();
                    } else if (node.querySelector && (node.querySelector('.server-buttons-grid-new') || node.querySelector('.server-buttons-grid') || node.querySelector('.super-admin-servers-grid'))) {
                        initRadarBots();
                    }
                }
            });
        });
    });

    const targetNode = document.body;
    observer.observe(targetNode, {
        childList: true,
        subtree: true
    });

    // 初始执行一次
    setTimeout(initRadarBots, 100);
}
//#endregion
//#region 页面关闭前数据保存与清理
// 页面关闭前保存管理员数据
function saveManagerDataBeforeUnload() {
    if (window.currentManagerId) {
        const account = window.adminAccounts.find(a => a.id === window.currentManagerId);
        if (account) {
            account.users = window.managerUsers;
            account.userGroups = window.managerUserGroups;
            try {
                localStorage.setItem('adminAccounts', JSON.stringify(window.adminAccounts));
            } catch (error) {
                console.error('保存管理员数据失败:', error);
            }
        }
    }
}

window.addEventListener('beforeunload', () => {
    saveManagerDataBeforeUnload();
    stopServerPolling();
    stopInboxPolling();
    stopAllTaskPolling();
});
//#endregion
//#region 启动时Token校验与自动登录
if (document.readyState === 'loading') {
    // 🔥 检查Token是否过期（7天有效期）
    async function checkTokenExpiry() {
        // 🔒 用户登录：1小时内自动登录，超过1小时需要重新输入密码
        const loginTime = localStorage.getItem('login_time');
        
        // 检查登录时间是否超过1小时
        const SESSION_TIMEOUT = 60 * 60 * 1000; // 1小时
        if (loginTime) {
            const timeSinceLogin = Date.now() - parseInt(loginTime);
            if (timeSinceLogin > SESSION_TIMEOUT) {
                // 超过1小时：只清“登录时间”，强制重新输入账号密码；token 不删除
                localStorage.removeItem('login_time');
                return;
            }
        }
        // 注意：这里不做 /verify，不做任何 token 删除
    }

    document.addEventListener('DOMContentLoaded', async () => {
        // 🔥 防止重复初始化：检查是否已经执行过
        if (_initPageExecuted) {
            return;
        }
        // 页面加载时检查Token是否过期（initPage内部会验证，这里只做清理）
        checkTokenExpiry();
        // initPage现在会先验证token再决定是否登录
        await initPage();
        observeServerButtons();
    });
} else {
    // 🔥 防止重复初始化：检查是否已经执行过
    if (!_initPageExecuted) {
        initPage();
        observeServerButtons();
    }
}
//#endregion
//#region 充值功能模块 (待开发)
// 🔥 充值按钮点击事件 - 暂时留空,以后开发
function handleRecharge() {
    console.log('充值功能 - 待开发');
    // TODO: 实现充值功能
}
//#endregion
//#region 服务器时间同步显示
function updateServerTime() {
    const now = new Date();
    // 使用芝加哥时区 (America/Chicago)
    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, '0');
    const day = String(now.getDate()).padStart(2, '0');
    const hours = String(now.getHours()).padStart(2, '0');
    const minutes = String(now.getMinutes()).padStart(2, '0');

    const display = `${year}/${month}/${day} ${hours}:${minutes}`;

    // 更新服务器管理页面时间（新样式：分别更新日期和时间）
    const serverTimeDateEl = document.getElementById('serverTimeDate');
    const serverTimeClockEl = document.getElementById('serverTimeClock');
    if (serverTimeDateEl) serverTimeDateEl.textContent = `${year}/${month}/${day}`;
    if (serverTimeClockEl) serverTimeClockEl.textContent = `${hours}:${minutes}`;
    
    // 更新管理员页面时间（新样式：分别更新日期和时间）
    const managerTimeDateEl = document.getElementById('managerTimeDate');
    const managerTimeClockEl = document.getElementById('managerTimeClock');
    if (managerTimeDateEl) managerTimeDateEl.textContent = `${year}/${month}/${day}`;
    if (managerTimeClockEl) managerTimeClockEl.textContent = `${hours}:${minutes}`;
    
    // 更新超级管理员面板时间
    const superAdminTimeDateEl = document.getElementById('superAdminTimeDate');
    const superAdminTimeClockEl = document.getElementById('superAdminTimeClock');
    if (superAdminTimeDateEl) superAdminTimeDateEl.textContent = `${year}/${month}/${day}`;
    if (superAdminTimeClockEl) superAdminTimeClockEl.textContent = `${hours}:${minutes}`;
    
    // 更新主页面时间
    const mainPageTimeDateEl = document.getElementById('mainPageTimeDate');
    const mainPageTimeClockEl = document.getElementById('mainPageTimeClock');
    if (mainPageTimeDateEl) mainPageTimeDateEl.textContent = `${year}/${month}/${day}`;
    if (mainPageTimeClockEl) mainPageTimeClockEl.textContent = `${hours}:${minutes}`;
}

// 每秒更新服务器时间
setInterval(updateServerTime, 1000);
updateServerTime(); // 立即执行一次
//#endregion
//#region 超级管理员费率配置管理
// Global state to track manual edits (to stop auto-sync)
const saManualEdits = {
    global: { recv: false, fail: false, private: false },
    sales: { recv: false, fail: false, private: false },
    user: { recv: false, fail: false, private: false }
};

// 提前声明函数，确保在DOMContentLoaded之前就可用
function saLoadAllSettings() {
    // 🔥 修复：只在超级管理员面板显示时才加载设置
    const superAdminPanel = document.getElementById('superAdminPanel');
    const superAdminToken = sessionStorage.getItem('super_admin_token');
    
    // 只有在超级管理员面板显示且已登录时才加载
    if (superAdminPanel && superAdminPanel.style.display !== 'none' && superAdminToken) {
        if (typeof saLoadGlobalRates === 'function') {
            saLoadGlobalRates();
        } else {
            console.warn('saLoadGlobalRates 函数未定义，延迟加载...');
            setTimeout(() => {
                if (typeof saLoadGlobalRates === 'function') {
                    saLoadGlobalRates();
                }
            }, 100);
        }
    }
}

document.addEventListener('DOMContentLoaded', () => {
    // Initial Load
    if (typeof saLoadAllSettings === 'function') {
        saLoadAllSettings();
    }
    if (typeof saBindAutoSyncEvents === 'function') {
        saBindAutoSyncEvents();
    }
});
// --- Auto Sync Logic ---
function saBindAutoSyncEvents() {
    const bindSync = (prefix, type) => {
        const sendInput = document.getElementById(`${prefix}Send`);
        const recvInput = document.getElementById(`${prefix}Recv`);
        const failInput = document.getElementById(`${prefix}Fail`);
        // Private input intentionally ignored for sync
        if (!sendInput) return;
        // When "Send" changes -> ALWAYS Force Sync Recv and Fail
        sendInput.addEventListener('input', () => {
            const val = parseFloat(sendInput.value);
            if (isNaN(val)) return;
            // Recv = Send
            if (recvInput) {
                recvInput.value = val;
            }
            // Fail = 1/3 of Send
            if (failInput) {
                failInput.value = (val / 3).toFixed(4);
            }
        });
        // Manual edits to Recv/Fail/Private just happen naturally 
        // and do not need any special logic to "block" future syncs.
    };
    bindSync('saGlobal', 'global');
    bindSync('saSales', 'sales');
}
// --- 1. 全局费率 (Global Rates) ---
// --- 1. 全局费率 (Global Rates) ---
async function saLoadGlobalRates() {
    try {
        // 🔒 获取超级管理员 token
        const superAdminToken = sessionStorage.getItem('super_admin_token');
        if (!superAdminToken) {
            // 🔥 静默处理：普通用户不应该看到这个错误，只在超级管理员面板中显示提示
            const superAdminPanel = document.getElementById('superAdminPanel');
            if (superAdminPanel && superAdminPanel.style.display !== 'none') {
                // 只有在超级管理员面板显示时才提示
                appendSuperAdminLog('未找到超级管理员token，请重新登录', 'error');
            }
            return;
        }
        
        const res = await fetch(`${API_BASE_URL}/admin/rates/global`, {
            headers: {
                'Authorization': `Bearer ${superAdminToken}`
            }
        });
        const data = await res.json();
        console.log("Global Rates:", data);

        if (data.success && data.rates) {
            const r = data.rates;
            // Update Inputs
            if (document.getElementById('saGlobalSend')) document.getElementById('saGlobalSend').value = r.send || '';
            if (document.getElementById('saGlobalRecv')) document.getElementById('saGlobalRecv').value = r.recv || '';
            if (document.getElementById('saGlobalFail')) document.getElementById('saGlobalFail').value = r.fail || '';
            if (document.getElementById('saGlobalPrivate')) document.getElementById('saGlobalPrivate').value = r.private || '';

            // Update Display Spans
            if (document.getElementById('saGlobalDispSend')) document.getElementById('saGlobalDispSend').textContent = r.send || '-';
            if (document.getElementById('saGlobalDispRecv')) document.getElementById('saGlobalDispRecv').textContent = r.recv || '-';
            if (document.getElementById('saGlobalDispFail')) document.getElementById('saGlobalDispFail').textContent = r.fail || '-';
            if (document.getElementById('saGlobalDispPrivate')) document.getElementById('saGlobalDispPrivate').textContent = r.private || '-';

            // 顺便保存到 localStorage 供非异步场景快速读取（如列表显示）
            localStorage.setItem('saGlobalSend', r.send || '0.00');
            localStorage.setItem('sa_rates_global', JSON.stringify(r));
        }
    } catch (e) {
        console.error("加载全局费率失败:", e);
        // 失败时尝试读取本地缓存
        const cached = localStorage.getItem('sa_rates_global');
        if (cached) {
            try {
                const r = JSON.parse(cached);
                if (document.getElementById('saGlobalDispSend')) document.getElementById('saGlobalDispSend').textContent = r.send || '-';
                if (document.getElementById('saGlobalSend')) document.getElementById('saGlobalSend').value = r.send || '';
            } catch (err) { }
        }
    }
}

function saShowGlobalEdit() {
    document.getElementById('saGlobalDisplay').style.display = 'none';
    document.getElementById('saGlobalEdit').style.display = 'flex';
}

async function saSaveGlobal() {
    const rates = {
        send: document.getElementById('saGlobalSend').value,
        recv: document.getElementById('saGlobalRecv').value,
        fail: document.getElementById('saGlobalFail').value,
        private: document.getElementById('saGlobalPrivate').value
    };

    try {
        // 🔒 获取超级管理员 token
        const superAdminToken = sessionStorage.getItem('super_admin_token');
        if (!superAdminToken) {
            await customAlert('❌ 未找到超级管理员token，请重新登录');
            return;
        }
        
        const res = await fetch(`${API_BASE_URL}/admin/rates/global`, {
            method: 'POST',
            headers: { 
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${superAdminToken}`
            },
            body: JSON.stringify({ rates })
        });
        const data = await res.json();
        if (data.success) {
            await customAlert('✅ 全局费率已保存 (Global Rates Saved)');
            await saLoadGlobalRates(); // 刷新数据

            // Switch back to display mode
            document.getElementById('saGlobalEdit').style.display = 'none';
            document.getElementById('saGlobalDisplay').style.display = 'flex';
        } else {
            await customAlert('❌ 保存失败: ' + (data.message || '未知错误'));
        }
    } catch (e) {
        await customAlert('❌ 网络错误: ' + e.message);
    }
}

function saResetGlobal() {
    document.getElementById('saGlobalSend').value = '';
    document.getElementById('saGlobalRecv').value = '';
    document.getElementById('saGlobalFail').value = '';
    document.getElementById('saGlobalPrivate').value = '';
    // Don't hide, just clear inputs
}

function saCancelGlobal() {
    saLoadGlobalRates(); // Reload to reset values
    document.getElementById('saGlobalEdit').style.display = 'none';
    document.getElementById('saGlobalDisplay').style.display = 'flex';
}

// --- 2. 管理员费率范围设置 (Admin Rate Range) ---
async function saVerifySalesperson() {
    const input = document.getElementById('saSalesSearchUser');
    const errorBox = document.getElementById('saSalesError');
    const settingArea = document.getElementById('saSalesSettingArea');

    if (!input || !input.value.trim()) {
        errorBox.style.display = 'block';
        errorBox.textContent = '⚠ 请输入管理员ID';
        settingArea.style.display = 'none';
        return;
    }

    const adminId = input.value.trim();
    
    try {
        // 🔒 获取超级管理员 token
        const superAdminToken = sessionStorage.getItem('super_admin_token');
        if (!superAdminToken) {
            errorBox.style.display = 'block';
            errorBox.textContent = '❌ 未找到超级管理员token，请重新登录';
            settingArea.style.display = 'none';
            return;
        }
        
        // 获取管理员费率范围
        const res = await fetch(`${API_BASE_URL}/admin/rates/admin-range?admin_id=${adminId}`, {
            headers: {
                'Authorization': `Bearer ${superAdminToken}`
            }
        });
        const data = await res.json();
        
        if (data.success) {
            errorBox.style.display = 'none';
            settingArea.style.display = 'flex';
            
            // 加载已保存的费率范围
            if (data.rate_range) {
                document.getElementById('saSalesRangeMin').value = data.rate_range.min || '';
                document.getElementById('saSalesRangeMax').value = data.rate_range.max || '';
            } else {
                document.getElementById('saSalesRangeMin').value = '';
                document.getElementById('saSalesRangeMax').value = '';
            }
        } else {
            errorBox.style.display = 'block';
            errorBox.textContent = '⚠ ' + (data.message || '管理员不存在或查询失败');
            settingArea.style.display = 'none';
        }
    } catch (e) {
        errorBox.style.display = 'block';
        errorBox.textContent = '⚠ 网络错误: ' + e.message;
        settingArea.style.display = 'none';
    }
}

async function saSaveSales() {
    const adminId = document.getElementById('saSalesSearchUser').value.trim();
    if (!adminId) return;
    
    const minRate = parseFloat(document.getElementById('saSalesRangeMin').value);
    const maxRate = parseFloat(document.getElementById('saSalesRangeMax').value);
    
    if (isNaN(minRate) || isNaN(maxRate)) {
        await customAlert('❌ 请输入有效的费率范围（数字）');
        return;
    }
    
    if (minRate < 0.0001) {
        await customAlert('❌ 最小费率不能小于0.0001');
        return;
    }
    
    if (maxRate < minRate) {
        await customAlert('❌ 最大费率不能小于最小费率');
        return;
    }
    
    try {
        // 🔒 获取超级管理员 token
        const superAdminToken = sessionStorage.getItem('super_admin_token');
        if (!superAdminToken) {
            await customAlert('❌ 未找到超级管理员token，请重新登录');
            return;
        }
        
        const res = await fetch(`${API_BASE_URL}/admin/rates/admin-range`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${superAdminToken}`
            },
            body: JSON.stringify({
                admin_id: adminId,
                rate_range: { min: minRate, max: maxRate }
            })
        });
        
        const data = await res.json();
        if (data.success) {
            await customAlert(`✅ 管理员 [${adminId}] 费率范围已保存`);
            document.getElementById('saSalesSettingArea').style.display = 'none';
            document.getElementById('saSalesSearchUser').value = '';
            // 刷新列表
            saLoadAdminList();
        } else {
            await customAlert('❌ 保存失败: ' + (data.message || '未知错误'));
        }
    } catch (e) {
        await customAlert('❌ 网络错误: ' + e.message);
    }
}

function saResetSales() {
    document.getElementById('saSalesRangeMin').value = '';
    document.getElementById('saSalesRangeMax').value = '';
}

function saCancelSales() {
    document.getElementById('saSalesSettingArea').style.display = 'none';
    document.getElementById('saSalesSearchUser').value = '';
    saResetSales();
}

// 加载管理员列表（显示费率范围）
async function saLoadAdminList() {
    // TODO: 实现加载管理员列表并显示费率范围的功能
    // 这里可以调用管理员列表API，然后显示每个管理员的费率范围
}

// --- 3. 指定用户费率 (Target User Rates) ---
async function saVerifyUser() {
    const input = document.getElementById('saUserSearchName');
    const errorBox = document.getElementById('saUserError');
    const settingArea = document.getElementById('saUserSettingArea');

    if (!input || !input.value.trim()) {
        errorBox.style.display = 'block';
        errorBox.textContent = '⚠ 请输入用户ID';
        settingArea.style.display = 'none';
        return;
    }

    const userId = input.value.trim();

    // 调用 API 获取该用户的现有费率
    try {
        errorBox.style.display = 'none';
        settingArea.style.display = 'block';

        // 暂时置空，让管理员填。
        // 理想情况是先 fetch user rates 回显，但目前接口设计是 set 为主
        document.getElementById('saUserSend').value = '';
        document.getElementById('saUserRecv').value = '';
        document.getElementById('saUserFail').value = '';
        document.getElementById('saUserPrivate').value = '';

    } catch (e) {
        errorBox.style.display = 'block';
        errorBox.textContent = '⚠ 用户不存在或查询失败';
        settingArea.style.display = 'none';
    }
}

async function saSaveUser() {
    const userId = document.getElementById('saUserSearchName').value.trim();
    if (!userId) return;

    const rates = {
        send: document.getElementById('saUserSend').value,
        recv: document.getElementById('saUserRecv').value,
        fail: document.getElementById('saUserFail').value,
        private: document.getElementById('saUserPrivate').value
    };

    // 过滤空值
    const cleanRates = {};
    if (rates.send) cleanRates.send = rates.send;
    if (rates.recv) cleanRates.recv = rates.recv;
    if (rates.fail) cleanRates.fail = rates.fail;
    if (rates.private) cleanRates.private = rates.private;

    // 如果全空，询问是否清除
    if (Object.keys(cleanRates).length === 0) {
        if (!confirm("未输入任何费率，这将清除该用户的自定义费率设置（恢复使用全局费率）。确定吗？")) return;
    }

    try {
        // 🔒 获取超级管理员 token
        const superAdminToken = sessionStorage.getItem('super_admin_token');
        if (!superAdminToken) {
            await customAlert('❌ 未找到超级管理员token，请重新登录');
            return;
        }
        
        const res = await fetch(`${API_BASE_URL}/admin/rates/user`, {
            method: 'POST',
            headers: { 
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${superAdminToken}`
            },
            body: JSON.stringify({
                user_id: userId,
                rates: Object.keys(cleanRates).length > 0 ? cleanRates : null
            })
        });
        const data = await res.json();
        if (data.success) {
            await customAlert(`✅ 用户 [${userId}] 费率已保存`);
            document.getElementById('saUserSettingArea').style.display = 'none';
            document.getElementById('saUserSearchName').value = '';
            // 刷新列表（如果有显示列表的话）
        } else {
            await customAlert('❌ 保存失败: ' + (data.message || '未知错误'));
        }
    } catch (e) {
        await customAlert('❌ 网络错误: ' + e.message);
    }
}

function saResetUser() {
    document.getElementById('saUserSend').value = '';
    document.getElementById('saUserRecv').value = '';
    document.getElementById('saUserFail').value = '';
    document.getElementById('saUserPrivate').value = '';
}

function saCancelUser() {
    document.getElementById('saUserSettingArea').style.display = 'none';
    document.getElementById('saUserSearchName').value = '';
    saResetUser();
}



//#endregion

//#region 超级管理员数据管理中心逻辑

let saAllUsersData = [];
let saSelectedUserId = null;

async function saSwitchDataTab(tab) {
    // 更新按钮状态
    ['user', 'admin', 'server'].forEach(t => {
        const btn = document.getElementById('btnSaData' + t.charAt(0).toUpperCase() + t.slice(1));
        if (btn) {
            btn.classList.toggle('active', t === tab);
        }
    });

    // 更新面板显示
    document.getElementById('saDataUserPanel').style.display = tab === 'user' ? 'block' : 'none';
    document.getElementById('saDataAdminPanel').style.display = tab === 'admin' ? 'block' : 'none';
    document.getElementById('saDataServerPanel').style.display = tab === 'server' ? 'block' : 'none';

    // 加载数据
    if (tab === 'user') {
        await saLoadAllUsers();
    }
}

async function saLoadAllUsers() {
    const container = document.getElementById('saAllUserList');
    const countEl = document.getElementById('saTotalUserCount');
    if (!container) return;

    container.innerHTML = '';
    if (countEl) countEl.textContent = '0';

    try {
        // 🔒 超级管理员页面：只使用super_admin_token，不允许降级到低权限token
        const token = sessionStorage.getItem('super_admin_token') || '';
        if (!token) {
            appendSuperAdminLog('未登录或会话已过期，请重新输入密码', 'error');
            return;
        }

        const response = await fetch(`${API_BASE_URL}/admin/users/all`, {
            headers: { 'Authorization': `Bearer ${token}` }
        });

        if (!response.ok) return;

        const data = await response.json();
        if (data.success) {
            saAllUsersData = data.users || [];
            if (countEl) countEl.textContent = data.total || saAllUsersData.length || 0;
            saRenderUserList(saAllUsersData);
        }
    } catch (e) {
        console.error('加载用户数据失败:', e);
    }
}

function saToggleUserSearch() {
    const container = document.getElementById('saUserSearchContainer');
    if (container) {
        container.style.display = container.style.display === 'none' ? 'flex' : 'none';
        if (container.style.display === 'flex') {
            setTimeout(() => document.getElementById('saUserSearchInput')?.focus(), 100);
        }
    }
}

function saConfirmUserSearch() {
    const input = document.getElementById('saUserSearchInput');
    if (!input) return;
    
    const keyword = input.value.trim();
    if (!keyword) {
        saSelectedUserId = null;
        saRenderUserList(saAllUsersData);
        return;
    }
    
    const foundUser = saAllUsersData.find(u =>
        String(u.user_id).includes(keyword) || 
        (u.username && u.username.includes(keyword))
    );
    
    if (foundUser) {
        saSelectedUserId = foundUser.user_id;
        saRenderUserList(saAllUsersData);
        setTimeout(() => {
            const selectedEl = document.querySelector(`[data-user-id="${foundUser.user_id}"]`);
            if (selectedEl) {
                selectedEl.scrollIntoView({ behavior: 'smooth', block: 'center' });
            }
        }, 100);
        // 隐藏搜索框
        document.getElementById('saUserSearchContainer').style.display = 'none';
        input.value = '';
    }
}

function saRenderUserList(users) {
    const container = document.getElementById('saAllUserList');
    if (!container || !users || users.length === 0) {
        if (container) container.innerHTML = '';
        return;
    }

    container.innerHTML = users.map(u => {
        const username = u.username || '未设置';
        const uid = u.user_id;
        const fullUserId = String(uid);
        let userIdDisplay = fullUserId.startsWith('u_') ? fullUserId.substring(2) : fullUserId;
        const balance = (u.credits || 0).toFixed(2);
        const serverCount = u.server_count || 0;
        const sendRate = u.send_rate || localStorage.getItem('saGlobalSend') || '0.00';
        const escapedUserId = fullUserId.replace(/"/g, '&quot;').replace(/'/g, '&#39;');
        const isSelected = saSelectedUserId === uid;
        
        return `
        <div class="user-button sa-user-card ${isSelected ? 'selected' : ''}" data-user-id="${uid}">
            <div class="user-button-content">
                <div class="user-server-count-badge ${serverCount > 0 ? 'flash' : ''}">${serverCount}</div>
                <div class="user-button-info">
                    <div class="user-button-top">
                        <span class="user-id-text">${username}(${userIdDisplay})</span>
                        <div style="display: flex; gap: 5px;">
                            <button class="sa-user-detail-btn" onclick="saViewUserDetail('${escapedUserId}', '${username}')">详情</button>
                        </div>
                    </div>
                    <div class="user-button-stats">
                        <div class="user-stat-item">
                            <span class="user-stat-label">rate:</span>
                            <span class="user-stat-value">${sendRate}</span>
                        </div>
                        <div class="user-stat-item">
                            <span class="user-stat-label">balance:</span>
                            <span class="user-stat-value">$${balance}</span>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        `;
    }).join('');
}

// 用户详情（目前沿用原“管理”入口的行为：跳转到充值面板并自动回填/验证）
function saViewUserDetail(uid, username) {
    // 这里先保持现有流程不变，后续如果要独立“详情弹窗/详情页”，直接在这里扩展即可
    saOpenQuickRecharge(uid, username);
}

// 移除用户
function saRemoveUser(userId) {
    if (!confirm(`确定要移除用户 ${userId} 吗？`)) return;
    // TODO: 实现移除用户的API调用
    appendSuperAdminLog(`移除用户功能待实现: ${userId}`, 'info');
}


// Quick helper to fill recharge modal
function saOpenQuickRecharge(uid, username) {
    switchSuperAdminTab('recharge');
    const input = document.getElementById('saRechargeUserIdInput');
    if (input) {
        input.value = username || uid;
        // Auto verify
        if (typeof saVerifyRechargeUser === 'function') {
            saVerifyRechargeUser();
        }
    }
}

async function saLoadServerStats() {
    // TODO: 实现服务器数据加载
}

let saAllAdminsData = [];

async function saLoadAllAdmins() {
    // TODO: 实现管理员数据加载
}

function saRenderAdminList(admins) {
    const container = document.getElementById('saAllAdminList');
    if (!container) return;

    if (!admins || admins.length === 0) {
        container.innerHTML = '<div style="text-align: center; color: #666; padding: 20px; width: 100%;">没有找到管理员</div>';
        // 更新统计
        if (document.getElementById('saTotalAdminCount')) document.getElementById('saTotalAdminCount').textContent = '0';
        if (document.getElementById('saOnlineAdminCount')) document.getElementById('saOnlineAdminCount').textContent = '0';
        if (document.getElementById('saManagedUsersCount')) document.getElementById('saManagedUsersCount').textContent = '0';
        if (document.getElementById('saTotalAdminPerformance')) document.getElementById('saTotalAdminPerformance').textContent = '0';
        return;
    }

    // 计算统计数据
    const totalManagedUsers = admins.reduce((sum, a) => sum + (parseInt(a.user_count) || 0), 0);
    const totalPerformance = admins.reduce((sum, a) => sum + (parseInt(a.performance) || 0), 0);
    const onlineCount = admins.filter(a => a.online === true).length;

    // 更新统计显示
    if (document.getElementById('saTotalAdminCount')) document.getElementById('saTotalAdminCount').textContent = admins.length;
    if (document.getElementById('saOnlineAdminCount')) document.getElementById('saOnlineAdminCount').textContent = onlineCount;
    if (document.getElementById('saManagedUsersCount')) document.getElementById('saManagedUsersCount').textContent = totalManagedUsers;
    if (document.getElementById('saTotalAdminPerformance')) document.getElementById('saTotalAdminPerformance').textContent = totalPerformance;

    // 渲染表格
    container.innerHTML = admins.map((a, index) => {
        const adminId = a.id || a.admin_id || 'Unknown';
        const username = a.username || adminId;
        const userCount = a.user_count || 0;
        const performance = a.performance || 0;
        const online = a.online === true;
        const created = a.created_at ? new Date(a.created_at).toLocaleDateString('zh-CN') : '-';

        return `
        <div style="display: grid; grid-template-columns: 80px 150px 120px 100px 100px 100px 100px 1fr; padding: 10px; border-bottom: 1px solid rgba(255,255,255,0.05); align-items: center; font-size: 13px; color: #ccc;">
            <div style="color: #888;">${index + 1}</div>
            <div style="font-family: monospace; color: #4facfe; font-size: 11px;">${adminId}</div>
            <div style="font-weight: bold; color: #fff; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">${username}</div>
            <div style="color: #fff;">${userCount}</div>
            <div style="color: #00ff88; font-weight: bold;">${performance}</div>
            <div style="color: ${online ? '#00ff88' : '#ff5252'};">
                ${online ? '在线' : '离线'}
            </div>
            <div style="color: #aaa; font-size: 11px;">${created}</div>
            <div style="display: flex; gap: 5px;">
                <button class="user-manage-btn" onclick="saViewAdminDetail('${adminId}')" 
                    style="background: #4facfe; font-size: 11px; padding: 4px 8px; border: none; border-radius: 4px; cursor: pointer; color: white;">
                    详情
                </button>
            </div>
        </div>
        `;
    }).join('');
}

function saFilterAdminList(keyword) {
    if (!keyword) {
        saRenderAdminList(saAllAdminsData);
        return;
    }
    const lower = keyword.toLowerCase();
    const filtered = saAllAdminsData.filter(a =>
        (a.id && a.id.toLowerCase().includes(lower)) ||
        (a.username && a.username.toLowerCase().includes(lower))
    );
    saRenderAdminList(filtered);
}

function saViewAdminDetail(adminId) {
    // 查看管理员详情
    console.log('查看管理员详情:', adminId);
    // TODO: 实现管理员详情查看功能
}

// 导出用户数据
function saExportUserData() {
    if (!saAllUsersData || saAllUsersData.length === 0) {
        appendSuperAdminLog('没有可导出的用户数据', 'warning');
        return;
    }
    
    const csv = [
        ['序号', '用户名', '用户ID', '余额', '发送量', '成功率', '注册时间'].join(','),
        ...saAllUsersData.map((u, index) => {
            const username = u.username || 'No Name';
            const uid = u.user_id;
            const balance = (u.credits || 0).toFixed(2);
            const sent = u.last_sent || 0;
            const success = u.total_success || 0;
            const fail = u.total_fail || 0;
            const total = success + fail;
            const successRate = total > 0 ? ((success / total) * 100).toFixed(1) : '0.0';
            const created = u.created_at ? new Date(u.created_at).toLocaleDateString('zh-CN') : '-';
            return [index + 1, username, uid, balance, sent, successRate + '%', created].join(',');
        })
    ].join('\n');
    
    const blob = new Blob(['\ufeff' + csv], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = `用户数据_${new Date().toISOString().split('T')[0]}.csv`;
    link.click();
    appendSuperAdminLog('用户数据导出成功', 'success');
}

// 导出管理员数据
function saExportAdminData() {
    if (!saAllAdminsData || saAllAdminsData.length === 0) {
        appendSuperAdminLog('没有可导出的管理员数据', 'warning');
        return;
    }
    
    const csv = [
        ['序号', '管理员ID', '用户名', '管理用户数', '总业绩', '在线状态', '创建时间'].join(','),
        ...saAllAdminsData.map((a, index) => {
            const adminId = a.id || a.admin_id || 'Unknown';
            const username = a.username || adminId;
            const userCount = a.user_count || 0;
            const performance = a.performance || 0;
            const online = a.online === true ? '在线' : '离线';
            const created = a.created_at ? new Date(a.created_at).toLocaleDateString('zh-CN') : '-';
            return [index + 1, adminId, username, userCount, performance, online, created].join(',');
        })
    ].join('\n');
    
    const blob = new Blob(['\ufeff' + csv], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = `管理员数据_${new Date().toISOString().split('T')[0]}.csv`;
    link.click();
    appendSuperAdminLog('管理员数据导出成功', 'success');
}


//#endregion

//#endregion 
