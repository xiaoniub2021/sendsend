
//#region 服务器管理模块（API调用、服务器连接）
let serverData = {
    connected: [],
    disconnected: []
};

try {
    const savedServerData = localStorage.getItem('serverData');
    if (savedServerData) {
        localStorage.removeItem('serverData');

    }
} catch (error) {
    console.error('清理localStorage失败:', error);
}

let adminAccounts = [];
try {
    const savedAccounts = localStorage.getItem('adminAccounts');
    if (savedAccounts) {
        adminAccounts = JSON.parse(savedAccounts);
    }
} catch (error) {
    console.error('加载管理员账号失败:', error);
}

// 统一去重（避免出现“点一次出来多个”）
function _dedupeAdminAccounts(list) {
    const map = new Map();
    (Array.isArray(list) ? list : []).forEach(a => {
        if (!a || !a.id) return;
        const key = String(a.id).trim();
        if (!key) return;
        const prev = map.get(key) || {};
        map.set(key, {
            id: key,
            password: (a.password !== undefined && a.password !== null) ? String(a.password) : (prev.password || ''),
            selectedServers: Array.isArray(a.selectedServers) ? a.selectedServers : (Array.isArray(prev.selectedServers) ? prev.selectedServers : []),
            userGroups: Array.isArray(a.userGroups) ? a.userGroups : (Array.isArray(prev.userGroups) ? prev.userGroups : undefined),
        });
    });
    return Array.from(map.values());
}
adminAccounts = _dedupeAdminAccounts(adminAccounts);

async function loadAdminAccountsFromAPI() {
    try {
        const resp = await fetch(`${API_BASE_URL}/admin/account?t=${Date.now()}`, { method: 'GET' });
        const data = await resp.json().catch(() => ({}));
        if (resp.ok && data.success && Array.isArray(data.admins)) {
            // 🔥 优先使用localStorage中的数据（保留用户删除的记录）
            const localAccounts = new Map((_dedupeAdminAccounts(adminAccounts)).map(a => [a.id, a]));

            // 获取本地已删除的ID列表
            let deletedIds = [];
            try {
                deletedIds = JSON.parse(localStorage.getItem('deletedAdminIds') || '[]');
            } catch (e) { }

            // 从API加载的数据，只添加localStorage中不存在的（避免恢复已删除的），且过滤掉已明确删除的ID
            data.admins.forEach(row => {
                const id = String((row && row.admin_id) || '').trim();
                if (!id) return;
                // 🔥 如果该ID在删除列表中，则跳过
                if (deletedIds.includes(id)) return;

                // 如果localStorage中已有，保留localStorage的版本（可能用户已删除）
                if (!localAccounts.has(id)) {
                    localAccounts.set(id, { id, password: '', selectedServers: [] });
                }
            });

            adminAccounts = _dedupeAdminAccounts(Array.from(localAccounts.values()));
            // 🔥 即时保存到localStorage（确保删除的记录不会恢复）
            try {
                localStorage.setItem('adminAccounts', JSON.stringify(adminAccounts));
            } catch {
                /* ignore */
            }
        }
    } catch (e) {
    }
}

// 自动检测本地/远程API (已移至顶部全局定义)
/*
const isLocalDev = window.location.hostname === 'localhost' ||
    window.location.hostname === '127.0.0.1' ||
    window.location.hostname === '';
const API_BASE_URL = isLocalDev
    ? window.location.origin + '/api'
    : 'https://autosender.up.railway.app/api';
*/

// 静默调试日志（已禁用）
function silentDebugLog(data) { }

// 带超时的fetch函数 (60秒超时，适应慢速API)
async function fetchWithTimeout(url, options = {}, timeout = 60000) {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeout);

    try {
        const response = await fetch(url, {
            ...options,
            signal: controller.signal
        });
        clearTimeout(timeoutId);
        return response;
    } catch (error) {
        clearTimeout(timeoutId);
        if (error.name === 'AbortError') {
            throw new Error(`请求超时 (${timeout / 1000}秒)`);
        }
        throw error;
    }
}

async function testAPIConnection() {
    try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 10000);

        const response = await fetch(`${API_BASE_URL}/servers`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json'
            },
            signal: controller.signal
        });

        clearTimeout(timeoutId);
        return { success: true, status: response.status };
    } catch (error) {
        console.error('API连接测试失败:', error);
        return {
            success: false,
            error: error.message,
            details: {
                name: error.name,
                message: error.message,
                type: error.name === 'AbortError' ? 'timeout' :
                    error.message.includes('Failed to fetch') ? 'network' :
                        error.message.includes('CORS') ? 'cors' : 'unknown'
            }
        };
    }
}

let _serversLoadedOnce = false;
let _serversLoading = false; // 🔥 防止重复请求
let _lastServersLoadTime = 0;
const SERVERS_LOAD_MIN_INTERVAL = 1000; // 🔥 最小请求间隔：1秒

async function loadServersFromAPI() {
    // 🔥 防止重复请求：如果正在加载或距离上次加载时间太短，则跳过
    const now = Date.now();
    if (_serversLoading) {
        console.log('[loadServers] 请求正在进行中，跳过重复请求');
        return;
    }
    if (now - _lastServersLoadTime < SERVERS_LOAD_MIN_INTERVAL) {
        console.log('[loadServers] 请求间隔太短，跳过重复请求');
        return;
    }

    _serversLoading = true;
    _lastServersLoadTime = now;

    try {
        // 创建带超时的 fetch controller (30秒超时)
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 30000);

        const response = await fetch(`${API_BASE_URL}/servers?t=${Date.now()}`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json'
            },
            signal: controller.signal
        });

        clearTimeout(timeoutId);

        if (!response.ok) {
            throw new Error(`API响应错误: ${response.status} ${response.statusText}`);
        }

        const data = await response.json();
        if (data.success && data.servers) {
            // console.log('加载服务器列表成功，服务器数量:', data.servers.length);

            serverData.connected = [];
            serverData.disconnected = [];

            // 使用Map确保同一个server_id只出现一次
            const serverMap = new Map();

            data.servers.forEach(s => {
                const server_id = s.server_id;
                if (!server_id) return;



                // 如果已经存在，根据状态决定保留哪个（connected优先）
                if (serverMap.has(server_id)) {
                    const existing = serverMap.get(server_id);
                    const newStatus = (s.status || '').toLowerCase();
                    // 🔥 如果新的是connected/available/ready，替换旧的
                    if (newStatus === 'connected' || newStatus === 'available' || newStatus === 'ready') {
                        if (existing.status !== 'connected' && existing.status !== 'available' && existing.status !== 'ready') {
                            serverMap.set(server_id, {
                                name: s.server_name || s.server_id,
                                url: s.server_url || '',
                                server_id: server_id,
                                status: (newStatus === 'available' || newStatus === 'ready') ? 'connected' : newStatus,
                                assigned_user_id: s.assigned_user_id || null,
                                last_seen: s.last_seen
                            });
                        }
                    }
                } else {
                    const serverItem = {
                        name: s.server_name || s.server_id,
                        url: s.server_url || '',
                        server_id: server_id,
                        status: (s.status || '').toLowerCase(),
                        assigned_user_id: s.assigned_user_id || null,
                        last_seen: s.last_seen
                    };

                    // 🔥 将 available 和 ready 状态都转换为 connected（用于显示）
                    if (serverItem.status === 'available' || serverItem.status === 'ready') {
                        serverItem.status = 'connected';
                    }

                    serverMap.set(server_id, serverItem);
                }
            });

            // 将Map转换为数组并分类
            serverMap.forEach(server => {
                // 🔥 显示所有非 disconnected 状态的服务器为 connected
                if (server.status === 'connected' || server.status === 'available' || server.status === 'ready') {
                    serverData.connected.push(server);
                } else {
                    serverData.disconnected.push(server);
                }
            });

            _serversLoadedOnce = true;

            const realNames = new Set([...serverData.connected].map(s => String(s.name || '').trim()).filter(Boolean));
            let cleaned = false;
            try {
                adminAccounts.forEach(acc => {
                    if (Array.isArray(acc.selectedServers)) {
                        const before = acc.selectedServers.length;
                        acc.selectedServers = acc.selectedServers.filter(n => realNames.has(String(n).trim()));
                        if (acc.selectedServers.length !== before) cleaned = true;
                    }
                    if (Array.isArray(acc.userGroups)) {
                        acc.userGroups.forEach(g => {
                            if (Array.isArray(g.servers)) {
                                const b = g.servers.length;
                                g.servers = g.servers.filter(n => realNames.has(String(n).trim()));
                                if (g.servers.length !== b) cleaned = true;
                            }
                        });
                    }
                });
                if (Array.isArray(managerUserGroups)) {
                    managerUserGroups.forEach(g => {
                        if (Array.isArray(g.servers)) {
                            const b = g.servers.length;
                            g.servers = g.servers.filter(n => realNames.has(String(n).trim()));
                            if (g.servers.length !== b) cleaned = true;
                        }
                    });
                }
                if (cleaned) {
                    localStorage.setItem('adminAccounts', JSON.stringify(adminAccounts));
                }
            } catch (e) {
                console.warn('清理旧服务器缓存失败:', e);
            }

            updateServerDisplay();
            // ❌ 移除此处调用，统一由 WebSocket 成功后触发
            // connectToAvailableServers();

            if (document.getElementById('adminManageServersGrid')) {
            }

        } else {
            // console.warn('API返回数据格式异常:', data);
        }
    } catch (error) {
        // 静默处理错误，不影响页面功能
        console.error('[loadServers] 加载服务器列表失败:', error);
        // 如果是超时或网络错误，可以考虑重试
        if (error.name === 'AbortError') {
            console.warn('[loadServers] 服务器列表加载超时，将使用本地模式');
        }
    } finally {
        // 🔥 重置加载状态（无论成功或失败）
        _serversLoading = false;
    }
}

let exclusivePhoneNumbers = [];
let currentSelectedPhone = null;

async function loadExclusivePhoneNumbers() {
    if (!currentUserId) {
        document.getElementById('exclusivePhoneSelector').style.display = 'none';
        return;
    }

    try {
        const response = await fetch(`${API_BASE_URL}/users/${currentUserId}/available-servers`);
        if (response.ok) {
            const data = await response.json();
            if (data.success && data.exclusive_servers && data.exclusive_servers.length > 0) {
                exclusivePhoneNumbers = [];
                for (const server of data.exclusive_servers) {
                    const phoneNumber = server.phone_number || server.server_name || server.server_id;
                    if (phoneNumber && !exclusivePhoneNumbers.find(p => p.phone === phoneNumber)) {
                        exclusivePhoneNumbers.push({
                            phone: phoneNumber,
                            server_id: server.server_id,
                            server_name: server.server_name
                        });
                    }
                }

                if (exclusivePhoneNumbers.length > 0) {
                    document.getElementById('exclusivePhoneSelector').style.display = 'block';
                    if (!currentSelectedPhone && exclusivePhoneNumbers.length > 0) {
                        currentSelectedPhone = exclusivePhoneNumbers[0].phone;
                    }
                    updateExclusivePhoneDisplay();
                } else {
                    document.getElementById('exclusivePhoneSelector').style.display = 'none';
                }
            } else {
                document.getElementById('exclusivePhoneSelector').style.display = 'none';
            }
        } else {
            document.getElementById('exclusivePhoneSelector').style.display = 'none';
        }
    } catch (error) {
        console.error('加载独享服务器电话号码失败:', error);
        document.getElementById('exclusivePhoneSelector').style.display = 'none';
    }
}

function updateExclusivePhoneDisplay() {
    const currentPhoneDisplay = document.getElementById('currentPhoneDisplay');
    const dropdown = document.getElementById('exclusivePhoneDropdown');
    const btn = document.getElementById('exclusivePhoneBtn');

    if (currentSelectedPhone && currentPhoneDisplay) {
        currentPhoneDisplay.textContent = currentSelectedPhone;
    }

    if (dropdown) {
        dropdown.innerHTML = '';
        exclusivePhoneNumbers.forEach(item => {
            const option = document.createElement('div');
            option.style.cssText = 'padding: 8px 12px; cursor: pointer; border-bottom: 1px solid #eee; transition: background 0.2s;';
            if (item.phone === currentSelectedPhone) {
                option.style.background = 'rgba(76, 175, 80, 0.2)';
                option.style.fontWeight = 'bold';
            }
            option.textContent = item.phone;
            option.onclick = () => {
                currentSelectedPhone = item.phone;
                updateExclusivePhoneDisplay();
                dropdown.style.display = 'none';
                loadInboxForPhone(item.phone);
            };
            option.onmouseenter = () => {
                if (item.phone !== currentSelectedPhone) {
                    option.style.background = 'rgba(76, 175, 80, 0.1)';
                }
            };
            option.onmouseleave = () => {
                if (item.phone !== currentSelectedPhone) {
                    option.style.background = 'transparent';
                }
            };
            dropdown.appendChild(option);
        });
    }

    if (btn) {
        if (exclusivePhoneNumbers.length === 1) {
            btn.innerHTML = `本机号码: <span id="currentPhoneDisplay">${currentSelectedPhone || '-'}</span>`;
            btn.onclick = null;
            btn.style.cursor = 'default';
        } else {
            btn.innerHTML = `本机号码: <span id="currentPhoneDisplay">${currentSelectedPhone || '-'}</span> <span style="font-size: 10px;">▼</span>`;
            btn.onclick = (e) => {
                e.stopPropagation();
                const dropdown = document.getElementById('exclusivePhoneDropdown');
                if (dropdown) {
                    const isVisible = dropdown.style.display === 'block';
                    dropdown.style.display = isVisible ? 'none' : 'block';
                }
            };
            btn.style.cursor = 'pointer';
        }
    }
}

function loadInboxForPhone(phoneNumber) {
    console.log('切换到电话号码:', phoneNumber);
}

document.addEventListener('click', (e) => {
    const selector = document.getElementById('exclusivePhoneSelector');
    const dropdown = document.getElementById('exclusivePhoneDropdown');
    if (selector && dropdown && !selector.contains(e.target)) {
        dropdown.style.display = 'none';
    }
});


function checkAuthToken() {
    const token = localStorage.getItem('auth_token');
    const loginTime = localStorage.getItem('login_time');
    
    if (!token) {
        return null;
    }
    
    if (loginTime) {
        const SESSION_TIMEOUT = 60 * 60 * 1000;
        const timeSinceLogin = Date.now() - parseInt(loginTime);
        if (timeSinceLogin > SESSION_TIMEOUT) {
            // 超过1小时：只清“登录时间”，强制重新输入账号密码；token 不删除
            localStorage.removeItem('login_time');
            if (typeof authToken !== 'undefined') {
                authToken = null;
            }
            return null;
        }
    }
    
    return token;
}

async function connectToAssignedServers() {
    const user_id = localStorage.getItem('user_id');
    if (!user_id) return;
    currentUserId = currentUserId || user_id;
    authToken = authToken || checkAuthToken();
    if (!authToken) return;
    connectToBackendWS(null);
}

let inboxPollingInterval = null;

function startInboxPolling(userId) {
}

function stopInboxPolling() {
    if (inboxPollingInterval) {
        clearInterval(inboxPollingInterval);
        inboxPollingInterval = null;
    }
}

// 兼容：旧版本曾有 stopServerPolling，这里提供空实现避免 beforeunload 报错中断
function stopServerPolling() {
    // no-op
}

async function pollInbox(userId) {
    return;
}

async function loadConversationMessages(chatId) {
    requestConversation(chatId);
}

// 🔥 显示删除服务器确认弹窗（自定义样式，不用系统UI）
async function showDeleteServerConfirm(serverName) {
    return new Promise((resolve) => {
        const modal = document.createElement('div');
        modal.className = 'custom-modal-overlay';
        modal.id = 'deleteServerConfirmModal';
        modal.style.display = 'flex';

        modal.innerHTML = `
            <div class="custom-modal-panel" style="width: 380px;">
                <div class="custom-modal-header">
                    <span class="custom-modal-title">⚠️ 删除服务器记录</span>
                    <button class="custom-modal-close" onclick="this.closest('.custom-modal-overlay').remove(); resolve(false);">×</button>
                </div>
                <div class="custom-modal-content">
                    <div class="custom-modal-message" style="text-align: center; padding: 10px 0;">
                        确定要删除服务器 <strong style="color: #ff4757;">${serverName}</strong> 的记录吗？<br>
                        <span style="font-size: 12px; color: #666; margin-top: 8px; display: block;">
                            删除历史服务器
                        </span>
                    </div>
                    <div class="custom-modal-buttons">
                        <button class="custom-modal-btn cancel" onclick="this.closest('.custom-modal-overlay').remove(); resolve(false);">取消</button>
                        <button class="custom-modal-btn confirm" onclick="this.closest('.custom-modal-overlay').remove(); resolve(true);" style="background: linear-gradient(135deg, #ff4757 0%, #ff3838 100%); color: white;">确认删除</button>
                    </div>
                </div>
            </div>
        `;

        document.body.appendChild(modal);
        setTimeout(() => modal.classList.add('show'), 10);

        // 点击遮罩层关闭
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                modal.classList.remove('show');
                setTimeout(() => {
                    modal.remove();
                    resolve(false);
                }, 150);
            }
        });

        // 处理按钮点击
        const cancelBtn = modal.querySelector('.custom-modal-btn.cancel');
        const confirmBtn = modal.querySelector('.custom-modal-btn.confirm');

        cancelBtn.onclick = () => {
            modal.classList.remove('show');
            setTimeout(() => {
                modal.remove();
                resolve(false);
            }, 150);
        };

        confirmBtn.onclick = () => {
            modal.classList.remove('show');
            setTimeout(() => {
                modal.remove();
                resolve(true);
            }, 150);
        };
    });
}

// 🔥 删除服务器记录
async function deleteServer(serverId) {
    try {
        const response = await fetch(`${API_BASE_URL}/servers/${serverId}`, {
            method: 'DELETE',
            headers: {
                'Content-Type': 'application/json'
            }
        });

        const result = await response.json();

        if (result.success) {
            await customAlert(`服务器记录已删除`);
            // 🔥 重新加载服务器列表（确保删除后立即更新）
            await loadServersFromAPI();
            // 刷新显示
            if (typeof updateServerDisplay === 'function') {
                updateServerDisplay();
            }
            // 如果通过WebSocket连接，请求最新列表
            if (typeof activeWs !== 'undefined' && activeWs && activeWs.readyState === WebSocket.OPEN) {
                activeWs.send(JSON.stringify({ action: 'get_servers' }));
            }
        } else {
            await customAlert(`删除失败: ${result.message || '未知错误'}`);
        }
    } catch (error) {
        console.error('删除服务器失败:', error);
        await customAlert(`删除失败: ${error.message}`);
    }
}

async function disconnectServer(serverId) {
    try {
        const response = await fetch(`${API_BASE_URL}/servers/${serverId}/disconnect`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' }
        });

        if (response.ok) {
            const data = await response.json();
            if (data.success) {
                showMessage('服务器已断开连接', 'success');
                await loadServersFromAPI();
            } else {
                await customAlert('断开连接失败: ' + (data.message || '未知错误'));
            }
        } else {
            const error = await response.json();
            await customAlert('断开连接失败: ' + (error.message || '网络错误'));
        }
    } catch (error) {
        await customAlert('断开连接失败: ' + error.message);
    }
}

if (localStorage.getItem('user_id')) {
    connectToAssignedServers();
}

let updateServerDisplayTimer = null;

// 根据按钮的类名获取状态文字
function getServerStatusText(button) {
    if (button.classList.contains('connected')) {
        if (button.classList.contains('private') || button.classList.contains('active')) {
            return '状态: 正在使用';
        } else if (button.classList.contains('selected')) {
            return '状态: 已选中';
        } else {
            return '状态: 已连接';
        }
    } else if (button.classList.contains('disconnected')) {
        return '状态: 断开连接';
    } else if (button.classList.contains('selected')) {
        return '状态: 已选中';
    } else if (button.classList.contains('private') || button.classList.contains('active')) {
        return '状态: 正在使用';
    }
    return '状态: 未知';
}

function updateServerDisplay() {
    if (updateServerDisplayTimer) {
        clearTimeout(updateServerDisplayTimer);
    }
    updateServerDisplayTimer = setTimeout(() => {
        const connectedContainer = document.getElementById('connectedServers');
        const disconnectedContainer = document.getElementById('disconnectedServers');
        if (!connectedContainer && !disconnectedContainer) {
            console.warn('updateServerDisplay: 找不到服务器容器元素');
            return;
        }

        // 确保 serverData 已初始化
        if (!serverData) {
            serverData = { connected: [], disconnected: [] };
        }
        if (!Array.isArray(serverData.connected)) {
            serverData.connected = [];
        }
        if (!Array.isArray(serverData.disconnected)) {
            serverData.disconnected = [];
        }

        const serverExclusiveMap = new Map();
        [...serverData.connected, ...(serverData.disconnected || [])].forEach(server => {
            if (server.assigned_user_id) {
                serverExclusiveMap.set(server.name, server.assigned_user_id);
            }
        });
        adminAccounts.forEach(account => {
            if (account.userGroups) {
                account.userGroups.forEach(group => {
                    if (group.servers) {
                        group.servers.forEach(serverName => {
                            if (!serverExclusiveMap.has(serverName)) {
                                serverExclusiveMap.set(serverName, group.userId);
                            }
                        });
                    }
                });
            }
        });

        const getExclusiveInfo = (serverName) => {
            const userId = serverExclusiveMap.get(serverName);
            if (userId) {
                // 提取用户ID（去掉前缀，只保留4位数字）
                const userIdOnly = userId.startsWith('u_') ? userId.substring(2) : userId;
                return { isExclusive: true, displayName: serverName, userIdDisplay: userIdOnly };
            }
            return { isExclusive: false, displayName: serverName };
        };
        if (connectedContainer) {
            const connectedFragment = document.createDocumentFragment();
            connectedContainer.innerHTML = '';
            serverData.connected.forEach(server => {
                const btn = document.createElement('button');
                btn.className = 'server-button connected';

                const exclusiveInfo = getExclusiveInfo(server.name);
                // 移除 inline style 设置，避免出现方块背景
                // if (exclusiveInfo.isExclusive) { ... }

                const portMatch = (server.url || '').match(/:(\d+)/);
                const port = portMatch ? portMatch[1] : (server.port || (server.name || '').match(/\d+/)?.[0] || '?');

                // 添加雷达机器人HTML结构
                const botHTML = SERVER_BOT_HTML;


                btn.innerHTML = botHTML + `
                    <div class="server-button-name" style="position: absolute; bottom: -20px; left: 50%; transform: translateX(-50%); font-size: 11px; color: #2d3436; white-space: nowrap; pointer-events: none; z-index: 100;">${port}</div>
                    <div class="server-tooltip">
                        <div style="font-weight: bold; margin-bottom: 4px;">${server.name}</div>
                        <div style="font-size: 11px; opacity: 0.9;">${server.url || ''}</div>
                        <div style="font-size: 11px; color: #00ff88; margin-top: 4px;" class="status-text">状态: 已连接</div>
                        ${exclusiveInfo.isExclusive ? `<div style="font-size: 11px; color: #ff6b6b; margin-top: 2px;">私享服务器:${exclusiveInfo.userIdDisplay}</div>` : ''}
                    </div>
                `;


                if (exclusiveInfo.isExclusive) {
                    btn.classList.add('private');
                }

                // 🔥 已连接服务器不显示删除按钮

                connectedFragment.appendChild(btn);
            });
            connectedContainer.appendChild(connectedFragment);
            // 初始化雷达机器人
            initRadarBots();
            const countEl = document.getElementById('connectedCount');
            if (countEl) countEl.textContent = `(${serverData.connected.length})`;
        } else if (connectedContainer) {
            // 即使没有服务器，也要清空容器（可能之前有内容）
            connectedContainer.innerHTML = '';
            const countEl = document.getElementById('connectedCount');
            if (countEl) countEl.textContent = '(0)';
        }

        if (disconnectedContainer) {
            const disconnectedFragment = document.createDocumentFragment();
            disconnectedContainer.innerHTML = '';
            serverData.disconnected.forEach(server => {
                const btn = document.createElement('button');
                btn.className = 'server-button disconnected';

                const portMatch = (server.url || '').match(/:(\d+)/);
                const port = portMatch ? portMatch[1] : (server.port || (server.name || '').match(/\d+/)?.[0] || '?');

                // 添加雷达机器人HTML结构
                const botHTML = SERVER_BOT_HTML;


                btn.innerHTML = botHTML + `
                    <div class="server-button-name" style="position: absolute; bottom: -20px; left: 50%; transform: translateX(-50%); font-size: 11px; color: #2d3436; white-space: nowrap; pointer-events: none; z-index: 100;">${port}</div>
                    <div class="server-tooltip">
                        <div style="font-weight: bold; margin-bottom: 4px;">${server.name}</div>
                        <div style="font-size: 11px; opacity: 0.9;">${server.url || ''}</div>
                        <div style="font-size: 11px; color: #888888; margin-top: 4px;" class="status-text">状态: 断开连接</div>
                    </div>
                `;
                btn.onclick = () => {
                    btn.classList.toggle('active');
                    // 更新状态文字
                    const statusText = btn.querySelector('.status-text');
                    if (statusText) {
                        statusText.textContent = getServerStatusText(btn);
                    }
                };

                // 🔥 添加删除按钮（红色圆形，悬浮显示在右上角）
                const deleteBtn = document.createElement('button');
                deleteBtn.className = 'server-delete-btn';
                deleteBtn.innerHTML = '×';
                deleteBtn.title = '删除服务器记录';
                deleteBtn.onclick = async (e) => {
                    e.stopPropagation();
                    if (await showDeleteServerConfirm(server.name)) {
                        await deleteServer(server.server_id);
                    }
                };
                btn.appendChild(deleteBtn);

                disconnectedFragment.appendChild(btn);
            });
            disconnectedContainer.appendChild(disconnectedFragment);
            // 初始化雷达机器人
            initRadarBots();
            const disconnectedCountEl = document.getElementById('disconnectedCount');
            if (disconnectedCountEl) disconnectedCountEl.textContent = `(${serverData.disconnected.length})`;
        } else if (disconnectedContainer) {
            // 即使没有服务器，也要清空容器（可能之前有内容）
            disconnectedContainer.innerHTML = '';
            const disconnectedCountEl = document.getElementById('disconnectedCount');
            if (disconnectedCountEl) disconnectedCountEl.textContent = '(0)';
        }
    }, 50);
}

function showAddAdminModal() {
    const modal = document.getElementById('addAdminModal');
    if (!modal) return;

    const idInput = document.getElementById('newAdminId');
    const passwordInput = document.getElementById('newAdminPassword');

    idInput.value = '';
    passwordInput.value = '';

    const idHandler = idInput._enterHandler;
    const passwordHandler = passwordInput._enterHandler;
    if (idHandler) idInput.removeEventListener('keypress', idHandler);
    if (passwordHandler) passwordInput.removeEventListener('keypress', passwordHandler);

    const idEnterHandler = (e) => {
        if (e.key === 'Enter') {
            e.preventDefault();
            passwordInput.focus();
        }
    };
    const passwordEnterHandler = (e) => {
        if (e.key === 'Enter') {
            e.preventDefault();
            addAdminAccount();
        }
    };

    idInput.addEventListener('keypress', idEnterHandler);
    passwordInput.addEventListener('keypress', passwordEnterHandler);

    idInput._enterHandler = idEnterHandler;
    passwordInput._enterHandler = passwordEnterHandler;

    requestAnimationFrame(() => {
        modal.classList.add('show');
        setTimeout(() => {
            idInput.focus();
        }, 50);
    });
}

function closeAddAdminModal() {
    const modal = document.getElementById('addAdminModal');
    if (!modal) return;

    modal.classList.remove('show');

    setTimeout(() => {
        document.getElementById('newAdminId').value = '';
        document.getElementById('newAdminPassword').value = '';
    }, 150);
}

async function addAdminAccount() {
    const id = document.getElementById('newAdminId').value.trim();
    const password = document.getElementById('newAdminPassword').value.trim();

    if (!id || !password) {
        await customAlert('请填写管理员ID和密码');
        return;
    }

    if (adminAccounts.some(a => a.id === id)) {
        closeAddAdminModal();
        setTimeout(async () => {
            await customAlert('该管理员ID已存在');
        }, 300);
        return;
    }

    try {
        // 服务器管理页面已通过密码验证，无需额外token
        const headers = {
            'Content-Type': 'application/json'
        };

        const response = await fetch(`${API_BASE_URL}/admin/account`, {
            method: 'POST',
            headers: headers,
            body: JSON.stringify({
                admin_id: id,
                password: password
            })
        });

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            console.warn('保存管理员账号到数据库失败:', errorData.message || response.statusText);
            closeAddAdminModal();
            setTimeout(async () => {
                await customAlert(`保存失败：${errorData.message || response.statusText || '未知错误'}`);
            }, 300);
            return;
        }

        // 成功后再落本地（避免“数据库失败但本地成功”的假同步）
        adminAccounts.push({ id, password, selectedServers: [] });
        try {
            localStorage.setItem('adminAccounts', JSON.stringify(adminAccounts));
        } catch (error) {
            console.error('保存管理员账号到localStorage失败:', error);
        }

    } catch (error) {
        console.warn('无法连接到API服务器保存管理员账号，但已保存到本地:', error);
        closeAddAdminModal();
        setTimeout(async () => {
            await customAlert('无法连接到API保存管理员账号（未写入数据库），本次不会保存到本地以免造成不同步');
        }, 300);
        return;
    }

    closeAddAdminModal();

    setTimeout(async () => {
        await customAlert('管理员账号已添加');
        updateAdminAccountDisplay();
    }, 150);
}

function showPasswordChangeModal() {
    const modal = document.getElementById('passwordChangeModal');
    if (!modal) return;

    const oldPasswordInput = document.getElementById('oldPasswordInput');
    const newPasswordInput = document.getElementById('newPasswordInput');

    oldPasswordInput.value = '';
    newPasswordInput.value = '';

    const oldHandler = oldPasswordInput._enterHandler;
    const newHandler = newPasswordInput._enterHandler;
    if (oldHandler) oldPasswordInput.removeEventListener('keypress', oldHandler);
    if (newHandler) newPasswordInput.removeEventListener('keypress', newHandler);

    const oldEnterHandler = (e) => {
        if (e.key === 'Enter') {
            e.preventDefault();
            newPasswordInput.focus();
        }
    };
    const newEnterHandler = (e) => {
        if (e.key === 'Enter') {
            e.preventDefault();
            updateServerManagerPassword();
        }
    };

    oldPasswordInput.addEventListener('keypress', oldEnterHandler);
    newPasswordInput.addEventListener('keypress', newEnterHandler);

    oldPasswordInput._enterHandler = oldEnterHandler;
    newPasswordInput._enterHandler = newEnterHandler;

    requestAnimationFrame(() => {
        modal.classList.add('show');
        setTimeout(() => {
            oldPasswordInput.focus();
        }, 50);
    });
}

function showAdminPasswordChangeModal() {
    const modal = document.getElementById('adminPasswordChangeModal');
    if (!modal) return;

    const oldPasswordInput = document.getElementById('adminOldPasswordInput');
    const newPasswordInput = document.getElementById('adminNewPasswordInput');

    oldPasswordInput.value = '';
    newPasswordInput.value = '';

    const oldHandler = oldPasswordInput._enterHandler;
    const newHandler = newPasswordInput._enterHandler;
    if (oldHandler) oldPasswordInput.removeEventListener('keypress', oldHandler);
    if (newHandler) newPasswordInput.removeEventListener('keypress', newHandler);

    const oldEnterHandler = (e) => {
        if (e.key === 'Enter') {
            e.preventDefault();
            newPasswordInput.focus();
        }
    };
    const newEnterHandler = (e) => {
        if (e.key === 'Enter') {
            e.preventDefault();
            updateAdminPassword();
        }
    };

    oldPasswordInput.addEventListener('keypress', oldEnterHandler);
    newPasswordInput.addEventListener('keypress', newEnterHandler);

    oldPasswordInput._enterHandler = oldEnterHandler;
    newPasswordInput._enterHandler = newEnterHandler;

    requestAnimationFrame(() => {
        modal.classList.add('show');
        setTimeout(() => {
            oldPasswordInput.focus();
        }, 50);
    });
}

function closeAdminPasswordChangeModal() {
    const modal = document.getElementById('adminPasswordChangeModal');
    if (!modal) return;

    modal.classList.remove('show');

    setTimeout(() => {
        document.getElementById('adminOldPasswordInput').value = '';
        document.getElementById('adminNewPasswordInput').value = '';
    }, 300);
}

function closePasswordChangeModal() {
    const modal = document.getElementById('passwordChangeModal');
    if (!modal) return;

    modal.classList.remove('show');

    setTimeout(() => {
        document.getElementById('oldPasswordInput').value = '';
        document.getElementById('newPasswordInput').value = '';
    }, 150);
}

function showRechargeModal() {
    const modal = document.getElementById('rechargeModal');
    if (!modal) return;

    document.getElementById('rechargeUserIdInput').value = '';
    document.getElementById('rechargeAmountInput').value = '';
    document.getElementById('rechargeUserInfo').style.display = 'none';
    document.getElementById('rechargeRecordsList').innerHTML = '<div style="padding: 15px; text-align: center; color: #999;">请先验证用户以查看充值记录</div>';
    currentRechargeUserId = null;

    modal.style.display = 'flex';
    setTimeout(() => {
        modal.classList.add('show');
        document.getElementById('rechargeUserIdInput').focus();
    }, 10);

    const userIdInput = document.getElementById('rechargeUserIdInput');
    const amountInput = document.getElementById('rechargeAmountInput');

    if (userIdInput._enterHandler) {
        userIdInput.removeEventListener('keypress', userIdInput._enterHandler);
    }
    if (amountInput._enterHandler) {
        amountInput.removeEventListener('keypress', amountInput._enterHandler);
    }

    userIdInput._enterHandler = (e) => {
        if (e.key === 'Enter') {
            e.preventDefault();
            verifyRechargeUser();
        }
    };
    userIdInput.addEventListener('keypress', userIdInput._enterHandler);

    amountInput._enterHandler = (e) => {
        if (e.key === 'Enter') {
            e.preventDefault();
            confirmRecharge();
        }
    };
    amountInput.addEventListener('keypress', amountInput._enterHandler);
}

function closeRechargeModal() {
    const modal = document.getElementById('rechargeModal');
    if (!modal) return;

    modal.classList.remove('show');
    setTimeout(() => {
        modal.style.display = 'none';
        document.getElementById('rechargeUserIdInput').value = '';
        document.getElementById('rechargeAmountInput').value = '';
        document.getElementById('rechargeUserInfo').style.display = 'none';
    }, 150);
}

//#region ID库功能模块
let idLibraryAccounts = [];
const ID_LIBRARY_STORAGE_KEY = 'idLibraryAccounts';

function loadIdLibraryFromStorage() {
    try {
        const stored = localStorage.getItem(ID_LIBRARY_STORAGE_KEY);
        if (stored) {
            idLibraryAccounts = JSON.parse(stored);
            // 确保每个账号都有usageStatus字段
            idLibraryAccounts.forEach(acc => {
                if (!acc.usageStatus) {
                    acc.usageStatus = 'new';
                }
            });
        }
    } catch (e) {
        console.error('加载ID库失败:', e);
        idLibraryAccounts = [];
    }
}

function saveIdLibraryToStorage() {
    try {
        localStorage.setItem(ID_LIBRARY_STORAGE_KEY, JSON.stringify(idLibraryAccounts));
    } catch (e) {
        console.error('保存ID库失败:', e);
    }
}

// 从服务器同步ID库
async function syncIdLibraryFromServer() {
    try {
        // 检查 API_BASE_URL 是否定义
        if (typeof API_BASE_URL === 'undefined') {
            // console.warn('API_BASE_URL 未定义，跳过服务器同步');
            return false;
        }

        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 3000); // 3秒超时，更快失败

        const response = await fetch(`${API_BASE_URL}/id-library`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json'
            },
            signal: controller.signal
        });

        clearTimeout(timeoutId);

        if (response.ok) {
            const data = await response.json();
            if (data.success && data.accounts) {
                idLibraryAccounts = data.accounts;
                saveIdLibraryToStorage();
                renderIdLibraryList();
                console.log('✓ ID库已从服务器同步');
                return true;
            }
        } else {
            // console.warn('从服务器同步ID库失败: HTTP', response.status);
        }
    } catch (e) {

    }
    return false;
}

// 同步ID库到所有服务器
async function syncIdLibraryToServer() {
    try {
        if (typeof API_BASE_URL === 'undefined') {
            return false;
        }

        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 5000); // 5秒超时

        const response = await fetch(`${API_BASE_URL}/id-library`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                accounts: idLibraryAccounts.map(acc => ({
                    appleId: acc.appleId,
                    password: acc.password,
                    status: acc.status || 'normal',
                    usageStatus: acc.usageStatus || 'new'
                }))
            }),
            signal: controller.signal
        });

        clearTimeout(timeoutId);

        if (response.ok) {
            const data = await response.json();
            if (data.success) {
                console.log('✓ ID库已同步到服务器');
                return true;
            }
        } else {
        }
    } catch (e) {
    }
    return false;
}

// 从服务器删除ID
async function deleteIdFromServer(appleId) {
    try {
        if (typeof API_BASE_URL === 'undefined') {
            return false;
        }

        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 5000); // 5秒超时

        const response = await fetch(`${API_BASE_URL}/id-library/${encodeURIComponent(appleId)}`, {
            method: 'DELETE',
            headers: {
                'Content-Type': 'application/json'
            },
            signal: controller.signal
        });

        clearTimeout(timeoutId);
        return response.ok;
    } catch (e) {
        // 静默处理错误
        if (e.name !== 'AbortError') {
            console.warn('从服务器删除ID失败:', e.message || e);
        }
        return false;
    }
}

// 更新ID的使用状态到服务器
async function updateIdUsageStatusOnServer(appleId, usageStatus) {
    try {
        // 检查 API_BASE_URL 是否定义
        if (typeof API_BASE_URL === 'undefined') {
            return false;
        }

        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 5000); // 5秒超时

        const response = await fetch(`${API_BASE_URL}/id-library/${encodeURIComponent(appleId)}`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                usageStatus: usageStatus
            }),
            signal: controller.signal
        });

        clearTimeout(timeoutId);
        return response.ok;
    } catch (e) {
        // 静默处理错误
        if (e.name !== 'AbortError') {
            console.warn('更新ID使用状态失败:', e.message || e);
        }
        return false;
    }
}

async function showIdLibraryModal() {
    const modal = document.getElementById('idLibraryModal');
    if (!modal) return;

    // 先尝试从服务器同步数据（静默失败，不影响本地功能）
    try {
        await syncIdLibraryFromServer();
    } catch (e) {
        // 忽略同步错误
    }

    // 如果服务器同步失败或没有数据，则从本地存储加载
    if (idLibraryAccounts.length === 0) {
        loadIdLibraryFromStorage();
    }
    renderIdLibraryList();

    document.getElementById('idLibraryAppleId').value = '';
    document.getElementById('idLibraryPassword').value = '';

    modal.style.display = 'flex';
    setTimeout(() => {
        modal.classList.add('show');
        document.getElementById('idLibraryAppleId').focus();
    }, 10);
}

function closeIdLibraryModal() {
    const modal = document.getElementById('idLibraryModal');
    if (!modal) return;

    modal.classList.remove('show');
    setTimeout(() => {
        modal.style.display = 'none';
    }, 150);
}

function toggleIdLibraryPassword() {
    const input = document.getElementById('idLibraryPassword');
    const btn = input.parentElement.querySelector('.password-toggle-btn');
    if (input.type === 'password') {
        input.type = 'text';
        btn.textContent = '🙈';
    } else {
        input.type = 'password';
        btn.textContent = '👁';
    }
}

async function saveIdLibraryAccount() {
    const appleId = document.getElementById('idLibraryAppleId').value.trim();
    const password = document.getElementById('idLibraryPassword').value.trim();

    if (!appleId) {
        await customAlert('请输入Apple ID');
        return;
    }
    if (!password) {
        await customAlert('请输入密码');
        return;
    }

    const exists = idLibraryAccounts.find(acc => acc.appleId.toLowerCase() === appleId.toLowerCase());
    if (exists) {
        if (await customConfirm(`账号 ${appleId} 已存在，是否更新密码？`)) {
            exists.password = password;
            exists.updatedAt = new Date().toISOString();
        } else {
            return;
        }
    } else {
        idLibraryAccounts.push({
            appleId: appleId,
            password: password,
            status: 'normal',
            usageStatus: 'new',
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString()
        });
    }

    saveIdLibraryToStorage();
    renderIdLibraryList();

    // 尝试同步到服务器（静默失败，不影响本地功能）
    try {
        await syncIdLibraryToServer();
    } catch (e) {
        // 忽略同步错误
    }

    document.getElementById('idLibraryAppleId').value = '';
    document.getElementById('idLibraryPassword').value = '';
    document.getElementById('idLibraryAppleId').focus();
}

function importIdLibraryAccounts() {
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = '.txt,.csv';
    input.onchange = async (e) => {
        const file = e.target.files[0];
        if (!file) return;

        try {
            const text = await file.text();
            const lines = text.split(/\r?\n/).filter(line => line.trim());
            let imported = 0;
            let skipped = 0;

            for (const line of lines) {
                const parts = line.split(/[,\t:\-]+/).map(p => p.trim());
                if (parts.length >= 2) {
                    const appleId = parts[0];
                    const password = parts[1];

                    if (appleId && password) {
                        const exists = idLibraryAccounts.find(acc => acc.appleId.toLowerCase() === appleId.toLowerCase());
                        if (!exists) {
                            idLibraryAccounts.push({
                                appleId: appleId,
                                password: password,
                                status: 'normal',
                                usageStatus: 'new',
                                createdAt: new Date().toISOString(),
                                updatedAt: new Date().toISOString()
                            });
                            imported++;
                        } else {
                            skipped++;
                        }
                    }
                }
            }

            saveIdLibraryToStorage();
            renderIdLibraryList();

            // 尝试同步到服务器（静默失败，不影响本地功能）
            try {
                await syncIdLibraryToServer();
            } catch (e) {
                // 忽略同步错误
            }

            await customAlert(`导入完成！\n新增: ${imported} 个\n跳过(已存在): ${skipped} 个`);
        } catch (err) {
            console.error('导入失败:', err);
            await customAlert('导入失败: ' + err.message);
        }
    };
    input.click();
}

async function clearAllIdLibraryAccounts() {
    if (idLibraryAccounts.length === 0) {
        await customAlert('列表已为空');
        return;
    }

    if (await customConfirm(`确定要清空所有 ${idLibraryAccounts.length} 个账号吗？\n此操作不可恢复！`)) {
        // 尝试从服务器删除所有ID（静默失败，不影响本地功能）
        try {
            for (const account of idLibraryAccounts) {
                await deleteIdFromServer(account.appleId);
            }
        } catch (e) {
            // 忽略删除错误
        }

        idLibraryAccounts = [];
        saveIdLibraryToStorage();
        renderIdLibraryList();
    }
}

async function deleteIdLibraryAccount(index) {
    if (index < 0 || index >= idLibraryAccounts.length) return;

    const account = idLibraryAccounts[index];
    if (await customConfirm(`确定要删除账号 ${account.appleId} 吗？`)) {
        idLibraryAccounts.splice(index, 1);
        saveIdLibraryToStorage();
        renderIdLibraryList();

        // 尝试从服务器删除（静默失败，不影响本地功能）
        try {
            await deleteIdFromServer(account.appleId);
        } catch (e) {
            // 忽略删除错误
        }
    }
}

function fillIdLibraryAccount(index) {
    if (index < 0 || index >= idLibraryAccounts.length) return;

    const account = idLibraryAccounts[index];
    document.getElementById('idLibraryAppleId').value = account.appleId;
    document.getElementById('idLibraryPassword').value = account.password;
    document.getElementById('idLibraryAppleId').focus();
}

function toggleIdLibraryAccountStatus(index) {
    if (index < 0 || index >= idLibraryAccounts.length) return;

    const account = idLibraryAccounts[index];
    account.status = account.status === 'normal' ? 'error' : 'normal';
    account.updatedAt = new Date().toISOString();
    saveIdLibraryToStorage();
    renderIdLibraryList();

    // 尝试同步到服务器（静默失败，不影响本地功能）
    try {
        syncIdLibraryToServer();
    } catch (e) {
        // 忽略同步错误
    }
}

async function toggleIdLibraryUsageStatus(index) {
    if (index < 0 || index >= idLibraryAccounts.length) return;

    const account = idLibraryAccounts[index];
    const newStatus = account.usageStatus === 'new' ? 'used' : 'new';
    account.usageStatus = newStatus;
    account.updatedAt = new Date().toISOString();
    saveIdLibraryToStorage();
    renderIdLibraryList();

    // 尝试同步到服务器（静默失败，不影响本地功能）
    try {
        await updateIdUsageStatusOnServer(account.appleId, newStatus);
    } catch (e) {
        // 忽略同步错误
    }
}

function maskPassword(password) {
    if (!password) return '';
    if (password.length <= 4) return '****';
    return password.substring(0, 2) + '****' + password.substring(password.length - 2);
}

function renderIdLibraryList() {
    const listContainer = document.getElementById('idLibraryList');
    if (!listContainer) return;

    if (idLibraryAccounts.length === 0) {
        listContainer.innerHTML = `
            <div class="id-library-empty">
                <div class="empty-icon">📭</div>
                <div class="empty-text">暂无账号</div>
                <div class="empty-hint">点击"保存"添加账号，或"导入"批量导入</div>
            </div>
        `;
    } else {
        listContainer.innerHTML = idLibraryAccounts.map((account, index) => `
            <div class="id-library-item ${account.status === 'error' ? 'error' : ''}">
                <div class="item-col col-index">${index + 1}</div>
                <div class="item-col col-account">${escapeHtml(account.appleId)}</div>
                <div class="item-col col-password">${maskPassword(account.password)}</div>
                <div class="item-col col-status">
                    <span class="status-badge ${account.status || 'normal'}" onclick="toggleIdLibraryAccountStatus(${index})" style="cursor: pointer;" title="点击切换状态">
                        ${(account.status || 'normal') === 'normal' ? '正常' : '异常'}
                    </span>
                </div>
                <div class="item-col col-usage-status">
                    <span class="usage-status-badge ${account.usageStatus || 'new'}" onclick="toggleIdLibraryUsageStatus(${index})" title="点击切换使用状态">
                        ${(account.usageStatus || 'new') === 'new' ? 'NEW' : 'USED'}
                    </span>
                </div>
                <div class="item-col col-actions">
                    <button class="item-action-btn btn-fill" onclick="fillIdLibraryAccount(${index})" title="填充到输入框">填充</button>
                    <button class="item-action-btn btn-delete" onclick="deleteIdLibraryAccount(${index})" title="删除此账号">删除</button>
                </div>
            </div>
        `).join('');
    }

    const normalCount = idLibraryAccounts.filter(acc => acc.status === 'normal').length;
    const errorCount = idLibraryAccounts.filter(acc => acc.status === 'error').length;

    document.getElementById('idLibraryTotal').textContent = idLibraryAccounts.length;
    document.getElementById('idLibraryNormal').textContent = normalCount;
    document.getElementById('idLibraryError').textContent = errorCount;
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}
//#endregion

//#region 超级管理员面板 - 充值功能
let saCurrentRechargeUserId = null;  // 超级管理员面板的当前充值用户ID

// 显示超级管理员充值面板
function showSuperAdminRechargePanel() {
    // 隐藏服务器列表
    const serversSection = document.getElementById('superAdminServersSection');
    const detailSection = document.getElementById('superAdminDetailSection');
    const rechargeSection = document.getElementById('superAdminRechargeSection');

    if (serversSection) serversSection.style.display = 'none';
    if (detailSection) detailSection.style.display = 'none';
    if (rechargeSection) rechargeSection.style.display = 'block';

    // 更新侧边栏按钮状态
    const sidebarBtns = document.querySelectorAll('.super-admin-sidebar .sidebar-btn');
    sidebarBtns.forEach(btn => btn.classList.remove('active'));
    // 找到Recharge按钮并激活
    sidebarBtns.forEach(btn => {
        if (btn.textContent.includes('Recharge')) {
            btn.classList.add('active');
        }
    });

    // 加载所有充值记录
    saLoadAllRechargeRecords();
}

// 显示超级管理员服务器面板 (用于侧边栏Servers按钮)
function showSuperAdminServersPanel() {
    const serversSection = document.getElementById('superAdminServersSection');
    const detailSection = document.getElementById('superAdminDetailSection');
    const rechargeSection = document.getElementById('superAdminRechargeSection');

    if (serversSection) serversSection.style.display = 'block';
    if (detailSection) detailSection.style.display = 'none';
    if (rechargeSection) rechargeSection.style.display = 'none';

    // 更新侧边栏按钮状态
    const sidebarBtns = document.querySelectorAll('.super-admin-sidebar .sidebar-btn');
    sidebarBtns.forEach(btn => btn.classList.remove('active'));
    sidebarBtns.forEach(btn => {
        if (btn.textContent.includes('Servers')) {
            btn.classList.add('active');
        }
    });
}

// 超级管理员面板 - 验证用户
async function saVerifyRechargeUser() {
    const userId = document.getElementById('saRechargeUserIdInput').value.trim();
    if (!userId) {
        await customAlert('请输入用户名');
        return;
    }

    try {
        const creditsResp = await fetch(`${API_BASE_URL}/user/${userId}/credits`);
        if (!creditsResp.ok) {
            if (creditsResp.status === 404) {
                await customAlert('用户不存在');
                return;
            }
            throw new Error('获取用户信息失败');
        }

        const creditsData = await creditsResp.json();
        if (!creditsData.success) {
            await customAlert('用户不存在');
            return;
        }

        const userResp = await fetch(`${API_BASE_URL}/user/${userId}/statistics`);
        let userData = null;
        if (userResp.ok) {
            const data = await userResp.json();
            if (data.success) {
                userData = data;
            }
        }

        saCurrentRechargeUserId = creditsData.user_id || userId;
        const credits = creditsData.credits || 0;

        const usage = userData?.usage || [];
        const rechargeRecords = usage.filter(item => item.action === 'recharge');
        const lastRecharge = rechargeRecords.length > 0 ? rechargeRecords[rechargeRecords.length - 1] : null;

        let totalSpent = 0;
        usage.forEach(item => {
            if (item.action !== 'recharge' && item.credits) {
                totalSpent += parseFloat(item.credits) || 0;
            }
        });

        // 显示用户信息
        let userIdDisplay = saCurrentRechargeUserId;
        if (userIdDisplay && userIdDisplay.startsWith('u_')) {
            userIdDisplay = userIdDisplay.substring(2);
        }
        const usernameDisplay = creditsData.username || userData?.username || userId || '-';

        document.getElementById('saRechargeInfoUserId').textContent = userIdDisplay;
        document.getElementById('saRechargeInfoUsername').textContent = usernameDisplay;
        document.getElementById('saRechargeInfoCredits').textContent = credits.toFixed(2);

        if (lastRecharge) {
            const lastRechargeTime = lastRecharge.ts ? new Date(lastRecharge.ts).toLocaleString('zh-CN', {
                year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit'
            }) : '-';
            const lastRechargeAmount = parseFloat(lastRecharge.amount || 0).toFixed(2);
            document.getElementById('saRechargeInfoLastRecharge').textContent = `${lastRechargeTime} (+${lastRechargeAmount})`;
        } else {
            document.getElementById('saRechargeInfoLastRecharge').textContent = '无';
        }

        document.getElementById('saRechargeInfoTotalSpent').textContent = totalSpent.toFixed(2);
        const createdDate = userData?.created ? new Date(userData.created).toLocaleString('zh-CN') : '-';
        document.getElementById('saRechargeInfoCreated').textContent = createdDate;

        // 显示用户信息面板
        document.getElementById('saRechargeUserInfoPanel').style.display = 'block';

        // 显示充值记录
        saDisplayRechargeRecords(rechargeRecords.map(r => ({
            ...r, user_id: saCurrentRechargeUserId, username: usernameDisplay
        })));

    } catch (error) {
        console.error('验证用户失败:', error);
        await customAlert('验证用户失败: ' + error.message);
    }
}

// 超级管理员面板 - 确认充值
async function saConfirmRecharge() {
    if (!saCurrentRechargeUserId) {
        await customAlert('请先验证用户');
        return;
    }

    const amount = parseFloat(document.getElementById('saRechargeAmountInput').value);
    if (!amount || amount === 0) {
        await customAlert('请输入有效的充值金额（支持负数）');
        return;
    }

    if (!await customConfirm(`确认给用户 ${saCurrentRechargeUserId} 充值 ${amount} 积分吗？`)) {
        return;
    }

    try {
        const response = await fetch(`${API_BASE_URL}/admin/users/${saCurrentRechargeUserId}/recharge`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ amount: amount })
        });

        const data = await response.json();
        if (data.success) {
            const amountDisplay = amount >= 0 ? `+${amount.toFixed(2)}` : amount.toFixed(2);
            await customAlert(`充值成功！${amountDisplay} 积分，当前余额: ${data.credits.toFixed(2)}`);
            document.getElementById('saRechargeAmountInput').value = '';
            // 刷新用户信息和记录
            await saVerifyRechargeUser();
        } else {
            await customAlert('充值失败: ' + (data.message || '未知错误'));
        }
    } catch (error) {
        console.error('充值失败:', error);
        await customAlert('充值失败: ' + error.message);
    }
}

// 超级管理员面板 - 重置
function saResetRecharge() {
    saCurrentRechargeUserId = null;
    document.getElementById('saRechargeUserIdInput').value = '';
    document.getElementById('saRechargeAmountInput').value = '';
    document.getElementById('saRechargeUserInfoPanel').style.display = 'none';
    document.getElementById('saRechargeRecordsList').innerHTML = '<div class="log-line system">请先验证用户以查看充值记录...</div>';
}

// 超级管理员面板 - 加载所有充值记录
async function saLoadAllRechargeRecords() {
    const container = document.getElementById('saRechargeRecordsList');
    if (!container) return;

    container.innerHTML = '<div class="log-line system">加载充值记录中...</div>';

    try {
        const response = await fetch(`${API_BASE_URL}/admin/recharge-records`);
        if (!response.ok) throw new Error('获取充值记录失败');

        const data = await response.json();
        if (data.success && data.records && data.records.length > 0) {
            saDisplayRechargeRecords(data.records);
        } else {
            container.innerHTML = '<div class="log-line system">暂无充值记录</div>';
        }
    } catch (error) {
        console.error('加载充值记录失败:', error);
        container.innerHTML = '<div class="log-line error">加载失败，请刷新重试</div>';
    }
}

// 超级管理员面板 - 显示充值记录
function saDisplayRechargeRecords(records) {
    const container = document.getElementById('saRechargeRecordsList');
    if (!container) return;

    if (!records || records.length === 0) {
        container.innerHTML = '<div class="log-line system">暂无充值记录</div>';
        return;
    }

    const sortedRecords = records.sort((a, b) => {
        return new Date(b.ts || 0).getTime() - new Date(a.ts || 0).getTime();
    });

    let html = '';
    sortedRecords.forEach((record, index) => {
        const time = record.ts ? new Date(record.ts).toLocaleString('zh-CN', {
            month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit'
        }) : '-';
        const amount = parseFloat(record.amount || 0);
        const amountDisplay = amount >= 0 ? `+${amount.toFixed(2)}` : amount.toFixed(2);
        const amountColor = amount >= 0 ? '#00ff88' : '#ff4757';
        const userId = record.username || record.user_id || '-';

        html += `<div class="log-line" style="display: flex; justify-content: space-between; padding: 4px 0; border-bottom: 1px solid #333;">`;
        html += `<span style="color: #888;">${index + 1}.</span>`;
        html += `<span style="color: #4facfe; flex: 1; margin-left: 10px;">${userId}</span>`;
        html += `<span style="color: #888; margin-right: 15px;">${time}</span>`;
        html += `<span style="color: ${amountColor}; font-weight: bold; min-width: 80px; text-align: right;">${amountDisplay}</span>`;
        html += `</div>`;
    });

    container.innerHTML = html;
}

// --- Super Admin User History Feature ---

let saHistoryCache = { tasks: [], recharge: [] };

async function saVerifyUserHistory() {
    const userId = document.getElementById('saHistoryUserIdInput').value.trim();
    if (!userId) {
        await customAlert('请输入用户名');
        return;
    }

    const resultPanel = document.getElementById('saUserHistoryResult');
    resultPanel.style.display = 'none';

    try {
        const response = await fetch(`${API_BASE_URL}/super-admin/user/${encodeURIComponent(userId)}/history`);
        if (!response.ok) {
            const errData = await response.json().catch(() => ({}));
            throw new Error(errData.message || ('查询失败: ' + response.status));
        }

        const data = await response.json();
        if (!data.success) throw new Error(data.message || '查询失败');

        // Fill Stats
        const globalStats = data.global_stats || {};
        document.getElementById('saHistoryTotalSent').textContent = globalStats.sent || 0;
        document.getElementById('saHistoryTotalSuccess').textContent = globalStats.success || 0;
        document.getElementById('saHistoryTotalFail').textContent = globalStats.fail || 0;

        const credits = (data.account && data.account.credits !== undefined) ? data.account.credits : '-';
        document.getElementById('saHistoryCredits').textContent = typeof credits === 'number' ? credits.toFixed(2) : credits;

        // Cache Data
        saHistoryCache.tasks = data.history_tasks || [];
        const usage = (data.account && data.account.usage) ? data.account.usage : [];
        saHistoryCache.recharge = usage.filter(r => r.action === 'recharge');

        // Render
        saRenderTaskHistory(saHistoryCache.tasks);
        saRenderRechargeHistory(saHistoryCache.recharge);

        // Show Panel
        resultPanel.style.display = 'flex';
        saSwitchHistoryTab('tasks');

    } catch (e) {
        console.error(e);
        await customAlert(e.message);
    }
}

function saSwitchHistoryTab(tab) {
    document.getElementById('btnShowTaskHistory').classList.remove('active');
    document.getElementById('btnShowRechargeHistory').classList.remove('active');

    document.getElementById('saTaskHistoryList').style.display = 'none';
    document.getElementById('saRechargeHistoryList').style.display = 'none';

    if (tab === 'tasks') {
        document.getElementById('btnShowTaskHistory').classList.add('active');
        document.getElementById('saTaskHistoryList').style.display = 'block';
    } else {
        document.getElementById('btnShowRechargeHistory').classList.add('active');
        document.getElementById('saRechargeHistoryList').style.display = 'block';
    }
}

function saRenderTaskHistory(tasks) {
    const container = document.getElementById('saTaskHistoryList');
    if (!tasks || tasks.length === 0) {
        container.innerHTML = '<div class="log-line system">暂无任务记录</div>';
        return;
    }

    let html = '';
    tasks.forEach(task => {
        const time = task.created_at ? new Date(task.created_at).toLocaleString() : '-';
        const statusColor = task.status === 'completed' ? '#00ff88' : (task.status === 'failed' ? '#ff5252' : '#ffd700');
        html += `<div class="log-line" style="border-bottom: 1px solid #333; padding: 8px 0;">
            <div style="display:flex; justify-content:space-between; margin-bottom:4px;">
                <span style="color:#aaa; font-size:12px;">${time}</span>
                <span style="color:${statusColor}; font-weight:bold; font-size:12px;">${task.status}</span>
            </div>
            <div style="color:#fff; margin-bottom:4px; font-size:13px;">${escapeHtml(task.message || '')}</div>
            <div style="font-size:12px; color:#666;">Phones: ${(task.phones || []).length} | Success: ${(task.phones || []).filter(p => p.status === 'sent').length}</div>
        </div>`;
    });
    container.innerHTML = html;
}

function saRenderRechargeHistory(records) {
    const container = document.getElementById('saRechargeHistoryList');
    if (!records || records.length === 0) {
        container.innerHTML = '<div class="log-line system">暂无充值记录</div>';
        return;
    }

    let html = '';
    const sorted = [...records].sort((a, b) => (b.ts || 0) - (a.ts || 0));
    sorted.forEach(r => {
        const time = r.ts ? new Date(r.ts).toLocaleString() : '-';
        const amount = parseFloat(r.amount || 0);
        const color = amount >= 0 ? '#00ff88' : '#ff5252';
        html += `<div class="log-line" style="display:flex; justify-content:space-between; border-bottom:1px solid #333; padding: 8px 0;">
            <span style="color:#ccc; font-size:13px;">${time}</span>
            <span style="color:${color}; font-weight:bold;">${amount >= 0 ? '+' + amount.toFixed(2) : amount.toFixed(2)}</span>
        </div>`;
    });
    container.innerHTML = html;
}

function switchSuperAdminTab(tabName) {
    // Hide all sections first
    const sections = [
        'superAdminServersSection',
        'superAdminUserSection',
        'superAdminRechargeSection',
        'superAdminRatesSection'
    ];
    sections.forEach(id => {
        const el = document.getElementById(id);
        if (el) el.style.display = 'none';
    });

    // Show active section
    if (tabName === 'servers') {
        showSuperAdminServersPanel();
    } else if (tabName === 'users') {
        const el = document.getElementById('superAdminUserSection');
        if (el) el.style.display = 'block';
        _updateSaSidebarActive('users');
    } else if (tabName === 'recharge') {
        showSuperAdminRechargePanel();
    } else if (tabName === 'rates') {
        const el = document.getElementById('superAdminRatesSection');
        if (el) el.style.display = 'block';
        _updateSaSidebarActive('rates');
    } else if (tabName === 'logs') {
        _updateSaSidebarActive('logs');
    } else if (tabName === 'settings') {
        _updateSaSidebarActive('settings');
    }
}

function _updateSaSidebarActive(tabName) {
    const sidebarBtns = document.querySelectorAll('.super-admin-sidebar .sidebar-btn');
    sidebarBtns.forEach(btn => {
        btn.classList.remove('active');
        const onclick = btn.getAttribute('onclick');
        if (onclick && onclick.includes(`'${tabName}'`)) {
            btn.classList.add('active');
        }
    });
}
//#endregion

let currentRechargeUserId = null;
async function verifyRechargeUser() {
    const userId = document.getElementById('rechargeUserIdInput').value.trim();
    if (!userId) {
        await customAlert('请输入用户ID');
        return;
    }

    try {
        const creditsResp = await fetch(`${API_BASE_URL}/user/${userId}/credits`);
        if (!creditsResp.ok) {
            if (creditsResp.status === 404) {
                await customAlert('用户不存在');
                return;
            }
            throw new Error('获取用户信息失败');
        }

        const creditsData = await creditsResp.json();
        if (!creditsData.success) {
            await customAlert('用户不存在');
            return;
        }

        const userResp = await fetch(`${API_BASE_URL}/user/${userId}/statistics`);
        let userData = null;
        if (userResp.ok) {
            const data = await userResp.json();
            if (data.success) {
                userData = data;
            }
        }

        currentRechargeUserId = creditsData.user_id || userId;
        const credits = creditsData.credits || 0;

        const usage = userData?.usage || [];
        const rechargeRecords = usage.filter(item => item.action === 'recharge');
        const lastRecharge = rechargeRecords.length > 0 ? rechargeRecords[rechargeRecords.length - 1] : null;

        let totalSpent = 0;
        usage.forEach(item => {
            if (item.action !== 'recharge' && item.credits) {
                totalSpent += parseFloat(item.credits) || 0;
            }
        });

        // 处理用户ID显示：如果是u_格式，提取4位数字；否则直接使用
        let userIdDisplay = currentRechargeUserId;
        if (userIdDisplay && userIdDisplay.startsWith('u_')) {
            userIdDisplay = userIdDisplay.substring(2);
        }
        const usernameDisplay = creditsData.username || userData?.username || userId || '-';
        document.getElementById('rechargeInfoUserId').textContent = userIdDisplay;
        document.getElementById('rechargeInfoUsername').textContent = usernameDisplay;
        document.getElementById('rechargeInfoCredits').textContent = credits.toFixed(2);
        if (lastRecharge) {
            const lastRechargeTime = lastRecharge.ts ? new Date(lastRecharge.ts).toLocaleString('zh-CN', {
                year: 'numeric',
                month: '2-digit',
                day: '2-digit',
                hour: '2-digit',
                minute: '2-digit'
            }) : '-';
            const lastRechargeAmount = parseFloat(lastRecharge.amount || 0).toFixed(2);
            document.getElementById('rechargeInfoLastRecharge').textContent = `${lastRechargeTime} (${lastRechargeAmount})`;
        } else {
            document.getElementById('rechargeInfoLastRecharge').textContent = '无';
        }
        document.getElementById('rechargeInfoTotalSpent').textContent = totalSpent.toFixed(2);
        const createdDate = userData?.created ? new Date(userData.created).toLocaleString('zh-CN') : '-';
        document.getElementById('rechargeInfoCreated').textContent = createdDate;

        document.getElementById('rechargeUserInfo').style.display = 'block';

        // 🔥 显示当前验证用户的充值记录，并自动切换到当前用户
        displayRechargeRecords(rechargeRecords.map(r => ({
            ...r,
            user_id: currentRechargeUserId,
            username: usernameDisplay
        })), currentRechargeUserId);

    } catch (error) {
        console.error('验证用户失败:', error);
        await customAlert('验证用户失败: ' + error.message);
    }
}

// 🔥 加载所有充值记录
async function loadAllRechargeRecords() {
    try {
        const container = document.getElementById('rechargeRecordsList');
        if (!container) return;

        container.innerHTML = '<div style="padding: 15px; text-align: center; color: #999;">加载中...</div>';

        const response = await fetch(`${API_BASE_URL}/admin/recharge-records`);
        if (!response.ok) {
            throw new Error('获取充值记录失败');
        }

        const data = await response.json();
        if (data.success && data.records && data.records.length > 0) {
            // 获取用户信息映射
            const userIdMap = {};
            for (const record of data.records) {
                if (!userIdMap[record.user_id]) {
                    try {
                        const userResp = await fetch(`${API_BASE_URL}/user/${record.user_id}/credits`);
                        if (userResp.ok) {
                            const userData = await userResp.json();
                            if (userData.success) {
                                userIdMap[record.user_id] = userData.username || record.user_id;
                            }
                        }
                    } catch (e) {
                        // 忽略单个用户查询失败
                    }
                }
            }

            // 显示所有充值记录
            displayRechargeRecords(data.records.map(r => ({
                ...r,
                username: userIdMap[r.user_id] || r.user_id
            })), null);
        } else {
            container.innerHTML = '<div style="padding: 15px; text-align: center; color: #999;">暂无充值记录</div>';
        }
    } catch (error) {
        console.error('加载所有充值记录失败:', error);
        const container = document.getElementById('rechargeRecordsList');
        if (container) {
            container.innerHTML = '<div style="padding: 15px; text-align: center; color: #ff6b6b;">加载失败，请刷新重试</div>';
        }
    }
}

function displayRechargeRecords(records, userId = null) {
    const container = document.getElementById('rechargeRecordsList');
    if (!records || records.length === 0) {
        container.innerHTML = '<div style="padding: 15px; text-align: center; color: #999;">暂无充值记录</div>';
        return;
    }

    const sortedRecords = records.sort((a, b) => {
        const timeA = new Date(a.ts || 0).getTime();
        const timeB = new Date(b.ts || 0).getTime();
        return timeB - timeA;
    });

    let html = '<div style="display: grid; grid-template-columns: 80px 1fr 200px 150px; gap: 15px; padding: 12px 15px; background: #f9f9f9; font-weight: bold; border-bottom: 2px solid #ddd; font-size: 14px; position: sticky; top: 0; z-index: 10;">';
    html += '<div>记录</div><div>用户</div><div>时间</div><div style="text-align: right;">充值金额</div></div>';

    sortedRecords.forEach((record, index) => {
        const time = record.ts ? new Date(record.ts).toLocaleString('zh-CN', {
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit'
        }) : '-';
        const amount = parseFloat(record.amount || 0);
        const amountDisplay = amount >= 0 ? `+${amount.toFixed(2)}` : amount.toFixed(2);
        const amountColor = amount >= 0 ? '#4CAF50' : '#f44336';
        const bgColor = index % 2 === 0 ? '#fff' : '#f9f9f9';
        // 显示用户ID或用户名
        let displayUserId = userId || record.user_id || currentRechargeUserId || '-';
        if (record.username && record.username !== record.user_id) {
            displayUserId = `${record.username}(${displayUserId})`;
        }
        html += `<div style="display: grid; grid-template-columns: 80px 1fr 200px 150px; gap: 15px; padding: 12px 15px; background: ${bgColor}; border-bottom: 1px solid #eee; font-size: 14px; align-items: center;">`;
        html += `<div style="color: #666;">${index + 1}</div>`;
        html += `<div style="color: #333;">${displayUserId}</div>`;
        html += `<div style="color: #666; font-size: 13px;">${time}</div>`;
        html += `<div style="color: ${amountColor}; font-weight: bold; text-align: right;">${amountDisplay}</div>`;
        html += '</div>';
    });

    container.innerHTML = html;
}

// 🔥 完成按钮：回到显示所有记录
async function finishRecharge() {
    currentRechargeUserId = null;
    document.getElementById('rechargeUserIdInput').value = '';
    document.getElementById('rechargeAmountInput').value = '';
    document.getElementById('rechargeUserInfo').style.display = 'none';
    await loadAllRechargeRecords();
}

async function confirmRecharge() {
    // 🔥 每次充值前必须验证用户
    if (!currentRechargeUserId) {
        await customAlert('请先验证用户，确认目标用户后再充值');
        return;
    }

    const amount = parseFloat(document.getElementById('rechargeAmountInput').value);
    // 🔥 支持负数（用于修正充值金额）
    if (!amount || amount === 0) {
        await customAlert('请输入有效的充值金额（支持负数）');
        return;
    }

    if (!await customConfirm(`确认给用户 ${currentRechargeUserId} 充值 ${amount} 积分吗？`)) {
        return;
    }

    try {
        // 服务器管理页面已通过密码验证，无需额外token
        const headers = { 'Content-Type': 'application/json' };
        const response = await fetch(`${API_BASE_URL}/admin/users/${currentRechargeUserId}/recharge`, {
            method: 'POST',
            headers: headers,
            body: JSON.stringify({
                amount: amount
            })
        });

        const data = await response.json();
        if (data.success) {
            const amountDisplay = amount >= 0 ? `+${amount.toFixed(2)}` : amount.toFixed(2);
            await customAlert(`充值成功！${amountDisplay} 积分，用户当前余额: ${data.credits.toFixed(2)}`);
            document.getElementById('rechargeAmountInput').value = '';
            // 🔥 充值后清空验证状态，必须重新验证才能再次充值
            currentRechargeUserId = null;
            document.getElementById('rechargeUserInfo').style.display = 'none';
            // 刷新当前用户的充值记录（如果已验证）
            if (currentRechargeUserId) {
                await verifyRechargeUser();
            } else {
                // 显示所有充值记录
                await loadAllRechargeRecords();
            }
        } else {
            const errorMsg = data.message || '未知错误';
            await customAlert('充值失败: ' + errorMsg);
        }
    } catch (error) {
        console.error('充值失败:', error);
        await customAlert('充值失败: ' + error.message);
    }
}

async function updateServerManagerPassword() {
    const oldPassword = document.getElementById('oldPasswordInput').value.trim();
    const newPassword = document.getElementById('newPasswordInput').value.trim();

    if (!oldPassword) {
        await customAlert('请输入旧密码');
        document.getElementById('oldPasswordInput').focus();
        return;
    }

    if (!newPassword) {
        await customAlert('请输入新密码');
        document.getElementById('newPasswordInput').focus();
        return;
    }

    try {
        const response = await fetch(`${API_BASE_URL}/server-manager/password`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                oldPassword: oldPassword,
                password: newPassword
            })
        });

        const data = await response.json();

        if (data.success) {
            setTimeout(async () => {
                await customAlert('修改成功');
                closePasswordChangeModal();
            }, 100);
        } else {
            await customAlert(data.message || '更新失败，请检查旧密码是否正确');
            document.getElementById('oldPasswordInput').focus();
        }
    } catch (error) {
        console.error('更新服务器管理密码失败:', error);
        await customAlert('网络错误，请检查API服务器连接');
    }
}

async function updateAdminPassword() {
    const oldPassword = document.getElementById('adminOldPasswordInput').value.trim();
    const newPassword = document.getElementById('adminNewPasswordInput').value.trim();

    if (!oldPassword) {
        await customAlert('请输入旧密码');
        document.getElementById('adminOldPasswordInput').focus();
        return;
    }

    if (!newPassword) {
        await customAlert('请输入新密码');
        document.getElementById('adminNewPasswordInput').focus();
        return;
    }

    // 获取当前登录的管理员ID（从变量或localStorage）
    let managerId = currentManagerId;
    if (!managerId) {
        managerId = localStorage.getItem('currentManagerId');
    }
    if (!managerId) {
        await customAlert('未找到当前管理员ID，请重新登录');
        return;
    }

    try {
        // 先验证旧密码（通过登录接口）
        const loginResponse = await fetch(`${API_BASE_URL}/admin/login`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                admin_id: managerId,
                password: oldPassword
            })
        });

        const loginData = await loginResponse.json();
        if (!loginData.success) {
            await customAlert('旧密码错误');
            document.getElementById('adminOldPasswordInput').focus();
            return;
        }

        // 更新密码
        const response = await fetch(`${API_BASE_URL}/admin/account/${managerId}`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                password: newPassword
            })
        });

        const data = await response.json();

        if (data.success) {
            setTimeout(async () => {
                await customAlert('修改成功');
                closeAdminPasswordChangeModal();
            }, 100);
        } else {
            await customAlert(data.message || '更新失败');
            document.getElementById('adminOldPasswordInput').focus();
        }
    } catch (error) {
        console.error('更新管理员密码失败:', error);
        await customAlert('网络错误，请检查API服务器连接');
    }
}

async function editAdminAccount(adminId) {
    const account = adminAccounts.find(a => a.id === adminId);
    if (!account) return;

    const newPassword = await customPrompt('请输入新密码:', account.password);
    if (newPassword && newPassword.trim()) {
        account.password = newPassword.trim();
        updateAdminAccountDisplay();
    }
}

async function deleteAdminAccount(adminId) {
    const confirmed = await customConfirm('确定要删除该管理员账户吗？');
    if (confirmed) {
        try {
            const response = await fetch(`${API_BASE_URL}/admin/account/${adminId}`, {
                method: 'DELETE',
                headers: { 'Content-Type': 'application/json' }
            });

            if (response.ok) {
                adminAccounts = adminAccounts.filter(a => a.id !== adminId);

                // 🔥 记录已删除的ID，防止僵尸账号复活
                try {
                    const deletedIds = JSON.parse(localStorage.getItem('deletedAdminIds') || '[]');
                    if (!deletedIds.includes(adminId)) {
                        deletedIds.push(adminId);
                        localStorage.setItem('deletedAdminIds', JSON.stringify(deletedIds));
                    }
                } catch (e) {
                    console.error('记录已删除ID失败:', e);
                }

                // 🔥 即时保存到localStorage
                try {
                    localStorage.setItem('adminAccounts', JSON.stringify(adminAccounts));
                } catch (error) {
                    console.error('保存管理员账号到localStorage失败:', error);
                }
                updateAdminAccountDisplay();
            } else {
                const errorData = await response.json();
                await customAlert(errorData.message || '删除管理员账号失败');
            }
        } catch (error) {
            await customAlert('无法连接到API服务器: ' + error.message);
        }
    }
}

function updateAdminAccountDisplay() {
    const container = document.getElementById('adminAccountList');
    const fragment = document.createDocumentFragment();
    container.innerHTML = '';
    adminAccounts.forEach(account => {
        const item = document.createElement('div');
        item.className = 'admin-account-item';
        const serverCount = (account.selectedServers && account.selectedServers.length) || 0;
        item.innerHTML = `
            <span class="admin-account-name">
                <span class="admin-account-badge ${serverCount > 0 ? 'flash' : ''}">${serverCount}</span>
                ${account.id}
            </span>
            <div class="admin-account-actions">
                <button class="admin-account-action-btn manage" onclick="manageAdminAccount('${account.id}')">管理</button>
            </div>
        `;
        fragment.appendChild(item);
    });
    container.appendChild(fragment);
}

let tempSelectedServers = []; // Temporary state for admin management
function manageAdminAccount(adminId) {
    const account = adminAccounts.find(a => a.id === adminId);
    if (!account) return;

    // Initialize temp state
    tempSelectedServers = [...(account.selectedServers || [])];

    // 只显示已连接的服务器
    const allServerObjs = [...serverData.connected];
    const allServers = allServerObjs.map(s => s.name || s.server_name || s.server_id);

    // 收集所有其他管理员已分配的服务器 (REAL state, not temp)
    const assignedServersMap = new Map(); // serverName -> adminId
    adminAccounts.forEach(acc => {
        if (acc.id !== adminId && Array.isArray(acc.selectedServers)) {
            acc.selectedServers.forEach(s => assignedServersMap.set(String(s).trim(), acc.id));
        }
    });

    // 收集已分配给用户的服务器
    serverData.connected.forEach(server => {
        if (server.assigned_user_id) {
            assignedServersMap.set(String(server.name || server.server_name || server.server_id).trim(), 'USER');
        }
    });

    const panel = document.getElementById('customModalPanel');
    panel.classList.add('admin-manage-modal');

    const content = document.getElementById('customModalContent');
    content.className = 'admin-manage-content';

    const titleEl = document.getElementById('customModalTitle');
    const messageEl = document.getElementById('customModalMessage');
    const buttonsEl = document.getElementById('customModalButtons');
    const inputEl = document.getElementById('customModalInput');

    // 获取该管理员的总业绩（如果存在）
    const performanceDisplay = account.totalPerformance ? account.totalPerformance : '0.00';

    titleEl.style.display = 'flex';
    titleEl.style.alignItems = 'center';
    titleEl.style.width = '100%';

    titleEl.innerHTML = `
        <span>管理员: <span class="admin-id-badge">${account.id}</span></span>
        <span class="admin-performance-badge">业绩: ${performanceDisplay}</span>
    `;

    messageEl.innerHTML = `
        <div style="display: flex; justify-content: space-between; align-items: center; padding: 10px 15px; background: rgba(0,0,0,0.05); border-radius: 8px; margin-bottom: 10px;">
            <div style="display: flex; gap: 25px; align-items: center;">
                <span><strong style="color:#666;">ID:</strong> ${account.id}</span>
                <span><strong style="color:#666;">密码:</strong> <span style="font-family:monospace;">${account.password || '***'}</span></span>
            </div>
            <div style="display: flex; gap: 10px;">
                <button class="admin-account-action-btn edit" onclick="editAdminPasswordInModal('${adminId}')" style="padding:5px 12px;font-size:12px;">修改密码</button>
                <button class="admin-account-action-btn delete" onclick="deleteAdminInModal('${adminId}')" style="padding:5px 12px;font-size:12px;">删除账号</button>
            </div>
        </div>
        <div style="display: flex; gap: 25px; align-items: center; padding: 8px 15px; margin-bottom: 15px; color: #666; font-size: 13px;">
            <span><strong>推广用户:</strong> ${account.userCount || 0}</span>
            <span><strong>费率:</strong> ${account.rate || '-'}</span>
            <span><strong>上次访问:</strong> ${account.lastAccess || '-'}</span>
        </div>
        <div style="font-size: 14px; font-weight: bold; color: #333; margin-bottom: 10px; padding-left: 15px;">私享服务器授权</div>
        <div class="server-buttons-grid" id="adminManageServersGrid" style="margin: 0 15px; width: calc(100% - 30px);">
            ${allServers.length > 0 ? (() => {
            // 分离可用服务器和已分配服务器
            const availableServers = [];
            const assignedToThisAdmin = [];
            const assignedToOthers = [];

            allServers.forEach(server => {
                const serverStr = String(server).trim();
                const assignedOwner = assignedServersMap.get(serverStr);
                const isSelected = tempSelectedServers.some(selected => String(selected).trim() === serverStr);

                if (isSelected) {
                    assignedToThisAdmin.push(server);
                } else if (assignedOwner) {
                    assignedToOthers.push({ server, owner: assignedOwner });
                } else {
                    availableServers.push(server);
                }
            });

            // 生成服务器按钮HTML
            const generateServerBtn = (server, assignedOwner = null) => {
                const serverStr = String(server).trim();
                const isSelected = tempSelectedServers.some(selected => String(selected).trim() === serverStr);
                const escapedServer = serverStr.replace(/"/g, '&quot;').replace(/'/g, '&#39;').replace(/\\/g, '\\\\');
                const portMatch = (allServerObjs.find(s => (s.name || s.server_name || s.server_id) === serverStr)?.url || '').match(/:(\d+)/);
                const port = portMatch ? portMatch[1] : (allServerObjs.find(s => (s.name || s.server_name || s.server_id) === serverStr)?.port || serverStr.match(/\d+/)?.[0] || '?');
                const statusText = isSelected ? '状态: 已选中' : '状态: 已连接';
                const botHTML = SERVER_BOT_HTML;

                if (assignedOwner) {
                    return '<button class="server-button connected private" disabled style="cursor: not-allowed; pointer-events: auto;" data-server-name="' + escapedServer + '">' + botHTML + '<div class="server-button-name" style="position: absolute; bottom: -20px; left: 50%; transform: translateX(-50%); font-size: 11px; color: #e1bee7; white-space: nowrap; pointer-events: none; z-index: 100;">' + port + '</div><div class="server-tooltip"><div style="font-weight: bold; margin-bottom: 4px;">' + escapedServer + '</div><div style="font-size: 11px; color: #ffeb3b; margin-top: 4px;" class="status-text">私享服务器</div><div style="font-size: 11px; color: #fff; margin-top: 2px;">已分配: ' + assignedOwner + '</div></div></button>';
                }

                return '<button class="server-button connected ' + (isSelected ? 'selected' : '') + '" data-server-name="' + escapedServer + '" onclick="toggleTempServerSelection(\'' + adminId + '\', \'' + escapedServer + '\', this)">' + botHTML + '<div class="server-button-name" style="position: absolute; bottom: -20px; left: 50%; transform: translateX(-50%); font-size: 11px; color: #2d3436; white-space: nowrap; pointer-events: none; z-index: 100;">' + port + '</div><div class="server-tooltip"><div style="font-weight: bold; margin-bottom: 4px;">' + escapedServer + '</div><div style="font-size: 11px; color: ' + (isSelected ? '#ffd700' : '#00ff88') + '; margin-top: 4px;" class="status-text">' + statusText + '</div></div></button>';
            };

            let html = '';
            // 先显示可用服务器
            availableServers.forEach(server => { html += generateServerBtn(server); });
            // 已分配给当前管理员的服务器
            assignedToThisAdmin.forEach(server => { html += generateServerBtn(server); });
            // 已分配给其他人的服务器在最后
            assignedToOthers.forEach(item => { html += generateServerBtn(item.server, item.owner); });

            return html;
        })() : '<div style="color: #999; padding: 20px; text-align: center; width: 100%;">暂无可用服务器</div>'}
        </div>
    `;
    inputEl.style.display = 'none';

    buttonsEl.innerHTML = `
        <button class="admin-manage-footer-btn cancel" onclick="closeCustomModal()">取消</button>
        <button class="admin-manage-footer-btn reset" onclick="resetAdminSelectionTemp('${adminId}')">重置</button>
        <button class="admin-manage-footer-btn select-all" onclick="selectAllServersTemp('${adminId}')">全选</button>
        <button class="admin-manage-footer-btn confirm" onclick="confirmAdminManage('${adminId}')">确定保存</button>
    `;

    const modal = document.getElementById('customModal');
    requestAnimationFrame(() => {
        modal.classList.add('show');
    });
}

function toggleTempServerSelection(adminId, serverName, button) {
    const index = tempSelectedServers.indexOf(serverName);
    if (index > -1) {
        // 取消选择
        tempSelectedServers.splice(index, 1);
        button.classList.remove('selected');
    } else {
        // Add to temp state
        tempSelectedServers.push(serverName);
        button.classList.add('selected');
    }
    // Update UI immediate feedback
    const statusText = button.querySelector('.status-text');
    if (statusText) {
        statusText.textContent = button.classList.contains('selected') ? '状态: 已选中' : '状态: 已连接';
        statusText.style.color = button.classList.contains('selected') ? '#ffd700' : '#00ff88';
    }
}


function resetAdminSelectionTemp(adminId) {
    tempSelectedServers = [];
    const grid = document.getElementById('adminManageServersGrid');
    const buttons = grid.querySelectorAll('.server-button');
    buttons.forEach(btn => {
        btn.classList.remove('selected');
        const statusText = btn.querySelector('.status-text');
        if (statusText) {
            statusText.textContent = '状态: 已连接';
            statusText.style.color = '#00ff88';
        }
    });
}

function selectAllServersTemp(adminId) {
    // 只选择已连接的服务器
    const allConnectedServers = serverData.connected.map(s => {
        return s.name || s.server_name || s.server_id || String(s);
    });

    // 过滤掉已被其他管理员分配的服务器 (REAL state)
    const availableServers = allConnectedServers.filter(serverName => {
        // Check Global State for occupancy (since we shouldn't steal from others unless we save, 
        // but visually we want to only select free ones)

        const isAssignedToOther = adminAccounts.some(acc => {
            // Check against real state of others
            return acc.id !== adminId &&
                Array.isArray(acc.selectedServers) &&
                acc.selectedServers.some(s => String(s).trim() === String(serverName).trim());
        });

        const isAssignedToUser = serverData.connected.some(server => {
            return server.assigned_user_id &&
                String(server.name || server.server_name || server.server_id).trim() === String(serverName).trim();
        });

        return !isAssignedToOther && !isAssignedToUser;
    });

    // Update Temp State
    tempSelectedServers = [...availableServers];

    // Update UI
    const grid = document.getElementById('adminManageServersGrid');
    if (grid) {
        const buttons = grid.querySelectorAll('.server-button');
        buttons.forEach(btn => {
            if (btn.classList.contains('private')) return; // Skip private ones

            let serverName = btn.dataset.serverName;
            if (serverName && availableServers.some(s => String(s).trim() === String(serverName).trim())) {
                btn.classList.add('selected');
                const statusText = btn.querySelector('.status-text');
                if (statusText) {
                    statusText.textContent = '状态: 已选中';
                    statusText.style.color = '#ffd700';
                }
            } else if (serverName) {
                // If not in available (e.g. was available but we are unselecting it? No, select all selects all available)
                // But if we have previously selected something that is NOT valid?
                // "Select All" usually implies selecting everything visible/valid.
            }
        });
    }
}




async function confirmAdminManage(adminId) {
    const account = adminAccounts.find(a => a.id === adminId);
    if (!account) return;

    const grid = document.getElementById('adminManageServersGrid');
    if (grid) {
        const selectedButtons = grid.querySelectorAll('.server-button.selected');
        const selectedServers = Array.from(selectedButtons).map(btn => {
            // 优先从 data-server-name 属性获取
            if (btn.dataset.serverName) {
                return btn.dataset.serverName.trim();
            }
            // 从tooltip中获取服务器名称
            const tooltip = btn.querySelector('.server-tooltip');
            if (tooltip) {
                const nameDiv = tooltip.querySelector('div[style*="font-weight: bold"]');
                if (nameDiv) return nameDiv.textContent.trim();
            }
            return (btn.textContent || '').trim();
        }).filter(Boolean);

        // 验证唯一性：检查是否有服务器被其他管理员分配
        const conflicts = [];
        selectedServers.forEach(serverName => {
            const isAssignedToOther = adminAccounts.some(acc => {
                return acc.id !== adminId &&
                    Array.isArray(acc.selectedServers) &&
                    acc.selectedServers.some(s => String(s).trim() === String(serverName).trim());
            });
            if (isAssignedToOther) {
                conflicts.push(serverName);
            }
        });

        if (conflicts.length > 0) {
            await customAlert(`以下服务器已被其他管理员分配，无法重复分配：\n${conflicts.join(', ')}`);
            return;
        }

        account.selectedServers = selectedServers;
    }

    try {
        localStorage.setItem('adminAccounts', JSON.stringify(adminAccounts));
    } catch (error) {
        console.error('保存管理员账号到localStorage失败:', error);
    }

    try {
        // 服务器管理页面已通过密码验证，无需额外token
        const response = await fetch(`${API_BASE_URL}/admin/account/${adminId}`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                selected_servers: account.selectedServers || []
            })
        });

        if (!response.ok) {
            const err = await response.json().catch(() => ({}));
            console.warn('保存管理员账号配置到数据库失败，但已保存到本地', response.status, err);
        }
    } catch (error) {
        console.warn('无法连接到API服务器保存管理员账号配置，但已保存到本地:', error);
    }

    closeCustomModal();

    updateAdminAccountDisplay();

    setTimeout(async () => {
        await customAlert('管理员配置已保存');
    }, 300);
}

async function editAdminPasswordInModal(adminId) {
    const account = adminAccounts.find(a => a.id === adminId);
    if (!account) return;

    const newPassword = await customPrompt('请输入新密码:', account.password);
    if (newPassword && newPassword.trim()) {
        account.password = newPassword.trim();
        try {
            localStorage.setItem('adminAccounts', JSON.stringify(adminAccounts));
        } catch (error) {
            console.error('保存管理员账号失败:', error);
        }
        setTimeout(() => {
            manageAdminAccount(adminId);
        }, 350);
    }
}

async function deleteAdminInModal(adminId) {
    const confirmed = await customConfirm('确定要删除该管理员账户吗？');
    if (!confirmed) {
        return;
    }

    // 🔥 先调用API删除，成功后再删除本地数据
    try {
        const response = await fetch(`${API_BASE_URL}/admin/account/${adminId}`, {
            method: 'DELETE',
            headers: {
                'Content-Type': 'application/json'
            }
        });

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            await customAlert(`删除失败：${errorData.message || response.statusText || '未知错误'}`);
            return;
        }

        // API删除成功，再删除本地数据
        adminAccounts = adminAccounts.filter(a => a.id !== adminId);

        try {
            localStorage.setItem('adminAccounts', JSON.stringify(adminAccounts));
        } catch (error) {
            console.error('保存管理员账号到localStorage失败:', error);
        }

        closeCustomModal();
        updateAdminAccountDisplay();
        await customAlert('管理员账号已删除');
    } catch (error) {
        console.error('删除管理员账号失败:', error);
        await customAlert(`无法连接到API服务器: ${error.message}`);
    }
}

//#endregion
