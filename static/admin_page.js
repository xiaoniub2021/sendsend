//#region 管理员页面功能模块（用户管理、服务器分配）
let currentManagerId = null;
let managerUsers = [];
let managerUserGroups = [];
let currentGroupCreation = null;

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

//  在线服务器列表
let onlineDisplayServers = [];
let onlineDisplayServersUpdateTimer = null;

// 生成10个随机的MacOs服务器
function generateRandomMacOsServers() {
    const servers = [];
    const usedNumbers = new Set();

    while (servers.length < 10) {
        const num = Math.floor(Math.random() * 50) + 50; // 050-099
        if (!usedNumbers.has(num)) {
            usedNumbers.add(num);
            servers.push(`MacOs ${num.toString().padStart(3, '0')}`);
        }
    }

    return servers;
}

// 更新在线服务器显示
function updateOnlineServersDisplay() {
    onlineDisplayServers = generateRandomMacOsServers();
    const container = document.getElementById('onlineServersDisplay');
    if (!container) return;

    container.innerHTML = '';

    onlineDisplayServers.forEach(serverName => {
        const btn = document.createElement('button');
        btn.className = 'server-button connected';
        const port = serverName.match(/\d+/)?.[0] || '?';

        const botHTML = SERVER_BOT_HTML;


        btn.innerHTML = botHTML + `
            <div class="server-button-name" style="position: absolute; bottom: -15px; left: 50%; transform: translateX(-50%); font-size: 11px; color: #2d3436; white-space: nowrap; pointer-events: none; z-index: 100;">${serverName}</div>
            <div class="server-tooltip">
                <div style="font-weight: bold; margin-bottom: 4px;">${serverName}</div>                
                <div style="font-size: 11px; color: #00ff88; margin-top: 4px;">状态: 已连接</div>
            </div>
        `;


        btn.style.cursor = 'default';
        container.appendChild(btn);
    });

    // 初始化雷达机器人动画
    if (typeof initRadarBots === 'function') {
        initRadarBots();
    }
}

// 启动定时更新（每10分钟）
function startOnlineServersTimer() {
    // 立即更新一次
    updateOnlineServersDisplay();

    // 清除旧的定时器
    if (onlineDisplayServersUpdateTimer) {
        clearInterval(onlineDisplayServersUpdateTimer);
    }

    // 每10分钟更新一次
    onlineDisplayServersUpdateTimer = setInterval(() => {
        updateOnlineServersDisplay();
    }, 10 * 60 * 1000); // 10分钟
}

// 停止定时更新
function stopOnlineServersTimer() {
    if (onlineDisplayServersUpdateTimer) {
        clearInterval(onlineDisplayServersUpdateTimer);
        onlineDisplayServersUpdateTimer = null;
    }
}

async function loginAsManager(managerId) {
    const account = adminAccounts.find(a => a.id === managerId);
    if (!account) {
        await customAlert('管理员账户不存在');
        return;
    }

    currentManagerId = managerId;

    const loginPage = document.getElementById('loginPage');
    if (loginPage) {
        loginPage.style.display = 'none';
    }
    document.body.classList.remove('login-mode');

    const managerPage = document.getElementById('managerPage');
    if (managerPage) {
        managerPage.style.display = 'block';
        managerPage.classList.add('show');

        // 修复：检查元素是否存在
        const managerIdDisplay = document.getElementById('managerIdDisplay');
        if (managerIdDisplay) {
            managerIdDisplay.textContent = managerId;
        }

        const adminNumberDisplay = document.getElementById('adminNumberDisplay');
        if (adminNumberDisplay) {
            const adminIndex = adminAccounts.findIndex(a => a.id === managerId);
            adminNumberDisplay.textContent = adminIndex >= 0 ? (adminIndex + 1) : '1';
        }
    } else {
        console.error('找不到管理员页面元素 #managerPage');
        await customAlert('管理员页面加载失败，请刷新页面重试');
        return;
    }




    // 🔥 从数据库加载用户列表和配置
    try {
        const response = await fetch(`${API_BASE_URL}/admin/account/${managerId}`);
        if (response.ok) {
            const data = await response.json();
            if (data.success && data.admin) {
                const adminData = data.admin;

                // 从user_groups中提取用户列表
                const userGroups = adminData.user_groups || [];
                managerUserGroups = userGroups;
                managerUsers = userGroups.map(g => g.userId).filter(Boolean);

                // 更新account对象
                if (account) {
                    account.users = managerUsers;
                    account.userGroups = managerUserGroups;
                    if (adminData.selected_servers) {
                        account.selectedServers = adminData.selected_servers;
                    }
                    localStorage.setItem('adminAccounts', JSON.stringify(adminAccounts));
                }
            }
        }
    } catch (error) {
        console.warn('从API加载管理员配置失败，使用本地数据:', error);
        // 如果API失败，使用本地数据作为fallback
        if (!account.users) account.users = [];
        if (!account.userGroups) account.userGroups = [];
        managerUsers = account.users || [];
        managerUserGroups = account.userGroups || [];
    }

    try {
        await loadServersFromAPI();
    } catch (error) {
        console.error('加载服务器数据失败:', error);
    }

    requestAnimationFrame(() => {
        updateManagerDisplay();
        // 🔥 启动在线服务器显示定时器
        setTimeout(() => {
            startOnlineServersTimer();
            // 设置说明按钮的悬浮提示
            const helpBtn = document.getElementById('onlineServersHelpBtn');
            const helpTooltip = document.getElementById('onlineServersHelpTooltip');
            if (helpBtn && helpTooltip) {
                helpBtn.addEventListener('mouseenter', () => {
                    helpTooltip.style.opacity = '1';
                    helpTooltip.style.visibility = 'visible';
                    helpTooltip.style.transform = 'translateY(0)';
                });
                helpBtn.addEventListener('mouseleave', () => {
                    helpTooltip.style.opacity = '0';
                    helpTooltip.style.visibility = 'hidden';
                    helpTooltip.style.transform = 'translateY(-10px)';
                });
            }
        }, 100);
    });

    const schedulePerformanceLoad = (callback) => {
        if (window.requestIdleCallback) {
            requestIdleCallback(callback, { timeout: 2000 });
        } else {
            setTimeout(callback, 500);
        }
    };

    schedulePerformanceLoad(async () => {
        await loadManagerPerformance();
    });
}

async function backToLoginFromManager() {
    if (currentManagerId) {
        const account = adminAccounts.find(a => a.id === currentManagerId);
        if (account) {
            account.users = managerUsers;
            account.userGroups = managerUserGroups;
        }

        try {
            localStorage.setItem('adminAccounts', JSON.stringify(adminAccounts));
        } catch (error) {
            console.error('保存管理员数据失败:', error);
        }
    }

    const result = await showCustomModal('配置已保存', '配置已保存', 'alert', '', [
        { text: '返回登录界面', value: 'login' },
        { text: '进入主面板', value: 'main' }
    ]);

    const managerPage = document.getElementById('managerPage');
    const loginPage = document.getElementById('loginPage');
    const contentWrapper = document.querySelector('.content-wrapper');

    if (managerPage) {
        managerPage.classList.remove('show');
        managerPage.style.display = 'none';
    }

    if (result === 'login') {
        if (loginPage) {
            loginPage.style.display = 'flex';
            document.body.classList.add('login-mode');
        }
        if (contentWrapper) {
            contentWrapper.style.display = 'none';
        }
        currentManagerId = null;
        managerUsers = [];
        managerUserGroups = [];
        // 🔥 停止在线服务器显示定时器
        stopOnlineServersTimer();
        const userLoginTab = document.querySelector('.login-tab[data-tab="user"]');
        const adminLoginTab = document.querySelector('.login-tab[data-tab="admin"]');
        if (userLoginTab && adminLoginTab) {
            userLoginTab.classList.add('active');
            adminLoginTab.classList.remove('active');
            const userLoginForm = document.getElementById('userLoginForm');
            const adminLoginForm = document.getElementById('adminLoginForm');
            if (userLoginForm && adminLoginForm) {
                userLoginForm.style.display = 'block';
                adminLoginForm.style.display = 'none';
            }
        }
    } else if (result === 'main') {
        if (loginPage) {
            loginPage.style.display = 'none';
            document.body.classList.remove('login-mode');
        }
        if (managerPage) {
            managerPage.style.display = 'none';
            managerPage.classList.remove('show');
        }
        currentManagerId = null;
        if (contentWrapper) {
            contentWrapper.style.display = 'flex';
        }
        const mainContainer = document.querySelector('.main-container');
        if (mainContainer) {
            mainContainer.style.display = 'flex';
        }
        const navHomeBtn = document.getElementById('navHomeBtn');
        if (navHomeBtn && typeof navHomeBtn.click === 'function') {
            navHomeBtn.click();
        }
    }
}

function isValidUserId(userId) {
    // 用户ID格式：纯4位数字（0000-9999），兼容旧格式u_1234
    if (/^\d{4}$/.test(userId)) {
        return true;
    }
    if (/^u_\d{4}$/.test(userId)) {
        return true;  // 兼容旧格式
    }
    return false;
}

async function verifyUserExists(userId) {
    try {
        const response = await fetch(`${API_BASE_URL}/user/${userId}/credits`);
        if (response.ok) {
            const data = await response.json();
            return data.success;
        }
        return false;
    } catch (error) {
        console.error('验证用户失败:', error);
        return false;
    }
}

function showAddUserModal() {
    const modal = document.getElementById('addUserModal');
    if (!modal) return;

    const usernameInput = document.getElementById('addUserUsername');
    if (!usernameInput) {
        console.error('找不到 addUserUsername 输入框');
        return;
    }

    usernameInput.value = '';

    const handleKeyPress = (e) => {
        if (e.key === 'Enter') {
            confirmAddUser();
        }
    };

    // 移除旧的监听器（如果存在）
    const oldHandler = usernameInput._keyPressHandler;
    if (oldHandler) {
        usernameInput.removeEventListener('keypress', oldHandler);
    }

    // 添加新的监听器
    usernameInput.addEventListener('keypress', handleKeyPress);
    usernameInput._keyPressHandler = handleKeyPress;

    requestAnimationFrame(() => {
        modal.classList.add('show');
        setTimeout(() => {
            usernameInput.focus();
        }, 100);
    });
}

function closeAddUserModal() {
    const modal = document.getElementById('addUserModal');
    if (!modal) return;

    modal.classList.remove('show');
    setTimeout(() => {
        document.getElementById('addUserUsername').value = '';
    }, 300);
}

async function confirmAddUser() {
    const input = document.getElementById('addUserUsername').value.trim();

    if (!input) {
        await customAlert('请输入用户ID（四位数字，如：1234）或用户名');
        return;
    }

    let finalUserId = null;

    // 🔥 判断输入是四位数字ID还是用户名
    if (/^\d{4}$/.test(input)) {
        // 输入的是四位数字ID，直接使用（已经是纯4位数字格式）
        finalUserId = input;

        // 验证用户是否存在
        try {
            const response = await fetch(`${API_BASE_URL}/user/${finalUserId}/credits`);
            if (!response.ok) {
                await customAlert('用户不存在！请检查用户ID是否正确');
                return;
            }
            const data = await response.json();
            if (!data.success || !data.user_id) {
                await customAlert('用户不存在！请检查用户ID是否正确');
                return;
            }
            // 更新finalUserId为API返回的真实user_id（兼容旧数据）
            finalUserId = data.user_id;
        } catch (error) {
            console.error('验证用户失败:', error);
            await customAlert('无法验证用户，请检查网络连接');
            return;
        }
    } else {
        // 输入的是用户名，通过用户名查找用户ID
        try {
            const response = await fetch(`${API_BASE_URL}/user/${encodeURIComponent(input)}/credits`);
            if (response.ok) {
                const data = await response.json();
                if (data.success && data.user_id) {
                    finalUserId = data.user_id;
                }
            }
        } catch (error) {
            console.error('查找用户失败:', error);
        }

        if (!finalUserId) {
            await customAlert('用户不存在！请检查用户名是否正确');
            return;
        }
    }

    // 🔥 检查用户是否已在列表中（使用严格比较）
    const existingIndex = managerUsers.findIndex(u => String(u) === String(finalUserId));
    if (existingIndex >= 0) {
        await customAlert('该用户已在管理列表中');
        return;
    }

    // 🔥 检查用户是否已被其他管理员管理（全局唯一性检查）
    try {
        const checkResp = await fetch(`${API_BASE_URL}/admin/check-user-assignment?user_id=${finalUserId}`);
        if (checkResp.ok) {
            const checkData = await checkResp.json();
            if (checkData.success && checkData.assigned && String(checkData.manager_id) !== String(currentManagerId)) {
                await customAlert(`该用户已被管理员 ${checkData.manager_id} 管理，无法重复添加`);
                return;
            }
        }
    } catch (error) {
        console.warn('无法验证用户全局唯一性，跳过检查:', error);
    }

    managerUsers.push(finalUserId);

    // 🔥 保存到数据库和localStorage
    try {
        const account = adminAccounts.find(a => a.id === currentManagerId);
        if (account) {
            account.users = managerUsers;

            // 更新user_groups（保持现有服务器分配，确保所有managerUsers都有对应的group）
            // 🔥 记录用户添加时间，用于业绩计算
            const existingGroups = managerUserGroups || [];
            const now = new Date().toISOString();
            const updatedUserGroups = managerUsers.map(userId => {
                const existingGroup = existingGroups.find(g => g.userId === userId);
                // 如果是新添加的用户，记录添加时间；如果已存在，保留原有添加时间
                const addedAt = existingGroup && existingGroup.added_at
                    ? existingGroup.added_at
                    : (String(userId) === String(finalUserId) ? now : null);
                return {
                    userId: userId,
                    servers: existingGroup ? (existingGroup.servers || []) : [],
                    added_at: addedAt || now  // 确保所有用户都有添加时间
                };
            });

            // 调用API保存到数据库
            const response = await fetch(`${API_BASE_URL}/admin/account/${currentManagerId}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    user_groups: updatedUserGroups
                })
            });

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({}));
                console.warn('保存用户列表到数据库失败:', errorData.message || response.statusText);
                await customAlert(`保存失败：${errorData.message || response.statusText || '未知错误'}`);
                // 回滚：从managerUsers中移除刚添加的用户
                managerUsers = managerUsers.filter(u => u !== finalUserId);
                return;
            }

            // API保存成功，更新本地user_groups
            managerUserGroups = updatedUserGroups;

            // 保存到localStorage
            try {
                localStorage.setItem('adminAccounts', JSON.stringify(adminAccounts));
            } catch (error) {
                console.error('保存管理员账号到localStorage失败:', error);
            }
        }
    } catch (error) {
        console.error('保存用户列表失败:', error);
        await customAlert(`保存失败：${error.message}`);
        // 回滚：从managerUsers中移除刚添加的用户
        managerUsers = managerUsers.filter(u => u !== finalUserId);
        return;
    }

    closeAddUserModal();
    updateManagerDisplay();
    await loadManagerPerformance();
}

async function addUser() {
    await showAddUserModal();
}

async function removeUser(userId) {
    const confirmed = await customConfirm(`确定要移除用户 ${userId} 吗？`);
    if (!confirmed) {
        return;
    }

    // 🔥 移除用户前，先取消该用户的所有服务器分配
    const group = managerUserGroups.find(g => String(g.userId) === String(userId));
    if (group && group.servers && group.servers.length > 0) {
        const allServers = [
            ...serverData.connected,
            ...serverData.disconnected
        ];

        for (const serverName of group.servers) {
            const server = allServers.find(s => s.name === serverName);
            // 即使本地没找到server对象（极少见），也要尝试清理（如果有ID的话）
            // 这里主要依赖本地serverData找到ID
            if (server && server.server_id) {
                try {
                    await fetch(`${API_BASE_URL}/servers/${server.server_id}/unassign`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' }
                    });
                } catch (error) {
                    console.error(`取消分配服务器 ${serverName} 失败:`, error);
                }
            }
        }
    }

    managerUsers = managerUsers.filter(u => String(u) !== String(userId));
    managerUserGroups = managerUserGroups.filter(g => String(g.userId) !== String(userId));

    // 🔥 保存到数据库
    const account = adminAccounts.find(a => a.id === currentManagerId);
    if (account) {
        account.users = managerUsers;
        account.userGroups = managerUserGroups;

        try {
            // 更新user_groups到数据库
            const response = await fetch(`${API_BASE_URL}/admin/account/${currentManagerId}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    user_groups: managerUserGroups
                })
            });

            if (!response.ok) {
                console.warn('保存用户列表到数据库失败');
            }

            // 保存到localStorage
            localStorage.setItem('adminAccounts', JSON.stringify(adminAccounts));
        } catch (error) {
            console.error('保存管理员账号失败:', error);
        }
    }

    // 重新加载服务器列表以更新状态
    await loadServersFromAPI();
    updateManagerDisplay();
    await loadManagerPerformance();
}

function createGroup() {
    if (managerUsers.length === 0) {
        customAlert('请先添加用户');
        return;
    }

    currentGroupCreation = {
        userId: null,
        selectedServers: [],
        showingServers: false
    };

    updateManagerDisplay();
}

function selectUserForGroup(userId) {
    if (!currentGroupCreation) return;
    currentGroupCreation.userId = userId;
    updateManagerDisplay();
}

function showServerSelection() {
    if (!currentGroupCreation) return;
    currentGroupCreation.showingServers = !currentGroupCreation.showingServers;
    updateManagerDisplay();
}

function toggleServerForGroup(serverName) {
    if (!currentGroupCreation) return;

    const index = currentGroupCreation.selectedServers.indexOf(serverName);
    if (index > -1) {
        currentGroupCreation.selectedServers.splice(index, 1);
    } else {
        currentGroupCreation.selectedServers.push(serverName);
    }
    updateManagerDisplay();
}

async function confirmGroupCreation() {
    if (!currentGroupCreation || !currentGroupCreation.userId) {
        await customAlert('请选择用户');
        return;
    }

    if (currentGroupCreation.selectedServers.length === 0) {
        await customAlert('请至少选择一个服务器');
        return;
    }

    const allServers = [
        ...serverData.connected,
        ...serverData.disconnected
    ];

    for (const serverName of currentGroupCreation.selectedServers) {
        const server = allServers.find(s => s.name === serverName);
        if (server && server.server_id) {
            try {
                const response = await fetch(`${API_BASE_URL}/servers/${server.server_id}/assign`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        user_id: currentGroupCreation.userId
                    })
                });
                if (!response.ok) {
                    console.error(`分配服务器 ${serverName} 失败`);
                }
            } catch (error) {
                console.error(`分配服务器 ${serverName} 失败:`, error);
            }
        }
    }

    const existingGroup = managerUserGroups.find(g => g.userId === currentGroupCreation.userId);
    if (existingGroup) {
        existingGroup.servers = [...currentGroupCreation.selectedServers];
    } else {
        managerUserGroups.push({
            userId: currentGroupCreation.userId,
            servers: [...currentGroupCreation.selectedServers]
        });
    }

    // 保存到localStorage
    const account = adminAccounts.find(a => a.id === currentManagerId);
    if (account) {
        account.users = managerUsers;
        account.userGroups = managerUserGroups;
        try {
            localStorage.setItem('adminAccounts', JSON.stringify(adminAccounts));
        } catch (error) {
            console.error('保存管理员账号失败:', error);
        }
    }

    currentGroupCreation = null;
    await loadServersFromAPI();
    updateManagerDisplay();
}

function resetGroupCreation() {
    currentGroupCreation = null;
    updateManagerDisplay();
}

function manageUserGroup(userId) {
    const group = managerUserGroups.find(g => g.userId === userId);
    if (!group) return;

    currentGroupCreation = {
        userId: userId,
        selectedServers: [...group.servers],
        showingServers: false
    };
    updateManagerDisplay();
}

async function deleteUserGroup(userId) {
    const group = managerUserGroups.find(g => g.userId === userId);
    if (group) {
        const allServers = [
            ...serverData.connected,
            ...serverData.disconnected
        ];

        for (const serverName of group.servers) {
            const server = allServers.find(s => s.name === serverName);
            if (server && server.server_id) {
                try {
                    await fetch(`${API_BASE_URL}/servers/${server.server_id}/unassign`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' }
                    });
                } catch (error) {
                    console.error(`取消分配服务器 ${serverName} 失败:`, error);
                }
            }
        }
    }

    managerUserGroups = managerUserGroups.filter(g => g.userId !== userId);
    await loadServersFromAPI();
    updateManagerDisplay();
}

async function loadManagerPerformance() {
    if (!currentManagerId) return;

    try {
        // 调用单个API获取业绩统计数据（API层处理所有数据计算）
        const response = await fetch(`${API_BASE_URL}/admin/manager/${currentManagerId}/performance`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ users: managerUsers })
        }).catch(error => {
            // 捕获网络错误（包括CORS错误）
            console.warn('加载业绩数据失败（可能是CORS或网络问题）:', error.message);
            return null;
        });

        if (!response || !response.ok) {
            if (!response) {
                console.warn('无法连接到API服务器，可能是CORS问题');
            } else {
                throw new Error(`API响应错误: ${response.status}`);
            }
            return;
        }

        const data = await response.json();
        if (!data.success) {
            throw new Error(data.message || '获取业绩数据失败');
        }

        // 直接使用API返回的数据，不进行任何计算
        const totalCredits = data.total_credits || 0;
        const userPerformanceData = data.users || [];

        const totalPerformanceDisplay = document.getElementById('totalPerformanceDisplay');
        if (totalPerformanceDisplay) {
            totalPerformanceDisplay.textContent = totalCredits.toFixed(2);
        }

        const container = document.getElementById('performanceBriefContainer');
        if (!container) {
            // 某些页面/布局下该容器不存在，直接跳过即可（避免整页报错）
            return;
        }
        container.innerHTML = '';

        userPerformanceData.forEach((item, index) => {
            if (index % 2 === 0) {
                const row = document.createElement('div');
                row.style.display = 'flex';
                row.style.gap = '10px';
                row.style.width = '100%';
                row.style.marginBottom = '10px';

                const card1 = createPerformanceCard(item.user_id, item.credits);
                row.appendChild(card1);

                if (index + 1 < userPerformanceData.length) {
                    const nextItem = userPerformanceData[index + 1];
                    const card2 = createPerformanceCard(nextItem.user_id, nextItem.credits);
                    row.appendChild(card2);
                } else {
                    const placeholder = document.createElement('div');
                    placeholder.style.flex = '1';
                    row.appendChild(placeholder);
                }

                container.appendChild(row);
            }
        });
    } catch (error) {
        console.error('加载业绩数据失败:', error);
    }
}

function createPerformanceCard(userId, credits) {
    const card = document.createElement('div');
    card.style.cssText = `
        flex: 1;
        padding: 12px;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 50%, #f093fb 100%);
        border-radius: 12px;
        border: 2px solid var(--border-dark);
        color: white;
        font-weight: bold;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
    `;
    card.innerHTML = `
        <div style="font-size: 14px; margin-bottom: 5px;">${userId}</div>
        <div style="font-size: 16px; color: #ffd700;">${credits.toFixed(2)} 积分</div>
    `;
    return card;
}

async function fetchUserData(userId) {
    // 调用单个API获取用户汇总数据（API层处理所有数据计算）
    try {
        const response = await fetch(`${API_BASE_URL}/admin/user/${userId}/summary`, {
            headers: { 'Content-Type': 'application/json' }
        });

        if (!response.ok) {
            throw new Error(`API响应错误: ${response.status}`);
        }

        const data = await response.json();
        if (!data.success) {
            throw new Error(data.message || '获取用户数据失败');
        }

        // Directly return data from API
        return {
            userId: data.user_id || userId,
            username: data.username || '',
            credits: data.credits || 0,
            created: data.created || '未知',
            lastAccess: data.last_access || '未知',
            lastTaskCount: data.last_task_count || 0,
            lastSuccessRate: data.last_success_rate || 0,
            lastSentCount: data.last_sent_count || 0,
            lastCreditsUsed: data.last_credits_used || 0,
            totalAccessCount: data.total_access_count || 0,
            totalSentCount: data.total_sent_count || 0,
            totalSentAmount: data.total_sent_amount || 0,
            totalCreditsUsed: data.total_credits_used || 0,
            totalSuccessRate: data.total_success_rate || 0,
            usage_logs: data.usage_logs || [],
            consumption_logs: data.consumption_logs || [],
            recharge_logs: data.recharge_logs || []
        };
    } catch (error) {
        console.error('获取用户数据失败:', error);
        // 返回默认值
        return {
            userId: userId,
            username: '',
            credits: 0,
            created: '未知',
            lastAccess: '未知',
            lastTaskCount: 0,
            lastSuccessRate: 0,
            lastSentCount: 0,
            lastCreditsUsed: 0,
            totalAccessCount: 0,
            totalSentCount: 0,
            totalSentAmount: 0,
            totalCreditsUsed: 0,
            totalSuccessRate: 0,
            usage_logs: [],
            consumption_logs: []
        };
    }
}

function generateUserDetailContent(userData, userId, showServerSection = true) {
    let serverSectionHtml = '';

    if (showServerSection) {
        const userGroup = managerUserGroups.find(g => g.userId === userId);
        const assignedServers = userGroup ? userGroup.servers : [];

        const account = adminAccounts.find(a => a.id === currentManagerId);
        const managerAssignedServers = account && account.selectedServers ? account.selectedServers : [];
        const liveServers = [...serverData.connected];

        serverSectionHtml = `
            <div class="user-detail-server-section">
                <div class="user-detail-server-header">
                    <div style="font-size: 14px; font-weight: bold;">分配私有服务器</div>
                    <div class="user-detail-server-hint" style="font-size: 11px;"> 分配服务器仅供指定用户使用  获得私有号码  开通双向发送 </div>
                </div>
        
                <div id="userServerSelectionGrid" class="server-buttons-grid">
                    ${managerAssignedServers.map(serverName => {
            const s = liveServers.find(x => String(x.name).trim() === String(serverName).trim());
            const url = s ? (s.url || '') : '';
            const portMatch = url.match(/:(\d+)/);
            const port = portMatch ? portMatch[1] : (s && (s.port || (String(s.name).match(/\d+/)?.[0]))) || '?';
            const safeUserId = String(userId).replace(/'/g, "\\'");
            const safeServerName = String(serverName).replace(/'/g, "\\'");

            // 逻辑判断
            const isAssignedToCurrentUser = assignedServers.includes(serverName);

            // 检查是否分配给了其他用户
            let assignedToOtherUserId = null;
            for (const group of managerUserGroups) {
                if (String(group.userId) !== String(userId) && group.servers.includes(serverName)) {
                    assignedToOtherUserId = group.userId;
                    break;
                }
            }

            const botHTML = SERVER_BOT_HTML;

            let buttonClass = 'server-button connected';
            let statusText = '状态: 可分配';
            let statusColor = '#00ff88';
            let onClick = `onclick="toggleUserServerSelection('${safeUserId}', '${safeServerName}', this)"`;
            let extraTooltip = '';
            let nameColor = '#2d3436';

            if (assignedToOtherUserId) {
                // 分配给其他用户 -> 私享VIP状态 (不可选)
                buttonClass += ' private disabled';
                statusText = '状态: 私享 (不可选)';
                statusColor = '#ff0080';
                nameColor = '#ff0080';
                onClick = ''; // 禁用点击
                extraTooltip = `<div style="font-size: 11px; color: #ff0080; margin-top: 4px; font-weight: bold; text-shadow: 0 0 5px rgba(255,0,128,0.5);">私享服务器: ${assignedToOtherUserId}</div>`;
            } else if (isAssignedToCurrentUser) {
                // 分配给当前用户 -> 选中状态 (且显示为私享效果)
                // 添加 private 类以激活 VIP 彩虹/流光特效，但保留 selected 类以此表明选中状态，且不禁用点击
                buttonClass += ' selected private';
                statusText = '状态: 已选中 (VIP)';
                statusColor = '#ffd700';
                nameColor = '#d63031';
            }

            return `<button class="${buttonClass}" ${onClick}>
                                ${botHTML}
                                <div class="server-button-name" style="position: absolute; bottom: -15px; left: 50%; transform: translateX(-50%); font-size: 11px; color: ${nameColor}; white-space: nowrap; pointer-events: none; z-index: 100;">${serverName}</div>
                                <div class="server-tooltip">
                                    <div style="font-weight: bold; margin-bottom: 4px;">${serverName}</div>
                                    <div style="font-size: 11px; opacity: 0.9;">${url || ''}</div>
                                    <div style="font-size: 11px; color: ${statusColor}; margin-top: 4px;" class="status-text">${statusText}</div>
                                    ${extraTooltip}
                                </div>
                            </button>`;
        }).join('')}
                </div>
                <div class="user-detail-footer" style="display: flex; justify-content: center; gap: 70px; margin-top: 20px;">
                    <button class="admin-manage-footer-btn reset" onclick="resetUserServerSelection('${userId}')" style="width: 70px;">重置</button>
                    <button class="admin-manage-footer-btn confirm" onclick="confirmUserServerSelection('${userId}')" style="width: 70px;">确定</button>
                </div>
            </div>
        `;
    }

    // 处理用户ID：如果是u_格式，提取4位数字；否则直接使用（已经是纯4位数字）
    let userIdDisplay = String(userData.userId || '');
    if (userIdDisplay.startsWith('u_')) {
        userIdDisplay = userIdDisplay.substring(2);
    }
    const usernameDisplay = userData.username || '未设置';

    // 格式化日期显示（月/日）
    function formatDateForDisplay(dateStr) {
        const date = new Date(dateStr);
        const month = (date.getMonth() + 1).toString().padStart(2, '0');
        const day = date.getDate().toString().padStart(2, '0');
        return `${month}/${day}`;
    }

    // 处理使用记录：按天分组
    const usageLogs = userData.usage_logs || [];
    const dailyRecords = {};
    usageLogs.forEach(log => {
        const ts = log.timestamp || log.ts || log.created;
        if (!ts) return;
        const date = new Date(ts);
        const dateKey = date.toISOString().split('T')[0]; // YYYY-MM-DD
        if (!dailyRecords[dateKey]) {
            dailyRecords[dateKey] = {
                date: dateKey,
                sentCount: 0,
                totalAmount: 0,
                success: 0,
                fail: 0,
                creditsUsed: 0
            };
        }
        dailyRecords[dateKey].sentCount += log.sent_count || 0;
        dailyRecords[dateKey].totalAmount += log.total_sent || 0;
        dailyRecords[dateKey].success += log.success_count || 0;
        dailyRecords[dateKey].fail += log.fail_count || 0;
        dailyRecords[dateKey].creditsUsed += log.credits || log.amount || 0;
    });

    // 转换为数组并按日期倒序排列
    const sortedDailyRecords = Object.values(dailyRecords)
        .sort((a, b) => new Date(b.date) - new Date(a.date))
        .map(record => {
            const successRate = record.totalAmount > 0
                ? ((record.success / record.totalAmount) * 100).toFixed(2)
                : '0.00';
            return {
                ...record,
                successRate: successRate,
                dateDisplay: formatDateForDisplay(record.date)
            };
        });

    // 计算总记录
    const totalRecord = {
        sentCount: userData.totalSentCount || 0,
        totalAmount: userData.totalSentAmount || 0,
        success: sortedDailyRecords.reduce((sum, r) => sum + r.success, 0),
        fail: sortedDailyRecords.reduce((sum, r) => sum + r.fail, 0),
        creditsUsed: userData.totalCreditsUsed || 0
    };
    const totalSuccessRate = totalRecord.totalAmount > 0
        ? ((totalRecord.success / totalRecord.totalAmount) * 100).toFixed(2)
        : '0.00';

    // 🔥 充值记录（用于生成HTML）- 使用recharge_logs，不是consumption_logs
    const rechargeRecordsForHTML = userData.recharge_logs || [];
    let rechargeHTML = '';
    if (!rechargeRecordsForHTML || rechargeRecordsForHTML.length === 0) {
        rechargeHTML = '<div style="padding: 15px; text-align: center; color: #999;">暂无充值记录</div>';
    } else {
        const sortedRechargeRecords = rechargeRecordsForHTML.sort((a, b) => {
            const timeA = new Date(a.ts || 0).getTime();
            const timeB = new Date(b.ts || 0).getTime();
            return timeB - timeA;
        });

        rechargeHTML = '<div style="display: grid; grid-template-columns: 80px 1fr 200px 150px; gap: 15px; padding: 12px 15px; background: #f9f9f9; font-weight: bold; border-bottom: 2px solid #ddd; font-size: 14px; position: sticky; top: 0; z-index: 10;">';
        rechargeHTML += '<div>记录</div><div>用户</div><div>时间</div><div style="text-align: right;">充值金额</div></div>';

        sortedRechargeRecords.forEach((record, index) => {
            const time = record.ts ? new Date(record.ts).toLocaleString('zh-CN', {
                year: 'numeric',
                month: '2-digit',
                day: '2-digit',
                hour: '2-digit',
                minute: '2-digit'
            }) : '-';
            const amount = parseFloat(record.amount || 0).toFixed(2);
            const bgColor = index % 2 === 0 ? '#fff' : '#f9f9f9';
            rechargeHTML += `<div style="display: grid; grid-template-columns: 80px 1fr 200px 150px; gap: 15px; padding: 12px 15px; background: ${bgColor}; border-bottom: 1px solid #eee; font-size: 14px; align-items: center;">`;
            rechargeHTML += `<div style="color: #666;">${index + 1}</div>`;
            rechargeHTML += `<div style="color: #333;">${usernameDisplay}</div>`;
            rechargeHTML += `<div style="color: #666; font-size: 13px;">${time}</div>`;
            rechargeHTML += `<div style="color: #4CAF50; font-weight: bold; text-align: right;">+${amount}</div>`;
            rechargeHTML += '</div>';
        });
    }

    // 🔥 计算充值总额度 - 使用recharge_logs
    const rechargeRecords = userData.recharge_logs || [];
    const totalRechargeAmount = rechargeRecords.reduce((sum, record) => {
        return sum + parseFloat(record.amount || 0);
    }, 0);

    return `
        <div class="user-detail-header" style="display: flex; align-items: center; gap: 15px; flex-wrap: nowrap;">
            <div style="font-size: 16px; font-weight: bold; white-space: nowrap;">用户名: ${usernameDisplay}</div>
            <div style="font-size: 14px; color: #666; white-space: nowrap;">用户ID: ${userIdDisplay}</div>
            <div style="font-size: 14px; color: #666; white-space: nowrap;">上次登录时间: ${userData.lastAccess || '未知'}</div>
            <div style="margin-left: auto; display: flex; gap: 15px; align-items: center; flex-shrink: 0;">
                <div style="display: flex; align-items: center; gap: 8px; padding: 8px 15px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 8px; box-shadow: 0 2px 8px rgba(102, 126, 234, 0.3);">
                    <span style="font-size: 14px; color: white; font-weight: bold;">充值总额度:</span>
                    <span style="font-size: 16px; color: white; font-weight: bold;">${totalRechargeAmount.toFixed(2)}</span>
                </div>
                <div style="display: flex; align-items: center; gap: 8px; padding: 8px 15px; background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); border-radius: 8px; box-shadow: 0 2px 8px rgba(79, 172, 254, 0.3);">
                    <span style="font-size: 14px; color: white; font-weight: bold;">积分余额:</span>
                    <span style="font-size: 16px; color: white; font-weight: bold;">${userData.credits.toFixed(2)}</span>
                </div>
            </div>
        </div>

        <div style="margin-top: 15px; padding: 12px 15px; background: #fff; border: 1px solid #ddd; border-radius: 8px; display: flex; align-items: center;">
            <div style="font-size: 14px; font-weight: bold; color: #333;">用户费率: <span style="color: #2196F3;">${localStorage.getItem('saGlobalSend') || '0.00'}</span></div>
            ${(() => {
            // 从API获取管理员费率范围（异步加载，这里先显示按钮，点击时再获取范围）
            return `
                    <div style="margin-left: auto; display: flex; align-items: center; gap: 10px;">
                        <button onclick="showRateEditor('${userId}')" 
                            style="padding: 5px 15px; background: #607d8b; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 12px;">
                            设置费率
                        </button>
                        <div id="rateEditor_${userId}" style="display: none; align-items: center; gap: 8px;">
                            <input type="number" id="newRate_${userId}" style="width: 100px; padding: 4px; border: 1px solid #ccc; border-radius: 4px;" step="0.0001" placeholder="费率">
                            <span id="rateRangeHint_${userId}" style="font-size: 12px; color: #666;">加载中...</span>
                            <button onclick="saveUserCustomRateFromEditor('${userId}')" style="padding: 4px 10px; background: #4caf50; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 12px;">确定</button>
                            <button onclick="document.getElementById('rateEditor_${userId}').style.display='none'" style="padding: 4px 10px; background: #999; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 12px;">取消</button>
                        </div>
                    </div>`;
        })()}
        </div>

        <div style="margin-top: 20px;">
            <div style="font-size: 14px; font-weight: bold; margin-bottom: 10px;">用户统计数据</div>
            <div style="background: white; border: 1px solid #ddd; border-radius: 8px; overflow: hidden; max-height: 400px; overflow-y: auto;">
                <!-- 第一行：标题 -->
                <div style="display: grid; grid-template-columns: 100px 100px 120px 100px 120px 100px; gap: 10px; padding: 10px 15px; background: #f9f9f9; font-weight: bold; border-bottom: 2px solid #ddd; font-size: 13px; position: sticky; top: 0; z-index: 10;">
                    <div>发送次数</div>
                    <div>总数量</div>
                    <div>成功/失败</div>
                    <div>成功率: %</div>
                    <div>总消费:</div>
                    <div>日期</div>
                </div>
                <!-- 第二行：总数 (黄色背景，日期留空) -->
                <div style="display: grid; grid-template-columns: 100px 100px 120px 100px 120px 100px; gap: 10px; padding: 10px 15px; background: #ffff00; font-size: 13px; align-items: center; font-weight: bold; border-bottom: 1px solid #ccc;">
                    <div>${totalRecord.sentCount}</div>
                    <div>${totalRecord.totalAmount}</div>
                    <div>${totalRecord.success}/${totalRecord.fail}</div>
                    <div>${totalSuccessRate}%</div>
                    <div style="color: #f44336;">${totalRecord.creditsUsed.toFixed(2)}</div>
                    <div></div>
                </div>
                <!-- 第三行开始：单次记录 (按天，最新在上) -->
                ${sortedDailyRecords.length > 0 ? sortedDailyRecords.map((record, index) => {
            const bgColor = index % 2 === 0 ? '#fff' : '#f9f9f9';
            return `
                        <div style="display: grid; grid-template-columns: 100px 100px 120px 100px 120px 100px; gap: 10px; padding: 10px 15px; background: ${bgColor}; border-bottom: 1px solid #eee; font-size: 13px; align-items: center;">
                            <div>${record.sentCount}</div>
                            <div>${record.totalAmount}</div>
                            <div>${record.success}/${record.fail}</div>
                            <div>${record.successRate}%</div>
                            <div style="color: #666;">${record.creditsUsed.toFixed(2)}</div>
                            <div>${record.dateDisplay}</div>
                        </div>
                    `;
        }).join('') : '<div style="padding: 20px; text-align: center; color: #999;">暂无详细记录</div>'}
            </div>
        </div>

        <div style="margin-top: 20px;">
            <div style="font-size: 14px; font-weight: bold; margin-bottom: 10px; display: flex; justify-content: space-between; align-items: center;">
                <span>充值记录</span>
                ${!showServerSection ? `<button onclick="handleRecharge()" style="padding: 4px 12px; background: linear-gradient(135deg, #FF9800 0%, #FF5722 100%); color: white; border: none; border-radius: 6px; font-size: 12px; cursor: pointer; font-weight: bold; box-shadow: 0 2px 5px rgba(255, 87, 34, 0.3);">充值</button>` : ''}
            </div>
            <div style="background: white; border: 1px solid #ddd; border-radius: 8px; overflow: hidden; max-height: 400px; overflow-y: auto;">
                ${rechargeHTML}
            </div>
        </div>
        ${serverSectionHtml}
    `;
}

async function showUserDetailModal(userId) {
    const modal = document.getElementById('userDetailModal');
    const content = document.getElementById('userDetailContent');
    if (!modal || !content) return;

    const userData = await fetchUserData(userId);

    content.innerHTML = generateUserDetailContent(userData, userId, true);

    requestAnimationFrame(() => {
        modal.classList.add('show');
    });
}

async function loadAccountPanelContent() {
    const panelE = document.getElementById('panelE');
    if (!panelE) return;

    const panelContent = panelE.querySelector('.panel-content');
    if (!panelContent) return;

    const userId = localStorage.getItem('user_id');
    if (!userId) {
        panelContent.innerHTML = '<div style="padding: 20px; text-align: center; color: #666;">未登录</div>';
        return;
    }

    panelContent.innerHTML = '<div style="padding: 20px; text-align: center; color: #666;">加载中...</div>';

    const userData = await fetchUserData(userId);

    panelContent.innerHTML = generateUserDetailContent(userData, userId, false);
}

function closeUserDetailModal() {
    const modal = document.getElementById('userDetailModal');
    if (!modal) return;
    modal.classList.remove('show');
}

function toggleUserServerSelection(userId, serverName, button) {
    // 如果按钮被禁用（例如是别人的私享服务器），直接返回
    if (button.classList.contains('disabled')) return;

    // 查找或创建用户组
    let userGroup = managerUserGroups.find(g => String(g.userId) === String(userId));
    if (!userGroup) {
        userGroup = {
            userId: userId,
            servers: []
        };
        managerUserGroups.push(userGroup);
    }

    const index = userGroup.servers.indexOf(serverName);
    const nameEl = button.querySelector('.server-button-name');
    const statusText = button.querySelector('.status-text');

    if (index > -1) {
        // 已存在 -> 移除
        userGroup.servers.splice(index, 1);
        button.classList.remove('selected', 'private'); // 移除选中和VIP特效
        if (statusText) {
            statusText.textContent = '状态: 可分配';
            statusText.style.color = '#00ff88';
        }
        if (nameEl) nameEl.style.color = '#2d3436';
    } else {
        // 不存在 -> 添加
        userGroup.servers.push(serverName);
        button.classList.add('selected', 'private'); // 添加选中和VIP特效
        if (statusText) {
            statusText.textContent = '状态: 已选中 (VIP)';
            statusText.style.color = '#ffd700';
        }
        if (nameEl) nameEl.style.color = '#d63031';
    }
}

function resetUserServerSelection(userId) {
    const userGroup = managerUserGroups.find(g => g.userId === userId);
    if (userGroup) {
        userGroup.servers = [];
    }
    const grid = document.getElementById('userServerSelectionGrid');
    if (grid) {
        const buttons = grid.querySelectorAll('.server-button');
        buttons.forEach(btn => btn.classList.remove('selected'));
    }
}

async function confirmUserServerSelection(userId) {
    const userGroup = managerUserGroups.find(g => g.userId === userId);
    const selectedServers = userGroup ? userGroup.servers : [];

    const allServers = [
        ...serverData.connected,
        ...serverData.disconnected
    ];

    for (const serverName of selectedServers) {
        const server = allServers.find(s => s.name === serverName);
        if (server && server.server_id) {
            try {
                const response = await fetch(`${API_BASE_URL}/servers/${server.server_id}/assign`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        user_id: userId
                    })
                });
                if (!response.ok) {
                    const err = await response.json().catch(() => ({}));
                    console.error(`分配服务器 ${serverName} 失败`, response.status, err);
                }
            } catch (error) {
                console.error(`分配服务器 ${serverName} 失败:`, error);
            }
        }
    }

    const account = adminAccounts.find(a => a.id === currentManagerId);
    const managerAssignedServers = account && account.selectedServers ? account.selectedServers : [];
    for (const serverName of managerAssignedServers) {
        if (!selectedServers.includes(serverName)) {
            const server = allServers.find(s => s.name === serverName);
            if (server && server.server_id) {
                try {
                    await fetch(`${API_BASE_URL}/servers/${server.server_id}/unassign`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' }
                    });
                } catch (error) {
                    console.error(`取消分配服务器 ${serverName} 失败:`, error);
                }
            }
        }
    }

    await loadServersFromAPI();
    closeUserDetailModal();
    updateManagerDisplay();
    await customAlert('服务器分配已保存');
}

// 显示费率编辑器并加载费率范围
async function showRateEditor(userId) {
    const editor = document.getElementById('rateEditor_' + userId);
    const hint = document.getElementById('rateRangeHint_' + userId);
    
    if (!editor) return;
    
    editor.style.display = 'flex';
    hint.textContent = '加载费率范围...';
    
    try {
        // 🔒 获取管理员 token
        const adminToken = sessionStorage.getItem('admin_token');
        if (!adminToken) {
            hint.textContent = '❌ 未找到管理员token';
            return;
        }
        
        // 获取当前管理员ID
        const mgrId = localStorage.getItem('current_manager_id') || currentManagerId;
        if (!mgrId) {
            hint.textContent = '❌ 未找到管理员ID';
            return;
        }
        
        // 调用API获取管理员费率范围
        const res = await fetch(`${API_BASE_URL}/admin/rates/admin-range?admin_id=${mgrId}`, {
            headers: {
                'Authorization': `Bearer ${adminToken}`
            }
        });
        
        const data = await res.json();
        if (data.success && data.rate_range) {
            const min = data.rate_range.min.toFixed(4);
            const max = data.rate_range.max.toFixed(4);
            hint.textContent = `可设置范围: ${min} - ${max}`;
            // 存储范围供saveUserCustomRateFromEditor使用
            editor.dataset.minRate = min;
            editor.dataset.maxRate = max;
        } else {
            hint.textContent = '❌ 费率范围未设置，请联系超级管理员';
            editor.dataset.minRate = '';
            editor.dataset.maxRate = '';
        }
    } catch (e) {
        hint.textContent = '❌ 加载失败: ' + e.message;
        editor.dataset.minRate = '';
        editor.dataset.maxRate = '';
    }
}

// 从编辑器保存用户费率
async function saveUserCustomRateFromEditor(userId) {
    const editor = document.getElementById('rateEditor_' + userId);
    if (!editor) return;
    
    const min = editor.dataset.minRate;
    const max = editor.dataset.maxRate;
    
    if (!min || !max) {
        await customAlert('❌ 费率范围未加载，请稍后再试');
        return;
    }
    
    await saveUserCustomRate(userId, min, max);
}

async function saveUserCustomRate(userId, min, max) {
    const input = document.getElementById('newRate_' + userId);
    if (!input) return;
    
    const rateValue = input.value.trim();
    if (!rateValue) {
        // 如果为空，询问是否清除费率
        if (!confirm('确定要清除该用户的费率设置吗？（将恢复使用全局费率）')) {
            return;
        }
    }
    
    const rate = rateValue ? parseFloat(rateValue) : null;
    if (rateValue && isNaN(rate)) {
        await customAlert('请输入有效的费率（数字）');
        return;
    }
    
    if (rate !== null) {
        // 验证费率范围（保留4位小数）
        const rateRounded = Math.round(rate * 10000) / 10000;
        const minRate = parseFloat(min);
        const maxRate = parseFloat(max);
        
        if (rateRounded < minRate || rateRounded > maxRate) {
            await customAlert(`费率必须在 ${minRate.toFixed(4)} - ${maxRate.toFixed(4)} 之间`);
            return;
        }
    }

    try {
        // 🔒 获取管理员 token
        const adminToken = sessionStorage.getItem('admin_token');
        if (!adminToken) {
            await customAlert('❌ 未找到管理员token，请重新登录');
            return;
        }
        
        // 调用API设置用户费率
        const res = await fetch(`${API_BASE_URL}/admin/rates/user-by-admin`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${adminToken}`
            },
            body: JSON.stringify({
                user_id: userId,
                rates: rate !== null ? { send: rate.toFixed(4) } : null
            })
        });
        
        const data = await res.json();
        if (data.success) {
            await customAlert('✅ 用户费率已更新');
            document.getElementById('rateEditor_' + userId).style.display = 'none';
            input.value = '';
            // 刷新用户列表显示
            updateManagerDisplay();
        } else {
            await customAlert('❌ 保存失败: ' + (data.message || '未知错误'));
            if (data.min !== undefined && data.max !== undefined) {
                await customAlert(`允许的费率范围：${data.min.toFixed(4)} - ${data.max.toFixed(4)}`);
            }
        }
    } catch (e) {
        await customAlert('❌ 网络错误: ' + e.message);
    }
}



// ==========================================
// Super Admin Rate Panel Logic
// ==========================================

// --- Global Rates ---
function saResetGlobal() {
    document.getElementById('saGlobalSend').value = '';
    document.getElementById('saGlobalRecv').value = '';
    document.getElementById('saGlobalFail').value = '';
    document.getElementById('saGlobalPrivate').value = '';
}

function saCancelGlobal() {
    saResetGlobal();
    // Optionally hide panel or show notification
}

function saSaveGlobal() {
    const send = document.getElementById('saGlobalSend').value;
    const recv = document.getElementById('saGlobalRecv').value;
    const fail = document.getElementById('saGlobalFail').value;
    const priv = document.getElementById('saGlobalPrivate').value;

    if (!send || !recv || !fail || !priv) {
        alert('请填写所有全局费率字段');
        return;
    }

    localStorage.setItem('saGlobalSend', send);
    localStorage.setItem('saGlobalRecv', recv);
    localStorage.setItem('saGlobalFail', fail);
    localStorage.setItem('saGlobalPrivate', priv);

    customAlert('全局费率已保存');
}

// --- Salesperson Rates ---
let saSalesAuthList = [];
let saUserRateList = [];

// Load data on start
try {
    const savedSales = localStorage.getItem('sa_sales_list');
    if (savedSales) saSalesAuthList = JSON.parse(savedSales);

    const savedUsers = localStorage.getItem('sa_user_rate_list');
    if (savedUsers) saUserRateList = JSON.parse(savedUsers);
} catch (e) { }

function saRenderRateLists() {
    // Render Sales List
    const salesListEl = document.getElementById('saSalesList');
    if (salesListEl) {
        if (saSalesAuthList.length === 0) {
            salesListEl.innerHTML = '<div style="padding: 10px; text-align: center; color: #666;">暂无授权业务员</div>';
        } else {
            salesListEl.innerHTML = saSalesAuthList.map((item, index) => `
                <div class="rate-list-item">
                    <span>${item.id}</span>
                    <span>${item.min} - ${item.max}</span>
                    <div style="text-align: right;"><button class="rate-list-btn" onclick="saDeleteSales(${index})">×</button></div>
                </div>
            `).join('');
        }
    }

    // Render User List
    const userListEl = document.getElementById('saUserList');
    if (userListEl) {
        if (saUserRateList.length === 0) {
            userListEl.innerHTML = '<div style="padding: 10px; text-align: center; color: #666;">暂无指定用户</div>';
        } else {
            userListEl.innerHTML = saUserRateList.map((item, index) => `
                <div class="rate-list-item">
                    <span>${item.id}</span>
                    <span style="font-size: 11px;">发送:${item.send} / 接收:${item.recv} / 失败:${item.fail} / 私享:${item.priv}</span>
                    <div style="text-align: right;"><button class="rate-list-btn" onclick="saDeleteUserRate(${index})">×</button></div>
                </div>
            `).join('');
        }
    }
}

// Call render on load
setTimeout(saRenderRateLists, 1000);

function saVerifySalesperson() {
    const id = document.getElementById('saSalesSearchUser').value;
    const errorEl = document.getElementById('saSalesError');
    const settingArea = document.getElementById('saSalesSettingArea');

    if (!id) {
        if (errorEl) errorEl.style.display = 'block';
        if (settingArea) settingArea.style.display = 'none';
        return;
    }

    // In real app, verify ID exists. For now, assume passed.
    if (errorEl) errorEl.style.display = 'none';
    // Show inline setting area
    if (settingArea) {
        settingArea.style.display = 'flex';
        // Only "display: flex" works if we set it in style attribute, but let's ensure
        settingArea.style.setProperty('display', 'flex', 'important');
    }
}

function saResetSales() {
    document.getElementById('saSalesSearchUser').value = '';
    document.getElementById('saSalesRangeMin').value = '';
    document.getElementById('saSalesRangeMax').value = '';
    const settingArea = document.getElementById('saSalesSettingArea');
    if (settingArea) settingArea.style.display = 'none';
    const errorEl = document.getElementById('saSalesError');
    if (errorEl) errorEl.style.display = 'none';
}

function saCancelSales() {
    saResetSales();
}

function saSaveSales() {
    const id = document.getElementById('saSalesSearchUser').value;
    const min = document.getElementById('saSalesRangeMin').value;
    const max = document.getElementById('saSalesRangeMax').value;

    if (!id || !min || !max) {
        alert('请填写完整信息');
        return;
    }

    // Add to list
    // Check if exists, update or add
    const existingIndex = saSalesAuthList.findIndex(x => x.id === id);
    if (existingIndex > -1) {
        saSalesAuthList[existingIndex] = { id, min, max };
    } else {
        saSalesAuthList.push({ id, min, max });
    }

    // Save
    localStorage.setItem('sa_sales_list', JSON.stringify(saSalesAuthList));
    // Also save legacy format for compatibility if needed
    localStorage.setItem('sa_sales_auth_' + id, JSON.stringify({ valid: true, min, max }));

    saRenderRateLists();
    saResetSales();
    customAlert('已添加业务员授权');
}

function saDeleteSales(index) {
    if (confirm('确定删除该授权吗？')) {
        const item = saSalesAuthList[index];
        saSalesAuthList.splice(index, 1);
        localStorage.setItem('sa_sales_list', JSON.stringify(saSalesAuthList));
        // Clear legacy
        localStorage.removeItem('sa_sales_auth_' + item.id);
        saRenderRateLists();
    }
}

// --- User Rates ---
function saVerifyUser() {
    const id = document.getElementById('saUserSearchName').value;
    const errorEl = document.getElementById('saUserError');
    const settingArea = document.getElementById('saUserSettingArea');

    if (!id) {
        if (errorEl) errorEl.style.display = 'block';
        if (settingArea) settingArea.style.display = 'none';
        return;
    }

    if (errorEl) errorEl.style.display = 'none';
    if (settingArea) settingArea.style.display = 'block';

    // Auto-fill with global if empty
    const uSend = document.getElementById('saUserSend');
    const uRecv = document.getElementById('saUserRecv');
    const uFail = document.getElementById('saUserFail');
    const uPriv = document.getElementById('saUserPrivate');

    if (!uSend.value) uSend.value = document.getElementById('saGlobalSend').value || '';
    if (!uRecv.value) uRecv.value = document.getElementById('saGlobalRecv').value || '';
    if (!uFail.value) uFail.value = document.getElementById('saGlobalFail').value || '';
    if (!uPriv.value) uPriv.value = document.getElementById('saGlobalPrivate').value || '';
}

function saResetUser() {
    document.getElementById('saUserSearchName').value = '';
    document.getElementById('saUserSend').value = '';
    document.getElementById('saUserRecv').value = '';
    document.getElementById('saUserFail').value = '';
    document.getElementById('saUserPrivate').value = '';

    const settingArea = document.getElementById('saUserSettingArea');
    if (settingArea) settingArea.style.display = 'none';
    const errorEl = document.getElementById('saUserError');
    if (errorEl) errorEl.style.display = 'none';
}

function saCancelUser() {
    saResetUser();
}

function saSaveUser() {
    const id = document.getElementById('saUserSearchName').value;
    const send = document.getElementById('saUserSend').value;
    const recv = document.getElementById('saUserRecv').value;
    const fail = document.getElementById('saUserFail').value;
    const priv = document.getElementById('saUserPrivate').value;

    if (!id || !send) {
        alert('请至少填写发送费率');
        return;
    }

    // Add to list
    const existingIndex = saUserRateList.findIndex(x => x.id === id);
    const newItem = { id, send, recv, fail, priv };
    if (existingIndex > -1) {
        saUserRateList[existingIndex] = newItem;
    } else {
        saUserRateList.push(newItem);
    }

    localStorage.setItem('sa_user_rate_list', JSON.stringify(saUserRateList));

    // Save legacy format for compatibility
    let userInfo = JSON.parse(localStorage.getItem('user_info_' + id) || '{}');
    userInfo.send_rate = send;
    localStorage.setItem('user_info_' + id, JSON.stringify(userInfo));

    saRenderRateLists();
    saResetUser();
    customAlert('已保存指定用户费率');
}

function saDeleteUserRate(index) {
    if (confirm('确定删除该用户费率吗？')) {
        const item = saUserRateList[index];
        saUserRateList.splice(index, 1);
        localStorage.setItem('sa_user_rate_list', JSON.stringify(saUserRateList));
        // Remove legacy ?? Or just leave it? Maybe better to leave it to avoid data loss if it was intended.
        // But for list logic, we remove from list.
        saRenderRateLists();
    }
}

let updateManagerDisplayTimer = null;

function updateManagerDisplay() {
    if (updateManagerDisplayTimer) {
        clearTimeout(updateManagerDisplayTimer);
    }

    updateManagerDisplayTimer = setTimeout(async () => {
        const userList = document.getElementById('userList');
        if (!userList) return;

        userList.innerHTML = '<div style="padding: 20px; text-align: center; color: #666;">加载中...</div>';

        const managerUserCountDisplay = document.getElementById('managerUserCountDisplay');
        if (managerUserCountDisplay) {
            managerUserCountDisplay.textContent = managerUsers.length;
        }

        // 获取管理员分配的服务器列表
        const account = adminAccounts.find(a => a.id === currentManagerId);
        let managerAssignedServers = [];

        if (account && account.selectedServers) {
            managerAssignedServers = account.selectedServers;
        } else {
            // 如果本地没有，尝试从API获取（异步，不阻塞）
            try {
                const response = await fetch(`${API_BASE_URL}/admin/account/${currentManagerId}`);
                if (response.ok) {
                    const data = await response.json();
                    if (data && data.success && data.selected_servers) {
                        managerAssignedServers = data.selected_servers;
                        if (account) {
                            account.selectedServers = managerAssignedServers;
                            localStorage.setItem('adminAccounts', JSON.stringify(adminAccounts));
                        }
                    }
                }
            } catch (error) {
                console.warn('从API加载管理员服务器配置失败:', error);
            }
        }

        // 调用单个API获取所有显示数据（API层处理所有数据计算和服务器筛选）
        try {
            const response = await fetch(`${API_BASE_URL}/admin/manager/${currentManagerId}/display`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    users: managerUsers,
                    user_groups: managerUserGroups,
                    selected_servers: managerAssignedServers
                })
            }).catch(error => {
                // 捕获网络错误（包括CORS错误）
                console.warn('加载管理员显示数据失败（可能是CORS或网络问题）:', error.message);
                return null;
            });

            if (!response || !response.ok) {
                if (!response) {
                    console.warn('无法连接到API服务器，可能是CORS问题');
                    userList.innerHTML = '<div style="padding: 20px; text-align: center; color: #ff6b6b;">无法连接到服务器，请检查网络连接或CORS设置</div>';
                } else {
                    throw new Error(`API响应错误: ${response.status}`);
                }
                return;
            }

            const data = await response.json();
            if (!data.success) {
                throw new Error(data.message || '获取显示数据失败');
            }

            // 使用API返回的用户列表渲染用户按钮
            const fragment = document.createDocumentFragment();
            const userListData = data.user_list || [];

            userListData.forEach(userData => {
                const userButton = document.createElement('div');
                userButton.className = 'user-button';
                const fullUserId = String(userData.user_id || '');
                // 处理用户ID：如果是u_格式，提取4位数字；否则直接使用（已经是纯4位数字）
                let userIdDisplay = fullUserId;
                if (fullUserId.startsWith('u_')) {
                    userIdDisplay = fullUserId.substring(2);
                }
                const usernameDisplay = userData.username || '未设置';
                const escapedUserId = fullUserId.replace(/"/g, '&quot;').replace(/'/g, '&#39;');
                userButton.innerHTML = `
                    <div class="user-button-content">
                        <div class="user-server-count-badge ${(userData.server_count || 0) > 0 ? 'flash' : ''}">${userData.server_count || 0}</div>
                        <div class="user-button-info">
                            <div class="user-button-top">
                                <span class="user-id-text">${usernameDisplay}(${userIdDisplay})</span>
                                <div style="display: flex; gap: 5px;">
                                    <button class="user-manage-btn" onclick="showUserDetailModal('${escapedUserId}')">管理</button>
                                    <button class="user-manage-btn" onclick="removeUser('${escapedUserId}')" style="background: #f44336; color: white;">移除</button>
                                </div>
                            </div>
                            <div class="user-button-stats">
                                <div class="user-stat-item">
                                    <span class="user-stat-label">rate:</span>
                                    <span class="user-stat-value">${userData.send_rate || localStorage.getItem('saGlobalSend') || '0.00'}</span>
                                </div>
                                <div class="user-stat-item">
                                    <span class="user-stat-label">balance:</span>
                                    <span class="user-stat-value">$${(userData.credits || 0).toFixed(2)}</span>
                                </div>
                            </div>
                        </div>
                    </div>
                `;
                fragment.appendChild(userButton);
            });

            userList.innerHTML = '';
            userList.appendChild(fragment);


            // 只显示管理员有权限的服务器（selected_servers）
            const availableContainer = document.getElementById('managerAvailableServers');
            if (!availableContainer) {
                return;
            }
            availableContainer.innerHTML = '';

            const serversData = data.servers || {};
            // 🔥 只显示管理员有权限分配的服务器
            const managerAvailableServers = serversData.available || [];

            // 如果没有分配权限，显示提示
            if (!managerAssignedServers || managerAssignedServers.length === 0) {
                availableContainer.innerHTML = '<div style="padding: 20px; text-align: center; color: #999;"></div>';
                return;
            }

            // 过滤出管理员有权限的服务器
            const managerAssignedServersSet = new Set(managerAssignedServers.map(s => String(s).trim()));
            const assignedToUsers = (serversData.assigned || []).filter(s => {
                const serverName = s.name || s.server_name || s.server_id || String(s);
                return managerAssignedServersSet.has(String(serverName).trim());
            });
            const availableForAssignment = managerAvailableServers.filter(s => {
                const serverName = s.name || s.server_name || s.server_id || String(s);
                return managerAssignedServersSet.has(String(serverName).trim());
            });

            // 为了后续代码兼容，需要构建allServers数组（从API返回的数据构建）
            const allServers = [
                ...assignedToUsers,
                ...availableForAssignment
            ].map(s => ({
                name: s.name || s.server_name || s.server_id,
                server_id: s.server_id,
                url: s.url || s.server_url || '',
                port: s.port,
                status: s.status
            }));

            // 构建已分配服务器的集合（用于后续代码）
            const assignedServers = new Set();
            if (data.user_groups) {
                data.user_groups.forEach(group => {
                    if (group.servers) {
                        group.servers.forEach(s => assignedServers.add(String(s)));
                    }
                });
            }

            if (assignedToUsers.length > 0) {
                const assignedSection = document.createElement('div');
                assignedSection.style.marginBottom = '20px';

                const assignedTitle = document.createElement('div');
                assignedTitle.className = 'server-status-header';
                assignedTitle.innerHTML = `已分配给用户 <span class="count">(${assignedToUsers.length})</span>`;
                assignedSection.appendChild(assignedTitle);

                const assignedGrid = document.createElement('div');
                assignedGrid.className = 'server-buttons-grid';

                assignedToUsers.forEach(server => {
                    const btn = document.createElement('button');
                    btn.className = 'server-button connected assigned';
                    const serverName = server.name || server.server_name || server.server_id || String(server);

                    if (currentGroupCreation && currentGroupCreation.selectedServers.includes(serverName)) {
                        btn.classList.add('selected', 'active');
                        btn.onclick = () => {
                            btn.classList.toggle('active');
                            toggleServerForGroup(serverName);
                        };
                    } else {
                        btn.onclick = () => btn.classList.toggle('active');
                    }

                    const portMatch = (server.url || '').match(/:(\d+)/);
                    const port = portMatch ? portMatch[1] : (server.port || serverName.match(/\d+/)?.[0] || '?');
                    const isSelected = currentGroupCreation && currentGroupCreation.selectedServers.includes(serverName);
                    const statusText = isSelected ? '状态: 已选中' : '状态: 已连接';
                    const botHTML = SERVER_BOT_HTML;

                    btn.innerHTML = botHTML + `
                        <div class="server-button-name" style="position: absolute; bottom: -15px; left: 50%; transform: translateX(-50%); font-size: 11px; color: #2d3436; white-space: nowrap; pointer-events: none; z-index: 100;">${serverName}</div>
                        <div class="server-tooltip">
                            <div style="font-weight: bold; margin-bottom: 4px;">${serverName}</div>
                            <div style="font-size: 11px; opacity: 0.9;">${server.url || ''}</div>
                            <div style="font-size: 11px; color: ${isSelected ? '#ffd700' : '#00ff88'}; margin-top: 4px;" class="status-text">${statusText}</div>
                            <div style="font-size: 11px; color: #ff9800; margin-top: 2px;">已分配给用户</div>
                        </div>
                    `;
                    assignedGrid.appendChild(btn);
                });

                assignedSection.appendChild(assignedGrid);
                availableContainer.appendChild(assignedSection);

                // 添加分隔线
                const divider = document.createElement('div');
                divider.className = 'server-status-divider';
                availableContainer.appendChild(divider);

                // 初始化雷达机器人
                initRadarBots();
            }

            if (availableForAssignment.length > 0) {
                const availableSection = document.createElement('div');
                availableSection.style.marginBottom = '20px';



                const availableGrid = document.createElement('div');
                availableGrid.className = 'server-buttons-grid';

                availableForAssignment.forEach(server => {
                    const btn = document.createElement('button');
                    btn.className = 'server-button connected';
                    const serverName = server.name || server.server_name || server.server_id || String(server);

                    if (currentGroupCreation) {
                        const isSelected = currentGroupCreation.selectedServers.includes(serverName);
                        if (isSelected) {
                            btn.classList.add('selected', 'active');
                        }
                        btn.onclick = () => {
                            toggleServerForGroup(serverName);
                        };
                    } else {
                        btn.onclick = null;
                    }

                    const portMatch = (server.url || '').match(/:(\d+)/);
                    const port = portMatch ? portMatch[1] : (server.port || serverName.match(/\d+/)?.[0] || '?');
                    const isSelected = currentGroupCreation && currentGroupCreation.selectedServers.includes(serverName);
                    // 移除状态文字中的“状态: 已选中”
                    const statusText = '状态: 已连接';
                    const botHTML = SERVER_BOT_HTML;

                    btn.innerHTML = botHTML + `
                        <div class="server-button-name" style="position: absolute; bottom: -15px; left: 50%; transform: translateX(-50%); font-size: 11px; color: #2d3436; white-space: nowrap; pointer-events: none; z-index: 100;">${serverName}</div>
                        <div class="server-tooltip">
                            <div style="font-weight: bold; margin-bottom: 4px;">${serverName}</div>
                            <div style="font-size: 11px; opacity: 0.9;">${server.url || ''}</div>
                            <div style="font-size: 11px; color: #00ff88; margin-top: 4px;" class="status-text">${statusText}</div>
                        </div>
                    `;

                    availableGrid.appendChild(btn);
                });

                availableSection.appendChild(availableGrid);
                availableContainer.appendChild(availableSection);
                // 初始化雷达机器人
                initRadarBots();
            }

            const groupsContainer = document.getElementById('userGroupsContainer');
            if (!groupsContainer) return;
            groupsContainer.innerHTML = '';

            if (currentGroupCreation) {
                const createArea = document.createElement('div');
                createArea.className = 'group-creation-area';

                const selectedServerNames = currentGroupCreation.selectedServers;
                const unselectedServers = allServers.filter(s =>
                    !selectedServerNames.includes(s.name) &&
                    (!assignedServers.has(s.name) || selectedServerNames.includes(s.name))
                );

                createArea.innerHTML = `
                    <div class="group-creation-row">
                        <button class="group-select-btn ${currentGroupCreation.userId ? 'selected' : ''}"
                                onclick="selectUserForGroup('${currentGroupCreation.userId || ''}')">
                            ${currentGroupCreation.userId || 'Please select user'}
                        </button>
                        ${!currentGroupCreation.userId ? managerUsers.map(userId => `
                            <button class="group-select-btn"
                                    onclick="selectUserForGroup('${userId}')">
                                ${userId}
                            </button>
                        `).join('') : ''}
                    </div>
                    <div class="group-creation-row">
                        <button class="group-select-btn ${currentGroupCreation.selectedServers.length > 0 ? 'selected' : ''}"
                                onclick="showServerSelection()">
                            ${currentGroupCreation.selectedServers.length > 0
                        ? currentGroupCreation.selectedServers[0]
                        : 'Please select backend server'}
                        </button>
                        ${currentGroupCreation.userId && currentGroupCreation.showingServers ? allServers.filter(s => !assignedServers.has(s.name) || selectedServerNames.includes(s.name)).map(server => {
                            const isSelected = selectedServerNames.includes(server.name);
                            return `<button class="group-select-btn ${isSelected ? 'selected' : ''}"
                                    onclick="toggleServerForGroup('${server.name}')">
                                    ${server.name}
                                </button>`;
                        }).join('') : ''}
                    </div>
                    ${currentGroupCreation.userId && selectedServerNames.length > 0 ? `
                    <div class="group-servers-display">
                        ${selectedServerNames.map(serverName => {
                            const server = allServers.find(s => s.name === serverName);
                            return server ? `
                                <div class="server-tag private">
                                    <div>${server.name}</div>
                                    <div class="server-tag-label">独享服务器</div>
                                </div>
                            ` : '';
                        }).join('')}
                        ${unselectedServers.length > 0 ? '<div style="width: 100%; height: 10px;"></div>' : ''}
                        ${unselectedServers.map(server => `
                            <div class="server-tag public">
                                <div>${server.name}</div>
                                <div class="server-tag-label">共享服务器</div>
                            </div>
                        `).join('')}
                    </div>
                    <div class="group-creation-row">
                        <button class="admin-manage-footer-btn reset" onclick="resetGroupCreation()">重置</button>
                        <button class="admin-manage-footer-btn manage" onclick="confirmGroupCreation()">管理</button>
                    </div>
                    ` : currentGroupCreation.userId && selectedServerNames.length === 0 ? `
                    <div class="group-creation-row">
                        <button class="admin-manage-footer-btn reset" onclick="resetGroupCreation()">重置</button>
                        <button class="admin-manage-footer-btn confirm" onclick="confirmGroupCreation()">确定</button>
                    </div>
                    ` : ''}
                `;
                groupsContainer.appendChild(createArea);
            }

            // 使用API返回的user_groups或本地的managerUserGroups渲染用户组
            const userGroupsToRender = data.user_groups || managerUserGroups;
            userGroupsToRender.forEach(group => {
                const section = document.createElement('div');
                section.className = 'user-group-section';

                const isEditing = currentGroupCreation && currentGroupCreation.userId === group.userId;

                const privateServers = group.servers || [];
                const publicServers = allServers.filter(s => !privateServers.includes(s.name) && !assignedServers.has(s.name));

                section.innerHTML = `
                    <div class="user-group-header">
                        <div class="user-group-name">用户: ${group.userId || group.user_id}</div>
                        <div class="user-group-actions">
                            ${isEditing ? '' : `<button class="admin-account-action-btn manage" onclick="manageUserGroup('${group.userId || group.user_id}')">管理</button>`}
                            <button class="admin-account-action-btn delete" onclick="deleteUserGroup('${group.userId || group.user_id}')">重置</button>
                        </div>
                    </div>
                    <div class="user-group-servers">
                        ${privateServers.map(server => `
                            <div class="server-tag private">
                                <div>${server}</div>
                                <div class="server-tag-label">独享私有服务器</div>
                            </div>
                        `).join('')}
                        ${publicServers.length > 0 ? '<div style="width: 100%; height: 10px;"></div>' : ''}
                        ${publicServers.map(server => `
                            <div class="server-tag public">
                                <div>${server.name}</div>
                                <div class="server-tag-label">公共共享服务器</div>
                            </div>
                        `).join('')}
                    </div>
                `;
                groupsContainer.appendChild(section);
            });
        } catch (error) {
            console.error('加载管理员显示数据失败:', error);
        }
    }, 50);
}



window.handleAdminLogin = handleAdminLogin;

let isSending = false;
let currentTaskId = null;
let taskStatusCheckTimer = null;
let taskStatusLastUpdate = null;
let taskStatusLastProgress = null;
let taskStatusLastProgressTime = null;
let currentChatId = null;
let unreadChatIds = new Set();
let newMessageNotification = null;
let clearedChatIds = new Set();
let globalStats = {
    taskCount: 0,
    totalSent: 0,
    totalSuccess: 0,
    totalFail: 0,
    totalTime: 0,
    totalPhoneCount: 0,
    inboxReceived: 0,
    inboxSent: 0,
    inboxTotal: 0
};
let sentPhoneNumbers = new Set();
const MAX_LOG_ITEMS = 200;
let logScrollPending = false;
let conversationScrollPending = false;

const _taskWsWaiters = new Map();

function _ensureTaskWaiter(taskId, timeoutMs = 30 * 60 * 1000) {
    if (_taskWsWaiters.has(taskId)) return _taskWsWaiters.get(taskId);
    let resolveFn, rejectFn;
    const p = new Promise((resolve, reject) => {
        resolveFn = resolve;
        rejectFn = reject;
    });
    const timeoutId = setTimeout(() => {
        _taskWsWaiters.delete(taskId);
        try { rejectFn(new Error('等待任务超时（WS）')); } catch { /* ignore */ }
    }, timeoutMs);
    const waiter = { promise: p, resolve: resolveFn, reject: rejectFn, timeoutId };
    _taskWsWaiters.set(taskId, waiter);
    return waiter;
}

function connectToBackendWS(_serverIgnored) {
    // 零轮询架构：使用原生WebSocket实时推送，无任何HTTP轮询
    if (activeWs && (activeWs.readyState === WebSocket.OPEN || activeWs.readyState === WebSocket.CONNECTING)) {
        // console.log('[WebSocket] 已连接或正在连接，跳过初始化');
        return;
    }

    if (!currentUserId) {
        currentUserId = localStorage.getItem('user_id');
    }
    if (!authToken) {
        authToken = localStorage.getItem('auth_token');
    }
    // 允许未登录时也建立 WS（用于显示“已连接实时推送服务”以及后续登录后订阅）
    // 订阅用户/任务会在 onopen 内根据 currentUserId 决定是否发送
    if (!currentUserId) {
        console.warn('[WebSocket] 缺少 user_id，将仅建立连接不订阅');
    }

    // 构建WebSocket URL（/ws/frontend端点）
    const wsUrl = API_BASE_URL
        .replace('http://', 'ws://')
        .replace('https://', 'wss://')
        .replace('/api', '') + '/ws/frontend';

    try {
        // 使用原生WebSocket客户端
        // 关键：把 wsUrl 打出来，方便定位“连的是哪个地址”
        console.log('[WebSocket] connecting ->', wsUrl, { user_id: currentUserId || null });
        activeWs = new WebSocket(wsUrl);

        // 心跳定时器
        let heartbeatTimer = null;

        activeWs.onopen = () => {
            // console.log('✓ WebSocket已连接');

            // 🔥 更新连接状态
            if (typeof updateConnectionStatus === 'function') {
                updateConnectionStatus(true);
            }

            // 使用 setTimeout 确保 WebSocket 状态完全同步
            setTimeout(() => {
                // 订阅用户更新
                if (currentUserId) {
                    sendWSCommand('subscribe_user', { user_id: currentUserId });
                }

                // 🔥 关键修复：断线重连后，必须重新订阅正在进行的任务，否则前端会永久卡死
                if (typeof isSending !== 'undefined' && isSending && typeof currentTaskId !== 'undefined' && currentTaskId) {
                    // console.log(`[WebSocket] 重连恢复，重新订阅任务 ${currentTaskId}`);
                    sendWSCommand('subscribe_task', { task_id: currentTaskId });
                }

                showMessage('已连接到实时推送服务', 'success');
            }, 100);

            // 启动心跳 - 每30秒发送一次ping
            if (heartbeatTimer) {
                clearInterval(heartbeatTimer);
            }
            heartbeatTimer = setInterval(() => {
                if (activeWs && activeWs.readyState === WebSocket.OPEN) {
                    sendWSCommand('ping', {});
                }
            }, 30000); // 30秒心跳
        };

        activeWs.onmessage = (event) => {
            try {
                const msg = JSON.parse(event.data);
                const msgType = msg.type;

                // console.log('[WebSocket] 收到消息:', msgType, msg);

                // 处理不同类型的消息
                if (msgType === 'task_update') {
                    handleServerMessage(msg, null);
                } else if (msgType === 'balance_update') {
                    handleServerMessage(msg, null);
                } else if (msgType === 'inbox_update') {
                    handleServerMessage(msg, null);
                } else if (msgType === 'subscribed') {
                    // console.log('✓ 任务订阅成功:', msg.data || msg);
                } else if (msgType === 'user_subscribed') {
                    // console.log('✓ 用户订阅成功');
                } else if (msgType === 'unsubscribed') {
                    // console.log('[WebSocket] 取消订阅:', msg);
                } else if (msgType === 'pong') {
                    // 心跳响应 - 保持连接活跃（不打印日志）
                } else if (msgType === 'error') {
                    console.error('[WebSocket] 错误:', msg.message);
                } else if (msgType === 'super_admin_response') {
                    // 处理超级管理员命令响应
                    handleSuperAdminResponse(msg);
                } else if (msgType === 'servers_list' || msgType === 'servers_list_update' || msgType === 'server_update') {
                    // 🔥 处理服务器列表更新（从API推送）
                    // console.log('✓ 收到服务器列表更新:', msgType);
                    if ((msgType === 'servers_list' || msgType === 'servers_list_update') && msg.servers) {
                        // 更新服务器列表
                        serverData.connected = [];
                        serverData.disconnected = [];
                        if (Array.isArray(msg.servers)) {
                            msg.servers.forEach(server => {
                                const serverItem = {
                                    name: server.server_name || server.server_id,
                                    url: server.server_url || '',
                                    server_id: server.server_id,
                                    status: (server.status || '').toLowerCase(),
                                    assigned_user_id: server.assigned_user_id || null,
                                    last_seen: server.last_seen
                                };

                                // 🔥 严格时间校验：即使状态是 connected，也要看心跳时间是否在 60 秒内
                                const now = Date.now();
                                const lastSeenTime = serverItem.last_seen ? new Date(serverItem.last_seen).getTime() : 0;
                                const isRecentlyActive = (now - lastSeenTime) < 60000; // 60秒以内

                                if (serverItem.status === 'connected' && isRecentlyActive) {
                                    serverData.connected.push(serverItem);
                                } else {
                                    // 任何超过 60 秒没动的，或者状态不是 connected 的，一律进断开列表
                                    serverItem.status = 'disconnected';
                                    serverData.disconnected.push(serverItem);
                                }
                            });
                        }
                        if (typeof updateServerDisplay === 'function') {
                            updateServerDisplay();
                        }
                        // 🚀 关键修复：收到列表后，立刻尝试连接分配给我的服务器
                        if (typeof connectToAssignedServers === 'function') {
                            connectToAssignedServers();
                        }
                        if (typeof connectToAvailableServers === 'function') {
                            connectToAvailableServers();
                        }
                    } else if (msgType === 'server_update') {
                        // 单个服务器更新，重新加载完整列表
                        if (typeof loadServersFromAPI === 'function') {
                            loadServersFromAPI();
                        }
                    }
                }
            } catch (e) {
                console.error('[WebSocket] 消息解析失败:', e);
            }
        };

        activeWs.onerror = (error) => {
            // 浏览器给的 error 很有限，但至少能提示“连接失败”
            console.error('[WebSocket] 连接错误:', error, 'url=', wsUrl);
            if (typeof updateConnectionStatus === 'function') {
                updateConnectionStatus(false);
            }
            try { showMessage('实时推送连接失败（WS）', 'warning'); } catch { /* ignore */ }
        };

        activeWs.onclose = (event) => {
            // console.log('[WebSocket] 断开连接:', event.code, event.reason);
            if (typeof updateConnectionStatus === 'function') {
                updateConnectionStatus(false);
            }

            // 清理心跳定时器
            if (heartbeatTimer) {
                clearInterval(heartbeatTimer);
                heartbeatTimer = null;
            }

            activeWs = null;

            // 5秒后自动重连
            setTimeout(() => {
                if (!activeWs) {
                    // console.log('[WebSocket] 尝试重连...');
                    connectToBackendWS(_serverIgnored);
                }
            }, 5000);
        };

    } catch (e) {
        console.error('[WebSocket] 初始化失败:', e);
        activeWs = null;
        // 🔥 更新连接状态为未连接
        if (typeof updateConnectionStatus === 'function') {
            updateConnectionStatus(false);
        }
        showMessage('WebSocket 初始化失败', 'error');
    }
}

function sendWSCommand(action, data = {}) {
    if (!activeWs || activeWs.readyState !== WebSocket.OPEN) {
        // console.warn('[WebSocket] 未连接，无法发送命令:', action);
        return false;
    }
    const payload = JSON.stringify({ action, data });
    // console.log('[WebSocket] 发送命令:', action, data);
    activeWs.send(payload);
    return true;
}

function updateUserInfoDisplay(credits) {
    const userInfoDisplay = document.getElementById('userInfoDisplay');
    const currentUsernameEl = document.getElementById('currentUsername');
    const currentCreditsEl = document.getElementById('currentCredits');

    if (userInfoDisplay && currentUsernameEl && currentCreditsEl) {
        const userInfo = JSON.parse(localStorage.getItem('user_info_' + currentUserId) || '{}');
        currentUsernameEl.textContent = userInfo.username || currentUserId;
        currentCreditsEl.textContent = credits !== undefined ? credits : '-';
        userInfoDisplay.style.display = 'inline-block';
    }
}

async function loadUserBackends() {
    if (!currentUserId) {
        currentUserId = localStorage.getItem('user_id');
    }
    if (!authToken) {
        authToken = checkAuthToken();
    }
    if (!currentUserId || !authToken) {
        return;
    }

    try {
        const response = await fetch(`${API_BASE_URL}/user/${currentUserId}/backends`, {
            headers: {
                'Authorization': `Bearer ${authToken}`
            }
        });

        if (response.ok) {
            const data = await response.json();
            const backends = data.backends || data.backend_servers || [];
            localStorage.setItem('user_backends', JSON.stringify(backends));
            return backends;
        }
    } catch (error) {
        console.error('加载后端服务器列表失败:', error);
    }
    return [];
}

async function checkAuth() {
    // 🔥 仅用于普通用户：1小时后要求重新输入账号密码（不删除 token）
    const SESSION_TIMEOUT = 60 * 60 * 1000;
    const loginTime = localStorage.getItem('login_time');
    
    if (loginTime) {
        const timeSinceLogin = Date.now() - parseInt(loginTime);
        if (timeSinceLogin > SESSION_TIMEOUT) {
            // 超过1小时：只清“登录时间”，强制重新输入账号密码；token 不删除
            localStorage.removeItem('login_time');
            return false;
        }
    }
    
    currentUserId = localStorage.getItem('user_id');
    authToken = localStorage.getItem('auth_token');
    if (!currentUserId || !authToken) {
        return false;
    }
    
    // 🔥 普通用户：1小时内直接允许使用（不调用 /verify）
    return true;
}

function getStringLength(str) {
    let length = 0;
    for (let i = 0; i < str.length; i++) {
        const charCode = str.charCodeAt(i);
        if (charCode >= 0x4E00 && charCode <= 0x9FFF) {
            length += 2;
        } else {
            length += 1;
        }
    }
    return length;
}

function updateCounts() {
    const numbersText = document.getElementById('numbersText').value;
    const numbers = numbersText.split(/[\n,]/).filter(n => n.trim()).length;
    const numbersCountEl = document.getElementById('numbersCount');

    if (numbers === 0) {
        numbersCountEl.textContent = `号码: ${numbers}`;
        numbersCountEl.classList.remove('has-numbers');
    } else {
        numbersCountEl.textContent = `号码: ${numbers}`;
        numbersCountEl.classList.add('has-numbers');
    }

    const messageText = document.getElementById('messageText').value;
    const charCount = getStringLength(messageText);
    const messageCountEl = document.getElementById('messageCount');

    if (charCount === 0) {
        messageCountEl.textContent = `字数: ${charCount}`;
        messageCountEl.classList.remove('has-content', 'over-limit');
    } else if (charCount <= 160) {
        messageCountEl.textContent = `字数: ${charCount}`;
        messageCountEl.classList.remove('over-limit');
        messageCountEl.classList.add('has-content');
    } else {
        const messageCount = Math.ceil(charCount / 160);
        messageCountEl.innerHTML = `字数: ${charCount}/160 <span class="message-count-badge">${messageCount}条</span>`;
        messageCountEl.classList.remove('has-content');
        messageCountEl.classList.add('over-limit');
    }
}

function importNumbers() {
    document.getElementById('numbersFile').click();
}

function importMessage() {
    document.getElementById('messageFile').click();
}

function clearNumbers() {
    const btn = document.getElementById('clearNumbersBtn');
    document.getElementById('numbersText').value = '';
    updateCounts();
    if (btn) {
        btn.blur();
        btn.style.background = '';
        btn.style.borderColor = '';
        btn.style.transform = '';
        btn.style.boxShadow = '';
        setTimeout(() => {
            btn.blur();
            btn.style.background = '';
            btn.style.borderColor = '';
        }, 100);
    }
}

function clearMessage() {
    const btn = document.getElementById('clearMessageBtn');
    document.getElementById('messageText').value = '';
    updateCounts();
    if (btn) {
        btn.blur();
        btn.style.background = '';
        btn.style.borderColor = '';
        btn.style.transform = '';
        btn.style.boxShadow = '';
        setTimeout(() => {
            btn.blur();
            btn.style.background = '';
            btn.style.borderColor = '';
        }, 100);
    }
}

document.getElementById('numbersFile').addEventListener('change', function (e) {
    const file = e.target.files[0];
    if (file) {
        const reader = new FileReader();
        reader.onload = function (e) {
            const content = e.target.result;
            const numbers = content.split(/[\n,]/)
                .map(n => n.trim())
                .filter(n => n.length > 0);
            document.getElementById('numbersText').value = numbers.join('\n');
            updateCounts();
        };
        reader.readAsText(file);
    }
    this.value = '';
});

document.getElementById('messageFile').addEventListener('change', function (e) {
    const file = e.target.files[0];
    if (file) {
        const reader = new FileReader();
        reader.onload = function (e) {
            document.getElementById('messageText').value = e.target.result;
            updateCounts();
        };
        reader.readAsText(file);
    }
    this.value = '';
});

function updateConnectionStatus(connected) {
    const statusEl = document.getElementById('connectionStatus');
    if (!statusEl) return;

    if (connected) {
        statusEl.innerHTML = '<span style="color: white; font-weight: bold;">●</span> 已连接';
        statusEl.className = 'connection-status status-connected';
    } else {
        statusEl.innerHTML = '<span style="color: white; font-weight: bold;">●</span> 未连接';
        statusEl.className = 'connection-status status-disconnected';
    }
}

let isConnectingServers = false;
let _noUsableServerWarned = false;
async function connectToAvailableServers() {
    if (!checkAuth()) return;

    if (isConnectingServers) return;
    isConnectingServers = true;

    try {
        const hasConnectedServers = serverData.connected && serverData.connected.length > 0;

        if (!hasConnectedServers) {
            // 🔥 如果服务器列表还未加载完成，等待加载完成后再检查
            if (!_serversLoadedOnce) {
                // 等待服务器列表加载完成（最多等待3秒）
                let waitCount = 0;
                const maxWait = 30; // 30次 * 100ms = 3秒
                while (!_serversLoadedOnce && waitCount < maxWait) {
                    await new Promise(resolve => setTimeout(resolve, 100));
                    waitCount++;
                }
                // 如果等待后仍然没有加载完成，直接返回（不显示警告）
                if (!_serversLoadedOnce) {
                    return;
                }
                // 重新检查服务器列表
                const hasServersNow = serverData.connected && serverData.connected.length > 0;
                if (!hasServersNow) {
                    updateConnectionStatus(false);
                    if (!_noUsableServerWarned) {
                        _noUsableServerWarned = true;
                        console.warn('[connectToAvailableServers] 没有可用的服务器');
                    }
                    return;
                }
            } else {
                // 服务器列表已加载但确实没有可用服务器
                updateConnectionStatus(false);
                if (!_noUsableServerWarned) {
                    _noUsableServerWarned = true;
                    console.warn('[connectToAvailableServers] 没有可用的服务器');
                }
                return;
            }
        }

        updateConnectionStatus(true);
        _noUsableServerWarned = false;
        // console.log('[connectToAvailableServers] 可用服务器数量:', (serverData.connected?.length || 0));
    } finally {
        isConnectingServers = false;
    }
}

//#endregion
