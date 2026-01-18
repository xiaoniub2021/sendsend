//#region 统计功能模块（A版和B版统计显示）
function initGlobalStatsDisplay() {
    globalStats.totalPhoneCount = sentPhoneNumbers.size;

    const totalCount = globalStats.totalSent + globalStats.inboxTotal;
    document.getElementById('taskCount').textContent = globalStats.taskCount;
    document.getElementById('phoneCount').textContent = globalStats.totalPhoneCount;
    document.getElementById('totalSentCount').textContent = totalCount;
    document.getElementById('successCount').textContent = globalStats.totalSuccess;
    document.getElementById('failCount').textContent = globalStats.totalFail;
    const totalAll = globalStats.totalSuccess + globalStats.totalFail;
    const successRate = totalAll > 0 ? (globalStats.totalSuccess / totalAll * 100) : 0;
    document.getElementById('successRate').textContent = `${successRate.toFixed(1)}%`;
    updateTimeDisplay();
    updateInlineStats();
}
//#endregion
//#region 面板切换模块（主页面、发送、收件箱、账号管理）
function switchPanel(panelType) {
    const workspaceAB = document.getElementById('workspaceAB');
    const panelB = document.getElementById('panelB');
    const panelC = document.getElementById('panelC');
    const panelD = document.getElementById('panelD');
    const panelE = document.getElementById('panelE');
    const navHomeBtn = document.getElementById('navHomeBtn');
    const navAccountBtn = document.getElementById('navAccountBtn');
    const navSendBtn = document.getElementById('navSendBtn');
    const navInboxBtn = document.getElementById('navInboxBtn');
    const logPanelBtn = document.getElementById('logPanelBtn');

    if (navHomeBtn) navHomeBtn.classList.remove('active');
    if (navAccountBtn) navAccountBtn.classList.remove('active');
    if (navSendBtn) navSendBtn.classList.remove('active');
    if (navInboxBtn) navInboxBtn.classList.remove('active');

    const isMobile = window.innerWidth <= 768;

    if (workspaceAB) {
        workspaceAB.style.display = 'none';
        workspaceAB.classList.remove('mobile-show', 'single-panel', 'show-log');
    }
    if (panelB) {
        panelB.classList.remove('mobile-show');
        panelB.style.display = 'none';
    }
    if (panelC) {
        panelC.classList.remove('mobile-show');
        panelC.style.display = 'none';
    }
    if (panelD) {
        panelD.classList.remove('mobile-show');
        panelD.style.display = 'none';
    }
    if (panelE) {
        panelE.classList.remove('mobile-show');
        panelE.style.display = 'none';
    }
    if (logPanelBtn) logPanelBtn.classList.remove('active');

    if (panelType === 'home') {
        if (isMobile) {
            if (panelD) {
                panelD.classList.add('mobile-show');
                panelD.style.display = 'flex';
            }
        } else {
            if (panelD) panelD.style.display = 'flex';
        }
        if (navHomeBtn) navHomeBtn.classList.add('active');
    } else if (panelType === 'send') {
        if (isMobile) {
            if (workspaceAB) {
                workspaceAB.classList.add('mobile-show', 'single-panel');
                workspaceAB.style.display = 'flex';
                workspaceAB.classList.remove('show-log');
            }
        } else {
            if (workspaceAB) {
                workspaceAB.style.display = 'flex';
                workspaceAB.classList.add('single-panel');
                workspaceAB.classList.remove('show-log');
            }
        }
        if (navSendBtn) navSendBtn.classList.add('active');
    } else if (panelType === 'inbox') {
        if (isMobile) {
            if (panelC) {
                panelC.classList.add('mobile-show');
                panelC.style.display = 'flex';
            }
        } else {
            if (panelC) panelC.style.display = 'flex';
        }
        if (navInboxBtn) navInboxBtn.classList.add('active');
    } else if (panelType === 'account') {
        if (isMobile) {
            if (panelE) {
                panelE.classList.add('mobile-show');
                panelE.style.display = 'flex';
            }
        } else {
            if (panelE) panelE.style.display = 'flex';
        }
        if (navAccountBtn) navAccountBtn.classList.add('active');
        loadAccountPanelContent();
    }
}

function toggleLogPanel() {
    const workspaceAB = document.getElementById('workspaceAB');
    const panelB = document.getElementById('panelB');
    const panelC = document.getElementById('panelC');
    const panelD = document.getElementById('panelD');
    const logPanelBtn = document.getElementById('logPanelBtn');

    const isMobile = window.innerWidth <= 768;

    if (isMobile) {
        syncLogToMobile();

        if (workspaceAB) {
            workspaceAB.classList.remove('mobile-show');
            workspaceAB.style.display = 'none';
        }
        if (panelC) {
            panelC.classList.remove('mobile-show');
            panelC.style.display = 'none';
        }
        if (panelD) {
            panelD.classList.remove('mobile-show');
            panelD.style.display = 'none';
        }
        if (panelB) {
            panelB.classList.add('mobile-show');
            panelB.style.display = 'flex';
        }
        if (logPanelBtn) logPanelBtn.classList.add('active');
    } else {
        if (workspaceAB && logPanelBtn) {
            if (workspaceAB.classList.contains('show-log')) {
                workspaceAB.classList.remove('show-log');
                workspaceAB.classList.add('single-panel');
                logPanelBtn.classList.remove('active');
            } else {
                workspaceAB.classList.remove('single-panel');
                workspaceAB.classList.add('show-log');
                logPanelBtn.classList.add('active');
            }
        }
    }
}

function syncLogToMobile() {
    const statusList = document.getElementById('statusList');
    const statusListMobile = document.getElementById('statusListMobile');
    if (statusList && statusListMobile) {
        statusListMobile.innerHTML = statusList.innerHTML;
        statusListMobile.scrollTop = statusListMobile.scrollHeight;
    }

    const statIds = ['taskCount', 'phoneCount', 'totalSentCount', 'successCount', 'failCount', 'successRate'];
    statIds.forEach(id => {
        const source = document.getElementById(id);
        const target = document.getElementById(id + 'Mobile');
        if (source && target) {
            target.textContent = source.textContent;
        }
    });
}

function backToSendPanel() {
    const workspaceAB = document.getElementById('workspaceAB');
    const panelB = document.getElementById('panelB');
    const panelC = document.getElementById('panelC');
    const panelD = document.getElementById('panelD');
    const logPanelBtn = document.getElementById('logPanelBtn');
    const navSendBtn = document.getElementById('navSendBtn');

    const isMobile = window.innerWidth <= 768;

    if (isMobile) {
        if (panelB) {
            panelB.classList.remove('mobile-show');
            panelB.style.display = 'none';
        }
        if (panelC) {
            panelC.classList.remove('mobile-show');
            panelC.style.display = 'none';
        }
        if (panelD) {
            panelD.classList.remove('mobile-show');
            panelD.style.display = 'none';
        }
        if (workspaceAB) {
            workspaceAB.classList.add('mobile-show', 'single-panel');
            workspaceAB.classList.remove('show-log');
            workspaceAB.style.display = 'flex';
        }
        if (logPanelBtn) logPanelBtn.classList.remove('active');
        document.querySelectorAll('.nav-btn').forEach(btn => btn.classList.remove('active'));
        if (navSendBtn) navSendBtn.classList.add('active');
    }
}
document.getElementById('navHomeBtn').addEventListener('click', function () {
    switchPanel('home');
});

document.getElementById('navSendBtn').addEventListener('click', function () {
    switchPanel('send');
});

document.getElementById('navInboxBtn').addEventListener('click', function () {
    switchPanel('inbox');
});

document.getElementById('navAccountBtn').addEventListener('click', function () {
    switchPanel('account');
});

document.getElementById('logPanelBtn').addEventListener('click', function () {
    toggleLogPanel();
});

document.getElementById('backToSendBtn').addEventListener('click', function () {
    backToSendPanel();
});

document.getElementById('clearLogsBtn').addEventListener('click', function () {
    document.getElementById('statusList').innerHTML = '';
    const statusListMobile = document.getElementById('statusListMobile');
    if (statusListMobile) statusListMobile.innerHTML = '';
});

document.getElementById('clearLogsBtnMobile').addEventListener('click', function () {
    document.getElementById('statusListMobile').innerHTML = '';
    const statusList = document.getElementById('statusList');
    if (statusList) statusList.innerHTML = '';
});

async function clearInbox() {
    const allChatIds = Array.from(document.querySelectorAll('.contact-item')).map(item => item.dataset.chatId).filter(id => id);

    const contactList = document.getElementById('contactList');
    contactList.innerHTML = '<div style="font-family: \'Xiaolai\', sans-serif; text-align:center; color:rgba(47,47,47,0.5); padding:20px; font-size:14px;">暂无对话</div><button class="btn-clear-inbox" id="clearInboxBtn" title="全部删除">全部删除</button>';
    document.getElementById('conversationDisplay').innerHTML = '<div style="font-family: \'Xiaolai\', sans-serif; text-align:center; color:rgba(47,47,47,0.5); padding:20px; font-size:14px;">选择一个对话开始聊天</div>';
    document.getElementById('chatHeader').innerHTML = ' 选择一个对话';

    allChatIds.forEach(chatId => {
        clearedChatIds.add(chatId);
    });

    inboxMessageStats = {};
    currentChatId = null;
    unreadChatIds.clear();
    updateInboxStats();
    updateNotificationCount();

    if (allChatIds.length > 0 && currentUserId) {
        try {
            await Promise.all(allChatIds.map(chatId =>
                fetch(`${API_BASE_URL}/user/${currentUserId}/conversations/${encodeURIComponent(chatId)}`, {
                    method: 'DELETE'
                }).catch(() => null)
            ));
        } catch { /* ignore */ }
    }

    document.getElementById('clearInboxBtn').addEventListener('click', clearInbox);
}

document.getElementById('clearInboxBtn').addEventListener('click', clearInbox);

// 计算字符串长度（中文字符算2个字符）
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

// 更新号码和消息计数
function updateCounts() {
    const numbersText = document.getElementById('numbersText');
    if (!numbersText) return;
    
    const numbers = numbersText.value.split(/[\n,]/).filter(n => n.trim()).length;
    const numbersCountEl = document.getElementById('numbersCount');

    if (numbersCountEl) {
        if (numbers === 0) {
            numbersCountEl.textContent = `号码: ${numbers}`;
            numbersCountEl.classList.remove('has-numbers');
        } else {
            numbersCountEl.textContent = `号码: ${numbers}`;
            numbersCountEl.classList.add('has-numbers');
        }
    }

    const messageText = document.getElementById('messageText');
    if (!messageText) return;
    
    const charCount = getStringLength(messageText.value);
    const messageCountEl = document.getElementById('messageCount');

    if (messageCountEl) {
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
}

// 导入号码文件
function importNumbers() {
    const fileInput = document.getElementById('numbersFile');
    if (fileInput) {
        fileInput.click();
    }
}

// 导入消息文件
function importMessage() {
    const fileInput = document.getElementById('messageFile');
    if (fileInput) {
        fileInput.click();
    }
}

// 清空号码
function clearNumbers() {
    const btn = document.getElementById('clearNumbersBtn');
    const numbersText = document.getElementById('numbersText');
    if (numbersText) {
        numbersText.value = '';
        updateCounts();
    }
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

// 清空消息
function clearMessage() {
    const btn = document.getElementById('clearMessageBtn');
    const messageText = document.getElementById('messageText');
    if (messageText) {
        messageText.value = '';
        updateCounts();
    }
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

// 处理号码文件选择
const numbersFileInput = document.getElementById('numbersFile');
if (numbersFileInput) {
    numbersFileInput.addEventListener('change', function (e) {
        const file = e.target.files[0];
        if (file) {
            const reader = new FileReader();
            reader.onload = function (e) {
                const content = e.target.result;
                const numbersText = document.getElementById('numbersText');
                if (numbersText) {
                    numbersText.value = content.trim();
                    updateCounts();
                }
            };
            reader.readAsText(file);
        }
        // 清空文件输入，允许重复选择同一文件
        e.target.value = '';
    });
}

// 处理消息文件选择
const messageFileInput = document.getElementById('messageFile');
if (messageFileInput) {
    messageFileInput.addEventListener('change', function (e) {
        const file = e.target.files[0];
        if (file) {
            const reader = new FileReader();
            reader.onload = function (e) {
                const content = e.target.result;
                const messageText = document.getElementById('messageText');
                if (messageText) {
                    messageText.value = content.trim();
                    updateCounts();
                }
            };
            reader.readAsText(file);
        }
        // 清空文件输入，允许重复选择同一文件
        e.target.value = '';
    });
}

document.getElementById('numbersText').addEventListener('input', updateCounts);
document.getElementById('messageText').addEventListener('input', updateCounts);
document.getElementById('importNumbersBtn').addEventListener('click', importNumbers);
document.getElementById('clearNumbersBtn').addEventListener('click', clearNumbers);
document.getElementById('importMessageBtn').addEventListener('click', importMessage);
document.getElementById('clearMessageBtn').addEventListener('click', clearMessage);
document.getElementById('sendBtn').addEventListener('click', startSending);
document.getElementById('sendReplyBtn').addEventListener('click', sendReply);
document.getElementById('replyInput').addEventListener('keydown', (e) => e.key === 'Enter' && !e.shiftKey && (e.preventDefault(), sendReply()));

const lockState = {};

function setupExpand(triggerId, wrapperId) {
    const trigger = document.getElementById(triggerId);
    const wrapper = document.getElementById(wrapperId);
    if (!trigger || !wrapper) return;

    trigger.addEventListener('click', (e) => {
        e.stopPropagation();
        if (lockState[wrapperId]) {
            lockState[wrapperId] = false;
            wrapper.classList.remove('locked');
            wrapper.classList.remove('expanded');
        } else {
            lockState[wrapperId] = true;
            wrapper.classList.add('locked');
            wrapper.classList.add('expanded');
        }
    });
}

function updateIntervalColor() {
    const intervalSelect = document.getElementById('intervalInput');
    if (intervalSelect) {
        const value = intervalSelect.value;
        intervalSelect.setAttribute('data-value', value);
    }
}

function updateScale() {
    const isMobile = window.innerWidth <= 768;
    if (isMobile) {
        document.body.style.transform = '';
        document.body.style.transformOrigin = '';
        document.body.style.zoom = '';
        return;
    }

    const devicePixelRatio = window.devicePixelRatio || 1;

    const designWidth = 1450;
    const designHeight = 580;

    const minMargin = 50;

    const minWindowWidth = designWidth + (minMargin * 2);
    const minWindowHeight = designHeight + (minMargin * 2);

    const windowWidth = window.innerWidth;
    const windowHeight = window.innerHeight;

    const availableWidth = windowWidth - (minMargin * 2);
    const availableHeight = windowHeight - (minMargin * 2);

    let scale = 1.0;

    if (windowWidth < minWindowWidth || windowHeight < minWindowHeight) {
        const scaleX = availableWidth / designWidth;
        const scaleY = availableHeight / designHeight;
        scale = Math.min(scaleX, scaleY);
        scale = Math.max(0.5, scale);
    }

    const contentWrapper = document.querySelector('.content-wrapper');
    if (contentWrapper) {
        contentWrapper.style.setProperty('width', designWidth + 'px', 'important');
        contentWrapper.style.setProperty('height', designHeight + 'px', 'important');
        contentWrapper.style.setProperty('max-width', designWidth + 'px', 'important');
        contentWrapper.style.setProperty('max-height', designHeight + 'px', 'important');
        contentWrapper.style.setProperty('min-width', designWidth + 'px', 'important');
        contentWrapper.style.setProperty('min-height', designHeight + 'px', 'important');

        contentWrapper.style.removeProperty('zoom');

        if (scale < 1.0) {
            contentWrapper.style.setProperty('transform', `scale(${scale})`, 'important');
            contentWrapper.style.setProperty('transform-origin', 'center center', 'important');
        } else {
            contentWrapper.style.removeProperty('transform');
            contentWrapper.style.removeProperty('transform-origin');
        }

        updateDebugInfo(windowWidth, windowHeight, scale, contentWrapper, devicePixelRatio, 1.0);
    }
}

function updateDebugInfo(w, h, scale, wrapper, devicePixelRatio, antiDpiZoom) {
    const debugInfo = document.getElementById('debugInfo');
    if (!debugInfo) return;

    const computed = window.getComputedStyle(wrapper);
    const transform = computed.transform === 'none' ? 'none' : computed.transform;
    const zoom = computed.zoom || '1';

    document.getElementById('debugInnerWidth').textContent = window.innerWidth + 'px';
    document.getElementById('debugInnerHeight').textContent = window.innerHeight + 'px';
    document.getElementById('debugOuterWidth').textContent = window.outerWidth + 'px';
    document.getElementById('debugOuterHeight').textContent = window.outerHeight + 'px';
    document.getElementById('debugScreenWidth').textContent = window.screen.width + 'px';
    document.getElementById('debugScreenHeight').textContent = window.screen.height + 'px';
    document.getElementById('debugAvailWidth').textContent = window.screen.availWidth + 'px';
    document.getElementById('debugAvailHeight').textContent = window.screen.availHeight + 'px';

    const dpiInfo = document.getElementById('debugDpiInfo');
    if (dpiInfo) {
        dpiInfo.textContent = `devicePixelRatio: ${devicePixelRatio.toFixed(2)} (系统缩放${(devicePixelRatio * 100).toFixed(0)}%), 抵消zoom: ${antiDpiZoom.toFixed(3)}`;
    }

    document.getElementById('debugWindowSize').textContent = `${w} × ${h}`;
    document.getElementById('debugScale').textContent = scale.toFixed(3);
    document.getElementById('debugTransform').textContent = transform;

    const zoomInfo = document.getElementById('debugZoom');
    if (zoomInfo) {
        zoomInfo.textContent = zoom;
    }

    debugInfo.style.display = window.showDebugInfo ? 'block' : 'none';
}
//#endregion
//#region 辅助功能模块（文件导入、清空、计数更新、缩放等）
async function init() {
    try {
        const mainContainer = document.querySelector('.main-container');
        if (mainContainer) {
            mainContainer.style.display = 'flex';
        }

        // 🔒 安全修复：必须验证token有效性
        if (typeof checkAuth === 'function') {
            const isAuth = await checkAuth();
            if (!isAuth) {
                // console.warn('未登录，跳过初始化');
                return;
            }
        } else {
            const userId = localStorage.getItem('user_id');
            const authToken = localStorage.getItem('auth_token');
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
            
            if (!userId || !authToken) {
                return;
            }
            
            // 🔥 普通用户：1小时内直接使用，不验证token（token已保留）
            // 1小时后需要重新输入密码登录，这里不做验证
        }
    } catch (error) {
        console.error('init() 函数执行出错:', error);
        // 即使出错也尝试继续，避免页面完全无法加载
    }

    // 🔥 安全检查：确保函数存在才调用
    if (typeof updateCounts === 'function') {
        updateCounts();
    }
    if (typeof initGlobalStatsDisplay === 'function') {
        initGlobalStatsDisplay();
    }
    if (typeof updateButtonState === 'function') {
        updateButtonState();
    }
    
    // 🔥 安全检查：确保函数存在才调用
    if (typeof updateConnectionStatus === 'function') {
        updateConnectionStatus(false);
    }
    if (typeof resetInboxOnConnect === 'function') {
        resetInboxOnConnect();
    }

    // 异步加载服务器数据，不阻塞UI
    // 🚀 单一启动点：建立 WebSocket 连接
    // 连接成功后，服务器会自动推送 servers_list，从而触发界面更新
    if (typeof connectToBackendWS === 'function') {
        connectToBackendWS();
    }

    // 双重保障：主动拉取一次服务器列表（防止WS推送延迟）
    if (typeof loadServersFromAPI === 'function') {
        loadServersFromAPI();
    }

    // ❌ 移除：普通用户页面不应该加载费率设置
    // try { loadGlobalRates(); } catch (e) { }

    const statusList = document.getElementById('statusList');
    if (statusList) {
        statusList.innerHTML = '';
    }

    // 🔥 安全检查：确保函数存在才调用
    if (typeof updateIntervalColor === 'function') {
        updateIntervalColor();
    }

    const intervalSelect = document.getElementById('intervalInput');
    if (intervalSelect && typeof updateIntervalColor === 'function') {
        intervalSelect.addEventListener('change', updateIntervalColor);
    }

    if (typeof updateNotificationCount === 'function') {
        updateNotificationCount();
    }

    if (typeof setupExpand === 'function') {
        setupExpand('numTrigger', 'numWrapper');
        setupExpand('logTrigger', 'logWrapper');
    }

    const panelB = document.getElementById('panelB');
    if (panelB) {
        panelB.classList.remove('mobile-show');
        panelB.style.display = 'none';
    }

    if (typeof updateScale === 'function') {
        updateScale();
        window.addEventListener('resize', updateScale);
    }

    window.showDebugInfo = false;
    document.addEventListener('keydown', function (e) {
        if (e.key === 'd' || e.key === 'D') {
            window.showDebugInfo = !window.showDebugInfo;
            const debugInfo = document.getElementById('debugInfo');
            if (debugInfo) {
                debugInfo.style.display = window.showDebugInfo ? 'block' : 'none';
            }
        }
    });

    // 登录后自动显示主页面
    if (typeof switchPanel === 'function') {
        switchPanel('home');
    }
}

// 🔥 添加showMainApp函数，供登录后调用
function showMainApp() {
    const loginPage = document.getElementById('loginPage');
    const contentWrapper = document.querySelector('.content-wrapper');
    const mainContainer = document.querySelector('.main-container');

    if (loginPage) loginPage.style.display = 'none';
    document.body.classList.remove('login-mode');
    if (contentWrapper) contentWrapper.style.display = 'flex';
    if (mainContainer) mainContainer.style.display = 'flex';
}

//#endregion
