
//#region 费率与货币转换逻辑
let globalExchangeRate = 30; // 1 USD = 30 Credits (基准: 3 Credits = 0.1 USD)
let displayMode = 'usd'; // 强制默认显示为 USD

// 核心转换函数
function formatCurrencyDisplay(credits) {
    const num = parseFloat(credits);
    if (isNaN(num)) return '-';

    if (displayMode === 'usd') {
        return (num * globalExchangeRate).toFixed(3) + ' USD';
    }
    return num + ' 积分';
}

// 切换显示模式
function toggleCurrencyMode() {
    displayMode = displayMode === 'credit' ? 'usd' : 'credit';
    localStorage.setItem('displayMode', displayMode);
    updateCurrencyUI();

    // 立即刷新页面上的余额显示
    updateDisplayedBalances();

    showAutoToast(`已切换为: ${displayMode.toUpperCase()}`, 'info');
}

function updateCurrencyUI() {
    const toggle = document.getElementById('currencyToggle');
    if (toggle) {
        if (displayMode === 'usd') {
            toggle.classList.add('usd');
        } else {
            toggle.classList.remove('usd');
        }
    }
}

// 刷新页面所有余额显示的函数
function updateDisplayedBalances() {
    // 1. 主余额
    // 📊 优先使用服务器数据，如果没有才使用本地缓存
    // 注意：余额应该从服务器获取，这里只是临时显示缓存
    const cachedBalance = localStorage.getItem('user_balance_cache');
    const balanceEl = document.getElementById('currentCredits');
    if (balanceEl && cachedBalance !== null) {
        balanceEl.textContent = formatCurrencyDisplay(cachedBalance);
    }

    // 2. 充值面板里的当前余额 (saRechargeInfoCredits)
    const saCreditsEl = document.getElementById('saRechargeInfoCredits');
    if (saCreditsEl && saCreditsEl.dataset.raw) {
        saCreditsEl.textContent = formatCurrencyDisplay(saCreditsEl.dataset.raw);
    }
}

// 超级管理员费率设置函数
function saSaveGlobalRate() {
    const usdVal = document.getElementById('saGlobalRateUSD').value;
    if (usdVal) {
        globalExchangeRate = parseFloat(usdVal);
        localStorage.setItem('globalExchangeRate', globalExchangeRate);
        showAutoToast('全局费率已生效 (本地记录)', 'success');
        updateDisplayedBalances();
    }
}

function saSaveSalesRateRange() {
    const min = document.getElementById('saSalesMinRate').value;
    const max = document.getElementById('saSalesMaxRate').value;
    showAutoToast(`业务员调价范围已设置: ${min} ~ ${max}`, 'success');
}

function saQueryUserRate() {
    const username = document.getElementById('saRateTargetUser').value;
    if (!username) return showAutoToast('请输入用户名', 'warning');

    document.getElementById('saUserRateControl').style.display = 'block';
    document.getElementById('saUserRatePlaceholder').style.display = 'none';
    document.getElementById('saUserCustomRate').value = globalExchangeRate;
    showAutoToast(`已加载用户 ${username} 的当前配置`, 'info');
}

function saSaveUserRate() {
    const username = document.getElementById('saRateTargetUser').value;
    const rate = document.getElementById('saUserCustomRate').value;
    showAutoToast(`用户 ${username} 的专属费率 ${rate} 已应用`, 'success');
}

// 侦听互换计算
document.addEventListener('DOMContentLoaded', () => {
    const usdInput = document.getElementById('saGlobalRateUSD');
    const creditInput = document.getElementById('saGlobalRateCredit');

    if (usdInput && creditInput) {
        usdInput.value = globalExchangeRate;
        creditInput.value = (1 / globalExchangeRate).toFixed(1);

        usdInput.addEventListener('input', () => {
            const val = parseFloat(usdInput.value);
            if (val > 0) creditInput.value = (1 / val).toFixed(1);
        });

        creditInput.addEventListener('input', () => {
            const val = parseFloat(creditInput.value);
            if (val > 0) usdInput.value = (1 / val).toFixed(5);
        });
    }
    updateCurrencyUI();
});
//#endregion
//#region API匹配

// API 配置 - 智能检测
let API_BASE_URL;

// 规则（按优先级）：URL 参数 -> 本地记忆 -> 自动判断
(function initApiBase() {
    function normalizeApiBase(raw) {
        let v = (raw || '').trim();
        if (!v) return null;

        // 允许只输入 host[:port]，自动补协议
        if (!/^https?:\/\//i.test(v)) {
            const proto = (location.protocol === 'https:') ? 'https://' : 'http://';
            v = proto + v;
        }

        // 去尾部 /
        v = v.replace(/\/+$/, '');

        // 自动补 /api
        if (!/\/api$/i.test(v)) v = v + '/api';
        return v;
    }

    function setApiBaseInternal(raw) {
        const norm = normalizeApiBase(raw);
        try {
            if (norm) localStorage.setItem('manual_api_base', norm);
            else localStorage.removeItem('manual_api_base');
        } catch (_) {}
        return norm;
    }

    // 手动设置（控制台用）：setApiBase('域名或IP[:端口]') / setApiBase('完整URL') / setApiBase('') 清除
    window.setApiBase = function setApiBase(v) {
        const norm = setApiBaseInternal(v);
        console.log('API: setApiBase ->', norm || '(auto)');
        location.reload();
    };

    const params = new URLSearchParams(location.search || '');
    const fromQuery = (params.get('api') || '').trim();
    const fromStorage = (() => {
        try { return (localStorage.getItem('manual_api_base') || '').trim(); } catch (_) { return ''; }
    })();

    const manual = fromQuery || fromStorage;
    if (manual) {
        const norm = fromQuery ? setApiBaseInternal(manual) : normalizeApiBase(manual);
        if (norm) {
            API_BASE_URL = norm;
            console.log('API: 手动 ->', API_BASE_URL);
            return;
        }
    }

    // 自动判断
    if (location.protocol === 'file:') {
        API_BASE_URL = 'http://127.0.0.1:28080/api';
        console.log('API: 本地文件 ->', API_BASE_URL);
        return;
    }
    if (location.hostname && location.hostname !== 'localhost' && location.hostname !== '127.0.0.1') {
        API_BASE_URL = location.origin + '/api';
        console.log('API: 同源 ->', API_BASE_URL);
        return;
    }
    API_BASE_URL = 'http://127.0.0.1:28080/api';
    console.log('API: 本地 ->', API_BASE_URL);
})();

const SERVER_BOT_HTML = `
    <div class="bot-container">
        <div class="signals">
            <div class="signal-ring"></div>
            <div class="signal-ring"></div>
            <div class="signal-ring"></div>
        </div>
        <div class="radar-bot">
            <div class="dish-assembly">
                <div class="dish-head">
                    <div class="dish-inner"></div>
                    <div class="dish-antenna"></div>
                </div>
            </div>
            <div class="body-unit">
                <div class="face-screen">
                    <div class="eye"></div>
                    <div class="eye"></div>
                </div>
                <div class="tech-line"></div>
            </div>
            <div class="base-unit"></div>
            <div class="thruster-glow"></div>
        </div>
    </div>
`;

//#endregion
//#region 全局变量声明
let currentUserId = null;
let authToken = null;
let activeWs = null;
let activeWsServer = null;
//#endregion
//#region 登录认证模块（用户登录、管理员登录、注册）
function switchToUser() {
    const loginPanel = document.getElementById('loginPanel');
    const adminToggle = document.getElementById('adminToggle');

    document.getElementById('userLoginForm').style.display = 'block';
    document.getElementById('adminLoginForm').style.display = 'none';
    document.getElementById('registerForm').style.display = 'none';

    loginPanel.classList.remove('admin-mode');
    adminToggle.classList.remove('active');

    document.querySelector('.login-logo').textContent = '用户登录';
    if (adminToggle) {
        adminToggle.textContent = 'Admin';
    }

    clearMessage();
}

window.switchToAdmin = function switchToAdmin() {
    const loginPanel = document.getElementById('loginPanel');
    const adminToggle = document.getElementById('adminToggle');
    const isAdminMode = loginPanel.classList.contains('admin-mode');

    if (isAdminMode) {
        document.getElementById('userLoginForm').style.display = 'block';
        document.getElementById('adminLoginForm').style.display = 'none';
        document.getElementById('registerForm').style.display = 'none';
        adminToggle.classList.remove('active');
        loginPanel.classList.remove('admin-mode');
        document.querySelector('.login-logo').textContent = '用户登录';
        adminToggle.textContent = 'Admin';
    } else {
        document.getElementById('userLoginForm').style.display = 'none';
        document.getElementById('adminLoginForm').style.display = 'block';
        document.getElementById('registerForm').style.display = 'none';
        adminToggle.classList.add('active');
        loginPanel.classList.add('admin-mode');
        document.querySelector('.login-logo').textContent = '管理员登录';
        adminToggle.textContent = 'User';
    }
    clearMessage();
}

//#endregion
//#region 登录/注册切换功能
function showRegister() {
    document.getElementById('userLoginForm').style.display = 'none';
    document.getElementById('adminLoginForm').style.display = 'none';
    document.getElementById('registerForm').style.display = 'block';
    const logo = document.querySelector('.login-logo');
    if (logo) logo.textContent = '用户注册';
    clearMessage();
}

function showLogin() {
    document.getElementById('registerForm').style.display = 'none';
    document.getElementById('adminLoginForm').style.display = 'none';
    document.getElementById('userLoginForm').style.display = 'block';
    const logo = document.querySelector('.login-logo');
    if (logo) logo.textContent = '用户登录';
    clearMessage();
}

function clearMessage() {
    const msg = document.getElementById('authMessage');
    msg.className = 'message-box';
    msg.textContent = '';
}

function showAutoToast(message, type = 'success') {
    const toast = document.createElement('div');
    toast.className = `auto-toast auto-toast-${type}`;
    toast.textContent = message;
    document.body.appendChild(toast);

    requestAnimationFrame(() => {
        toast.classList.add('show');
    });

    setTimeout(() => {
        toast.classList.remove('show');
        setTimeout(() => {
            document.body.removeChild(toast);
        }, 300);
    }, 3000);
}

function showMessage(text, type) {
    if (type === 'error') {
        showAutoToast(text, type);
    } else {
        showAutoToast(text, type);
    }
}

async function handleLogin() {
    const usernameEl = document.getElementById('loginUsername');
    const passwordEl = document.getElementById('loginPassword');

    if (!usernameEl || !passwordEl) {
        console.error('找不到登录输入框');
        return;
    }

    const username = usernameEl.value.trim();
    const password = passwordEl.value.trim();

    if (!username || !password) {
        if (typeof showMessage === 'function') {
            showMessage('请输入用户名和密码', 'error');
        } else {
            await customAlert('请输入用户名和密码');
        }
        return;
    }

    try {
        const response = await fetch(`${API_BASE_URL}/login`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, password })
        });

        const data = await response.json();

        if (response.ok && (data.ok || data.success)) {
            currentUserId = data.user_id;
            authToken = data.token;
            const loginTime = Date.now();
            localStorage.setItem('user_id', data.user_id);
            localStorage.setItem('auth_token', data.token);
            localStorage.setItem('username', username);
            localStorage.setItem('login_time', loginTime.toString());

            // 📊 统计数据：保存到localStorage（无限期），但显示时优先使用服务器数据
            // 这些数据只用于优化显示，不影响账号安全
            if (data.balance !== undefined) {
                localStorage.setItem('user_balance_cache', data.balance);
            }
            if (data.usage_records) {
                localStorage.setItem('user_usage_records_cache', JSON.stringify(data.usage_records));
            }
            if (data.access_records) {
                localStorage.setItem('user_access_records_cache', JSON.stringify(data.access_records));
            }
            if (data.inbox_conversations) {
                localStorage.setItem('user_inbox_conversations_cache', JSON.stringify(data.inbox_conversations));
            }
            if (data.task_results || data.history_tasks) {
                localStorage.setItem('user_history_tasks_cache', JSON.stringify(data.task_results || data.history_tasks));
            }

            if (typeof showMessage === 'function') {
                showMessage('登录成功！正在跳转...', 'success');
            }

            setTimeout(() => {
                const loginPage = document.getElementById('loginPage');
                const contentWrapper = document.querySelector('.content-wrapper');
                const mainContainer = document.querySelector('.main-container');

                if (loginPage) loginPage.style.display = 'none';
                document.body.classList.remove('login-mode');
                if (contentWrapper) contentWrapper.style.display = 'flex';
                if (mainContainer) mainContainer.style.display = 'flex';

                if (data.balance !== undefined && typeof updateUserInfoDisplay === 'function') {
                    updateUserInfoDisplay(data.balance);
                }

                if (typeof showMainApp === 'function') {
                    showMainApp();
                }

                if (typeof window.init === 'function') {
                    window.init();
                }

                // 自动显示主页面（在init之后调用，确保主页面正确显示）
                if (typeof switchPanel === 'function') {
                    setTimeout(() => {
                        switchPanel('home');
                    }, 200);
                }
            }, 500);
        } else {
            const errorMsg = data.message || '密码错误';
            if (typeof showMessage === 'function') {
                showMessage(errorMsg, 'error');
            } else {
                await customAlert(errorMsg);
            }
        }
    } catch (error) {
        let errorMsg = '登录失败';
        try {
            if (error.response) {
                const errorData = await error.response.json();
                errorMsg = errorData.message || '密码错误';
            } else if (error.message && error.message.includes('fetch')) {
                errorMsg = '网络连接失败，请稍后重试';
            }
        } catch (e) {
            errorMsg = '密码错误';
        }

        if (typeof showMessage === 'function') {
            showMessage(errorMsg, 'error');
        } else {
            await customAlert(errorMsg);
        }
    }
}
window.handleLogin = handleLogin;
async function handleAdminLogin() {
    const username = document.getElementById('adminLoginUsername').value.trim();
    const password = document.getElementById('adminLoginPassword').value.trim();

    if (!username || !password) {
        showMessage('请输入管理员用户名和密码', 'error');
        return;
    }

    // 始终优先走 API 管理员登录，拿到真实 admin_token（否则 /admin/account 会 403）
    // 注意：严禁把 password/token 打到日志里
    async function doAdminLogin() {
        try {
            if (typeof showMessage === 'function') {
                showMessage('正在验证管理员身份...', 'info');
            }

            const response = await fetch(`${API_BASE_URL}/admin/login`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ admin_id: username, password: password })
            });

            const data = await response.json().catch(() => ({}));


            if (response.ok && (data.ok || data.success) && data.token) {
                // 🔒 管理员登录：使用sessionStorage，关闭页面就清除
                // 密码验证通过后，保存token到sessionStorage用于本次会话的API调用
                if (data.token) {
                    sessionStorage.setItem('admin_token', data.token);
                }
                // 不保存 admin_id 和 admin_username，防止自动登录

                showMessage('管理员登录成功！正在跳转...', 'success');
                setTimeout(() => {
                    // 不再依赖 data.has_manager_access（API 不一定返回该字段）
                    loginAsManager(username);
                }, 500);
                return;
            }

            const errorMsg = data.message || '管理员登录失败，请检查用户名和密码';
            if (typeof showMessage === 'function') showMessage(errorMsg, 'error');
            else await customAlert(errorMsg);
        } catch (error) {
            const errorMsg = (error && error.message && error.message.includes('fetch')) ? '网络连接失败，请稍后重试' : '管理员登录失败';
            if (typeof showMessage === 'function') showMessage(errorMsg, 'error');
            else await customAlert(errorMsg);
        }
    }

    await doAdminLogin();
}

async function handleRegister() {
    const usernameEl = document.getElementById('registerUsername');
    const passwordEl = document.getElementById('registerPassword');
    const confirmPasswordEl = document.getElementById('registerConfirmPassword');

    if (!usernameEl || !passwordEl || !confirmPasswordEl) {
        console.error('找不到注册输入框');
        return;
    }

    const username = usernameEl.value.trim();
    const password = passwordEl.value.trim();
    const confirmPassword = confirmPasswordEl.value.trim();

    if (!username) {
        if (typeof showMessage === 'function') {
            showMessage('请输入用户名', 'error');
        } else {
            await customAlert('请输入用户名');
        }
        return;
    }

    if (username.length < 4) {
        if (typeof showMessage === 'function') {
            showMessage('用户名至少需要4位', 'error');
        } else {
            await customAlert('用户名至少需要4位');
        }
        return;
    }

    if (!/^[a-zA-Z0-9]+$/.test(username)) {
        if (typeof showMessage === 'function') {
            showMessage('用户名只能包含字母或数字', 'error');
        } else {
            await customAlert('用户名只能包含字母或数字');
        }
        return;
    }

    if (!password) {
        if (typeof showMessage === 'function') {
            showMessage('请输入密码', 'error');
        } else {
            await customAlert('请输入密码');
        }
        return;
    }

    if (password.length < 4) {
        if (typeof showMessage === 'function') {
            showMessage('密码至少需要4位', 'error');
        } else {
            await customAlert('密码至少需要4位');
        }
        return;
    }

    if (!confirmPassword) {
        if (typeof showMessage === 'function') {
            showMessage('请确认密码', 'error');
        } else {
            await customAlert('请确认密码');
        }
        return;
    }

    if (password !== confirmPassword) {
        if (typeof showMessage === 'function') {
            showMessage('两次输入的密码不一致', 'error');
        } else {
            await customAlert('两次输入的密码不一致');
        }
        return;
    }

    async function doRegister() {
        try {
            if (typeof showMessage === 'function') {
                showMessage('正在注册...', 'info');
            }

            const response = await fetch(`${API_BASE_URL}/register`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    username: username,
                    password: password
                })
            });

            const data = await response.json();

            if (response.ok && (data.ok || data.success)) {
                document.getElementById('registerUsername').value = '';
                document.getElementById('registerPassword').value = '';
                document.getElementById('registerConfirmPassword').value = '';

                if (typeof showLogin === 'function') {
                    showLogin();
                }
                const loginUsernameEl = document.getElementById('loginUsername');
                const loginPasswordEl = document.getElementById('loginPassword');
                if (loginUsernameEl) {
                    loginUsernameEl.value = username;
                }
                if (loginPasswordEl) {
                    loginPasswordEl.value = '';
                }

                setTimeout(async () => {
                    await customAlert('注册成功！');
                }, 300);
            } else {
                const errorMsg = data.message || '注册失败';
                if (typeof showMessage === 'function') {
                    showMessage(errorMsg, 'error');
                } else {
                    await customAlert(errorMsg);
                }
            }
        } catch (error) {
            let errorMsg = '注册失败';
            if (error.message && error.message.includes('fetch')) {
                errorMsg = '网络连接失败，请稍后重试';
            } else {
                try {
                    if (error.response) {
                        const errorData = await error.response.json();
                        errorMsg = errorData.message || '注册失败';
                    }
                } catch (e) {
                    errorMsg = '注册失败';
                }
            }

            if (typeof showMessage === 'function') {
                showMessage(errorMsg, 'error');
            } else {
                await customAlert(errorMsg);
            }
        }
    }

    doRegister();
}
window.showAdminModal = function showAdminModal() {
    const modal = document.getElementById('adminModal');
    if (!modal) return;

    requestAnimationFrame(() => {
        modal.classList.add('show');
        setTimeout(() => {
            document.getElementById('adminPasswordInput').focus();
        }, 50);
    });
}

function closeAdminModal() {
    const modal = document.getElementById('adminModal');
    if (!modal) return;

    modal.classList.remove('show');

    setTimeout(() => {
        document.getElementById('adminPasswordInput').value = '';
        document.getElementById('adminMessage').className = 'modal-message';
    }, 300);
}

async function verifyAdminPassword() {
    const password = document.getElementById('adminPasswordInput').value;
    const msg = document.getElementById('adminMessage');

    if (!password) {
        msg.className = 'modal-message error';
        msg.textContent = '请输入密码';
        setTimeout(() => {
            msg.className = 'modal-message';
            msg.textContent = '';
        }, 3000);
        return;
    }


    try {

        const response = await fetch(`${API_BASE_URL}/server-manager/login`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ password: password })
        });

        const data = await response.json();

        if (!response.ok || !data.success || !data.token) {
            const errorMsg = data.message || '密码错误';
            msg.className = 'modal-message error';
            msg.textContent = errorMsg;
            setTimeout(() => {
                msg.className = 'modal-message';
                msg.textContent = '';
            }, 5000);
            return;
        }

        if (data.success) {
            // 🔒 服务器管理页面：使用sessionStorage，关闭页面就清除
            // 密码验证通过后，保存token到sessionStorage用于本次会话的API调用
            try {
                if (data.token) {
                    sessionStorage.setItem('server_manager_token', data.token);
                }
            } catch { /* ignore */ }
            closeAdminModal();
            const loginPage = document.getElementById('loginPage');
            const adminPage = document.getElementById('adminPage');

            if (loginPage) {
                loginPage.style.display = 'none';
                document.body.classList.remove('login-mode');
            }

            if (adminPage) {
                adminPage.style.display = 'block';
                adminPage.classList.add('show');

                const scheduleUpdate = (callback) => {
                    if (window.requestIdleCallback) {
                        requestIdleCallback(callback, { timeout: 1000 });
                    } else {
                        setTimeout(callback, 100);
                    }
                };

                const scheduleServerUpdate = (callback) => {
                    if (window.requestIdleCallback) {
                        requestIdleCallback(callback, { timeout: 500 });
                    } else {
                        setTimeout(callback, 200);
                    }
                };

                // 关键：先拉取数据，再渲染（否则页面为空）
                scheduleServerUpdate(async () => {
                    try { await loadServersFromAPI(); } catch { /* ignore */ }
                    try { updateServerDisplay(); } catch { /* ignore */ }
                });

                scheduleUpdate(async () => {
                    try { await loadAdminAccountsFromAPI(); } catch { /* ignore */ }
                    try { updateAdminAccountDisplay(); } catch { /* ignore */ }
                });
            }
        } else {
            msg.className = 'modal-message error';
            msg.textContent = data.message || '密码错误，请重试';
            setTimeout(() => {
                msg.className = 'modal-message';
                msg.textContent = '';
            }, 5000);
        }
    } catch (error) {
        let errorMsg = '密码错误';
        try {
            if (error.response) {
                const errorData = await error.response.json();
                errorMsg = errorData.message || '密码错误';
            } else if (error.message && error.message.includes('fetch')) {
                errorMsg = '网络连接失败，请稍后重试';
            }
        } catch (e) {
            errorMsg = '密码错误';
        }

        msg.className = 'modal-message error';
        msg.textContent = errorMsg;
        setTimeout(() => {
            msg.className = 'modal-message';
            msg.textContent = '';
        }, 5000);
    }
}

async function backToLogin() {
    try {
        // localStorage.setItem('serverData', JSON.stringify(serverData));
        localStorage.setItem('adminAccounts', JSON.stringify(adminAccounts));
    } catch (error) {
        console.error('保存数据失败:', error);
    }

    const result = await showCustomModal('配置已保存', '配置已保存', 'alert', '', [
        { text: '返回登录界面', value: 'login' },
        { text: '进入主面板', value: 'main' }
    ]);

    if (result === 'login') {
        const adminPage = document.getElementById('adminPage');
        const managerPage = document.getElementById('managerPage');
        const loginPage = document.getElementById('loginPage');
        if (adminPage) {
            adminPage.classList.remove('show');
            adminPage.style.display = 'none';
        }
        if (managerPage) {
            managerPage.classList.remove('show');
            managerPage.style.display = 'none';
        }
        if (loginPage) {
            loginPage.style.display = 'flex';
            document.body.classList.add('login-mode');
        }
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
        // 停止管理员页面的定时器
        if (typeof stopOnlineServersTimer === 'function') {
            stopOnlineServersTimer();
        }
    } else if (result === 'main') {
        const adminPage = document.getElementById('adminPage');
        const managerPage = document.getElementById('managerPage');
        const loginPage = document.getElementById('loginPage');
        if (adminPage) {
            adminPage.classList.remove('show');
            adminPage.style.display = 'none';
        }
        if (managerPage) {
            managerPage.classList.remove('show');
            managerPage.style.display = 'none';
        }
        if (loginPage) {
            loginPage.style.display = 'none';
            document.body.classList.remove('login-mode');
        }
        const contentWrapper = document.querySelector('.content-wrapper');
        const mainContainer = document.querySelector('.main-container');
        if (contentWrapper) {
            contentWrapper.style.display = 'flex';
        }
        if (mainContainer) {
            mainContainer.style.display = 'flex';
        }
        const navHomeBtn = document.getElementById('navHomeBtn');
        if (navHomeBtn && typeof navHomeBtn.click === 'function') {
            navHomeBtn.click();
        }
        // 停止管理员页面的定时器
        if (typeof stopOnlineServersTimer === 'function') {
            stopOnlineServersTimer();
        }
    }
}

document.getElementById('adminPasswordInput').addEventListener('keypress', function (e) {
    if (e.key === 'Enter') {
        verifyAdminPassword();
    }
});

document.getElementById('loginPassword').addEventListener('keypress', function (e) {
    if (e.key === 'Enter') {
        handleLogin();
    }
});

document.getElementById('adminLoginPassword').addEventListener('keypress', function (e) {
    if (e.key === 'Enter') {
        handleAdminLogin();
    }
});

document.getElementById('registerUsername').addEventListener('keypress', function (e) {
    if (e.key === 'Enter') {
        document.getElementById('registerPassword').focus();
    }
});

document.getElementById('registerPassword').addEventListener('keypress', function (e) {
    if (e.key === 'Enter') {
        document.getElementById('registerConfirmPassword').focus();
    }
});

document.getElementById('registerConfirmPassword').addEventListener('keypress', function (e) {
    if (e.key === 'Enter') {
        handleRegister();
    }
});

function togglePassword(inputId, button) {
    const input = document.getElementById(inputId);
    if (input.type === 'password') {
        input.type = 'text';
        button.textContent = '🙈';
    } else {
        input.type = 'password';
        button.textContent = '👁️';
    }
}
let customModalResolve = null;

function showCustomModal(title, message, type = 'alert', defaultValue = '', customButtons = null) {
    const modal = document.getElementById('customModal');
    const panel = document.getElementById('customModalPanel');
    const titleEl = document.getElementById('customModalTitle');
    const messageEl = document.getElementById('customModalMessage');
    const inputEl = document.getElementById('customModalInput');
    const buttonsEl = document.getElementById('customModalButtons');

    panel.className = 'custom-modal-panel';

    titleEl.textContent = title;
    if (typeof message === 'string') {
        messageEl.textContent = message;
    } else {
        messageEl.innerHTML = message;
    }

    if (type === 'prompt') {
        inputEl.style.display = 'block';
        inputEl.value = defaultValue;
        inputEl.focus();
    } else {
        inputEl.style.display = 'none';
    }

    buttonsEl.innerHTML = '';
    if (customButtons && Array.isArray(customButtons)) {
        customButtons.forEach(btnConfig => {
            const btn = document.createElement('button');
            btn.className = 'custom-modal-btn confirm';
            btn.textContent = btnConfig.text;
            btn.onclick = () => closeCustomModal(btnConfig.value);
            buttonsEl.appendChild(btn);
        });
    } else if (type === 'alert') {
        const btn = document.createElement('button');
        btn.className = 'custom-modal-btn confirm';
        btn.textContent = '确定';
        btn.onclick = () => closeCustomModal(true);
        buttonsEl.appendChild(btn);
    } else if (type === 'confirm') {
        const cancelBtn = document.createElement('button');
        cancelBtn.className = 'custom-modal-btn cancel';
        cancelBtn.textContent = '取消';
        cancelBtn.onclick = () => closeCustomModal(false);
        buttonsEl.appendChild(cancelBtn);

        const confirmBtn = document.createElement('button');
        confirmBtn.className = 'custom-modal-btn confirm';
        confirmBtn.textContent = '确定';
        confirmBtn.onclick = () => closeCustomModal(true);
        buttonsEl.appendChild(confirmBtn);
    } else if (type === 'prompt') {
        const cancelBtn = document.createElement('button');
        cancelBtn.className = 'custom-modal-btn cancel';
        cancelBtn.textContent = '取消';
        cancelBtn.onclick = () => closeCustomModal(null);
        buttonsEl.appendChild(cancelBtn);

        const confirmBtn = document.createElement('button');
        confirmBtn.className = 'custom-modal-btn confirm';
        confirmBtn.textContent = '确定';
        confirmBtn.onclick = () => {
            const value = inputEl.value.trim();
            closeCustomModal(value || null);
        };
        buttonsEl.appendChild(confirmBtn);
    }

    requestAnimationFrame(() => {
        modal.classList.add('show');
    });

    const handleEnter = (e) => {
        if (e.key === 'Enter') {
            if (type === 'prompt') {
                const value = inputEl.value.trim();
                closeCustomModal(value || null);
            } else {
                closeCustomModal(true);
            }
            inputEl.removeEventListener('keypress', handleEnter);
        }
    };
    if (type === 'prompt') {
        inputEl.addEventListener('keypress', handleEnter);
    }

    return new Promise((resolve) => {
        customModalResolve = resolve;
    });
}

function closeCustomModal(result) {
    const modal = document.getElementById('customModal');
    const panel = document.getElementById('customModalPanel');
    const content = document.getElementById('customModalContent');

    if (!modal) {
        if (customModalResolve) {
            customModalResolve(result);
            customModalResolve = null;
        }
        return;
    }

    modal.classList.remove('show');

    setTimeout(() => {
        if (panel) panel.className = 'custom-modal-panel';
        if (content) content.className = 'custom-modal-content';

        if (customModalResolve) {
            customModalResolve(result);
            customModalResolve = null;
        }
    }, 300);
}

async function customAlert(message) {
    await showCustomModal('提示', message, 'alert');
}

async function customConfirm(message) {
    return await showCustomModal('确认', message, 'confirm');
}

async function customPrompt(message, defaultValue = '') {
    return await showCustomModal('输入', message, 'prompt', defaultValue);
}
function handleForgotPassword() {
    customAlert('请联系管理员并提供正确的用户名');
}

//#endregion
