//#region API消息处理模块
function handleServerMessage(data, serverId = null) {
    if (!data || typeof data !== 'object') return;

    if (data.type === 'auth' && data.ok) {
        try { sendWSCommand('subscribe_servers', {}); } catch { /* ignore */ }
        return;
    }

    if (data.type === 'balance_update') {
        const newBalance = data.balance !== undefined ? data.balance : data.credits;
        if (newBalance !== undefined) {
            localStorage.setItem('user_balance', newBalance);
            if (typeof updateUserInfoDisplay === 'function') {
                updateUserInfoDisplay(newBalance);
            }
        }
        return;
    }

    if (data.type === 'usage_update' || data.type === 'usage_records_update') {
        if (data.records || data.usage_records) {
            localStorage.setItem('user_usage_records', JSON.stringify(data.records || data.usage_records));
            if (typeof updateUsageRecordsDisplay === 'function') {
                updateUsageRecordsDisplay(data.records || data.usage_records);
            }
        }
        return;
    }

    if (data.type === 'access_update' || data.type === 'access_records_update') {
        if (data.records || data.access_records) {
            localStorage.setItem('user_access_records', JSON.stringify(data.records || data.access_records));
            if (typeof updateAccessRecordsDisplay === 'function') {
                updateAccessRecordsDisplay(data.records || data.access_records);
            }
        }
        return;
    }

    if (data.type === 'servers_event') {
        try { loadServersFromAPI(); } catch { /* ignore */ }
        try { loadExclusivePhoneNumbers(); } catch { /* ignore */ }
        return;
    }

    if (data.type === 'balance_update' && data.balance !== undefined) {
        localStorage.setItem('user_balance', data.balance);
        try { updateUserInfoDisplay(data.balance); } catch { /* ignore */ }
        return;
    }

    if (data.type === 'usage_update' && data.usage_records) {
        localStorage.setItem('user_usage_records', JSON.stringify(data.usage_records));
        return;
    }

    if (data.type === 'access_update' && data.access_records) {
        localStorage.setItem('user_access_records', JSON.stringify(data.access_records));
        return;
    }

    if (data.type === 'task_update' && data.data) {
        const LOCATION = '[dadfunction.js][handleServerMessage]';
        const payload = data.data;
        const taskId = payload.task_id || data.task_id;
        const traceId = payload.trace_id || (taskId ? (localStorage.getItem(`trace:${taskId}`) || '') : '');
        const sc = payload.shards || {};
        const rp = payload.result || {};

        // 如果是当前追踪的任务，记录进度
        if (taskTracker.currentTaskId === taskId) {
            if (payload.status === 'pending') {
                taskTracker.logStep('📤 分片分配', `trace_id=${traceId} 待处理: ${sc.pending || 0}, 运行中: ${sc.running || 0}, 已完成: ${sc.done || 0}/${sc.total || 0}`, LOCATION);
            } else if (payload.status === 'running') {
                taskTracker.logStep('⚙️ Worker处理中', `trace_id=${traceId} 已完成: ${sc.done || 0}/${sc.total || 0}, 成功: ${rp.success || 0}, 失败: ${rp.fail || 0}`, LOCATION);
            }
        }

        if (payload.status === 'pending' || payload.status === 'running') {
            addStatusMessage(`任务 ${taskId} 进行中... shard ${sc.done || 0}/${sc.total || 0} 成功 ${rp.success || 0} 失败 ${rp.fail || 0}`, 'info');
        }

        if (payload.status === 'done') {
            if (taskTracker.currentTaskId === taskId) {
                taskTracker.logStep('✅ 所有分片完成', `trace_id=${traceId} 成功: ${rp.success || 0}, 失败: ${rp.fail || 0}, 总计: ${rp.sent || 0}`, LOCATION);
            }
            addStatusMessage(`任务 ${taskId} 完成：成功 ${rp.success || 0} 失败 ${rp.fail || 0} 发送 ${rp.sent || 0}`, 'success');
            const total = Number(rp.sent || (Number(rp.success || 0) + Number(rp.fail || 0)));
            updateGlobalStats(total, Number(rp.success || 0), Number(rp.fail || 0));

            isSending = false;
            updateButtonState();
            stopTaskStatusCheck();

            // resolve waiter
            if (taskId && _taskWsWaiters.has(taskId)) {
                const w = _taskWsWaiters.get(taskId);
                _taskWsWaiters.delete(taskId);
                try { clearTimeout(w.timeoutId); } catch { /* ignore */ }
                try { w.resolve(payload); } catch { /* ignore */ }
            }
        }
        return;
    }

    if (data.type === 'status_update') {
        if (data.message === "TASK_COMPLETED") {
            isSending = false;
            updateButtonState();
            stopTaskStatusCheck();
            return;
        }
        addStatusMessage(data.message, data.message_type || 'info');

        if (data.message && typeof data.message === 'string') {
            const timeMatch = data.message.match(/发送完成\s+用时:\s*(\d+)秒/i);
            if (timeMatch) {
                const timeUsed = parseInt(timeMatch[1]) || 0;
                if (timeUsed > 0) {
                    globalStats.totalTime += timeUsed;
                    updateTimeDisplay();
                }
            }

            const statsMatch = data.message.match(/Total:\s*(\d+)\s+numbers:\s*(\d+)\s+Success:\s*(\d+)\s+Failed:\s*(\d+)/i);
            if (statsMatch) {
                const totalMessages = parseInt(statsMatch[1]) || 0;
                const phoneCount = parseInt(statsMatch[2]) || 0;
                const success = parseInt(statsMatch[3]) || 0;
                const fail = parseInt(statsMatch[4]) || 0;
                const messageCount = phoneCount > 0 ? Math.floor(totalMessages / phoneCount) : 1;
                const successMessages = success * messageCount;
                const failMessages = fail * messageCount;
                if (phoneCount > 0 || success > 0 || fail > 0) {
                    updateGlobalStats(totalMessages, successMessages, failMessages);
                }
            } else {
                const successMatch = data.message.match(/Success[：:]\s*(\d+)/i);
                const failMatch = data.message.match(/Failed[：:]\s*(\d+)/i);
                const totalMatch = data.message.match(/Total[：:]\s*(\d+)/i);

                if (successMatch || failMatch || totalMatch) {
                    const success = successMatch ? parseInt(successMatch[1]) : 0;
                    const fail = failMatch ? parseInt(failMatch[1]) : 0;
                    const total = totalMatch ? parseInt(totalMatch[1]) : (success + fail);
                    if (total > 0 || success > 0 || fail > 0) {
                        updateGlobalStats(total, success, fail);
                    }
                }
            }
        }
    } else if (data.type === 'connected') {
    } else if (data.type === 'initial_chats') {
        updateContactList(data.data);
    } else if (data.type === 'new_messages') {
        if (data.data.count > 0) {
            showNotification(`收到 ${data.data.count} 条新消息！`, 'info');
        }
        updateContactList(data.data.chat_list, data.data.updated_chats);
        if (data.data && data.data.updated_chats && data.data.updated_chats.length > 0) {
            const updatedChatId = data.data.updated_chats[0];
            const chat = data.data.chat_list.find(c => c.chat_id === updatedChatId);
            if (chat && (!currentChatId || currentChatId !== updatedChatId) && data.data.count > 0) {
                showNewMessageNotification(updatedChatId, chat.name, chat.last_message_preview);
            }
        }
        if (currentChatId && data.data.updated_chats && data.data.updated_chats.includes(currentChatId)) {
            const conversationDisplay = document.getElementById('conversationDisplay');
            const tempMessage = conversationDisplay.querySelector('[data-temp-message="true"]');
            if (!tempMessage) {
                requestConversation(currentChatId);
            }
        }
    } else if (data.type === 'conversation_data') {
        const conversationDisplay = document.getElementById('conversationDisplay');
        const tempMessage = conversationDisplay.querySelector('[data-temp-message="true"]');
        if (tempMessage && data.chat_id === currentChatId) {
            if (data.data && data.data.messages && data.data.messages.length > 0) {
                const lastMsg = data.data.messages[data.data.messages.length - 1];
                const tempMsgText = tempMessage.querySelector('span').textContent.trim();
                const lastMsgText = (lastMsg.text || '').trim();
                if (lastMsg.is_from_me && lastMsgText === tempMsgText) {
                    tempMessage.removeAttribute('data-temp-message');
                    let received = 0;
                    let sent = 0;
                    data.data.messages.forEach(msg => {
                        if (msg.is_from_me) {
                            sent++;
                        } else {
                            received++;
                        }
                    });
                    inboxMessageStats[data.chat_id] = { received: received, sent: sent };
                    updateInboxStats();
                    return;
                }
            }
            tempMessage.removeAttribute('data-temp-message');
            if (data.data && data.data.messages) {
                let received = 0;
                let sent = 0;
                data.data.messages.forEach(msg => {
                    if (msg.is_from_me) {
                        sent++;
                    } else {
                        received++;
                    }
                });
                inboxMessageStats[data.chat_id] = { received: received, sent: sent };
                updateInboxStats();
            }
            return;
        }
        displayConversation(data.data, data.chat_id);
    } else if (data.status === "success" && data.message === "回复已发送") {
        document.getElementById('replyInput').value = '';
    } else if (data.status === "error" && data.message.includes("回复发送失败")) {
    }
}

function addStatusMessage(message, type = 'info') {
    const statusList = document.getElementById('statusList');
    const timestamp = new Date().toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit', hour12: false });

    const colors = {
        total: '#9C27B0',
        time: '#2196F3',
        success: '#4CAF50',
        fail: '#FF8A80',
        rate: '#FF9800'
    };

    function formatMessage(msg) {
        let formatted = msg;
        formatted = `<span style="color: #000000;">${formatted}</span>`;
        formatted = formatted.replace(/Total:\s*(\d+)/gi,
            `Total: <span style="color: ${colors.total}; font-weight: bold;">$1</span>`);
        formatted = formatted.replace(/Success:\s*(\d+)/gi,
            `Success: <span style="color: ${colors.success}; font-weight: bold;">$1</span>`);
        formatted = formatted.replace(/Failed:\s*(\d+)/gi,
            `Failed: <span style="color: ${colors.fail}; font-weight: bold;">$1</span>`);
        formatted = formatted.replace(/(成功率\s*)(\d+\/\d+\([^)]+\))/gi,
            `$1<span style="color: ${colors.rate}; font-weight: bold;">$2</span>`);
        formatted = formatted.replace(/(用时:\s*)([\d:]+|[\d.]+[秒分时])/gi,
            `$1<span style="color: ${colors.time}; font-weight: bold;">$2</span>`);
        return formatted;
    }

    if (message.includes('开始发送')) {
        const startMatch = message.match(/开始发送\s+([^\s(]+)\s*\(From\s+([^)]+)\s+to\s+([^)]+)\)/);
        if (startMatch) {
            const taskId = startMatch[1];
            const fromPhone = startMatch[2];
            const toPhone = startMatch[3];

            const messageEl = document.createElement('div');
            messageEl.className = 'log-item';
            messageEl.innerHTML = `<span style="color: #000000; font-weight: bold;">[${timestamp}] > 开始发送 ${taskId} :</span><br><span style="color: #000000;"> (From ${fromPhone} to ${toPhone})</span><br>`;
            statusList.appendChild(messageEl);
        } else {
            const messageEl = document.createElement('div');
            messageEl.className = 'log-item';
            messageEl.innerHTML = `<span style="color: #000000; font-weight: bold;">[${timestamp}] > ${message}</span>`;
            statusList.appendChild(messageEl);
        }
    }
    else if (message.includes('发送完成')) {
        const messageEl = document.createElement('div');
        messageEl.className = 'log-item';
        let formattedMsg = message;
        formattedMsg = formattedMsg.replace(/(用时:\s*)([\d.]+秒)/g,
            `$1<span style="color: ${colors.time}; font-weight: bold;">$2</span>`);
        formattedMsg = formattedMsg.replace(/发送完成\s+/, '发送完成    ');
        messageEl.innerHTML = `<span style="color: #000000;">[${timestamp}] > ${formattedMsg}</span><br>`;
        statusList.appendChild(messageEl);
    }
    else if (message.includes('Total:') && message.includes('Success:')) {
        const failPattern = /(\u274C\s*失败的消息:)/;
        const hasFailures = failPattern.test(message);

        let resultPart = message;
        let failPart = '';

        if (hasFailures) {
            const failMatch = message.match(failPattern);
            if (failMatch) {
                const failIndex = failMatch.index;
                resultPart = message.substring(0, failIndex).trim();
                failPart = message.substring(failIndex).trim();
            }
        }

        let statsMsg = resultPart.trim();
        statsMsg = statsMsg.replace(/\s+成功率\s+/, '   成功率 ');

        const messageEl = document.createElement('div');
        messageEl.className = 'log-item';
        messageEl.innerHTML = `<span style="color: #000000;">[${timestamp}] > ${formatMessage(statsMsg)}</span>`;
        statusList.appendChild(messageEl);

        if (failPart) {
            const emptyEl = document.createElement('div');
            emptyEl.className = 'log-item';
            emptyEl.style.height = '8px';
            statusList.appendChild(emptyEl);

            const failTitleEl = document.createElement('div');
            failTitleEl.className = 'log-item';
            failTitleEl.innerHTML = '<span style="color: #000000; font-weight: bold;">&#10060; 失败的消息:</span>';
            statusList.appendChild(failTitleEl);

            let failText = failPart;
            if (failText.indexOf('失败的消息:') >= 0) {
                failText = failText.substring(failText.indexOf('失败的消息:') + '失败的消息:'.length).trim();
            }
            failText = failText.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
            const failLines = failText.split('\n')
                .map(line => line.trim())
                .filter(line => line && /^\[\d+\]/.test(line));

            failLines.forEach(line => {
                const failItemEl = document.createElement('div');
                failItemEl.className = 'log-item';
                const phoneMatch = line.match(/^\[(\d+)\]\s*(.+?)\s*-\s*(.+)$/);
                if (phoneMatch) {
                    const index = phoneMatch[1];
                    const phone = phoneMatch[2];
                    const originalStatus = phoneMatch[3];

                    const errorCodeMatch = originalStatus.match(/\(错误码:\s*(\d+)\)/);
                    let displayText;
                    if (errorCodeMatch) {
                        displayText = ' 此号码不支持IMessage';
                    } else {
                        displayText = ' 此号码不支持IMessage';
                    }
                    failItemEl.innerHTML = `<span style="color: #000000;">[${index}] ${phone} -${displayText}</span>`;
                } else {
                    const simpleMatch = line.match(/^\[(\d+)\]\s*(.+?)\s*-/);
                    if (simpleMatch) {
                        failItemEl.innerHTML = `<span style="color: #000000;">[${simpleMatch[1]}] ${simpleMatch[2]} - 此号码不支持IMessage</span>`;
                    } else {
                        failItemEl.innerHTML = `<span style="color: #000000;">${line.replace(/-\s*[^-]+$/, ' - 此号码不支持IMessage')}</span>`;
                    }
                }
                statusList.appendChild(failItemEl);
            });
        }
    }
    else {
        const messageEl = document.createElement('div');
        messageEl.className = 'log-item';
        messageEl.innerHTML = `<span style="color: #000000;">[${timestamp}] > ${message}</span>`;
        statusList.appendChild(messageEl);
    }

    const logItems = statusList.querySelectorAll('.log-item');
    if (logItems.length > MAX_LOG_ITEMS) {
        for (let i = 0; i < logItems.length - MAX_LOG_ITEMS; i++) {
            logItems[i].remove();
        }
    }

    const statusListMobile = document.getElementById('statusListMobile');
    if (statusListMobile) {
        statusListMobile.innerHTML = statusList.innerHTML;
        const mobileLogItems = statusListMobile.querySelectorAll('.log-item');
        if (mobileLogItems.length > MAX_LOG_ITEMS) {
            for (let i = 0; i < mobileLogItems.length - MAX_LOG_ITEMS; i++) {
                mobileLogItems[i].remove();
            }
        }
        statusListMobile.scrollTop = statusListMobile.scrollHeight;
    }

    if (!logScrollPending) {
        logScrollPending = true;
        requestAnimationFrame(() => {
            statusList.scrollTop = statusList.scrollHeight;
            logScrollPending = false;
        });
    }
}

//#endregion
//#region 发送短信API交互功能模块（零轮询：WebSocket 实时推送）
// 零轮询架构：create(生成任务) -> API 立即推送到 Worker -> WebSocket 实时接收进度

function _sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function _authToken() {
    const token = localStorage.getItem('auth_token') || '';
    const loginTime = localStorage.getItem('login_time');
    
    if (!token) {
        return '';
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
            return '';
        }
    }
    
    if (token && typeof authToken !== 'undefined') {
        authToken = token;
    }
    return token;
}


const _activeTaskWatcher = {
    taskId: null,
    eventSource: null,
    pollTimer: null
};

function _stopTaskWatchersOnly() {
    try {
        if (_activeTaskWatcher.eventSource) {
            _activeTaskWatcher.eventSource.close();
            _activeTaskWatcher.eventSource = null;
        }
        if (_activeTaskWatcher.pollTimer) {
            clearInterval(_activeTaskWatcher.pollTimer);
            _activeTaskWatcher.pollTimer = null;
        }
    } catch (e) {
        console.warn('stopAllTaskPolling failed:', e);
    }
    _activeTaskWatcher.taskId = null;
}

function stopAllTaskPolling() {
    _stopTaskWatchersOnly();
    isSending = false;
    updateButtonState();
}

async function _createTask({ message, numbers, taskType = 'normal' }) {
    const LOCATION = '[dadfunction.js][_createTask]';
    const apiStart = performance.now();
    
    taskTracker.logStep('→ 准备API请求', '验证token和用户ID', LOCATION);
    if (!currentUserId) {
        currentUserId = localStorage.getItem('user_id') || '';
    }
    const token = _authToken();
    if (!token || !currentUserId) {
        taskTracker.logStep('❌ Token验证失败', token ? '缺少用户ID' : '缺少登录token', LOCATION);
        throw new Error(token ? '缺少用户ID，请重新登录' : '缺少登录token，请重新登录');
    }
    taskTracker.logStep('✓ 验证通过', `用户ID: ${currentUserId}`, LOCATION);
    
    // 给每次发送链路加 trace_id，后端/worker 会完整带回（用于定位卡点）
    const traceId = (crypto && crypto.randomUUID) ? crypto.randomUUID() : `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    taskTracker.logStep('→ 发送HTTP请求', `POST ${API_BASE_URL}/task/create trace_id=${traceId}`, LOCATION);
    const requestStart = performance.now();
    const resp = await fetch(`${API_BASE_URL}/task/create`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({
            user_id: currentUserId,
            trace_id: traceId,
            message,
            numbers,
            count: 1,
            task_type: taskType
        })
    });
    const requestTime = (performance.now() - requestStart).toFixed(0);
    taskTracker.logStep('✓ HTTP响应收到', `状态码: ${resp.status}, 网络耗时: ${requestTime}ms`, LOCATION);
    
    let data = null;
    try { data = await resp.json(); } catch { /* ignore */ }
    
    const totalTime = (performance.now() - apiStart).toFixed(0);
    
    if (!resp.ok || !data || !data.ok) {
        const msg = (data && (data.message || data.msg)) || `${resp.status} ${resp.statusText}`;
        if (data && data.message === 'insufficient_credits') {
            const required = data.required != null ? Number(data.required).toFixed(2) : '?';
            const current = (data.current != null ? data.current : data.credits) != null ? Number(data.current || data.credits).toFixed(2) : '?';
            taskTracker.logStep('❌ API返回错误', `积分不足：需要 ${required}，当前 ${current}`, LOCATION);
            throw new Error(`积分不足：需要 ${required}，当前 ${current}`);
        }
        taskTracker.logStep('❌ API返回错误', msg, LOCATION);
        throw new Error(`创建任务失败：${msg}`);
    }
    
    taskTracker.logStep('✓ API调用完成', `总耗时: ${totalTime}ms`, LOCATION);
    
    // 把 trace_id 记住，后续 WS task_update 里也会带回来，便于前端对齐
    if (data && data.task_id) {
        try { localStorage.setItem(`trace:${data.task_id}`, data.trace_id || traceId); } catch { /* ignore */ }
    }
    return data.task_id;
}

async function _fetchTaskStatus(taskId) {
    const resp = await fetch(`${API_BASE_URL}/task/${taskId}/status`);
    let data = null;
    try { data = await resp.json(); } catch { /* ignore */ }
    if (!resp.ok || !data || !data.ok) {
        const msg = (data && (data.message || data.msg)) || `${resp.status} ${resp.statusText}`;
        throw new Error(`任务状态查询失败：${msg}`);
    }
    return data;
}

async function _waitTaskDone(taskId, totalTasks = 0) {
    if (!activeWs || activeWs.readyState !== WebSocket.OPEN) {
        throw new Error('WebSocket 未连接，无法监听任务进度。请刷新页面重试。');
    }

    _stopTaskWatchersOnly();
    _activeTaskWatcher.taskId = taskId;
    _activeTaskWatcher.totalTasks = totalTasks;

    // 创建 Promise 等待 WebSocket 推送的任务完成消息
    return new Promise((resolve, reject) => {
        const timeoutId = setTimeout(() => {
            _taskWsWaiters.delete(taskId);
            reject(new Error('任务等待超时（30分钟）'));
        }, 30 * 60 * 1000);

        const waiter = {
            promise: null,
            resolve: (payload) => {
                clearTimeout(timeoutId);
                _taskWsWaiters.delete(taskId);
                console.log(`[WebSocket] 任务 ${taskId} 完成`);
                resolve(payload);
            },
            reject: (err) => {
                clearTimeout(timeoutId);
                _taskWsWaiters.delete(taskId);
                reject(err);
            },
            timeoutId
        };

        _taskWsWaiters.set(taskId, waiter);
        console.log(`[WebSocket] 等待任务 ${taskId} 通过 WebSocket 推送完成...`);
    });
}

// 任务执行追踪器
const taskTracker = {
    startTime: null,
    steps: [],
    currentTaskId: null,
    
    start(taskId) {
        this.startTime = performance.now();
        this.steps = [];
        this.currentTaskId = taskId;
        this.logStep('开始', '任务开始执行', '[dadfunction.js][taskTracker.start]');
    },
    
    logStep(name, detail = '', location = '') {
        const now = performance.now();
        const elapsed = this.startTime ? (now - this.startTime).toFixed(0) : 0;
        const stepElapsed = this.steps.length > 0 ? (now - this.steps[this.steps.length - 1].time).toFixed(0) : elapsed;
        
        this.steps.push({
            name,
            detail,
            location,
            time: now,
            elapsed: parseFloat(elapsed),
            stepElapsed: parseFloat(stepElapsed)
        });
        
        const stepMsg = stepElapsed > 0 ? ` [+${stepElapsed}ms]` : '';
        const totalMsg = elapsed > 0 ? ` [总耗时: ${elapsed}ms]` : '';
        const locationTag = location ? `${location} ` : '';
        addStatusMessage(`${locationTag}[${name}] ${detail}${stepMsg}${totalMsg}`, 'info');
    },
    
    finish() {
        if (this.startTime) {
            const total = (performance.now() - this.startTime).toFixed(0);
            addStatusMessage(`✅ 任务完成，总耗时: ${total}ms`, 'success');
            
            // 显示耗时分析（只显示耗时>10ms的步骤）
            if (this.steps.length > 1) {
                let slowSteps = [];
                for (let i = 1; i < this.steps.length; i++) {
                    const step = this.steps[i];
                    const prevStep = this.steps[i - 1];
                    const stepTime = step.time - prevStep.time;
                    if (stepTime > 10) {
                        slowSteps.push({
                            name: step.name,
                            location: step.location || '',
                            time: stepTime.toFixed(0),
                            percent: ((stepTime / (performance.now() - this.startTime)) * 100).toFixed(1)
                        });
                    }
                }
                
                if (slowSteps.length > 0) {
                    let analysis = '📊 耗时分析 (主要步骤):\n';
                    slowSteps.forEach(step => {
                        const locationTag = step.location ? `${step.location} ` : '';
                        analysis += `  • ${locationTag}${step.name}: ${step.time}ms (${step.percent}%)\n`;
                    });
                    addStatusMessage(analysis, 'info');
                }
            }
        }
        this.reset();
    },
    
    reset() {
        this.startTime = null;
        this.steps = [];
        this.currentTaskId = null;
    }
};

async function startSending() {
    const LOCATION = '[dadfunction.js][startSending]';
    
    if (isSending) {
        await customAlert("已有任务正在执行，请等待当前任务完成");
        return;
    }

    const stepStart = performance.now();
    
    // 1. 核心预检查（前置拦截，防止发了任务却收不到结果）
    taskTracker.logStep('1. 检查WebSocket', '验证实时连接', LOCATION);
    if (!activeWs || activeWs.readyState !== WebSocket.OPEN) {
        taskTracker.logStep('❌ 检查失败', 'WebSocket未连接', LOCATION);
        await customAlert('🔴 实时服务未连接，请刷新页面后重试');
        taskTracker.reset();
        return;
    }
    taskTracker.logStep('✓ WebSocket检查', '连接正常', LOCATION);

    // 2. 资源检查（防止无Worker空跑）
    taskTracker.logStep('2. 检查Worker服务器', '验证可用资源', LOCATION);
    if (!serverData.connected || serverData.connected.length === 0) {
        taskTracker.logStep('⚠️ Worker检查', '当前无在线服务器', LOCATION);
        const confirmSend = await customConfirm('⚠️ 当前显示无在线服务器，任务可能无法执行。\n\n是否仍要强制发送？');
        if (!confirmSend) {
            taskTracker.reset();
            return;
        }
    } else {
        taskTracker.logStep('✓ Worker检查', `发现 ${serverData.connected.length} 个在线服务器`, LOCATION);
    }

    // 3. 获取输入数据
    taskTracker.logStep('3. 获取输入数据', '解析号码和消息', LOCATION);
    const numbersText = document.getElementById('numbersText').value || "";
    const message = document.getElementById('messageText').value || "";

    if (!numbersText.trim()) {
        taskTracker.logStep('❌ 输入检查失败', '号码为空', LOCATION);
        await customAlert('请输入发送号码');
        taskTracker.reset();
        return;
    }

    const numbers = numbersText
        .split(/[\n,]/)
        .map(s => (s || '').trim())
        .filter(Boolean);
    
    taskTracker.logStep('✓ 输入解析完成', `号码数: ${numbers.length}, 消息长度: ${message.length}`, LOCATION);

    isSending = true;
    updateButtonState();

    try {
        // 4. 创建任务
        taskTracker.logStep('4. 创建任务', '调用API创建任务', LOCATION);
        const createStart = performance.now();
        const taskId = await _createTask({ message, numbers, taskType: 'normal' });
        const createTime = (performance.now() - createStart).toFixed(0);
        taskTracker.start(taskId);
        taskTracker.logStep('✓ 任务已创建', `任务ID: ${taskId} (耗时: ${createTime}ms)`, LOCATION);
        console.log(`[发送] 任务 ${taskId} 已创建`);

        // 5. 订阅任务状态
        taskTracker.logStep('5. 订阅任务状态', '通过WebSocket订阅更新', LOCATION);
        sendWSCommand('subscribe_task', { task_id: taskId });
        taskTracker.logStep('✓ 订阅成功', '等待实时更新', LOCATION);

        // 启动状态检查兜底（但不依赖它作为主要反馈）
        startTaskStatusCheck(taskId);

        const waiter = _ensureTaskWaiter(taskId);

        // 6. 等待Worker处理
        taskTracker.logStep('6. 等待Worker处理', '分片分配和执行中...', LOCATION);
        const waitStart = performance.now();
        const result = await waiter.promise;
        const waitTime = (performance.now() - waitStart).toFixed(0);
        
        // 记录最终结果
        if (result && result.result) {
            taskTracker.logStep('✓ Worker处理完成', `成功: ${result.result.success || 0}, 失败: ${result.result.fail || 0}, 耗时: ${waitTime}ms`, LOCATION);
        } else {
            taskTracker.logStep('✓ Worker处理完成', `耗时: ${waitTime}ms`, LOCATION);
        }

        taskTracker.finish();
        console.log(`[发送] 任务 ${taskId} 完成`);
        stopTaskStatusCheck();
    } catch (err) {
        taskTracker.logStep('❌ 任务失败', err.message, LOCATION);
        console.error("[startSending error]", err);
        // 区分错误类型友善提示
        let errMsg = err.message || "未知错误";
        if (errMsg.includes('积分不足')) {
            await customAlert("❌ 发送失败：" + errMsg);
        } else {
            await customAlert("❌ 发送异常: " + errMsg);
        }
        taskTracker.reset();
        stopTaskStatusCheck();
    } finally {
        isSending = false;
        updateButtonState();
    }
}


function updateButtonState() {
    const sendBtn = document.getElementById('sendBtn');
    sendBtn.disabled = isSending;
    if (isSending) {
        sendBtn.textContent = '正在发送...';
    } else {
        sendBtn.textContent = '发送';
    }
}

function stopTaskStatusCheck() {
    if (taskStatusCheckTimer) {
        clearTimeout(taskStatusCheckTimer);
        taskStatusCheckTimer = null;
    }
    currentTaskId = null;
    taskStatusLastUpdate = null;
    taskStatusLastProgress = null;
    taskStatusLastProgressTime = null;
}

function startTaskStatusCheck(taskId) {
    stopTaskStatusCheck();
    currentTaskId = taskId;
    taskStatusLastUpdate = Date.now();
    taskStatusLastProgress = null;
    taskStatusLastProgressTime = null;
    let failCount = 0;
    let backoffMs = 5000; // 初始5秒（仅兜底，不做高频轮询）

    // 兜底轮询：仅用于 WS 异常/漏推时补救
    // - WS 正常时降低频率，避免把后端打出 524
    // - 连续失败（含 524）时指数退避并最终熔断停掉轮询
    const tick = async () => {
        if (!isSending || !currentTaskId) {
            stopTaskStatusCheck();
            return;
        }

        try {
            const statusData = await _fetchTaskStatus(currentTaskId);
            const taskStatus = statusData.status;
            failCount = 0; // 成功一次就清零

            // WS 正常时降低轮询频率
            const wsOk = (activeWs && activeWs.readyState === WebSocket.OPEN);
            backoffMs = wsOk ? 15000 : 5000;

            // 如果任务已完成或失败，恢复按钮
            if (taskStatus === 'done' || taskStatus === 'failed' || taskStatus === 'error') {
                console.log(`[任务状态检查] 任务 ${currentTaskId} 状态: ${taskStatus}，主动触发完成`);

                // 🔥 关键修复：不要只停定时器，要主动 resolve 主流程的 waiter，
                // 这样 startSending 里的 await waiter.promise 才能解除阻塞，
                // 从而正常执行后续的完成逻辑（如打印日志、isSending = false 等）。
                // 防止出现“轮询查到了完成，但主流程还在死等 WebSocket”的不一致状态。

                if (currentTaskId && _taskWsWaiters.has(currentTaskId)) {
                    const w = _taskWsWaiters.get(currentTaskId);
                    _taskWsWaiters.delete(currentTaskId);
                    try { clearTimeout(w.timeoutId); } catch { /* ignore */ }

                    if (taskStatus === 'done') {
                        // 构造一个模拟的 payload
                        const payload = {
                            task_id: currentTaskId,
                            status: 'done',
                            result: statusData.result || {}
                        };
                        try { w.resolve(payload); } catch { /* ignore */ }
                    } else {
                        try { w.reject(new Error(`任务结束状态: ${taskStatus}`)); } catch { /* ignore */ }
                    }
                } else {
                    // 如果没有 waiter（极少见），那只能手动恢复 UI
                    isSending = false;
                    updateButtonState();
                }

                stopTaskStatusCheck();
                return;
            }

            // 如果任务还在运行，检查是否有进展
            if (taskStatus === 'running' || taskStatus === 'pending') {
                // 构建当前进度信息（用于比较是否有进展）
                const currentProgress = {
                    status: taskStatus,
                    shards_done: (statusData.shards && statusData.shards.done) || 0,
                    shards_running: (statusData.shards && statusData.shards.running) || 0,
                    shards_pending: (statusData.shards && statusData.shards.pending) || 0,
                    result_success: (statusData.result && statusData.result.success) || 0,
                    result_fail: (statusData.result && statusData.result.fail) || 0,
                    result_sent: (statusData.result && statusData.result.sent) || 0
                };

                // 比较当前进度与上一次进度
                const progressChanged = !taskStatusLastProgress ||
                    JSON.stringify(currentProgress) !== JSON.stringify(taskStatusLastProgress);

                if (progressChanged) {
                    // 有进展，更新进度记录
                    taskStatusLastProgress = currentProgress;
                    taskStatusLastProgressTime = Date.now();
                    // console.log(`[任务状态检查] 任务 ${currentTaskId} 有进展:`, currentProgress);
                } else {
                    // 没有进展，但只要状态是 running，我们就信任服务器
                    // 只有当长时间（例如10秒）连状态都查不到时，_fetchTaskStatus 才会抛错
                    const now = Date.now();
                    // 仅仅记录日志，不再自动杀掉任务，防止误杀
                    // console.log(`[任务状态检查] 任务 ${currentTaskId} 暂无进度更新...`);
                }
            }
        } catch (err) {
            // 查询失败，不算卡住，继续等待（不恢复按钮）
            console.error('[任务状态检查] 查询失败（不算卡住，继续等待）:', err);
            // 查询失败时不恢复按钮，因为无法获取准确信息
            failCount += 1;

            // 524/超时类错误 -> 快速退避，避免风暴
            const emsg = String(err && (err.message || err) || '');
            const is524 = emsg.includes('524') || emsg.includes('status 524');

            if (failCount >= 3) {
                // 指数退避，上限 60s
                backoffMs = Math.min(60000, Math.floor(backoffMs * 1.8));
            }

            if (is524 && failCount >= 5) {
                console.warn('[任务状态检查] 连续触发 524，停止HTTP轮询，改用 WebSocket 等待（避免压垮后端）');
                stopTaskStatusCheck();
                return;
            }
        } finally {
            // 用 setTimeout 而不是 setInterval，避免请求堆叠
            if (isSending && currentTaskId) {
                taskStatusCheckTimer = setTimeout(tick, backoffMs);
            }
        }
    };

    // 立刻跑一次（但不会高频刷）
    taskStatusCheckTimer = setTimeout(tick, 1000);
}
//#endregion 发送短信API交互功能模块（零轮询：WebSocket 实时推送）
//#region 收件箱模块（C版面板 - 消息接收和回复）
function updateNotificationCount() {
    const navInboxBtn = document.getElementById('navInboxBtn');
    let notificationCountEl = null;
    if (navInboxBtn) {
        notificationCountEl = navInboxBtn.querySelector('.notification-count');
    }

    const unreadCount = unreadChatIds.size;

    if (notificationCountEl) {
        if (unreadCount > 0) {
            notificationCountEl.textContent = unreadCount > 99 ? '99+' : unreadCount;
            notificationCountEl.classList.add('has-unread');
        } else {
            notificationCountEl.textContent = '0';
            notificationCountEl.classList.remove('has-unread');
        }
    }

    updateInboxStats();
}

function resetInboxOnConnect() {
    const contactList = document.getElementById('contactList');
    if (contactList) {
        contactList.innerHTML = '<div style="font-family: \'Xiaolai\', sans-serif; text-align:center; color:rgba(47,47,47,0.5); padding:20px; font-size:14px;">暂无对话</div>';
    }

    const conversationDisplay = document.getElementById('conversationDisplay');
    if (conversationDisplay) {
        conversationDisplay.innerHTML = '<div style="font-family: \'Xiaolai\', sans-serif; text-align:center; color:rgba(47,47,47,0.5); padding:20px; font-size:14px;">选择一个对话开始聊天</div>';
    }

    currentChatId = null;
    unreadChatIds.clear();
    inboxMessageStats = {};
    updateNotificationCount();
    updateInboxStats();
    const replyInput = document.getElementById('replyInput');
    if (replyInput) {
        replyInput.disabled = true;
    }
    const sendReplyBtn = document.getElementById('sendReplyBtn');
    if (sendReplyBtn) {
        sendReplyBtn.disabled = true;
    }
}

let inboxMessageStats = {};

function updateInboxStats() {
    let totalReceived = 0;
    let totalSent = 0;
    Object.values(inboxMessageStats).forEach(stats => {
        totalReceived += stats.received || 0;
        totalSent += stats.sent || 0;
    });
    const total = totalReceived + totalSent;

    const inboxStatsEl = document.getElementById('inboxStats');
    if (inboxStatsEl) {
        inboxStatsEl.textContent = `接收: ${totalReceived}  发送: ${totalSent}  总数: ${total}`;
    }

    globalStats.inboxReceived = totalReceived;
    globalStats.inboxSent = totalSent;
    globalStats.inboxTotal = total;

    const totalCount = globalStats.totalSent + globalStats.inboxTotal;
    const totalAll = globalStats.totalSuccess + globalStats.totalFail;
    const successRate = totalAll > 0 ? (globalStats.totalSuccess / totalAll * 100) : 0;

    globalStats.totalPhoneCount = sentPhoneNumbers.size;

    document.getElementById('taskCount').textContent = globalStats.taskCount;
    document.getElementById('phoneCount').textContent = globalStats.totalPhoneCount;
    document.getElementById('totalSentCount').textContent = totalCount;
    document.getElementById('successCount').textContent = globalStats.totalSuccess;
    document.getElementById('failCount').textContent = globalStats.totalFail;
    document.getElementById('successRate').textContent = `${successRate.toFixed(1)}%`;

    const taskCountMobile = document.getElementById('taskCountMobile');
    const phoneCountMobile = document.getElementById('phoneCountMobile');
    const totalSentCountMobile = document.getElementById('totalSentCountMobile');
    const successCountMobile = document.getElementById('successCountMobile');
    const failCountMobile = document.getElementById('failCountMobile');
    const successRateMobile = document.getElementById('successRateMobile');
    if (taskCountMobile) taskCountMobile.textContent = globalStats.taskCount;
    if (phoneCountMobile) phoneCountMobile.textContent = globalStats.totalPhoneCount;
    if (totalSentCountMobile) totalSentCountMobile.textContent = totalCount;
    if (successCountMobile) successCountMobile.textContent = globalStats.totalSuccess;
    if (failCountMobile) failCountMobile.textContent = globalStats.totalFail;
    if (successRateMobile) successRateMobile.textContent = `${successRate.toFixed(1)}%`;

    updateInlineStats();
}

function updateContactList(chats, updatedChatIds = []) {
    const contactList = document.getElementById('contactList');

    if (!chats || chats.length === 0) {
        contactList.innerHTML = '<div style="font-family: \'Xiaolai\', sans-serif; text-align:center; color:rgba(47,47,47,0.5); padding:20px; font-size:14px;">暂无对话</div><button class="btn-clear-inbox" id="clearInboxBtn" title="全部删除">全部删除</button>';
        updateNotificationCount();
        updateInboxStats();
        document.getElementById('clearInboxBtn').addEventListener('click', clearInbox);
        return;
    }

    updatedChatIds.forEach(chatId => {
        if (chatId !== currentChatId) {
            unreadChatIds.add(chatId);
        }
    });

    updateNotificationCount();

    const fragment = document.createDocumentFragment();

    chats.forEach(chat => {
        const contactItem = document.createElement('div');
        contactItem.className = 'contact-item';
        if (chat.chat_id === currentChatId) {
            contactItem.classList.add('active');
        }
        if (unreadChatIds.has(chat.chat_id) && chat.chat_id !== currentChatId) {
            contactItem.classList.add('unread');
        }
        contactItem.dataset.chatId = chat.chat_id;
        contactItem.innerHTML = `
            <div class="avatar">😎</div>
            <div class="contact-info">
                <div class="contact-name">${chat.name}</div>
                <div class="contact-preview">${chat.last_message_preview || ''}</div>
            </div>
        `;
        contactItem.addEventListener('click', function () {
            selectChat(chat.chat_id, chat.name);
        });
        fragment.appendChild(contactItem);
    });

    contactList.innerHTML = '';
    contactList.appendChild(fragment);
    const clearBtn = document.createElement('button');
    clearBtn.className = 'btn-clear-inbox';
    clearBtn.id = 'clearInboxBtn';
    clearBtn.title = '全部删除';
    clearBtn.textContent = '全部删除';
    clearBtn.addEventListener('click', clearInbox);
    contactList.appendChild(clearBtn);

    chats.forEach(chat => {
        if (!inboxMessageStats[chat.chat_id]) {
            requestConversationForStats(chat.chat_id);
        }
    });

    updateInboxStats();
}

async function requestConversationForStats(chatId) {
    if (!currentUserId) return;

    try {
        const response = await fetch(`${API_BASE_URL}/user/${currentUserId}/conversations/${chatId}/messages`);
        const data = await response.json();

        if (data.success && data.messages) {
            handleServerMessage({
                type: 'conversation_data',
                chat_id: chatId,
                data: { messages: data.messages }
            });
        }
    } catch (err) {
        console.error("获取对话数据失败:", err);
    }
}

function selectChat(chatId, chatName) {
    if (currentChatId === chatId) return;
    currentChatId = chatId;
    unreadChatIds.delete(chatId);
    updateNotificationCount();
    document.querySelectorAll('.contact-item').forEach(item => {
        item.classList.remove('active', 'unread');
        if (item.dataset.chatId === chatId) {
            item.classList.add('active');
        }
    });
    const chatHeader = document.getElementById('chatHeader');
    const chatPhoneNumber = document.getElementById('chatPhoneNumber');
    if (chatPhoneNumber) {
        const phoneNumber = chatName || chatId;
        chatPhoneNumber.textContent = phoneNumber;
    }
    document.getElementById('replyInput').disabled = false;
    document.getElementById('sendReplyBtn').disabled = false;
    requestConversation(chatId);
}

async function requestConversation(chatId) {
    if (!currentUserId) return;

    const conversationDisplay = document.getElementById('conversationDisplay');
    if (conversationDisplay) {
        conversationDisplay.innerHTML = '<div style="text-align:center; color:rgba(47,47,47,0.5);">加载中...</div>';
    }
    try {
        const resp = await fetch(`${API_BASE_URL}/user/${currentUserId}/conversations/${encodeURIComponent(chatId)}/messages`);
        const data = await resp.json();
        if (data && data.success && data.messages) {
            handleServerMessage({ type: 'conversation_data', chat_id: chatId, data: { messages: data.messages } });
        } else if (conversationDisplay) {
            conversationDisplay.innerHTML = '<div style="text-align:center; color:rgba(47,47,47,0.5);">暂无消息</div>';
        }
    } catch (e) {
        console.error('requestConversation failed:', e);
        if (conversationDisplay) {
            conversationDisplay.innerHTML = '<div style="text-align:center; color:rgba(47,47,47,0.5);">加载失败</div>';
        }
    }
}

function displayConversation(data, chatId) {
    const conversationDisplay = document.getElementById('conversationDisplay');

    let received = 0;
    let sent = 0;
    if (data && data.messages && data.messages.length > 0) {
        data.messages.forEach(msg => {
            if (msg.is_from_me) {
                sent++;
                if (chatId && !sentPhoneNumbers.has(chatId)) {
                    sentPhoneNumbers.add(chatId);
                    globalStats.totalPhoneCount = sentPhoneNumbers.size;
                }
            } else {
                received++;
            }
        });
    }
    inboxMessageStats[chatId] = { received: received, sent: sent };
    updateInboxStats();

    if (!data || !data.messages || data.messages.length === 0) {
        conversationDisplay.innerHTML = '<div style="text-align:center; color:rgba(47,47,47,0.5);">暂无消息</div>';
        return;
    }

    const tempMessage = conversationDisplay.querySelector('[data-temp-message="true"]');
    if (tempMessage && chatId === currentChatId) {
        if (data.messages && data.messages.length > 0) {
            const lastMsg = data.messages[data.messages.length - 1];
            const tempMsgText = tempMessage.querySelector('span').textContent.trim();
            const lastMsgText = (lastMsg.text || lastMsg.message || '').trim();
            if (lastMsg.is_from_me && lastMsgText === tempMsgText) {
                tempMessage.removeAttribute('data-temp-message');
                return;
            }
        }
        tempMessage.removeAttribute('data-temp-message');
    }

    const fragment = document.createDocumentFragment();
    data.messages.forEach(msg => {
        const bubble = document.createElement('div');
        bubble.className = msg.is_from_me ? 'chat-bubble right' : 'chat-bubble left';

        let timeStr = '';
        if (msg.timestamp) {
            try {
                const date = new Date(msg.timestamp);
                if (!isNaN(date.getTime())) {
                    timeStr = date.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit', hour12: false });
                } else {
                    timeStr = msg.timestamp;
                }
            } catch (e) {
                timeStr = msg.timestamp || '';
            }
        }

        bubble.innerHTML = `
            <span>${msg.text || msg.message || ''}</span>
            <div class="chat-time">${timeStr}</div>
        `;
        fragment.appendChild(bubble);
    });

    conversationDisplay.innerHTML = '';
    conversationDisplay.appendChild(fragment);

    updateInboxStats();

    if (!conversationScrollPending) {
        conversationScrollPending = true;
        requestAnimationFrame(() => {
            conversationDisplay.scrollTop = conversationDisplay.scrollHeight;
            conversationScrollPending = false;
        });
    }
}

function sendReply() {
    const replyInput = document.getElementById('replyInput');
    const message = replyInput.value.trim();
    if (!message || !currentChatId) return;

    const conversationDisplay = document.getElementById('conversationDisplay');
    const bubble = document.createElement('div');
    bubble.className = 'chat-bubble right';
    bubble.setAttribute('data-temp-message', 'true');
    bubble.innerHTML = `
        <span>${message}</span>
        <div class="chat-time">${new Date().toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' })}</div>
    `;
    conversationDisplay.appendChild(bubble);
    conversationDisplay.scrollTop = conversationDisplay.scrollHeight;
    replyInput.value = '';

    if (currentChatId && !sentPhoneNumbers.has(currentChatId)) {
        sentPhoneNumbers.add(currentChatId);
        globalStats.totalPhoneCount = sentPhoneNumbers.size;
        updateInlineStats();
        const phoneCountEl = document.getElementById('phoneCount');
        if (phoneCountEl) phoneCountEl.textContent = globalStats.totalPhoneCount;
    }

    if (!inboxMessageStats[currentChatId]) {
        inboxMessageStats[currentChatId] = { received: 0, sent: 0 };
    }
    inboxMessageStats[currentChatId].sent++;
    updateInboxStats();

    sendReplyViaAPI(currentChatId, message);
}

async function sendReplyViaAPI(chatId, message) {
    if (!currentUserId) return;
    try {
        const taskId = await _createTask({ message, numbers: [chatId], taskType: 'reply' });

        // 检查 WebSocket 连接状态（用于接收结果）
        if (!activeWs || activeWs.readyState !== WebSocket.OPEN) {
            throw new Error('WebSocket 未连接，无法接收任务结果。请刷新页面重试或检查网络连接。');
        }

        const ok = sendWSCommand('subscribe_task', { task_id: taskId });
        if (!ok) {
            throw new Error('接收任务结果失败：WebSocket 未连接');
        }

        const waiter = _ensureTaskWaiter(taskId, 5 * 60 * 1000);
        await waiter.promise;
    } catch (e) {
        console.error('sendReplyViaAPI failed:', e);
    }
}

function showNotification(message, type = 'info') {
    console.log(`[${type.toUpperCase()}] ${message}`);
}

function showNewMessageNotification(chatId, senderName, messagePreview) {
    console.log('showNewMessageNotification called:', chatId, senderName);
    const oldBubble = document.querySelector('.message-bubble-notification');
    if (oldBubble) {
        oldBubble.remove();
    }

    const bubble = document.createElement('div');
    bubble.className = 'message-bubble-notification';
    bubble.innerHTML = `📨 新消息: ${senderName}`;

    const clearBtn = document.getElementById('clearLogsBtn');
    if (clearBtn) {
        const rect = clearBtn.getBoundingClientRect();
        bubble.style.top = `${rect.top - 50}px`;
    } else {
        bubble.style.top = '50%';
    }

    document.body.appendChild(bubble);
    console.log('Bubble notification created and added to DOM');

    setTimeout(() => {
        if (bubble && bubble.parentNode) {
            bubble.remove();
        }
    }, 3000);
}

function updateGlobalStats(total = 0, success = 0, fail = 0) {
    if (total > 0 || success > 0 || fail > 0) {
        globalStats.taskCount++;
    }
    globalStats.totalSent += total;
    globalStats.totalSuccess += success;
    globalStats.totalFail += fail;

    const totalAll = globalStats.totalSuccess + globalStats.totalFail;
    const successRate = totalAll > 0 ? (globalStats.totalSuccess / totalAll * 100) : 0;

    const totalCount = globalStats.totalSent + globalStats.inboxTotal;

    globalStats.totalPhoneCount = sentPhoneNumbers.size;

    document.getElementById('taskCount').textContent = globalStats.taskCount;
    document.getElementById('phoneCount').textContent = globalStats.totalPhoneCount;
    document.getElementById('totalSentCount').textContent = totalCount;
    document.getElementById('successCount').textContent = globalStats.totalSuccess;
    document.getElementById('failCount').textContent = globalStats.totalFail;
    document.getElementById('successRate').textContent = `${successRate.toFixed(1)}%`;

    const taskCountMobile = document.getElementById('taskCountMobile');
    const phoneCountMobile = document.getElementById('phoneCountMobile');
    const totalSentCountMobile = document.getElementById('totalSentCountMobile');
    const successCountMobile = document.getElementById('successCountMobile');
    const failCountMobile = document.getElementById('failCountMobile');
    const successRateMobile = document.getElementById('successRateMobile');
    if (taskCountMobile) taskCountMobile.textContent = globalStats.taskCount;
    if (phoneCountMobile) phoneCountMobile.textContent = globalStats.totalPhoneCount;
    if (totalSentCountMobile) totalSentCountMobile.textContent = totalCount;
    if (successCountMobile) successCountMobile.textContent = globalStats.totalSuccess;
    if (failCountMobile) failCountMobile.textContent = globalStats.totalFail;
    if (successRateMobile) successRateMobile.textContent = `${successRate.toFixed(1)}%`;

    updateInlineStats();
}

function updateTimeDisplay() {
    const timeUsedEl = document.getElementById('timeUsed');
    if (timeUsedEl) {
        const totalSeconds = globalStats.totalTime;
        if (totalSeconds < 60) {
            timeUsedEl.textContent = `${totalSeconds}s`;
        } else if (totalSeconds < 3600) {
            const minutes = Math.floor(totalSeconds / 60);
            const seconds = totalSeconds % 60;
            timeUsedEl.textContent = `${minutes}分${seconds}秒`;
        } else {
            const hours = Math.floor(totalSeconds / 3600);
            const minutes = Math.floor((totalSeconds % 3600) / 60);
            const seconds = totalSeconds % 60;
            timeUsedEl.textContent = `${hours}时${minutes}分${seconds}秒`;
        }
    }
}

function updateInlineStats() {
    const totalAll = globalStats.totalSuccess + globalStats.totalFail;
    const successRate = totalAll > 0 ? (globalStats.totalSuccess / totalAll * 100) : 0;

    const totalCount = globalStats.totalSent + globalStats.inboxTotal;

    const taskCountInline = document.getElementById('taskCountInline');
    const phoneCountInline = document.getElementById('phoneCountInline');
    const totalSentCountInline = document.getElementById('totalSentCountInline');
    const successCountInline = document.getElementById('successCountInline');
    const failCountInline = document.getElementById('failCountInline');
    const successRateInline = document.getElementById('successRateInline');

    if (taskCountInline) taskCountInline.textContent = globalStats.taskCount;
    if (phoneCountInline) phoneCountInline.textContent = globalStats.totalPhoneCount;
    if (totalSentCountInline) totalSentCountInline.textContent = totalCount;
    if (successCountInline) successCountInline.textContent = globalStats.totalSuccess;
    if (failCountInline) failCountInline.textContent = globalStats.totalFail;
    if (successRateInline) successRateInline.textContent = `${successRate.toFixed(1)}%`;
}

//#endregion
