const API_BASE = '';

const createForm = document.getElementById('create-form');
const createResult = document.getElementById('create-result');
const createBtn = document.getElementById('create-btn');
const taskIdInput = document.getElementById('task-id');
const lookupBtn = document.getElementById('lookup-btn');
const lookupResult = document.getElementById('lookup-result');
const recentList = document.getElementById('recent-list');

const RECENT_KEY = 'task-scheduler-recent';
const MAX_RECENT = 10;

function getRecent() {
  try {
    const raw = localStorage.getItem(RECENT_KEY);
    return raw ? JSON.parse(raw) : [];
  } catch {
    return [];
  }
}

function addRecent(id) {
  let list = getRecent().filter((x) => x !== id);
  list.unshift(id);
  list = list.slice(0, MAX_RECENT);
  localStorage.setItem(RECENT_KEY, JSON.stringify(list));
  renderRecent();
}

function renderRecent() {
  const list = getRecent();
  if (list.length === 0) {
    recentList.innerHTML = '<li class="hint">No tasks yet. Create one above.</li>';
    return;
  }
  recentList.innerHTML = list
    .map(
      (id) =>
        `<li><a href="#" data-task-id="${escapeHtml(id)}">${escapeHtml(id)}</a></li>`
    )
    .join('');
  recentList.querySelectorAll('a').forEach((a) => {
    a.addEventListener('click', (e) => {
      e.preventDefault();
      taskIdInput.value = a.dataset.taskId;
      lookupResult.innerHTML = '';
      fetchTask(a.dataset.taskId);
    });
  });
}

function escapeHtml(s) {
  const div = document.createElement('div');
  div.textContent = s;
  return div.innerHTML;
}

function setResult(el, html, isError = false) {
  el.innerHTML = html;
  el.className = 'result' + (isError ? ' error' : ' success');
}

function showLoading(el) {
  el.innerHTML = '<span class="loading">Loading…</span>';
  el.className = 'result';
}

function statusClass(s) {
  const v = (s || '').toLowerCase();
  if (v === 'queued' || v === 'running' || v === 'success' || v === 'failed' || v === 'dead') return v;
  return 'queued';
}

function formatDate(iso) {
  if (!iso) return '—';
  try {
    const d = new Date(iso);
    return d.toLocaleString();
  } catch {
    return iso;
  }
}

function renderTaskDetail(task) {
  const status = task.status || 'queued';
  return `
    <dl>
      <dt>ID</dt>
      <dd><code>${escapeHtml(task.id)}</code></dd>
      <dt>Type</dt>
      <dd>${escapeHtml(task.type)}</dd>
      <dt>Status</dt>
      <dd><span class="status ${statusClass(status)}">${escapeHtml(status)}</span></dd>
      <dt>Scheduled at</dt>
      <dd>${formatDate(task.scheduledAt)}</dd>
      <dt>Started at</dt>
      <dd>${formatDate(task.startedAt)}</dd>
      <dt>Finished at</dt>
      <dd>${formatDate(task.finishedAt)}</dd>
      <dt>Attempts</dt>
      <dd>${escapeHtml(String(task.attempts ?? 0))} / ${escapeHtml(String(task.maxRetries ?? 0))}</dd>
      ${task.lastError ? `<dt>Last error</dt><dd class="error">${escapeHtml(task.lastError)}</dd>` : ''}
      <dt>Payload</dt>
      <dd><pre>${escapeHtml(task.payload || '')}</pre></dd>
    </dl>
  `;
}

async function createTask(payload) {
  const res = await fetch(API_BASE + '/tasks', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.error || res.statusText || 'Failed to create task');
  }
  return data;
}

async function fetchTask(id) {
  const res = await fetch(API_BASE + '/tasks/' + encodeURIComponent(id));
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    if (res.status === 404) {
      lookupResult.innerHTML = `<p class="error">Task not found.</p>`;
      lookupResult.className = 'result task-detail';
      return;
    }
    lookupResult.innerHTML = `<p class="error">${escapeHtml(data.error || 'Failed to fetch task')}</p>`;
    lookupResult.className = 'result task-detail';
    return;
  }
  lookupResult.innerHTML = renderTaskDetail(data);
  lookupResult.className = 'result task-detail';
}

createForm.addEventListener('submit', async (e) => {
  e.preventDefault();
  createResult.innerHTML = '';
  const type = document.getElementById('task-type').value.trim();
  const payload = document.getElementById('task-payload').value.trim();
  const retryCount = parseInt(document.getElementById('retry-count').value, 10) || 0;
  createBtn.disabled = true;
  createResult.className = 'result';
  createResult.innerHTML = '<span class="loading">Scheduling…</span>';
  try {
    const result = await createTask({ type, payload, retryCount, nextRetryAt: 0 });
    const id = result.task_id || result.taskID;
    addRecent(id);
    setResult(
      createResult,
      `Scheduled. Task ID: <a href="#" data-fill-id="${escapeHtml(id)}">${escapeHtml(id)}</a>`,
      false
    );
    createResult.querySelector('a').addEventListener('click', (ev) => {
      ev.preventDefault();
      taskIdInput.value = id;
      lookupResult.innerHTML = '';
      fetchTask(id);
    });
  } catch (err) {
    setResult(createResult, escapeHtml(err.message), true);
  } finally {
    createBtn.disabled = false;
  }
});

lookupBtn.addEventListener('click', () => {
  const id = taskIdInput.value.trim();
  if (!id) return;
  lookupResult.innerHTML = '';
  showLoading(lookupResult);
  fetchTask(id);
});

taskIdInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') {
    e.preventDefault();
    lookupBtn.click();
  }
});

renderRecent();
