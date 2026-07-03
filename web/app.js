const state = {
  currentUrl: null,
  discoveredMarket: null,
  discoveredSymbol: null,
  discoveredActivityEnd: null,
  pollTimer: null,
  editingJobId: null,
  page: 1,
  perPage: 20,
  total: 0,
  totalPages: 1,
  filterMarket: "",
  filterSearch: "",
};

const $ = (selector) => document.querySelector(selector);

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    "\"": "&quot;",
    "'": "&#39;",
  }[char]));
}

function fmtNumber(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "—";
  return number.toLocaleString("en-US", { maximumFractionDigits: 2 });
}

function fmtTime(value) {
  if (!value) return "—";
  return new Date(value).toLocaleString("zh-CN", { hour12: false });
}

function fmtSnapshotTs(ts) {
  if (!ts) return "—";
  const m = String(ts).match(/^(\d{4}-\d{2}-\d{2})T(\d{2})(\d{2})(\d{2})$/);
  return m ? `${m[1]} ${m[2]}:${m[3]}:${m[4]}` : ts;
}

async function api(path, options) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!response.ok) {
    let errorText;
    try {
      const payload = await response.json();
      errorText = payload.error || response.statusText;
    } catch {
      const body = await response.text().catch(() => "");
      errorText = `HTTP ${response.status}: ${body.slice(0, 200)}`;
    }
    throw new Error(errorText);
  }
  return await response.json();
}

function inferFromUrl(rawUrl) {
  const url = String(rawUrl || "").trim();
  let path = url;
  try {
    path = new URL(url).pathname;
  } catch {
    path = url;
  }
  const segments = path.split("/").filter(Boolean);
  const slug = segments[segments.length - 1] || "";
  const lowerSlug = slug.toLowerCase();
  const market = lowerSlug.includes("spot") ? "spot" : "um";

  const patterns = [
    /^([a-z0-9]+)-spot-/i,
    /^([a-z0-9]+)-futures-/i,
    /(?:^|-)futures-([a-z0-9]+)(?:-|$)/i,
    /(?:^|-)spot-[a-z0-9-]*-([a-z0-9]+)$/i,
    /(?:^|-)wave-([a-z0-9]+)(?:-|$)/i,
    /(?:^|-)reward-?([a-z0-9]+)(?:-|$)/i,
  ];
  for (const pattern of patterns) {
    const match = slug.match(pattern);
    if (match?.[1]) return { market, symbol: match[1].toUpperCase() };
  }

  const tokens = slug.split(/[^a-z0-9]+/i).filter(Boolean);
  const ignored = new Set(["spot", "futures", "trading", "competition", "challenge", "activity", "reward", "main", "sprint"]);
  const token = [...tokens].reverse().find((item) => /^[a-z0-9]{2,24}$/i.test(item) && !ignored.has(item.toLowerCase()));
  if (!token) throw new Error("无法从 URL 识别 symbol。");
  return { market, symbol: token.toUpperCase() };
}

function normalizeTaskUrl(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  try {
    const url = new URL(raw);
    url.hash = "";
    url.searchParams.delete("ref");
    return `${url.origin}${url.pathname.replace(/\/+$/, "")}${url.search}`;
  } catch {
    return raw.replace(/\/+$/, "");
  }
}

async function discoverResourceIds() {
  const url = String($("#urlInput")?.value || "").trim();
  if (!url) return;
  state.currentUrl = url;
  const discoverBtn = $("#discoverBtn");
  const resultsEl = $("#discoveryResults");
  discoverBtn.disabled = true;
  discoverBtn.textContent = "发现中...";
  resultsEl.innerHTML = `<div class="empty box">正在打开浏览器发现 resourceId...</div>`;
  try {
    const inferred = inferFromUrl(url);
    state.discoveredMarket = inferred.market;
    state.discoveredSymbol = inferred.symbol;
    const result = await api("/api/discover", {
      method: "POST",
      body: JSON.stringify({ url, proxy: "auto", browserWaitMs: 30000, force: true }),
    });
    state.discoveredActivityEnd = result.activityEnd || null;
    renderDiscoveryCards(result, inferred);
  } catch (error) {
    resultsEl.innerHTML = `<div class="err">${escapeHtml(error.message)}</div>`;
  } finally {
    discoverBtn.disabled = false;
    discoverBtn.textContent = "发现 resourceId";
  }
}

function renderDiscoveryCards(result, inferred) {
  const candidates = result.candidates || [];
  const title = result.title || "";
  const el = $("#discoveryResults");
  if (!candidates.length) {
    el.innerHTML = `<div class="empty box">未发现 resourceId。</div>`;
    return;
  }
  el.innerHTML = `
    <div class="discovery-meta">
      <strong>${escapeHtml(inferred.market.toUpperCase())}</strong>
      <span>${escapeHtml(inferred.symbol)}</span>
      ${title ? `<code>${escapeHtml(title)}</code>` : ""}
    </div>
    <div class="discovery-grid">
      ${candidates.map((c) => {
        const rid = c.resourceId;
        const cls = c.hasJob ? "discovery-card used" : "discovery-card fresh";
        const nameText = c.hasJob ? escapeHtml(c.jobName) : "未启动";
        return `
          <button class="${cls}" data-rid="${escapeHtml(String(rid))}">
            <span class="dc-name">${nameText}</span>
            <span class="dc-id">${escapeHtml(String(rid))}</span>
            <span class="dc-meta">${escapeHtml(inferred.market.toUpperCase())} · ${escapeHtml(inferred.symbol)}</span>
          </button>
        `;
      }).join("")}
    </div>
  `;
  el.querySelectorAll(".discovery-card").forEach((btn) => {
    btn.addEventListener("click", () => {
      const rid = btn.dataset.rid;
      startScrapeJob(rid, inferred.market, inferred.symbol, state.currentUrl, state.discoveredActivityEnd);
    });
  });
}

async function startScrapeJob(resourceId, market, symbol, url, activityEnd) {
  try {
    await api("/api/scrape/jobs", {
      method: "POST",
      body: JSON.stringify({
        resourceId,
        market,
        symbol,
        url,
        proxy: "auto",
        mode: "scrape",
        activityEnd,
      }),
    });
    await loadJobs(1);
  } catch (error) {
    alert(`抓取失败：${error.message}`);
  }
}

let _editJobId = null;

function openEditModal(job) {
  _editJobId = job.id;
  const p = job.payload || {};
  document.getElementById("editMarket").value = (p.market || "um").toLowerCase();
  document.getElementById("editToken").value = (p.token || (p.symbol || "").replace(/USDT$/i, "") || "").toUpperCase();
  document.getElementById("editSymbol").value = (p.symbol || "").toUpperCase();
  document.getElementById("editName").value = job.name || p.name || "";
  document.getElementById("editRewardToken").value = p.rewardToken || document.getElementById("editToken").value || "";
  const tiers = p.rewardTiers && p.rewardTiers.length ? p.rewardTiers : [
    {rankMin:6, rankMax:20, amount:"0"},
    {rankMin:21, rankMax:50, amount:"0"},
    {rankMin:51, rankMax:200, amount:"0"},
    {rankMin:201, rankMax:1000, amount:"0"},
  ];
  for (let i = 0; i < 4; i++) {
    const t = tiers[i] || {};
    const minEl = document.getElementById("editTier" + i + "Min");
    const maxEl = document.getElementById("editTier" + i + "Max");
    const amtEl = document.getElementById("editTier" + i + "Amt");
    if (minEl) minEl.value = t.rankMin != null ? t.rankMin : "";
    if (maxEl) maxEl.value = t.rankMax != null ? t.rankMax : "";
    if (amtEl) amtEl.value = t.amount || "0";
  }
  const rid = p.resourceId || "";
  document.getElementById("editModalMeta").textContent = rid ? `resourceId ${rid}` : "";
  document.getElementById("editModal").style.display = "";
}

document.getElementById("editCancelBtn").addEventListener("click", () => {
  document.getElementById("editModal").style.display = "none";
  _editJobId = null;
});

document.getElementById("editSaveBtn").addEventListener("click", async () => {
  const jobId = _editJobId;
  if (!jobId) return;
  const market = document.getElementById("editMarket").value;
  const token = document.getElementById("editToken").value.trim().toUpperCase();
  const symbol = document.getElementById("editSymbol").value.trim().toUpperCase();
  const name = document.getElementById("editName").value.trim();
  const rewardToken = document.getElementById("editRewardToken").value.trim().toUpperCase();
  if (!token || !symbol) { alert("Token 和 Symbol 不能为空"); return; }
  const rewardTiers = [];
  for (let i = 0; i < 4; i++) {
    const minEl = document.getElementById("editTier" + i + "Min");
    const maxEl = document.getElementById("editTier" + i + "Max");
    const amtEl = document.getElementById("editTier" + i + "Amt");
    const rmin = parseInt(minEl?.value);
    const rmax = parseInt(maxEl?.value);
    const amt = amtEl?.value?.trim() || "0";
    if (rmin >= 1 && rmax >= rmin) {
      rewardTiers.push({ rankMin: rmin, rankMax: rmax, amount: amt || "0" });
    }
  }
  try {
    await api(`/api/jobs/${jobId}/params`, {
      method: "PUT",
      body: JSON.stringify({ market, token, symbol, name: name || undefined, rewardToken: rewardToken || undefined, rewardTiers: rewardTiers.length ? rewardTiers : undefined }),
    });
    document.getElementById("editModal").style.display = "none";
    _editJobId = null;
    await loadJobs(state.page);
  } catch (error) {
    alert(`保存失败：${error.message}`);
  }
});

function lastLine(text) {
  if (!text) return "";
  const lines = text.trim().split("\n");
  return lines[lines.length - 1] || "";
}

function renderJobs(jobs) {
  const { page, totalPages, total } = state;
  $("#jobCount").textContent = `${total}`;
  $("#jobList").innerHTML = jobs.map((job) => {
    const payload = job.payload || {};
    const progress = job.progress || {};
    const percent = Math.max(0, Math.min(100, Number(progress.percent || 0)));
    const rowsText = progress.rowsFetched ? ` · ${progress.rowsFetched}/1000 rows` : "";
    const pagesText = progress.totalPages ? ` · page ${progress.currentPage}/${progress.totalPages}` : "";
    const statusClass = job.status === "completed" ? "ok" : job.status === "failed" ? "fail" : "run";
    const errorReason = job.status === "failed" && job.stderr ? lastLine(job.stderr) : "";
    const url = normalizeTaskUrl(payload.url);
    const jobName = job.name || payload.name || payload.resourceId || job.id;
    const rid = payload.resourceId ? String(payload.resourceId) : "";
    const displayName = rid ? `${jobName}  [${rid}]` : jobName;
    const activityEnd = payload.activityEnd;
    const isExpired = activityEnd && new Date(activityEnd + " +08:00") <= new Date();
    const expiredClass = isExpired ? " expired" : "";
    const snapshotTs = job.latestSnapshot;
    return `
      <article class="job ${statusClass}${expiredClass}" data-job-id="${escapeHtml(job.id)}">
        <div>
          <strong>
            <span class="job-name" data-preview="${escapeHtml(job.id)}">${escapeHtml(String(displayName))}</span>
          </strong>
          <p>${escapeHtml(url || "无 URL")} · ${escapeHtml((payload.market || "").toUpperCase())} ${escapeHtml(payload.symbol || "")}</p>
          <small>${escapeHtml(job.status)} · ${escapeHtml(fmtTime(job.createdAt))}${job.finishedAt ? ` · ${escapeHtml(fmtTime(job.finishedAt))}` : ""}</small>
          ${snapshotTs ? `<div class="snapshot-ts">数据时间 <b>${escapeHtml(fmtSnapshotTs(snapshotTs))}</b> (北京时间)</div>` : ""}
        </div>
        <div class="job-actions">
          ${job.status === "running" || job.status === "queued"
            ? `<button class="ghost mini danger" type="button" data-kill="${escapeHtml(job.id)}">终止任务</button>`
            : `<button class="ghost mini" type="button" data-rerun="${escapeHtml(job.id)}">再次抓取</button>`
          }
           <button class="ghost mini" type="button" data-rename="${escapeHtml(job.id)}">配置</button>
          <button class="ghost mini danger" type="button" data-delete="${escapeHtml(job.id)}">删除</button>
        </div>
        <div class="progress">
          <div class="progress-line">
            <span>${escapeHtml(progress.label || "等待进度")}${escapeHtml(pagesText)}${escapeHtml(rowsText)}</span>
            <b>${escapeHtml(percent)}%</b>
          </div>
          <div class="progress-track"><i style="width:${percent}%"></i></div>
        </div>
          ${errorReason ? `<div class="job-error">${escapeHtml(errorReason)}</div>` : ""}
          ${job.stderr ? `<details class="stderr"><summary>详情</summary><pre>${escapeHtml(job.stderr.slice(-900))}</pre></details>` : ""}
      </article>
    `;
  }).join("") || `<div class="empty box">没有任务。</div>`;

  $("#jobList").querySelectorAll("[data-preview]").forEach((button) => {
    button.addEventListener("click", () => {
      window.open(`/preview.html?job=${button.dataset.preview}`, "_blank");
    });
  });
  $("#jobList").querySelectorAll("[data-rerun]").forEach((button) => {
    button.addEventListener("click", async () => {
      const job = jobs.find((item) => item.id === button.dataset.rerun);
      if (!job) return;
      const p = job.payload || {};
      await startScrapeJob(p.resourceId, p.market, p.symbol, p.url);
    });
  });
  $("#jobList").querySelectorAll("[data-kill]").forEach((button) => {
    button.addEventListener("click", async () => {
      if (!confirm("确认终止此任务？")) return;
      try {
        await api(`/api/jobs/${button.dataset.kill}/kill`, { method: "POST" });
      } catch (error) {
        alert(`终止失败：${error.message}`);
      }
    });
  });
  $("#jobList").querySelectorAll("[data-rename]").forEach((button) => {
    button.addEventListener("click", () => {
      const job = jobs.find((item) => item.id === button.dataset.rename);
      if (!job) return;
      openEditModal(job);
    });
  });
  $("#jobList").querySelectorAll("[data-delete]").forEach((button) => {
    button.addEventListener("click", async () => {
      if (!confirm("确认删除此任务？")) return;
      await api(`/api/jobs/${button.dataset.delete}`, { method: "DELETE" });
      await loadJobs(state.page);
    });
  });

  renderPagination();
}

function renderPagination() {
  const { page, totalPages, total } = state;
  let existing = $("#pagination");
  if (!existing) {
    existing = document.createElement("div");
    existing.id = "pagination";
    existing.className = "pagination";
    $("#jobList").after(existing);
  }

  if (totalPages <= 1 && total <= state.perPage) {
    existing.innerHTML = `<span class="page-info">共 ${total} 条</span>`;
    return;
  }

  const pages = paginationRange(page, totalPages, 2);
  let html = `<span class="page-info">共 ${total} 条 · 第 ${page}/${totalPages} 页</span><div class="page-buttons">`;
  html += `<button class="ghost mini" data-page="1" ${page <= 1 ? "disabled" : ""}>«</button>`;
  html += `<button class="ghost mini" data-page="${page - 1}" ${page <= 1 ? "disabled" : ""}>‹</button>`;
  for (const p of pages) {
    if (p === null) {
      html += `<span class="page-ellipsis">…</span>`;
    } else {
      html += `<button class="ghost mini ${p === page ? "active" : ""}" data-page="${p}">${p}</button>`;
    }
  }
  html += `<button class="ghost mini" data-page="${page + 1}" ${page >= totalPages ? "disabled" : ""}>›</button>`;
  html += `<button class="ghost mini" data-page="${totalPages}" ${page >= totalPages ? "disabled" : ""}>»</button>`;
  html += "</div>";
  existing.innerHTML = html;

  existing.querySelectorAll("[data-page]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const p = parseInt(btn.dataset.page, 10);
      if (p && p !== state.page) goToPage(p);
    });
  });
}

function paginationRange(current, total, around) {
  if (total <= 7) {
    return Array.from({ length: total }, (_, i) => i + 1);
  }
  const set = new Set();
  set.add(1);
  for (let i = Math.max(2, current - around); i <= Math.min(total - 1, current + around); i++) {
    set.add(i);
  }
  set.add(total);
  const sorted = [...set].sort((a, b) => a - b);
  const result = [];
  let prev = 0;
  for (const p of sorted) {
    if (p - prev > 1) result.push(null);
    result.push(p);
    prev = p;
  }
  return result;
}

async function goToPage(page) {
  if (page < 1 || page > state.totalPages) return;
  state.page = page;
  await loadJobs(page);
}

function buildSuggestions(entries) {
  state._cacheMap = {};
  const el = $("#urlSuggestions");
  if (!entries.length) { el.innerHTML = ""; return; }
  el.innerHTML = entries.map((e) => {
    const url = e.url || "";
    state._cacheMap[url] = e;
    const ids = (e.candidates || []).map((c) => String(typeof c === "object" ? (c.resourceId || c) : c)).join(", ");
    const title = e.title || "";
    const label = `${title ? title + " — " : ""}${ids}`;
    return `<div class="suggestion-row">
      <button class="suggestion-item" type="button" data-url="${escapeHtml(url)}">
        <span class="si-label">${escapeHtml(label)}</span>
        <span class="si-url">${escapeHtml(url)}</span>
      </button>
      <button class="suggestion-del" type="button" data-key="${escapeHtml(e.key || url)}" title="删除">×</button>
    </div>`;
  }).join("");
  el.querySelectorAll(".suggestion-item").forEach((btn) => {
    btn.addEventListener("click", () => {
      const url = btn.dataset.url;
      $("#urlInput").value = url;
      el.style.display = "none";
      showCachedDiscovery(url);
    });
  });
  el.querySelectorAll(".suggestion-del").forEach((btn) => {
    btn.addEventListener("click", async (e) => {
      e.stopPropagation();
      const key = btn.dataset.key;
      try {
        await api(`/api/discover/cache/${encodeURIComponent(key)}`, { method: "DELETE" });
        const row = btn.closest(".suggestion-row");
        if (row) row.remove();
        if (!el.children.length) el.style.display = "none";
      } catch (err) {
        alert("删除失败: " + err.message);
      }
    });
  });
}

function showCachedDiscovery(url) {
  const entry = state._cacheMap?.[url];
  const el = $("#discoveryResults");
  if (!entry) {
    el.innerHTML = `<div class="empty box">没有缓存的发现结果，请点击"发现 resourceId"。</div>`;
    return;
  }
  state.currentUrl = url;
  const inferred = inferFromUrl(url);
  state.discoveredMarket = inferred.market;
  state.discoveredSymbol = inferred.symbol;
  const rawCandidates = entry.candidates || [];
  const jobs = state._jobs || [];
  const existingJobs = new Map();
  for (const job of jobs) {
    const rid = job.payload?.resourceId;
    if (rid) existingJobs.set(String(rid).trim(), job.name || job.payload?.name || "");
  }
  const candidates = rawCandidates.map((c) => {
    const rid = String(typeof c === "object" ? (c.resourceId || c) : c);
    const jobName = existingJobs.get(rid);
    return { resourceId: rid, hasJob: jobName !== undefined, jobName: jobName || "" };
  });
  renderDiscoveryCards({ candidates, title: entry.title }, inferred);
}

async function loadSuggestions() {
  try {
    const data = await api("/api/discover/cache");
    buildSuggestions(data.entries || []);
  } catch {
    // silently ignore
  }
}

async function loadJobs(page) {
  const params = new URLSearchParams();
  if (page) params.set("page", page);
  if (state.filterMarket) params.set("market", state.filterMarket);
  if (state.filterSearch) params.set("search", state.filterSearch);
  const payload = await api(`/api/jobs?${params}`);
  state._jobs = payload.jobs || [];
  state.page = payload.pagination?.page ?? 1;
  state.totalPages = payload.pagination?.totalPages ?? 1;
  state.total = payload.pagination?.total ?? 0;
  renderJobs(state._jobs);
}

function bind() {
  $("#discoverBtn").addEventListener("click", discoverResourceIds);
  $("#urlInput").addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      discoverResourceIds();
    }
  });
  $("#urlInput").addEventListener("focus", () => {
    const suggestions = $("#urlSuggestions");
    if (suggestions.children.length) suggestions.style.display = "flex";
  });
  $("#urlInput").addEventListener("blur", () => {
    setTimeout(() => { $("#urlSuggestions").style.display = "none"; }, 200);
  });
  $("#urlInput").addEventListener("input", () => {
    const suggestions = $("#urlSuggestions");
    if (suggestions.children.length) suggestions.style.display = "flex";
  });

  // Filter: market
  document.querySelectorAll("#marketFilter .filter-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelectorAll("#marketFilter .filter-btn").forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      state.filterMarket = btn.dataset.market;
      state.page = 1;
      loadJobs(state.page);
    });
  });

  // Filter: search on Enter
  $("#searchInput").addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      state.filterSearch = e.target.value.trim();
      state.page = 1;
      loadJobs(state.page);
    }
  });
}

async function boot() {
  bind();
  try {
    await loadJobs();
  } catch (error) {
    $("#jobList").innerHTML = `<div class="empty box">${escapeHtml(error.message)}</div>`;
  }
  loadSuggestions();
  state.pollTimer = setInterval(() => {
    loadJobs(state.page).catch((error) => {
      console.warn("loadJobs poll error:", error);
    });
  }, 5000);
}

boot().catch((error) => {
  document.body.innerHTML = `<pre style="padding:24px;color:#f8f2df;background:#151515;min-height:100vh">${escapeHtml(error.stack || error.message)}</pre>`;
});
