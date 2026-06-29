const state = {
  currentUrl: null,
  discoveredMarket: null,
  discoveredSymbol: null,
  pollTimer: null,
  editingJobId: null,
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
      body: JSON.stringify({ url, proxy: "auto", browserWaitMs: 30000 }),
    });
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
      ${candidates.map((id) => `
        <button class="discovery-card" data-rid="${escapeHtml(String(id))}">
          <span class="dc-id">${escapeHtml(String(id))}</span>
          <span class="dc-meta">${escapeHtml(inferred.market.toUpperCase())} · ${escapeHtml(inferred.symbol)}</span>
        </button>
      `).join("")}
    </div>
  `;
  el.querySelectorAll(".discovery-card").forEach((btn) => {
    btn.addEventListener("click", () => {
      const rid = btn.dataset.rid;
      startScrapeJob(rid, inferred.market, inferred.symbol, state.currentUrl);
    });
  });
}

async function startScrapeJob(resourceId, market, symbol, url) {
  await api("/api/scrape/jobs", {
    method: "POST",
    body: JSON.stringify({
      resourceId,
      market,
      symbol,
      url,
      proxy: "auto",
      mode: "scrape",
    }),
  });
  await loadJobs();
}

async function editJobName(jobId, currentName) {
  const newName = prompt("编辑任务名称：", currentName);
  if (!newName || newName === currentName) return;
  try {
    await api(`/api/jobs/${jobId}`, {
      method: "PATCH",
      body: JSON.stringify({ name: newName }),
    });
    await loadJobs();
  } catch (error) {
    alert(`编辑失败：${error.message}`);
  }
}

function groupJobs(jobs) {
  return jobs.slice(0, 50);
}

function renderJobs(jobs) {
  const list = groupJobs(jobs);
  $("#jobCount").textContent = list.length;
  $("#jobList").innerHTML = list.map((job) => {
    const payload = job.payload || {};
    const progress = job.progress || {};
    const percent = Math.max(0, Math.min(100, Number(progress.percent || 0)));
    const rowsText = progress.rowsFetched ? ` · ${progress.rowsFetched}/1000 rows` : "";
    const pagesText = progress.totalPages ? ` · page ${progress.currentPage}/${progress.totalPages}` : "";
    const statusClass = job.status === "completed" ? "ok" : job.status === "failed" ? "fail" : "run";
    const url = normalizeTaskUrl(payload.url);
    const jobName = job.name || payload.name || payload.resourceId || job.id;
    return `
      <article class="job ${statusClass}" data-job-id="${escapeHtml(job.id)}">
        <div>
          <strong>
            <span class="job-name" data-edit="${escapeHtml(job.id)}">${escapeHtml(String(jobName))}</span>
          </strong>
          <p>${escapeHtml(url || "无 URL")} · ${escapeHtml((payload.market || "").toUpperCase())} ${escapeHtml(payload.symbol || "")}</p>
          <small>${escapeHtml(job.status)} · ${escapeHtml(fmtTime(job.createdAt))}${job.finishedAt ? ` · ${escapeHtml(fmtTime(job.finishedAt))}` : ""} · resourceId ${escapeHtml(String(payload.resourceId || ""))}</small>
        </div>
        <div class="job-actions">
          <button class="ghost mini" type="button" data-rerun="${escapeHtml(job.id)}">再次抓取</button>
          <button class="ghost mini" type="button" data-preview="${escapeHtml(job.id)}">预览</button>
          <button class="ghost mini" type="button" data-rename="${escapeHtml(job.id)}">改名</button>
          <button class="ghost mini danger" type="button" data-delete="${escapeHtml(job.id)}">删除</button>
        </div>
        <div class="progress">
          <div class="progress-line">
            <span>${escapeHtml(progress.label || "等待进度")}${escapeHtml(pagesText)}${escapeHtml(rowsText)}</span>
            <b>${escapeHtml(percent)}%</b>
          </div>
          <div class="progress-track"><i style="width:${percent}%"></i></div>
        </div>
        ${job.stderr && job.status === "failed" ? `<pre class="stderr">${escapeHtml(job.stderr.slice(-900))}</pre>` : ""}
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
  $("#jobList").querySelectorAll("[data-rename]").forEach((button) => {
    button.addEventListener("click", async () => {
      const job = jobs.find((item) => item.id === button.dataset.rename);
      if (!job) return;
      const current = job.name || job.payload?.name || job.payload?.resourceId || job.id;
      await editJobName(job.id, String(current));
    });
  });
  $("#jobList").querySelectorAll("[data-delete]").forEach((button) => {
    button.addEventListener("click", async () => {
      if (!confirm("确认删除此任务？")) return;
      await api(`/api/jobs/${button.dataset.delete}`, { method: "DELETE" });
      await loadJobs();
    });
  });
}

async function loadJobs() {
  const payload = await api("/api/jobs");
  renderJobs(payload.jobs || []);
}

function bind() {
  $("#discoverBtn").addEventListener("click", discoverResourceIds);
  $("#urlInput").addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      discoverResourceIds();
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
  state.pollTimer = setInterval(() => {
    loadJobs().catch((error) => {
      console.warn("loadJobs poll error:", error);
    });
  }, 5000);
}

boot().catch((error) => {
  document.body.innerHTML = `<pre style="padding:24px;color:#f8f2df;background:#151515;min-height:100vh">${escapeHtml(error.stack || error.message)}</pre>`;
});
