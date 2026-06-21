const state = {
  activeQuery: null,
  currentPreview: null,
  nicknameQuery: "",
  pollTimer: null,
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
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || response.statusText);
  return payload;
}

function formPayload(form) {
  const data = Object.fromEntries(new FormData(form).entries());
  for (const key of Object.keys(data)) {
    if (data[key] === "") delete data[key];
  }
  delete data.nicknameSearch;
  const inferred = inferFromUrl(data.url);
  data.market = inferred.market;
  data.symbol = inferred.symbol;
  if (data.browserWaitMs) data.browserWaitMs = Number(data.browserWaitMs);
  data.mode = "scrape";
  return data;
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
    /(?:^|-)spot-[a-z0-9-]*-([a-z0-9]+)$/i,
    /(?:^|-)futures-([a-z0-9]+)(?:-|$)/i,
    /^([a-z0-9]+)-spot-/i,
    /^([a-z0-9]+)-futures-/i,
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

async function updateDerived() {
  const form = $("#scrapeForm");
  const url = new FormData(form).get("url");
  if (!url) {
    $("#derivedBox").textContent = "粘贴活动 URL 后自动识别 spot/um 和 symbol。";
    return;
  }
  try {
    const derived = inferFromUrl(url);
    state.activeQuery = { ...derived, url };
    $("#derivedBox").innerHTML = `
      <strong>${escapeHtml(derived.market.toUpperCase())}</strong>
      <span>${escapeHtml(derived.symbol)}</span>
      <span>Top 1000</span>
      <code>${escapeHtml(url)}</code>
    `;
  } catch (error) {
    $("#derivedBox").innerHTML = `<span class="err">${escapeHtml(error.message)}</span>`;
  }
}

function previewFromJob(job) {
  const result = Array.isArray(job.result) ? job.result[0] : null;
  return result?.preview || null;
}

function renderPreview(preview) {
  const meta = $("#previewMeta");
  const links = $("#downloadLinks");
  const charts = $("#deltaCharts");
  const body = $("#previewRows");
  state.currentPreview = preview;
  if (!preview) {
    meta.textContent = "暂无数据。";
    links.innerHTML = "";
    charts.innerHTML = "";
    body.innerHTML = `<tr><td colspan="7" class="empty">还没有抓取结果。</td></tr>`;
    return;
  }

  const updated = preview.meta?.updatedTime ? new Date(Number(preview.meta.updatedTime)).toLocaleString("zh-CN", { hour12: false }) : "—";
  const allRows = preview.rows || [];
  const query = state.nicknameQuery.trim().toLowerCase();
  const rows = query ? allRows.filter((row) => String(row.nickname || "").toLowerCase().includes(query)) : allRows;
  const searchText = query ? ` · search "${state.nicknameQuery}" ${rows.length}/${allRows.length}` : "";
  const restoredSum = preview.restoredTradingVolumeSum ? ` · restored ${fmtNumber(preview.restoredTradingVolumeSum)}` : "";
  meta.textContent = `${preview.name || "leaderboard"} · rows ${preview.count || allRows.length || 0}${searchText} · resourceId ${preview.resourceId || "—"}${restoredSum} · updated ${updated}`;
  links.innerHTML = [
    preview.xlsxUrl ? `<a href="${escapeHtml(preview.xlsxUrl)}" target="_blank" rel="noreferrer">下载 XLSX</a>` : "",
    preview.csvUrl ? `<a href="${escapeHtml(preview.csvUrl)}" target="_blank" rel="noreferrer">下载 CSV</a>` : "",
    preview.jsonUrl ? `<a href="${escapeHtml(preview.jsonUrl)}" target="_blank" rel="noreferrer">查看 JSON</a>` : "",
    preview.discoveryUrl ? `<a href="${escapeHtml(preview.discoveryUrl)}" target="_blank" rel="noreferrer">Discovery</a>` : "",
  ].filter(Boolean).join("");
  charts.innerHTML = preview.delta?.combinedChartUrl ? `
    <figure>
      <img src="${escapeHtml(preview.delta.combinedChartUrl)}" alt="增量图" loading="lazy" />
      <figcaption>增量图 · 10-25 / 26-50 / 180-200</figcaption>
    </figure>
  ` : "";

  body.innerHTML = rows.map((row) => `
    <tr>
      <td class="rank">${escapeHtml(row.rank)}</td>
      <td>${escapeHtml(row.nickname || "—")}</td>
      <td>${escapeHtml(row.userId || "—")}</td>
      <td class="num">${escapeHtml(fmtNumber(row.grade))}</td>
      <td class="num">${escapeHtml(fmtNumber(row.restoredTradingVolume || row.tradingVolume))}</td>
      <td class="num">${escapeHtml(row.deltaRestoredTradingVolume == null ? "—" : fmtNumber(row.deltaRestoredTradingVolume))}</td>
      <td>${escapeHtml(row.region || "—")}</td>
    </tr>
  `).join("") || `<tr><td colspan="7" class="empty">${query ? "没有匹配昵称。" : "结果文件没有 rows。"}</td></tr>`;
}

async function loadLatestForActiveQuery() {
  if (!state.activeQuery) return;
  const query = new URLSearchParams({
    market: state.activeQuery.market,
    symbol: state.activeQuery.symbol,
  });
  if (state.activeQuery.label) query.set("label", state.activeQuery.label);
  const payload = await api(`/api/scrape/latest?${query.toString()}`);
  renderPreview(payload.result);
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

function quickTaskKey(job) {
  const payload = job.payload || {};
  const urlKey = normalizeTaskUrl(payload.url);
  if (urlKey) return `url:${urlKey}`;
  return `job:${job.id}`;
}

function groupQuickTasks(jobs) {
  const groups = new Map();
  for (const job of jobs) {
    const key = quickTaskKey(job);
    if (!groups.has(key)) {
      groups.set(key, {
        key,
        latest: job,
        jobs: [],
      });
    }
    groups.get(key).jobs.push(job);
  }
  return [...groups.values()];
}

async function rerunQuickTask(job) {
  const payload = job.payload || {};
  const url = payload.url || "";
  if (!url) return;
  const inferred = inferFromUrl(url);
  const market = payload.market || inferred.market;
  const symbol = payload.symbol || inferred.symbol;
  const form = $("#scrapeForm");
  form.elements.url.value = url;
  state.activeQuery = {
    market,
    symbol,
    url,
  };
  await updateDerived();
  await api("/api/scrape/jobs", {
    method: "POST",
    body: JSON.stringify({
      url,
      market,
      symbol,
      browserWaitMs: 30000,
      proxy: "auto",
      mode: "scrape",
    }),
  });
  await loadJobs();
}

async function rerunCurrentTask() {
  if (!state.activeQuery?.market || !state.activeQuery?.symbol) return;
  await api("/api/scrape/jobs", {
    method: "POST",
    body: JSON.stringify({
      url: state.activeQuery.url || undefined,
      market: state.activeQuery.market,
      symbol: state.activeQuery.symbol,
      browserWaitMs: 30000,
      proxy: "auto",
      mode: "scrape",
    }),
  });
  await loadJobs();
}

function renderJobs(jobs) {
  const groups = groupQuickTasks(jobs);
  $("#jobCount").textContent = groups.length;
  $("#jobList").innerHTML = groups.slice(0, 20).map((group) => {
    const job = group.latest;
    const payload = job.payload || {};
    const preview = previewFromJob(job);
    const progress = job.progress || {};
    const percent = Math.max(0, Math.min(100, Number(progress.percent || 0)));
    const rowsText = progress.rowsFetched ? ` · ${progress.rowsFetched}/1000 rows` : "";
    const pagesText = progress.totalPages ? ` · page ${progress.currentPage}/${progress.totalPages}` : "";
    const statusClass = job.status === "completed" ? "ok" : job.status === "failed" ? "fail" : "run";
    const url = normalizeTaskUrl(payload.url);
    return `
      <article class="job ${statusClass}" data-job-id="${escapeHtml(job.id)}">
        <div>
          <strong>${escapeHtml((payload.market || "").toUpperCase())} ${escapeHtml(payload.symbol || "")} · ${escapeHtml(payload.name || "")}</strong>
          <p>${escapeHtml(url || "无 URL")}</p>
          <small>最新 ${escapeHtml(job.status)} · ${escapeHtml(fmtTime(job.createdAt))}${job.finishedAt ? ` · finished ${escapeHtml(fmtTime(job.finishedAt))}` : ""} · 历史 ${group.jobs.length} 次</small>
        </div>
        <div class="job-actions">
          <button class="ghost mini" type="button" data-rerun="${escapeHtml(job.id)}" ${url ? "" : "disabled"}>再次抓取</button>
          <button class="ghost mini" type="button" data-preview="${escapeHtml(job.id)}" ${preview ? "" : "disabled"}>预览</button>
        </div>
        <div class="progress">
          <div class="progress-line">
            <span>${escapeHtml(progress.label || "等待进度")}${escapeHtml(pagesText)}${escapeHtml(rowsText)}</span>
            <b>${escapeHtml(percent)}%</b>
          </div>
          <div class="progress-track"><i style="width:${percent}%"></i></div>
        </div>
        ${job.stderr && job.status === "failed" ? `<pre>${escapeHtml(job.stderr.slice(-900))}</pre>` : ""}
      </article>
    `;
  }).join("") || `<div class="empty box">没有快捷任务。</div>`;

  $("#jobList").querySelectorAll("[data-preview]").forEach((button) => {
    button.addEventListener("click", () => {
      const job = jobs.find((item) => item.id === button.dataset.preview);
      renderPreview(previewFromJob(job));
    });
  });
  $("#jobList").querySelectorAll("[data-rerun]").forEach((button) => {
    button.addEventListener("click", async () => {
      const job = jobs.find((item) => item.id === button.dataset.rerun);
      if (job) await rerunQuickTask(job);
    });
  });
}

async function loadJobs() {
  const payload = await api("/api/jobs");
  renderJobs(payload.jobs || []);
}

async function loadDefaultPreview() {
  const payload = await api("/api/scrape/latest?market=um&symbol=REUSDT");
  if (payload.result) {
    state.activeQuery = { market: "um", symbol: "REUSDT", url: payload.result.url || "" };
    renderPreview(payload.result);
  }
}

async function createScrapeJob(event) {
  event.preventDefault();
  const form = event.currentTarget;
  let payload;
  try {
    payload = formPayload(form);
  } catch (error) {
    $("#derivedBox").innerHTML = `<span class="err">${escapeHtml(error.message)}</span>`;
    return;
  }
  state.activeQuery = { market: payload.market, symbol: payload.symbol, url: payload.url };
  await updateDerived();
  await api("/api/scrape/jobs", {
    method: "POST",
    body: JSON.stringify(payload),
  });
  await loadJobs();
}

function bind() {
  const form = $("#scrapeForm");
  form.addEventListener("submit", createScrapeJob);
  form.addEventListener("input", updateDerived);
  form.addEventListener("change", updateDerived);
  $("#nicknameSearchBtn").addEventListener("click", () => {
    state.nicknameQuery = $("#nicknameSearch").value.trim();
    renderPreview(state.currentPreview);
  });
  $("#nicknameSearch").addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      state.nicknameQuery = event.currentTarget.value.trim();
      renderPreview(state.currentPreview);
    }
  });
  $("#rerunCurrentBtn").addEventListener("click", rerunCurrentTask);
}

async function boot() {
  bind();
  await updateDerived();
  await loadDefaultPreview();
  loadJobs().catch((error) => {
    $("#jobList").innerHTML = `<div class="empty box">${escapeHtml(error.message)}</div>`;
  });
  state.pollTimer = setInterval(loadJobs, 5000);
}

boot().catch((error) => {
  document.body.innerHTML = `<pre style="padding:24px;color:#f8f2df;background:#151515;min-height:100vh">${escapeHtml(error.stack || error.message)}</pre>`;
});
