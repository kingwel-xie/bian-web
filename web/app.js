const state = {
  activeQuery: null,
  currentPreview: null,
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
    const formData = Object.fromEntries(new FormData(form).entries());
    state.activeQuery = { ...derived, url, resourceId: formData.resourceId || undefined };
    const rid = state.activeQuery.resourceId ? ` · ID ${state.activeQuery.resourceId}` : "";
    $("#derivedBox").innerHTML = `
      <strong>${escapeHtml(derived.market.toUpperCase())}</strong>
      <span>${escapeHtml(derived.symbol)}</span>
      <span>Top 1000${escapeHtml(rid)}</span>
      <code>${escapeHtml(url)}</code>
    `;
  } catch (error) {
    $("#derivedBox").innerHTML = `<span class="err">${escapeHtml(error.message)}</span>`;
  }
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
  const resourceId = payload.resourceId || undefined;
  const form = $("#scrapeForm");
  form.elements.url.value = url;
  if (form.elements.resourceId) form.elements.resourceId.value = resourceId || "";
  state.activeQuery = {
    market,
    symbol,
    url,
    resourceId,
  };
  await updateDerived();
  await api("/api/scrape/jobs", {
    method: "POST",
    body: JSON.stringify({
      url,
      market,
      symbol,
      resourceId,
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
      resourceId: state.activeQuery.resourceId || undefined,
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
    const preview = null;
    const progress = job.progress || {};
    const percent = Math.max(0, Math.min(100, Number(progress.percent || 0)));
    const rowsText = progress.rowsFetched ? ` · ${progress.rowsFetched}/1000 rows` : "";
    const pagesText = progress.totalPages ? ` · page ${progress.currentPage}/${progress.totalPages}` : "";
    const idsText = progress.candidateResourceIds ? ` · ID [${progress.candidateResourceIds.join(", ")}]` : "";
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
          <button class="ghost mini" type="button" data-preview="${escapeHtml(job.id)}">预览</button>
          <button class="ghost mini danger" type="button" data-delete="${escapeHtml(job.id)}">删除</button>
        </div>
        <div class="progress">
          <div class="progress-line">
            <span>${escapeHtml(progress.label || "等待进度")}${escapeHtml(pagesText)}${escapeHtml(rowsText)}${escapeHtml(idsText)}</span>
            <b>${escapeHtml(percent)}%</b>
          </div>
          <div class="progress-track"><i style="width:${percent}%"></i></div>
        </div>
        ${job.stderr && job.status === "failed" ? `<pre class="stderr">${escapeHtml(job.stderr.slice(-900))}</pre>` : ""}
      </article>
    `;
  }).join("") || `<div class="empty box">没有快捷任务。</div>`;

  $("#jobList").querySelectorAll("[data-preview]").forEach((button) => {
    button.addEventListener("click", () => {
      window.open(`/preview.html?job=${button.dataset.preview}`, "_blank");
    });
  });
  $("#jobList").querySelectorAll("[data-rerun]").forEach((button) => {
    button.addEventListener("click", async () => {
      const job = jobs.find((item) => item.id === button.dataset.rerun);
      if (job) await rerunQuickTask(job);
    });
  });
  $("#jobList").querySelectorAll("[data-delete]").forEach((button) => {
    button.addEventListener("click", async () => {
      if (!confirm("确认删除此快捷任务？")) return;
      await api(`/api/jobs/${button.dataset.delete}`, { method: "DELETE" });
      await loadJobs();
    });
  });
}

async function loadJobs() {
  const payload = await api("/api/jobs");
  renderJobs(payload.jobs || []);
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
  state.activeQuery = { market: payload.market, symbol: payload.symbol, url: payload.url, resourceId: payload.resourceId || undefined };
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
}

async function boot() {
  bind();
  await updateDerived();
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
