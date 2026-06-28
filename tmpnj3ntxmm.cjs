
const { chromium } = require('playwright');

const activities = JSON.parse(process.env.ACTIVITIES_JSON || '[]');
const proxy = process.env.PLAYWRIGHT_PROXY || '';
const waitMs = Number(process.env.BROWSER_WAIT_MS || '30000');

function parseJson(text) {
  if (!text) return null;
  try { return JSON.parse(text); } catch { return null; }
}

function addCandidate(result, id, source) {
  const value = Number(id);
  if (!Number.isFinite(value) || value <= 0) return;
  if (!result.candidates.some((item) => item.resourceId === value)) {
    result.candidates.push({ resourceId: value, source });
  }
}

(async () => {
  const launchOptions = { headless: true };
  if (proxy) launchOptions.proxy = { server: proxy };
  const browser = await chromium.launch(launchOptions);
  const context = await browser.newContext({
    locale: 'zh-CN',
    timezoneId: 'Asia/Shanghai',
    userAgent: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
    viewport: { width: 1440, height: 1100 },
  });

  const results = [];
  for (const activity of activities) {
    const result = { name: activity.name, url: activity.url, title: null, candidates: [], events: [], errors: [] };
    const page = await context.newPage();

    page.on('response', async (response) => {
      const url = response.url();
      if (!url.includes('/growth-paas/')) return;
      const request = response.request();
      const reqBody = parseJson(request.postData() || '');
      const event = { url, method: request.method(), status: response.status(), request: reqBody };

      if (url.includes('/resource/summary/participant/list')) {
        try {
          const payload = parseJson(await response.text());
          event.response = payload;
          if (payload && Array.isArray(payload.data)) {
            for (const item of payload.data) addCandidate(result, item.resourceId, 'participant/list response');
          }
          if (reqBody && Array.isArray(reqBody.resourceIdList)) {
            for (const id of reqBody.resourceIdList) addCandidate(result, id, 'participant/list request');
          }
        } catch (error) {
          result.errors.push(String(error));
        }
      } else if (url.includes('/resource/summary/list')) {
        if (reqBody && reqBody.resourceId) addCandidate(result, reqBody.resourceId, 'summary/list request');
        try {
          const payload = parseJson(await response.text());
          event.response = payload;
        } catch {}
      } else if (url.includes('/user/user-group-eligibility')) {
        if (reqBody && reqBody.resourceId) addCandidate(result, reqBody.resourceId, 'user-group-eligibility request');
      }

      result.events.push(event);
    });

    try {
      await page.goto(activity.url, { waitUntil: 'domcontentloaded', timeout: 60000 });
      await page.waitForTimeout(waitMs);
      await page.evaluate(() => window.scrollTo(0, Math.floor(document.body.scrollHeight * 0.55))).catch(() => {});
      await page.waitForTimeout(Math.max(2000, Math.floor(waitMs / 3)));
      result.title = await page.title().catch(() => null);
    } catch (error) {
      result.errors.push(String(error));
    } finally {
      await page.close().catch(() => {});
    }
    results.push(result);
  }

  await browser.close();
  console.log(JSON.stringify(results));
})().catch((error) => {
  console.error(error && error.stack ? error.stack : String(error));
  process.exit(1);
});
