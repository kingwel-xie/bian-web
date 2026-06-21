# Binance Leaderboard Workflow

用于抓取 Binance 活动排行榜、生成快照、导出表格，并在 Web 页面查看每日增量和图表。

当前 Web 控制台默认入口：

```text
http://127.0.0.1:48234/css888
```

## 功能

- 抓取 Binance 现货或 U 本位合约活动排行榜。
- 保存每日排行榜快照到 `data/`。
- 计算真实成交量字段，优先使用接口里的 `tradingVolume`，缺失时再用 `grade ** 2` 还原。
- 按昵称 `nickName` 对比前后快照，生成每日增量。
- 为 RE 活动生成合并增量柱状图，包含 `10-25`、`26-50`、`180-200` 三段。
- 支持导出 CSV、JSON、XLSX、PNG。

## Docker 运行

```bash
docker compose up -d --build
```

服务配置在 `docker-compose.yml`：

```yaml
WEB_PORT: "48234"
DATA_ROOT: /data
```

本地目录 `./data` 会挂载到容器内 `/data`。

查看状态：

```bash
docker compose ps
docker compose logs -f leaderboard
```

## Web 路由

- `/css888`：前端页面
- `/api/overview`：活动列表
- `/api/scrape/latest?market=um&symbol=REUSDT`：读取最新快照预览
- `/api/scrape/jobs`：创建抓取任务
- `/files/...`：只用于读取生成的公开导出文件

`/files` 已禁止访问隐藏目录和敏感后缀，例如 `.har`、`.env`、`.pem`、`.key`、`.p12`、`.crt`、`.db`。

## RE 活动

当前 RE 合约活动使用：

```text
market: um
symbol: REUSDT
resourceId: 100002776
```

活动链接：

```text
https://www.binance.com/zh-CN/activity/trading-competition/futures-re-challenge?utm_source=appanns
```

读取最新 RE 数据：

```bash
curl -sS 'http://127.0.0.1:48234/api/scrape/latest?market=um&symbol=REUSDT'
```

## 数据目录

运行数据在 `data/`，不要提交到仓库。

常见结构：

```text
data/
  .workflow/
    jobs.json
    schedules.json
  um_re/
    YYYY-MM-DD_um_re_top1000.json
    YYYY-MM-DD_um_re_top1000.csv
    YYYY-MM-DD_um_re_delta_by_nickname_combined.png
```

`jobs.json` 是任务历史，可能包含抓取结果摘要；数据量会随任务数量增长。

## 隐私和推送前检查

不要提交这些内容：

- `data/`
- `email.env`
- `.env`
- `*.har`
- `*.pem`
- `*.key`
- `*.p12`
- `*.crt`
- `*.db`
- `*.sqlite`

HAR 文件尤其敏感，可能包含浏览器请求里的 cookie、token 或会话字段。旧备份目录已移到项目外：

```text
/root/docker/bian-web-private-backups/.cleanup_backup_20260621_042454
```

SMTP 配置请复制示例文件再本地填写：

```bash
cp email.env.example email.env
chmod 600 email.env
```

推送前建议执行：

```bash
find . -type f \( -name '*.har' -o -name '*.env' -o -name 'email.env' -o -name '*.pem' -o -name '*.key' -o -name '*.p12' -o -name '*.crt' \)
```

如果有真实密钥、邮箱授权码、HAR 文件，不要提交。

## 命令行抓取

示例：

```bash
docker run --rm --network host \
  -v "$PWD/data:/data" \
  binance-leaderboard-workflow \
  run \
  "https://www.binance.com/zh-CN/activity/trading-competition/futures-re-challenge?utm_source=appanns" \
  --name um_re \
  --symbol REUSDT \
  --output-root /data
```

如果需要本机代理：

```bash
docker run --rm --network host \
  -v "$PWD/data:/data" \
  binance-leaderboard-workflow \
  run \
  "URL" \
  --name tokenname \
  --symbol TOKENUSDT \
  --output-root /data \
  --proxy http://127.0.0.1:7890
```

## 邮件导出

`email.env.example` 是模板，不包含真实凭据。填写真实 SMTP 信息后可以用：

```bash
python3 send_exports_email.py --env-file email.env --dry-run
```

确认无误后去掉 `--dry-run`。

