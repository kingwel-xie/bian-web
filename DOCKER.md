# Docker Usage

Build the image from this directory:

```bash
docker build -t binance-leaderboard-workflow .
```

Or use Compose:

```bash
docker compose up -d --build
```

Start the web console:

```bash
docker run --rm --network host \
  -v /home/css/下载/bian-master/leaderboard-workflow/data:/data \
  -e DATA_ROOT=/data \
  binance-leaderboard-workflow
```

Open:

```text
http://127.0.0.1:48234/css888
```

Run a known activity from CLI. Mount `leaderboard-workflow/data` to `/data`; outputs will be written to `/data/bill`, `/data/aig`, etc.

```bash
docker run --rm --network host \
  -v /home/css/下载/bian-master/leaderboard-workflow/data:/data \
  binance-leaderboard-workflow \
  run \
  "https://www.binance.com/zh-CN/activity/trading-competition/futures-bill-challenge" \
  --output-root /data
```

AIG:

```bash
docker run --rm --network host \
  -v /home/css/下载/bian-master/leaderboard-workflow/data:/data \
  binance-leaderboard-workflow \
  run \
  "https://www.binance.com/zh-CN/activity/trading-competition/futures-aigensyn-challenge" \
  --output-root /data
```

If your Binance access depends on a local proxy at `127.0.0.1:7890`, `--network host` lets the container use it. You can also force it:

```bash
docker run --rm --network host \
  -v /home/css/下载/bian-master/leaderboard-workflow/data:/data \
  binance-leaderboard-workflow \
  run \
  "https://www.binance.com/zh-CN/activity/trading-competition/futures-bill-challenge" \
  --output-root /data \
  --proxy http://127.0.0.1:7890
```

For a new activity whose URL slug cannot infer the symbol:

```bash
docker run --rm --network host \
  -v /home/css/下载/bian-master/leaderboard-workflow/data:/data \
  binance-leaderboard-workflow \
  run \
  "URL" \
  --name tokenname \
  --symbol TOKENUSDT \
  --output-root /data
```

The first run for an activity initializes the folder and does not emit rank-delta ratios. Once there are at least two daily snapshots, the workflow outputs the `#20/#50/#200` rank-delta to market quote-volume ratios.
