# BTC-USD Match Stream Bot

Watches the Coinbase Exchange public WebSocket "matches" feed for BTC-USD and
sends [ntfy.sh](https://ntfy.sh) push notifications when:

- an **aggressive taker** streak (`AGGRESSIVENESS_N` consecutive trades from
  one taker order) ends, or
- a **large resting order** streak (`RESTING_N` consecutive trades matched
  against one maker order) ends.

Runs as a manually-triggered GitHub Actions job — nothing runs until you
click a button, and you stop it by cancelling the run.

## Files

- `BTC_Matched_order_aggressive_and_resting_analysis.py` — the analyzer script.
- `requirements.txt` — Python dependencies (`websocket-client`, `requests`).
- `.github/workflows/run.yml` — the GitHub Actions workflow, triggered
  manually (`workflow_dispatch`).

## One-time setup

1. Create a new GitHub repository (public or private) and push these three
   files/folders into it, keeping the folder structure exactly as-is
   (the `.github/workflows/run.yml` path matters).
2. That's it — no secrets or config are required to get it running as-is,
   since the ntfy topic URL is already hardcoded in `BTC_Matched_order_aggressive_and_resting_analysis.py`.

## Turning it ON

1. Go to your repo on GitHub → the **Actions** tab.
2. Click **BTC Match Stream** in the left sidebar.
3. Click **Run workflow** (top right) → **Run workflow** again to confirm.
4. Click into the running job to watch live logs.

## Turning it OFF

1. Go to **Actions** → the currently running **BTC Match Stream** job.
2. Click **Cancel workflow** in the top right.

The script loops forever on its own (auto-reconnecting to the WebSocket), so
cancelling is the only way to stop it early. If you forget, GitHub will kill
the job automatically after ~6 hours (the `timeout-minutes: 350` setting in
the workflow, just under GitHub's hard 360-minute limit for hosted runners)
— you'd then need to click "Run workflow" again to resume.

## Notifications

Alerts are pushed to `https://ntfy.sh/btc-tick-stream-notif` (see `NTFY_URL`
in `BTC_Matched_order_aggressive_and_resting_analysis.py`). Subscribe to that topic in the ntfy app or at that URL in a
browser to receive them. Anyone who knows this topic name can also read
these notifications, since it's a public repo — if you'd rather keep the
topic name private:

1. In `BTC_Matched_order_aggressive_and_resting_analysis.py`, change:
   ```python
   NTFY_URL = "https://ntfy.sh/btc-tick-stream-notif"
   ```
   to:
   ```python
   import os
   NTFY_URL = os.environ["NTFY_URL"]
   ```
2. In your repo: **Settings → Secrets and variables → Actions → New repository secret**,
   name it `NTFY_URL`, value your full ntfy URL.
3. In `.github/workflows/run.yml`, add an `env:` block to the "Run the bot" step:
   ```yaml
   - name: Run the bot
     run: python BTC_Matched_order_aggressive_and_resting_analysis.py
     env:
       NTFY_URL: ${{ secrets.NTFY_URL }}
   ```

## Limits to be aware of

- GitHub-hosted runners cap any single job at 6 hours, regardless of
  `timeout-minutes`. For a bot you want running longer than that unattended,
  a self-hosted runner (e.g. a spare VM or Raspberry Pi registered to this
  repo) or a small always-on VPS would be a better fit than Actions.
