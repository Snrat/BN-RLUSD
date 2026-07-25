#!/bin/bash
# RLUSD 每小时数据更新（服务器 cron 兜底，与 GitHub Actions 互为备份）
# crontab: 3,23,43 * * * * /root/RLUSD/scripts/cron_update.sh
set -u
REPO=/root/RLUSD
LOG=$REPO/logs/cron.log
export GIT_SSH_COMMAND="ssh -i /root/.ssh/id_ed25519_1panel -o IdentitiesOnly=yes"
mkdir -p "$REPO/logs"

exec 9>"$REPO/logs/.cron.lock"
flock -n 9 || exit 0   # 防止与上一次运行重叠

{
  echo "===== $(date -u '+%Y-%m-%d %H:%M UTC') ====="
  cd "$REPO" || exit 1
  git pull --rebase -q origin main
  before=$(md5sum data.json | cut -d' ' -f1)
  python3 scripts/update_hourly.py
  after=$(md5sum data.json | cut -d' ' -f1)
  if [ "$before" != "$after" ]; then
    git add data.json
    git commit -q -m "data: hourly RLUSD update $(date -u +%Y-%m-%dT%H:%MZ) (server cron)"
    git push -q origin main && echo "pushed" || echo "push failed（下个班次重试）"
  else
    echo "无新数据"
  fi
} >> "$LOG" 2>&1

# 日志保留最近 2000 行
tail -n 2000 "$LOG" > "$LOG.tmp" && mv "$LOG.tmp" "$LOG"
