"""一次性回填：抓取 2026-07-14 08:00 (UTC+8) 至今每个整点的 RLUSD 余额。"""

import hourly

if __name__ == "__main__":
    end = hourly.current_hour_ts()
    print(f"回填范围: {hourly.iso(hourly.START_TS)} ~ {hourly.iso(end)}")
    data = hourly.fill(hourly.START_TS, end)
    p = data["params"]
    print(f"第一周({p['week1_snapshot_hours']}h快照) 日均存款 {p['week1_avg_deposit']:,.0f}"
          f" | 利用率 {p['week1_utilization']:.4%} | 未利用 {p['week1_unused']:,.0f}")
