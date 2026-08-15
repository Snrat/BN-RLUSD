"""一次性回填：抓取 2026-07-14 08:00 (UTC+8) 至今每个整点的 RLUSD 余额。"""

import hourly

if __name__ == "__main__":
    end = hourly.current_hour_ts()
    print(f"回填范围: {hourly.iso(hourly.START_TS)} ~ {hourly.iso(end)}")
    data = hourly.fill(hourly.START_TS, end)
    p = data["params"]
    print(f"未利用资金: 周际趋势 {p['unused_per_week']:,.0f}/周"
          f" | 第五周预期 {p['unused_week5']:,.0f} | 残差 ±{p['unused_sigma']:,.0f}")
    for i, w in enumerate(p["fit_weeks"]):
        print(f"  第{i + 1}周({w['snapshot_hours']}h快照) 实际 {w['actual_apr']:.2%}"
              f" | 日均存款 {w['avg_deposit']:,.0f} | 利用 {w['utilized']:,.0f}"
              f" | 未利用 {w['unused']:,.0f}")
