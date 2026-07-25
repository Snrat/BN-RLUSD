"""增量更新：补齐 data.json 最后一条至今的整点数据（GitHub Actions 每小时运行）。"""

from datetime import datetime, timezone

import hourly

if __name__ == "__main__":
    data = hourly.load_data()
    if data["hours"]:
        last_ts = int(datetime.strptime(data["hours"][-1]["t"], "%Y-%m-%dT%H:00:00Z")
                      .replace(tzinfo=timezone.utc).timestamp())
        start = last_ts + 3600
    else:
        start = hourly.START_TS
    end = hourly.current_hour_ts()
    if start > end:
        print("数据已是最新，无需更新")
    else:
        print(f"更新范围: {hourly.iso(start)} ~ {hourly.iso(end)}")
        hourly.fill(start, end)
