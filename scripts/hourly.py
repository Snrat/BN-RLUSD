"""按小时抓取 RLUSD 余额并维护 data.json 的核心逻辑。

backfill_hourly.py 与 update_hourly.py 共用本模块。
"""

import json
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import rlusd_rpc as rpc

DATA_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data.json")

START_TS = int(datetime(2026, 7, 14, 0, 0, 0, tzinfo=timezone.utc).timestamp())  # 7/14 08:00 (UTC+8)
WEEK1_START = int(datetime(2026, 7, 17, 0, 0, 0, tzinfo=timezone.utc).timestamp())  # 7/17 08:00 (UTC+8)
WEEK1_END = int(datetime(2026, 7, 24, 0, 0, 0, tzinfo=timezone.utc).timestamp())  # 7/24 08:00 (UTC+8)
WEEK2_START = WEEK1_END  # 7/24 08:00 (UTC+8)
WEEK2_END = int(datetime(2026, 7, 31, 0, 0, 0, tzinfo=timezone.utc).timestamp())  # 7/31 08:00 (UTC+8)
WEEK3_START = WEEK2_END  # 7/31 08:00 (UTC+8)
WEEK3_END = int(datetime(2026, 8, 7, 0, 0, 0, tzinfo=timezone.utc).timestamp())  # 8/7 08:00 (UTC+8)
WEEK4_START = WEEK3_END  # 8/7 08:00 (UTC+8)
WEEK4_END = int(datetime(2026, 8, 14, 0, 0, 0, tzinfo=timezone.utc).timestamp())  # 8/14 08:00 (UTC+8)
# 已结算四周的实际 APR（币安公布）。奖励按美元等值 XRP 计算，币安公布的 XRP 计价价格
# （1.1077 / 1.0827 / 1.0351 / 1.0094）只用于折算发放币数，不影响 USD 口径 APR
# (周开始, 周结束, 实际 APR)
SETTLED_WEEKS = [
    (WEEK1_START, WEEK1_END, 0.2225),  # 第一次分发 2026-07-24
    (WEEK2_START, WEEK2_END, 0.0822),  # 第二次分发 2026-07-31
    (WEEK3_START, WEEK3_END, 0.0808),  # 第三次分发 2026-08-07
    (WEEK4_START, WEEK4_END, 0.0769),  # 第四次分发 2026-08-14
]

WEEKLY_REWARD = 200_000.0  # 第 1~4 周奖池：每周 200,000 美元等值 XRP（按当周币安公布价格折算币数）
WEEK5_WEEKLY_REWARD = 250_000.0  # 第五周奖池提高至 250,000 美元等值 XRP
ANNUAL_REWARD_POOL = WEEKLY_REWARD * 365 / 7  # 10,428,571.43（第 1~4 周，用于实际 APR 反推利用金额）
WEEK5_ANNUAL_REWARD_POOL = WEEK5_WEEKLY_REWARD * 365 / 7  # 13,035,714.29（第五周预估用）

MAX_WORKERS = 3  # 并发压低 + 4 节点轮询，避免触发免费 RPC 限流


def iso(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:00:00Z")


def current_hour_ts():
    return int(time.time()) // 3600 * 3600


class Anchors:
    """区块号/账本索引 与时间的线性插值锚点（12s/块、4s/账本，误差可忽略）"""

    def __init__(self, start_ts):
        b0, t0 = rpc.eth_find_block_at_or_before(start_ts)
        b1, t1 = rpc.eth_latest_block()
        self._eth = (b0, t0, b1, t1)
        l0, s0 = rpc.xrp_find_ledger_at_or_before(start_ts)
        l1, s1 = rpc.xrp_latest_ledger()
        self._xrp = (l0, s0, l1, s1)

    def block_at(self, ts):
        b0, t0, b1, t1 = self._eth
        return min(b1, round(b0 + (ts - t0) * (b1 - b0) / (t1 - t0)))

    def ledger_at(self, ts):
        l0, s0, l1, s1 = self._xrp
        return min(l1, round(l0 + (ts - s0) * (l1 - l0) / (s1 - s0)))


def fetch_hour(ts, anchors):
    """抓取某一整点（unix 秒）的 ETH/XRP 余额合计，失败返回 None"""
    block = anchors.block_at(ts)
    ledger = anchors.ledger_at(ts)
    try:
        eth = rpc.eth_total_at(block)
        xrp = rpc.xrp_total_at(ledger)
    except Exception as e:  # noqa: BLE001
        print(f"  [失败] {iso(ts)}: {e}")
        return None
    return {"t": iso(ts), "eth": round(eth, 2), "xrp": round(xrp, 2),
            "total": round(eth + xrp, 2)}


def fetch_hours(timestamps):
    """并发抓取多个整点，返回 {ts: entry|None}，带进度输出"""
    anchors = Anchors(min(timestamps))
    results = {}
    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(fetch_hour, ts, anchors): ts for ts in timestamps}
        for fut in as_completed(futures):
            results[futures[fut]] = fut.result()
            done += 1
            if done % 20 == 0 or done == len(timestamps):
                print(f"  进度 {done}/{len(timestamps)}")
    return results


def compute_params(hours_map):
    """四周实际 APR 反推各周利用金额与未利用资金，按未利用资金的周际趋势预估第五周。

    币安按 UTC+0 自然日（00:00~24:00）内用户最低持仓统计当日有效金额，
    故每周统计口径为：日内各整点快照取最小值得当日持仓，再对周内 7 天取平均。
    已结算四周奖池均为每周 200,000 美元等值 XRP，故各周利用金额 = 年化池 ÷ 该周实际 APR。
    四周未利用资金（日均最低持仓 − 利用金额）对其按周序号做最小二乘，
    外推第五周取值（含变化方向）；预估时 利用金额 = 当日最低持仓 − 第五周未利用预期值。
    第五周奖池提高至 250,000（WEEK5_ANNUAL_REWARD_POOL），预估时替换分子。
    """
    weeks = []
    for start, end, apr in SETTLED_WEEKS:
        by_day = {}
        n_hours = 0
        for ts in range(start, end, 3600):
            if ts in hours_map and hours_map[ts]:
                n_hours += 1
                by_day.setdefault(ts // 86400, []).append(hours_map[ts]["total"])
        daily_mins = [min(v) for v in by_day.values()]
        if len(daily_mins) < 7 or n_hours < 160:
            raise RuntimeError(f"{iso(start)} 周窗口数据不足（{len(daily_mins)} 天 / {n_hours} 小时），无法拟合")
        min_avg = sum(daily_mins) / len(daily_mins)
        utilized = ANNUAL_REWARD_POOL / apr
        weeks.append({
            "window": [iso(start), iso(end)],
            "snapshot_hours": n_hours,
            "actual_apr": apr,
            "min_deposit_avg": round(min_avg, 2),
            "utilized": round(utilized, 2),
            "unused": round(min_avg - utilized, 2),
        })
    unused = [w["unused"] for w in weeks]
    unused_avg = sum(unused) / len(unused)

    # 未利用资金的周际趋势：对 (周序号 1..4, 未利用资金) 做最小二乘，
    # 外推第 5 周取值作为第五周未利用资金预期（含变化方向），sigma 为趋势残差
    xs = list(range(1, len(weeks) + 1))
    xm = sum(xs) / len(xs)
    sxx = sum((x - xm) ** 2 for x in xs)
    slope = sum((x - xm) * (u - unused_avg) for x, u in zip(xs, unused)) / sxx  # 每周变化量
    unused_w5 = unused_avg + slope * (len(weeks) + 1 - xm)
    sigma = math.sqrt(
        sum((u - (unused_avg + slope * (x - xm))) ** 2 for x, u in zip(xs, unused))
        / (len(xs) - 2))
    return {
        "weekly_reward": WEEK5_WEEKLY_REWARD,
        "annual_reward_pool": round(WEEK5_ANNUAL_REWARD_POOL, 2),
        "prediction_target": "week5",
        "apr_display_start": iso(WEEK1_START),  # 曲线覆盖第一~五周：前四周画实际 APR，第五周画预估值
        "unused_avg": round(unused_avg, 2),
        "unused_per_week": round(slope, 2),
        "unused_week5": round(unused_w5, 2),
        "unused_sigma": round(sigma, 2),
        "fit_weeks": weeks,
    }


def apply_apr(entry, params, day_min_total):
    """按所在 UTC+0 自然日的最低持仓计算拟合 APR（%）。

    币安按 UTC+0 00:00~24:00 内最低持仓统计当日计息基数，故同一自然日内
    所有小时点共用该日全天最低总存款（日内恒定）；当天尚未完结时，
    自然只能取当日迄今最低值，随新低出现而阶梯式更新。
    已结算周（< 8/14 08:00 UTC+8）按当周 200,000 池与当周实际未利用资金回算，
    曲线与实际 APR 精确吻合；第五周按 250,000 池与外推的未利用预期值计算，
    故曲线在 08-14 处含奖池提升的跳变。
    当日最低持仓 ≤ 未利用资金时模型失效，曲线留空。
    """
    entry.pop("apr_optimistic", None)  # 清理旧的三口径字段
    entry.pop("apr_pessimistic", None)
    entry.pop("apr_mid", None)
    t = entry["t"]
    if t < iso(WEEK4_END):
        pool = ANNUAL_REWARD_POOL
        unused = next((w["unused"] for w in params["fit_weeks"]
                       if w["window"][0] <= t < w["window"][1]),
                      params["unused_week5"])
    else:
        pool = WEEK5_ANNUAL_REWARD_POOL
        unused = params["unused_week5"]
    utilized = day_min_total - unused
    entry["apr"] = round(pool / utilized * 100, 4) if utilized > 0 else None
    return entry


def load_data():
    if os.path.exists(DATA_PATH):
        with open(DATA_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {"updated": None, "params": None, "hours": []}


def save_data(hours_map):
    """hours_map: {ts: entry}。重算参数与 APR 后写回 data.json"""
    params = compute_params(hours_map)
    # 先按 UTC+0 自然日汇总全天最低总存款（已完结日取全天最低，
    # 当天未完结时即为迄今最低），同一日内 APR 恒定
    day_min = {}
    for ts, entry in hours_map.items():
        if not entry:
            continue
        day = ts // 86400
        prev = day_min.get(day)
        day_min[day] = entry["total"] if prev is None else min(prev, entry["total"])
    entries = [apply_apr(hours_map[ts], params, day_min[ts // 86400])
               for ts in sorted(hours_map) if hours_map[ts]]
    data = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "params": params,
        "hours": entries,
    }
    with open(DATA_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    print(f"已写入 {DATA_PATH}: {len(entries)} 个小时点")
    return data


def fill(start_ts, end_ts):
    """补齐 [start_ts, end_ts] 内缺失的整点数据并保存"""
    data = load_data()
    hours_map = {}
    for h in data["hours"]:
        ts = int(datetime.strptime(h["t"], "%Y-%m-%dT%H:00:00Z")
                 .replace(tzinfo=timezone.utc).timestamp())
        hours_map[ts] = h
    missing = [ts for ts in range(start_ts, end_ts + 1, 3600)
               if ts not in hours_map or not hours_map[ts].get("total")]
    if missing:
        print(f"需抓取 {len(missing)} 个小时点（{iso(missing[0])} ~ {iso(missing[-1])}）")
        results = fetch_hours(missing)
        failed = 0
        for ts, entry in results.items():
            if entry:
                hours_map[ts] = entry
            else:
                failed += 1
        if failed:
            print(f"警告: {failed} 个小时点抓取失败，将在下次运行时重试")
    else:
        print("无缺失小时点")
    return save_data(hours_map)
