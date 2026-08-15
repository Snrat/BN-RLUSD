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
WEEK4_ACTUAL_APR = 0.0769  # 第四周实际 APR（币安公布），用于校准第五周预估

WEEKLY_REWARD = 200_000.0  # 第 1~4 周奖池：每周 200,000 美元等值 XRP（按当周币安公布价格折算币数）
WEEK5_WEEKLY_REWARD = 250_000.0  # 第五周奖池提高至 250,000 美元等值 XRP
ACTIVITY_APR = 0.2238
ANNUAL_REWARD_POOL = WEEKLY_REWARD * 365 / 7  # 10,428,571.43（第 1~4 周，用于实际 APR 反推利用金额）
WEEK5_ANNUAL_REWARD_POOL = WEEK5_WEEKLY_REWARD * 365 / 7  # 13,035,714.29（第五周预估用）
WEEK1_UTILIZED = WEEKLY_REWARD / (ACTIVITY_APR * 7 / 365)  # ≈ 46,597,730

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
    """第一周参数（历史参考）+ 第四周实际 APR 校准参数（用于第五周预估）。

    第四周实际 APR 已由币安公布（WEEK4_ACTUAL_APR），第四周奖励池仍为 200,000，
    故第四周利用金额 = 第 1~4 周年化池 ÷ 第四周实际 APR；
    第四周利用率/未利用额用第四周的小时快照等权平均计算。
    第五周奖池提高至 250,000（WEEK5_ANNUAL_REWARD_POOL），预估时替换分子。
    """
    w1 = [hours_map[ts]["total"]
          for ts in range(WEEK1_START, WEEK1_END, 3600)
          if ts in hours_map and hours_map[ts]]
    if len(w1) < 160:
        raise RuntimeError(f"第一周窗口数据不足（{len(w1)}/168），无法计算参数")
    avg1 = sum(w1) / len(w1)

    w4 = [hours_map[ts]["total"]
          for ts in range(WEEK4_START, WEEK4_END, 3600)
          if ts in hours_map and hours_map[ts]]
    if len(w4) < 160:
        raise RuntimeError(f"第四周窗口数据不足（{len(w4)}/168），无法校准参数")
    avg4 = sum(w4) / len(w4)
    utilized4 = ANNUAL_REWARD_POOL / WEEK4_ACTUAL_APR  # ≈ 135.61M（按第四周 200,000 池反推）
    utilization4 = utilized4 / avg4
    return {
        "weekly_reward": WEEK5_WEEKLY_REWARD,
        "activity_apr": ACTIVITY_APR,
        "annual_reward_pool": round(WEEK5_ANNUAL_REWARD_POOL, 2),
        "prediction_target": "week5",
        "apr_display_start": iso(WEEK4_END),  # 曲线只显示第五周，第四周已是实际值无预估意义
        # 第一周（已结算，历史参考）
        "week1_window": [iso(WEEK1_START), iso(WEEK1_END)],
        "week1_snapshot_hours": len(w1),
        "week1_avg_deposit": round(avg1, 2),
        "week1_utilized": round(WEEK1_UTILIZED, 2),
        "week1_utilization": round(WEEK1_UTILIZED / avg1, 6),
        "week1_unused": round(avg1 - WEEK1_UTILIZED, 2),
        # 第四周（实际 APR 校准，第五周预估的基础）
        "week4_window": [iso(WEEK4_START), iso(WEEK4_END)],
        "week4_snapshot_hours": len(w4),
        "week4_actual_apr": WEEK4_ACTUAL_APR,
        "week4_avg_deposit": round(avg4, 2),
        "week4_utilized": round(utilized4, 2),
        "week4_utilization": round(utilization4, 6),
        "week4_unused": round(avg4 - utilized4, 2),
    }


def apply_apr(entry, params):
    """按最新小时余额计算第五周三种预估 APR（%，第五周 250,000 池 + 第四周校准参数）"""
    total = entry["total"]
    pool = WEEK5_ANNUAL_REWARD_POOL
    apr_opt = pool / (total * params["week4_utilization"]) * 100
    utilized_pes = total - params["week4_unused"]
    apr_pes = pool / utilized_pes * 100 if utilized_pes > 0 else None
    entry["apr_optimistic"] = round(apr_opt, 4)
    entry["apr_pessimistic"] = round(apr_pes, 4) if apr_pes is not None else None
    entry["apr_mid"] = round((apr_opt + apr_pes) / 2, 4) if apr_pes is not None else None
    return entry


def load_data():
    if os.path.exists(DATA_PATH):
        with open(DATA_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {"updated": None, "params": None, "hours": []}


def save_data(hours_map):
    """hours_map: {ts: entry}。重算参数与 APR 后写回 data.json"""
    params = compute_params(hours_map)
    entries = [apply_apr(hours_map[ts], params)
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
