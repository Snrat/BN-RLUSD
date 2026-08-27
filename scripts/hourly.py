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
WEEK5_START = WEEK4_END  # 8/14 08:00 (UTC+8)
WEEK5_END = int(datetime(2026, 8, 21, 0, 0, 0, tzinfo=timezone.utc).timestamp())  # 8/21 08:00 (UTC+8)
# 已结算周的实际 APR（币安公布）。
# 第 1~4 周奖池为每周 200,000 USD 等值 XRP；第五周起改为每周 250,000 XRP（固定币数），
# 年化 APR 按结算时 XRP 价格折算 USD 生成。
# (周开始, 周结束, 实际 APR, 奖池数量, 奖池币种, 结算 XRP 价格, 异常标记)
SETTLED_WEEKS = [
    (WEEK1_START, WEEK1_END, 0.2225, 200_000, "USD", None, None),  # 第一次分发 2026-07-24
    (WEEK2_START, WEEK2_END, 0.0822, 200_000, "USD", None, None),  # 第二次分发 2026-07-31
    (WEEK3_START, WEEK3_END, 0.0808, 200_000, "USD", None, "夏日理财季活动"),  # 第三次分发 2026-08-07；活动推高未利用资金，趋势外推时剔除
    (WEEK4_START, WEEK4_END, 0.0769, 200_000, "USD", None, None),  # 第四次分发 2026-08-14
    (WEEK5_START, WEEK5_END, 0.0807, 250_000, "XRP", 1.2681, None),  # 第五次分发 2026-08-21，结算价 1.2681
]

NEXT_WEEK_REWARD_XRP = 250_000.0  # 第六周奖池未公布，暂按与第五周相同 25 万 XRP/周假设，USD 价值随 XRP 价格浮动

MAX_WORKERS = 3  # 并发压低 + 4 节点轮询，避免触发免费 RPC 限流

BYBIT_START = int(datetime(2026, 8, 19, 0, 0, 0, tzinfo=timezone.utc).timestamp())  # Bybit 统计起点 8/19 08:00 (UTC+8)
BYBIT_BASE_APR = 0.035  # 基础 APR 固定 3.5%
# 已知各 UTC+0 自然日的额外 APR（按当日 24h 最低持仓计算），新分发公布后在此补充
BYBIT_KNOWN_EXTRA_APR = {
    "2026-08-19": 0.09,    # 启动日，固定利率
    "2026-08-20": 0.0718,  # 奖池制：额外 APR = 日奖池 ÷ 当日最低持仓 × 365
    "2026-08-21": 0.0409,  # 合计 7.59% = 基础 3.5% + 额外 4.09%
    "2026-08-22": 0.0321,  # 合计 6.71% = 基础 3.5% + 额外 3.21%
    "2026-08-23": 0.0317,  # 合计 6.67% = 基础 3.5% + 额外 3.17%
    "2026-08-24": 0.0309,  # 合计 6.59% = 基础 3.5% + 额外 3.09%
    "2026-08-25": 0.0303,  # 合计 6.53% = 基础 3.5% + 额外 3.03%
    "2026-08-26": 0.0298,  # 合计 6.48% = 基础 3.5% + 额外 2.98%
}
BYBIT_FIXED_EXTRA_DAYS = {"2026-08-19"}  # 固定利率日不参与日奖池反推
BYBIT_DAILY_REWARD_XRP = 20_000.0  # 日奖池按 XRP 计：由已公布日反推约 2 万 XRP/天，USD 等值随币价波动
BINANCE_TICKER = "https://api.binance.com/api/v3/ticker/price?symbol=XRPUSDT"
BINANCE_KLINES = "https://api.binance.com/api/v3/klines?symbol=XRPUSDT&interval=1d"


def _fetch_json(url, timeout=15):
    import urllib.request
    req = urllib.request.Request(url, headers={"User-Agent": rpc.USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def _kraken_ohlc(interval_minutes, since_ts=0):
    """Kraken OHLC 备用源。返回 [time, open, high, low, close, ...] 列表"""
    url = f"https://api.kraken.com/0/public/OHLC?pair=XRPUSD&interval={interval_minutes}"
    if since_ts:
        url += f"&since={since_ts}"
    r = _fetch_json(url)
    if r.get("error"):
        raise RuntimeError(f"Kraken error: {r['error']}")
    res = r["result"]
    return res[next(k for k in res if k != "last")]


def xrp_daily_avg_price(day_str):
    """某 UTC 自然日 XRP/USD 的 (开+高+低+收)/4 均价（币安日线，失败回退 Kraken）"""
    day_start = int(datetime.strptime(day_str, "%Y-%m-%d")
                    .replace(tzinfo=timezone.utc).timestamp())
    try:
        klines = _fetch_json(f"{BINANCE_KLINES}&startTime={day_start * 1000}&limit=1")
        o, h, l, c = (float(klines[0][i]) for i in (1, 2, 3, 4))
        return (o + h + l + c) / 4
    except Exception:  # noqa: BLE001
        pass
    try:
        for k in _kraken_ohlc(1440, day_start - 86400):
            if int(k[0]) == day_start:
                return sum(float(x) for x in k[1:5]) / 4
    except Exception as e:  # noqa: BLE001
        print(f"  [警告] 获取 {day_str} XRP 日线失败: {e}")
    return None


def xrp_latest_price():
    """XRP/USD 最新价（币安 ticker，失败回退 Kraken）"""
    try:
        return float(_fetch_json(BINANCE_TICKER)["price"])
    except Exception:  # noqa: BLE001
        pass
    try:
        r = _fetch_json("https://api.kraken.com/0/public/Ticker?pair=XRPUSD")
        if not r.get("error"):
            res = r["result"]
            return float(res[next(iter(res))]["c"][0])
    except Exception as e:  # noqa: BLE001
        print(f"  [警告] 获取 XRP 最新价失败: {e}")
    return None


def xrp_hourly_prices(start_ts, end_ts):
    """批量抓取 [start_ts, end_ts] 的 XRP/USD 1h 收盘价（币安 K线，失败回退 Kraken），
    返回 {unix秒: 价格}，失败返回 {}"""
    try:
        prices = {}
        t = start_ts * 1000
        while t <= end_ts * 1000:
            klines = _fetch_json("https://api.binance.com/api/v3/klines"
                                 f"?symbol=XRPUSDT&interval=1h&startTime={t}&limit=1000")
            if not klines:
                break
            for k in klines:
                prices[int(k[0]) // 1000] = float(k[4])  # 开盘价时刻 -> 收盘价
            t = klines[-1][0] + 3600_000
            if len(klines) < 1000:
                break
        if prices:
            return prices
    except Exception:  # noqa: BLE001
        pass
    try:  # Kraken 备用：单次最多 720 根
        prices = {}
        t = start_ts
        while t <= end_ts:
            for k in _kraken_ohlc(60, t):
                if start_ts <= int(k[0]) <= end_ts:
                    prices[int(k[0])] = float(k[4])
            t += 720 * 3600
        return prices
    except Exception as e:  # noqa: BLE001
        print(f"  [警告] 批量获取 XRP 小时价格失败: {e}")
        return {}


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
    """抓取某一整点（unix 秒）的币安/Bybit ETH/XRP 余额合计，失败返回 None

    Bybit 地址仅自 BYBIT_START（2026-08-19 08:00 UTC）起统计，更早的小时不带该字段。
    """
    block = anchors.block_at(ts)
    ledger = anchors.ledger_at(ts)
    try:
        eth = rpc.eth_total_at(block)
        xrp = rpc.xrp_total_at(ledger)
        entry = {"t": iso(ts), "eth": round(eth, 2), "xrp": round(xrp, 2),
                 "total": round(eth + xrp, 2)}
        if ts >= BYBIT_START:
            bybit_eth = rpc.eth_total_at(block, rpc.BYBIT_ETH_ADDRESSES)
            bybit_xrp = rpc.xrp_total_at(ledger, rpc.BYBIT_XRP_ADDRESSES)
            entry["bybit_eth"] = round(bybit_eth, 2)
            entry["bybit_xrp"] = round(bybit_xrp, 2)
            entry["bybit_total"] = round(bybit_eth + bybit_xrp, 2)
    except Exception as e:  # noqa: BLE001
        print(f"  [失败] {iso(ts)}: {e}")
        return None
    return entry


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
    """已结算周按实际 APR 反推各周利用金额与差额，外推下一周。

    币安按 UTC+0 自然日（00:00~24:00）内用户最低持仓统计当日有效金额，
    故每周统计口径为：日内各整点快照取最小值得当日持仓，再对周内 7 天取平均。
    各周奖池：USD 周直接按美元计；XRP 周按日结算——日奖池 币数/7 每天按当日
    XRP 均价折 USD，全周奖池 = 币数 × 周日均价均值（小时价格存于 entry["xrp_usd"]，
    缺失时回退公布的结算价）。
    各周利用金额 = 当周年化奖池(USD) ÷ 该周实际 APR；
    差额 = 日均最低持仓 − 利用金额。
    第三周因夏日理财季活动异常（SETTLED_WEEKS 中标记），外推时剔除；
    且仅使用与下一周同奖池币种（XRP）的周做外推——XRP 池周目前只有第五周，
    故第六周差额直接锚定第五周实际值。
    """
    weeks = []
    for i, (start, end, apr, reward, currency, settle_price, anomaly) in enumerate(SETTLED_WEEKS):
        by_day = {}
        n_hours = 0
        day_prices = {}
        for ts in range(start, end, 3600):
            if ts in hours_map and hours_map[ts]:
                n_hours += 1
                by_day.setdefault(ts // 86400, []).append(hours_map[ts]["total"])
                if hours_map[ts].get("xrp_usd"):
                    day_prices.setdefault(ts // 86400, []).append(hours_map[ts]["xrp_usd"])
        daily_mins = [min(v) for v in by_day.values()]
        if len(daily_mins) < 7 or n_hours < 160:
            raise RuntimeError(f"{iso(start)} 周窗口数据不足（{len(daily_mins)} 天 / {n_hours} 小时），无法拟合")
        min_avg = sum(daily_mins) / len(daily_mins)
        if currency == "USD":
            pool_usd = float(reward)
            avg_price = None
        else:
            daily_avg = [sum(v) / len(v) for d, v in sorted(day_prices.items()) if v]
            avg_price = (sum(daily_avg) / len(daily_avg)) if len(daily_avg) >= 7 else settle_price
            pool_usd = reward * avg_price  # 逐日结算：全周 = 币数 × 日均价均值
        annual_pool = pool_usd * 365 / 7
        utilized = annual_pool / apr
        weeks.append({
            "window": [iso(start), iso(end)],
            "snapshot_hours": n_hours,
            "actual_apr": apr,
            "reward": reward,
            "reward_currency": currency,
            "settle_price": settle_price,
            "avg_price": round(avg_price, 4) if avg_price else None,
            "pool_usd": round(pool_usd, 2),
            "min_deposit_avg": round(min_avg, 2),
            "utilized": round(utilized, 2),
            "unused": round(min_avg - utilized, 2),
            "anomaly": anomaly,
        })

    # 差额外推：剔除异常周，且只取与下一周同奖池币种（XRP）的周。
    # 同币种周 ≥2 时按周序号最小二乘；只有 1 周（当前情形）时锚定该周实际值
    fit = [(i + 1, w["unused"]) for i, w in enumerate(weeks)
           if not w["anomaly"] and w["reward_currency"] == "XRP"]
    if len(fit) >= 2:
        xs = [x for x, _ in fit]
        vals = [u for _, u in fit]
        xm = sum(xs) / len(xs)
        um = sum(vals) / len(vals)
        sxx = sum((x - xm) ** 2 for x in xs)
        slope = sum((x - xm) * (u - um) for x, u in fit) / sxx
        unused_next = um + slope * (len(weeks) + 1 - xm)
        dof = max(len(fit) - 2, 1)
        sigma = math.sqrt(
            sum((u - (um + slope * (x - xm))) ** 2 for x, u in fit) / dof)
    else:
        slope = 0.0
        unused_next = fit[-1][1]
        sigma = None
    return {
        "next_week_reward_xrp": NEXT_WEEK_REWARD_XRP,
        "prediction_target": f"week{len(weeks) + 1}",
        "apr_display_start": iso(WEEK1_START),  # 曲线覆盖全部已结算周（实际 APR 回算）+ 当前周（预估）
        "unused_per_week": round(slope, 2),
        "unused_next_week": round(unused_next, 2),
        "unused_sigma": round(sigma, 2) if sigma is not None else None,
        "excluded_anomaly_weeks": [i + 1 for i, w in enumerate(weeks) if w["anomaly"]],
        "fit_weeks": weeks,
    }


def apply_apr(entry, params, day_min_total):
    """按所在 UTC+0 自然日的最低持仓计算拟合 APR（%）。

    币安按 UTC+0 00:00~24:00 内最低持仓统计当日计息基数，故同一自然日内
    所有小时点共用该日全天最低总存款（日内恒定）；当天尚未完结时，
    自然只能取当日迄今最低值，随新低出现而阶梯式更新。
    已结算周按当周年化奖池（XRP 周 = 币数 × 周日均价均值，逐日结算）与当周实际差额回算，
    曲线与实际 APR 精确吻合；当前未结算周逐日计算：每天按其当日 XRP 均价
    （当天取最新价），年化池 = NEXT_WEEK_REWARD_XRP × 当日价 × 365/7。
    当日最低持仓 ≤ 差额时模型失效，曲线留空。
    """
    entry.pop("apr_optimistic", None)  # 清理旧的三口径字段
    entry.pop("apr_pessimistic", None)
    entry.pop("apr_mid", None)
    t = entry["t"]
    week = next((w for w in params["fit_weeks"]
                 if w["window"][0] <= t < w["window"][1]), None)
    if week:
        pool = week["pool_usd"] * 365 / 7
        unused = week["unused"]
    else:
        price = params.get("_day_price", {}).get(t // 86400)
        if not price:
            entry["apr"] = None
            return entry
        pool = NEXT_WEEK_REWARD_XRP * price * 365 / 7
        unused = params["unused_next_week"]
    utilized = day_min_total - unused
    entry["apr"] = round(pool / utilized * 100, 4) if utilized > 0 else None
    return entry


def load_data():
    if os.path.exists(DATA_PATH):
        with open(DATA_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {"updated": None, "params": None, "hours": []}


def apply_bybit_apr(hours_map, params, price_now):
    """Bybit APR：基础 3.5% 固定 + 额外 APR（按 UTC+0 自然日最低持仓计算）。

    日奖池按 XRP 计（约 2 万 XRP/天）：由已公布额外 APR 的非固定日结合当日
    XRP 均价反推 XRP 池大小；预估日 额外 APR = XRP 池 × 当前 XRP 价格
    ÷ 当日最低持仓 × 365。已公布的自然日直接使用实际值，
    同一日内所有小时点共用该日全天最低持仓（日内恒定，当天未完结取迄今最低）。
    价格接口不可用时回退为 USD 口径（已公布日反推的 USD 奖池均值）。
    """
    day_min = {}
    for ts, entry in hours_map.items():
        if entry and entry.get("bybit_total") is not None:
            day = ts // 86400
            prev = day_min.get(day)
            day_min[day] = entry["bybit_total"] if prev is None else min(prev, entry["bybit_total"])
    pools_xrp, pools_usd = [], []
    for day_str, apr in BYBIT_KNOWN_EXTRA_APR.items():
        if day_str in BYBIT_FIXED_EXTRA_DAYS:
            continue
        day = int(datetime.strptime(day_str, "%Y-%m-%d")
                  .replace(tzinfo=timezone.utc).timestamp()) // 86400
        if day not in day_min:
            continue
        pools_usd.append(apr * day_min[day] / 365)
        price = xrp_daily_avg_price(day_str)
        if price:
            pools_xrp.append(apr * day_min[day] / 365 / price)
    pool_xrp = sum(pools_xrp) / len(pools_xrp) if pools_xrp else None
    pool_usd = sum(pools_usd) / len(pools_usd) if pools_usd else None
    params["bybit"] = {
        "base_apr": BYBIT_BASE_APR,
        "daily_reward_xrp": round(pool_xrp, 2) if pool_xrp else BYBIT_DAILY_REWARD_XRP,
        "daily_reward_pool": round(pool_xrp * price_now, 2) if pool_xrp and price_now
                             else (round(pool_usd, 2) if pool_usd else None),
        "xrp_price": round(price_now, 4) if price_now else None,
        "known_extra_apr": BYBIT_KNOWN_EXTRA_APR,
    }
    for ts, entry in hours_map.items():
        if not entry or entry.get("bybit_total") is None:
            continue
        day = ts // 86400
        extra = BYBIT_KNOWN_EXTRA_APR.get(entry["t"][:10])
        if extra is None:
            if pool_xrp and price_now:
                extra = pool_xrp * price_now / day_min[day] * 365
            elif pool_usd:
                extra = pool_usd / day_min[day] * 365
        entry["bybit_apr"] = (round((BYBIT_BASE_APR + extra) * 100, 4)
                              if extra is not None else None)


def save_data(hours_map):
    """hours_map: {ts: entry}。重算参数与 APR 后写回 data.json"""
    # 每个小时点附上 XRP/USD 价格（1h K线收盘价，批量抓取）——
    # XRP 奖池周按逐日价格结算，须先于 compute_params 挂好
    if hours_map:
        prices = xrp_hourly_prices(min(hours_map), max(hours_map))
        for ts, entry in hours_map.items():
            if entry and ts in prices:
                entry["xrp_usd"] = prices[ts]
    params = compute_params(hours_map)
    # 最新 XRP 价格：优先数据内最新小时价，缺失时回退实时 ticker。
    # 第六周奖池为 XRP 计且逐日结算，USD 年化池随价格浮动
    price_now = next((hours_map[ts]["xrp_usd"] for ts in sorted(hours_map, reverse=True)
                      if hours_map[ts] and hours_map[ts].get("xrp_usd")), None) \
        or xrp_latest_price()
    params["xrp_price_now"] = round(price_now, 4) if price_now else None
    params["annual_reward_pool"] = (round(NEXT_WEEK_REWARD_XRP * price_now * 365 / 7, 2)
                                    if price_now else None)
    # 未结算周逐日价格：已完结日取当日小时价均值，当天取最新价
    day_price = {}
    for ts, entry in hours_map.items():
        if entry and entry.get("xrp_usd"):
            day_price.setdefault(ts // 86400, []).append(entry["xrp_usd"])
    day_price = {d: sum(v) / len(v) for d, v in day_price.items()}
    if day_price and price_now:
        day_price[max(day_price)] = price_now
    params["_day_price"] = day_price
    apply_bybit_apr(hours_map, params, price_now)
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
    params.pop("_day_price", None)  # 内部临时结构，不入库
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
