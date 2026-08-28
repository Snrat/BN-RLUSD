"""RLUSD 余额链上查询公共模块。

数据源（2026-07-25 实测：归档查询正确 + 40 连发压测无限制）：
- ETH: mevblocker.io（主，73ms）、blastapi（备，130ms）
  注：tenderly / onfinality / merkle / drpc 压测均快速 429 或无归档数据，已弃用
- XRP: s1/s2.ripple.com（全历史 Clio 服务器），支持 account_lines 历史账本
"""

import json
import time
import urllib.request

ETH_RPCS = [
    "https://rpc.mevblocker.io",
    "https://eth-mainnet.public.blastapi.io",
    "https://eth.drpc.org",  # 备用归档节点（压测会限流，仅作兜底）
]
XRP_RPCS = ["https://s1.ripple.com:51234/", "https://s2.ripple.com:51234/"]

# 部分端点的 WAF 会拒绝 python-urllib 默认 UA，需伪装浏览器
USER_AGENT = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

RLUSD_ETH_CONTRACT = "0x8292bb45bf1ee4d140127049757c2e0ff06317ed"
ETH_ADDRESSES = [
    "0x28c6c06298d514db089934071355e5743bf21d60",
    "0xdfd5293d8e347dfe59e90efd55b2956a1343963d",
    "0x21a31ee1afc51d94c2efccaa2092ad1028285549",
    "0x4ed6cf63bd9c009d247ee51224fc1c7041f517f1",
    "0x98adef6f2ac8572ec48965509d69a8dd5e8bba9d",
    "0xf977814e90da44bfa03b6295a0616a897441acec",  # Binance 8，8/15 起持有 RLUSD
    "0x5a52e96bacdabb82fd05763e25335261b270efcb",  # 8/28 起持有 RLUSD
]
XRP_ADDRESSES = [
    "rDAE53VfMvftPB4ogpWGWvzkQxfht6JPxr",
    "rQUp2PKzH3vCtKs5H9tsPPE1rTsN6fhjqn",
]
XRP_ISSUER = "rMxCKbEDwqr76QuheSUMdEGf4B9xJ8m5De"

# Bybit 热钱包（2026-08 公示）
BYBIT_ETH_ADDRESSES = [
    "0x62425cD6BDcB6bFE51558EA465B063486B70dc9f",
    "0xf440139a62b2B939699C5b3e09F88E40464Ab9bc",
]
BYBIT_XRP_ADDRESSES = [
    "rDgBDkeTe5rbtRn2DJP8kHvQTuw28h8UVr",
    "rJn2zAPdFA193sixJwuFixRkYDUtx3apQh",
]

RIPPLE_EPOCH_OFFSET = 946684800  # XRPL 时间 = unix 时间 - 此偏移

BALANCE_OF_SELECTOR = "0x70a08231"


def _post(url, payload, timeout=20):
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", "User-Agent": USER_AGENT},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


import itertools

_rotate = itertools.count()  # 轮询起点，把流量分摊到各节点，避免触发限流


def _check_rpc_error(resp):
    """JSON-RPC 层错误（限流/参数错误等，HTTP 仍是 200）也视为失败以便切换节点"""
    items = resp if isinstance(resp, list) else [resp]
    for it in items:
        if isinstance(it, dict) and it.get("error"):
            raise RuntimeError(f"RPC error: {it['error']}")


def _call_with_failover(endpoints, payload, attempts=10):
    last_err = None
    start = next(_rotate)
    for attempt in range(attempts):
        url = endpoints[(start + attempt) % len(endpoints)]
        try:
            resp = _post(url, payload)
            _check_rpc_error(resp)
            return resp
        except Exception as e:  # noqa: BLE001 - 失败即切换节点重试
            last_err = e
            time.sleep(min(1.0 * (attempt + 1), 8))
    raise RuntimeError(f"所有 RPC 均失败: {last_err}")


# ---------------- ETH ----------------

def eth_rpc(payload):
    return _call_with_failover(ETH_RPCS, payload)


def eth_latest_block():
    """返回 (区块号, 时间戳unix)"""
    r = eth_rpc({"jsonrpc": "2.0", "id": 1, "method": "eth_getBlockByNumber",
                 "params": ["latest", False]})
    b = r["result"]
    return int(b["number"], 16), int(b["timestamp"], 16)


def eth_block_timestamp(number):
    r = eth_rpc({"jsonrpc": "2.0", "id": 1, "method": "eth_getBlockByNumber",
                 "params": [hex(number), False]})
    return int(r["result"]["timestamp"], 16)


def eth_find_block_at_or_before(ts):
    """二分查找时间戳 <= ts 的最大区块号，返回 (区块号, 实际时间戳)"""
    hi, hi_ts = eth_latest_block()
    if hi_ts <= ts:
        return hi, hi_ts
    lo = 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if eth_block_timestamp(mid) <= ts:
            lo = mid
        else:
            hi = mid - 1
    return lo, eth_block_timestamp(lo)


def eth_rlusd_balances(block_number, addresses=None):
    """返回指定区块各地址的 RLUSD 余额列表（优先 JSON-RPC 批量请求）"""
    addrs = addresses if addresses is not None else ETH_ADDRESSES
    block_tag = hex(block_number)
    calls = [
        {
            "jsonrpc": "2.0", "id": i, "method": "eth_call",
            "params": [
                {"to": RLUSD_ETH_CONTRACT,
                 "data": BALANCE_OF_SELECTOR + addr[2:].rjust(64, "0")},
                block_tag,
            ],
        }
        for i, addr in enumerate(addrs)
    ]
    try:
        results = eth_rpc(calls)
        if not isinstance(results, list):
            raise ValueError("节点不支持批量请求")
        results.sort(key=lambda r: r["id"])
        return [int(r["result"], 16) / 1e18 for r in results]
    except Exception:  # noqa: BLE001 - 批量失败则逐条查询
        out = []
        for c in calls:
            r = eth_rpc(c)
            out.append(int(r["result"], 16) / 1e18)
        return out


def eth_total_at(block_number, addresses=None):
    return sum(eth_rlusd_balances(block_number, addresses))


# ---------------- XRP ----------------

def xrp_rpc(method, params):
    return _call_with_failover(XRP_RPCS, {"method": method, "params": params})


def xrp_latest_ledger():
    """返回 (账本索引, close_time unix)"""
    r = xrp_rpc("ledger", [{"ledger_index": "validated", "transactions": False,
                            "expand": False}])
    led = r["result"]["ledger"]
    return int(led["ledger_index"]), int(led["close_time"]) + RIPPLE_EPOCH_OFFSET


def xrp_ledger_close_time(index):
    r = xrp_rpc("ledger", [{"ledger_index": index, "transactions": False,
                            "expand": False}])
    return int(r["result"]["ledger"]["close_time"]) + RIPPLE_EPOCH_OFFSET


def xrp_find_ledger_at_or_before(ts):
    """二分查找 close_time <= ts 的最大账本索引，返回 (索引, 实际close_time unix)"""
    hi, hi_ts = xrp_latest_ledger()
    if hi_ts <= ts:
        return hi, hi_ts
    lo = 32570  # 全历史起点
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if xrp_ledger_close_time(mid) <= ts:
            lo = mid
        else:
            hi = mid - 1
    return lo, xrp_ledger_close_time(lo)


def xrp_rlusd_balance(account, ledger_index):
    r = xrp_rpc("account_lines", [{
        "account": account,
        "peer": XRP_ISSUER,
        "ledger_index": ledger_index,
        "limit": 200,
    }])
    lines = r["result"].get("lines", [])
    return sum(float(l["balance"]) for l in lines)


def xrp_total_at(ledger_index, addresses=None):
    addrs = addresses if addresses is not None else XRP_ADDRESSES
    return sum(xrp_rlusd_balance(a, ledger_index) for a in addrs)
