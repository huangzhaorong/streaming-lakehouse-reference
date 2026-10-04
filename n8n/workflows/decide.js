// SLR 交易决策核心 — 纯函数 decide()/updateEquity()/DEFAULTS + n8n 编排包装器 main()。
// 风控逻辑与原 consensus/main.py:424-533 逐条对照移植（见 docs/plans/2026-10-04 计划 Task 5 映射表）。
//
// 单测: node --test n8n/tests/（decide.test.mjs 追加 module.exports 后 require 本文件——
//       因此本文件顶层不得引用 n8n 运行时变量，编排逻辑一律放 main() 内，
//       文件尾的 `if (typeof items !== 'undefined')` 守卫保证 CJS 环境安全加载）
// 同步: 修改本文件后必须运行 `python3 n8n/build_workflow.py` 重新生成 trading-decision.json
//       （test_workflow_json.py 断言 RiskDecide 节点 jsCode 与本文件逐字一致）

const ENV = (typeof $env !== 'undefined') ? $env : {};

const DEFAULTS = {
  ENTER_SIMILARITY: 0.20,       // CONSENSUS_ENTER_SIMILARITY
  EXIT_SIMILARITY: 0.10,        // CONSENSUS_EXIT_SIMILARITY
  TRADE_SIZE_USD: 50,           // CONSENSUS_TRADE_SIZE_USD
  MAX_SIGNAL_AGE: 120,          // CONSENSUS_MAX_SIGNAL_AGE（秒）
  MAX_PRICE_AGE: 30,            // CONSENSUS_MAX_PRICE_AGE（秒）
  HEARTBEAT_PRICE_LAG: 60,      // time()-max(timestamp(crypto_price)) 上限（秒）
  HEARTBEAT_SIGNAL_LAG: 180,    // time()-max(timestamp(lancer_queries_total)) 上限（秒）
  MAX_DAILY_DRAWDOWN: 0.02,     // CONSENSUS_MAX_DAILY_DRAWDOWN
  TAKER_FEE: 0.001,             // CONSENSUS_TAKER_FEE
  SLIPPAGE: 0.0005,             // CONSENSUS_SLIPPAGE
  MIN_VIABILITY_RATIO: 1.5,     // CONSENSUS_MIN_VIABILITY_RATIO
  STARTING_BALANCE: 1000,       // CONSENSUS_STARTING_BALANCE
  MODEL: ENV.SLR_LLM_MODEL || 'qwen-plus',
};

const round4 = (x) => Math.round(x * 10000) / 10000;
// 6 位微秒格式（.000000Z）：clearing-house.sql TO_TIMESTAMP 的 SSSSSS 严格 6 位，
// 3 位毫秒会解析为 NULL 静默腐蚀湖内 event_time
const isoNow = (nowSec) => new Date(nowSec * 1000).toISOString().replace(/\.(\d{3})Z$/, '.$1000Z');

/**
 * lancer /api/signals/{pair} 原始响应 → decide() 消费的扁平信号。
 * 聚合语义移植自 consensus _get_similarity：similarity=matches[0]，outcome/max 取均值。
 * 空 matches / error 对象 / null → null（decide 记 HOLD signal_unavailable；
 * 与 consensus 的"返回 0 值→触发强平"不同——信号缺失不强平，见台账 I4 裁决）。
 */
function normalizeSignal(raw) {
  if (!raw || typeof raw !== 'object' || raw.error) return null;
  if (typeof raw.similarity === 'number') return raw; // 已是扁平形状
  const matches = Array.isArray(raw.matches) ? raw.matches : null;
  if (!matches || matches.length === 0) return null;
  const outcomes = matches.filter((m) => 'outcome_pct' in m).map((m) => m.outcome_pct);
  const maxOutcomes = matches.filter((m) => 'outcome_max_pct' in m).map((m) => m.outcome_max_pct);
  return {
    similarity: matches[0].similarity ?? 0,
    avg_outcome: outcomes.length ? outcomes.reduce((a, b) => a + b, 0) / outcomes.length : 0,
    avg_max_outcome: maxOutcomes.length ? maxOutcomes.reduce((a, b) => a + b, 0) / maxOutcomes.length : 0,
    queried_at: raw.queried_at || '',
  };
}

/**
 * 单 pair 交易裁决（纯函数，不修改 st）。
 * @param cfg DEFAULTS；@param cycle {now(秒), priceLag, signalLag}
 * @param st 决策状态 {cash, positions, killswitch, circuit_broken}
 * @param sig lancer 信号 {similarity, avg_outcome, avg_max_outcome, queried_at} | null
 * @param priceInfo {price, ts(秒)}；@param sentiment 'Bullish'|'Bearish'|'Neutral'
 * @returns {action:'BUY'|'SELL'|'HOLD'|'BLOCKED', reason?, order?, trade?, pnl?}
 */
function decide(cfg, cycle, st, pair, rawSig, priceInfo, sentiment) {
  // price<=0 早退（consensus 语义）：无价格时任何开平仓都不做
  const price = priceInfo ? priceInfo.price : 0;
  if (!(price > 0)) {
    return { action: 'HOLD', reason: 'no_price', pair };
  }
  const sig = normalizeSignal(rawSig);
  if (!sig) {
    return { action: 'HOLD', reason: 'signal_unavailable', pair };
  }
  const now = cycle.now;
  const pos = (st.positions && st.positions[pair]) || { quantity: 0, entry_price: 0 };
  const holding = pos.quantity > 0;
  const sim = sig.similarity;

  // --- 开仓阻断检查（顺序与 consensus 一致；只挡 BUY，SELL 永远放行）---
  let entryBlocked = null;
  if (st.killswitch) {
    entryBlocked = 'kill_switch';
  } else if (st.circuit_broken) {
    entryBlocked = 'drawdown';
  } else if (sig.queried_at) {
    const ts = Date.parse(sig.queried_at) / 1000;
    if (!Number.isNaN(ts) && now - ts > cfg.MAX_SIGNAL_AGE) entryBlocked = 'stale_signal';
  }
  if (!entryBlocked && priceInfo && priceInfo.ts > 0 && now - priceInfo.ts > cfg.MAX_PRICE_AGE) {
    entryBlocked = 'stale_price';
  }
  if (!entryBlocked && cycle.priceLag > cfg.HEARTBEAT_PRICE_LAG) entryBlocked = 'no_heartbeat';
  if (!entryBlocked && cycle.signalLag > cfg.HEARTBEAT_SIGNAL_LAG) entryBlocked = 'no_signal_heartbeat';
  if (!entryBlocked && sig.avg_max_outcome > 0) {
    const feesPct = (cfg.TAKER_FEE * 2 + cfg.SLIPPAGE * 2) * 100; // = 0.3
    if (sig.avg_max_outcome < feesPct * cfg.MIN_VIABILITY_RATIO) entryBlocked = 'uneconomic'; // < 0.45
  }

  // --- 入场 ---
  if (!holding && !entryBlocked && sim >= cfg.ENTER_SIMILARITY && sentiment === 'Bullish' && sig.avg_outcome > 0) {
    if ((st.cash || 0) >= cfg.TRADE_SIZE_USD && price > 0) {
      const quantity = cfg.TRADE_SIZE_USD / price;
      return {
        action: 'BUY', pair,
        order: {
          pair, side: 'BUY', price, quantity,
          signal_similarity: round4(sim), signal_sentiment: sentiment, time: isoNow(now),
        },
      };
    }
    return { action: 'HOLD', reason: 'insufficient_cash', pair };
  }

  // --- 被阻断但信号达阈值：记 BLOCKED 审计 ---
  if (!holding && entryBlocked && sim >= cfg.ENTER_SIMILARITY && sentiment === 'Bullish') {
    return {
      action: 'BLOCKED', reason: entryBlocked, pair,
      trade: {
        pair, side: 'BUY', price, quantity: price > 0 ? cfg.TRADE_SIZE_USD / price : 0,
        signal_similarity: round4(sim), signal_sentiment: sentiment, time: isoNow(now),
        status: 'BLOCKED', reason: entryBlocked,
      },
    };
  }

  // --- 出场（永不受 entryBlocked 限制——保资本优先）---
  if (holding && (sim < cfg.EXIT_SIMILARITY || sentiment === 'Bearish')) {
    const quantity = pos.quantity;
    return {
      action: 'SELL', pair,
      order: {
        pair, side: 'SELL', price, quantity,
        signal_similarity: round4(sim), signal_sentiment: sentiment, time: isoNow(now),
      },
      pnl: (price - pos.entry_price) * quantity,
    };
  }

  return { action: 'HOLD', reason: entryBlocked || 'no_trigger', pair };
}

/**
 * 每周期一次的权益/熔断更新（就地修改 st）。持仓估值优先现价，缺价回退 entry_price
 *（consensus 原版只用 entry_price 近似；此处改进——n8n 每轮本就拉全量现价）。
 */
function updateEquity(st, prices, cfg, dateStr) {
  let equity = st.cash || 0;
  for (const [pair, pos] of Object.entries(st.positions || {})) {
    if (pos.quantity > 0) {
      equity += pos.quantity * (prices[pair] !== undefined ? prices[pair] : pos.entry_price);
    }
  }
  if (!st.day_baseline || st.day_baseline.date !== dateStr) {
    st.day_baseline = { date: dateStr, equity };
    st.circuit_broken = false; // UTC 日切重置熔断（与 consensus 一致）
  } else if (st.day_baseline.equity > 0 && !st.circuit_broken) {
    const dd = (equity - st.day_baseline.equity) / st.day_baseline.equity;
    if (dd < -cfg.MAX_DAILY_DRAWDOWN) st.circuit_broken = true;
  }
  st.equity = equity;
  return equity;
}

// ---------------------------------------------------------------------------
// n8n 编排包装器（仅在 n8n Code 节点运行时执行；测试环境经 typeof 守卫跳过）
// ---------------------------------------------------------------------------
function main() {
  const cfg = DEFAULTS;
  const st = $getWorkflowStaticData('global');
  if (st.cash === undefined) {
    st.cash = cfg.STARTING_BALANCE;
    st.positions = st.positions || {};
    st.trade_log = st.trade_log || [];
    // killswitch 可能已由 webhook 分支先置位——初始化不得清除（I7）
    st.killswitch = st.killswitch ?? false;
    st.circuit_broken = st.circuit_broken ?? false;
  }
  const now = Math.floor(Date.now() / 1000);
  const num = (v, d) => { const n = Number(v); return Number.isFinite(n) ? n : d; };
  const cycle = {
    now,
    priceLag: num($('PromPriceLag').first().json?.data?.result?.[0]?.value?.[1], 0),
    signalLag: num($('PromSignalLag').first().json?.data?.result?.[0]?.value?.[1], 0),
  };
  // 全 pair 现价（query_range 序列末点）
  const prices = {};
  for (const r of ($('PromPrices').first().json?.data?.result ?? [])) {
    const vals = (r.values && r.values.length) ? r.values[r.values.length - 1] : null;
    if (vals) prices[r.metric.pair] = { price: Number(vals[1]), ts: Number(vals[0]) };
  }
  // 每周期一次权益/熔断更新（cycle_ts 守卫，SplitInBatches 多 pair 只算一次）
  const cycleTs = $('Init').first().json.cycle_ts;
  if (st.equity_cycle !== cycleTs) {
    const px = {};
    for (const [k, v] of Object.entries(prices)) px[k] = v.price;
    updateEquity(st, px, cfg, new Date().toISOString().slice(0, 10));
    st.equity_cycle = cycleTs;
  }
  st.last_prices = st.last_prices || {};
  return items.map((item) => {
    const d = item.json; // BuildPrompt/ParseSentiment 输出: {pair, cycle_ts, signal, sentiment, narrative}
    const priceInfo = prices[d.pair] || { price: 0, ts: 0 };
    if (priceInfo.price > 0) st.last_prices[d.pair] = priceInfo.price;
    st.signals_checked = (st.signals_checked || 0) + 1; // slr_signals_checked 指标源
    const r = decide(cfg, cycle, st, d.pair, d.signal, priceInfo, d.sentiment);
    return { json: { ...r, narrative: d.narrative || '', price: priceInfo.price, cycle_ts: cycleTs, cycle_count: d.cycle_count } };
  });
}

if (typeof items !== 'undefined') {
  return main();
}
