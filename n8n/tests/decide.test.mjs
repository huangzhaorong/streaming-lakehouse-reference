// decide() 风控核心单测：与 consensus/main.py:424-533 逐条对照。
// 源 = n8n/workflows/decide.js（构建进 trading-decision.json 的 RiskDecide 节点，
// test_workflow_json.py 断言两者同步）。
// 运行: node --test n8n/tests/
import { readFileSync, writeFileSync, mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createRequire } from 'node:module';
import test from 'node:test';
import assert from 'node:assert';

const require = createRequire(import.meta.url);
const src = readFileSync(new URL('../workflows/decide.js', import.meta.url), 'utf8');
const dir = mkdtempSync(join(tmpdir(), 'decide-'));
const mod = join(dir, 'decide.cjs');
writeFileSync(mod, src + '\nmodule.exports={decide,updateEquity,normalizeSignal,DEFAULTS};');
const { decide, updateEquity, normalizeSignal, DEFAULTS } = require(mod);

const cycle = { now: 1_700_000_000, priceLag: 1, signalLag: 1 };
const sig = {
  similarity: 0.5, avg_outcome: 0.8, avg_max_outcome: 1.2,
  queried_at: new Date(1_700_000_000_000 - 5000).toISOString().replace('.000Z', 'Z'),
};
const px = { price: 100, ts: cycle.now - 1 };
const st0 = () => ({
  cash: 1000, positions: {}, killswitch: false, circuit_broken: false,
  day_baseline: { date: '2026-10-04', equity: 1000 }, trade_log: [],
});

test('BUY: 高相似度+Bullish+avg_outcome>0+无障碍', () => {
  const r = decide(DEFAULTS, cycle, st0(), 'BTC-USD', sig, px, 'Bullish');
  assert.equal(r.action, 'BUY');
  assert.ok(Math.abs(r.order.quantity - 0.5) < 1e-9);
  assert.equal(r.order.side, 'BUY');
  assert.equal(r.order.signal_sentiment, 'Bullish');
});

test('SELL 永远放行: killswitch 下 Bearish 仍平仓', () => {
  const st = st0(); st.killswitch = true;
  st.positions['BTC-USD'] = { quantity: 0.5, entry_price: 100 };
  const r = decide(DEFAULTS, cycle, st, 'BTC-USD', { ...sig, similarity: 0.05 }, { price: 110, ts: cycle.now - 1 }, 'Bearish');
  assert.equal(r.action, 'SELL');
  assert.ok(Math.abs(r.pnl - 5) < 1e-9);
});

test('SELL: 相似度跌破 EXIT 阈值', () => {
  const st = st0();
  st.positions['ETH-USD'] = { quantity: 1, entry_price: 90 };
  const r = decide(DEFAULTS, cycle, st, 'ETH-USD', { ...sig, similarity: 0.09 }, px, 'Neutral');
  assert.equal(r.action, 'SELL');
});

test('BLOCKED: killswitch 挡开仓并给出 reason', () => {
  const st = st0(); st.killswitch = true;
  const r = decide(DEFAULTS, cycle, st, 'BTC-USD', sig, px, 'Bullish');
  assert.equal(r.action, 'BLOCKED');
  assert.equal(r.reason, 'kill_switch');
  assert.equal(r.trade.status, 'BLOCKED');
});

test('BLOCKED: drawdown 熔断挡开仓', () => {
  const st = st0(); st.circuit_broken = true;
  assert.equal(decide(DEFAULTS, cycle, st, 'BTC-USD', sig, px, 'Bullish').reason, 'drawdown');
});

test('BLOCKED: stale_signal(>120s)', () => {
  const old = { ...sig, queried_at: new Date((cycle.now - 200) * 1000).toISOString().replace('.000Z', 'Z') };
  const r = decide(DEFAULTS, cycle, st0(), 'BTC-USD', old, px, 'Bullish');
  assert.equal(r.action, 'BLOCKED');
  assert.equal(r.reason, 'stale_signal');
});

test('BLOCKED: stale_price(>30s)', () => {
  const r = decide(DEFAULTS, cycle, st0(), 'BTC-USD', sig, { price: 100, ts: cycle.now - 40 }, 'Bullish');
  assert.equal(r.reason, 'stale_price');
});

test('BLOCKED: 心跳 priceLag>60 / signalLag>180', () => {
  assert.equal(decide(DEFAULTS, { ...cycle, priceLag: 61 }, st0(), 'BTC-USD', sig, px, 'Bullish').reason, 'no_heartbeat');
  assert.equal(decide(DEFAULTS, { ...cycle, signalLag: 181 }, st0(), 'BTC-USD', sig, px, 'Bullish').reason, 'no_signal_heartbeat');
});

test('BLOCKED: uneconomic(avg_max_outcome < 0.3*1.5=0.45)', () => {
  const r = decide(DEFAULTS, cycle, st0(), 'BTC-USD', { ...sig, avg_max_outcome: 0.3 }, px, 'Bullish');
  assert.equal(r.reason, 'uneconomic');
});

test('HOLD: Neutral 且无仓位 / avg_outcome<=0 不开仓', () => {
  assert.equal(decide(DEFAULTS, cycle, st0(), 'BTC-USD', sig, px, 'Neutral').action, 'HOLD');
  assert.equal(decide(DEFAULTS, cycle, st0(), 'BTC-USD', { ...sig, avg_outcome: 0 }, px, 'Bullish').action, 'HOLD');
});

test('HOLD: 现金不足不下单', () => {
  const st = st0(); st.cash = 10;
  assert.equal(decide(DEFAULTS, cycle, st, 'BTC-USD', sig, px, 'Bullish').action, 'HOLD');
});

test('HOLD: 信号缺失（lancer 不可达）', () => {
  assert.equal(decide(DEFAULTS, cycle, st0(), 'BTC-USD', null, px, 'Bullish').action, 'HOLD');
});

// --- C2 修复：真实 lancer 响应形状（{matches:[...]}）聚合，移植自 consensus _get_similarity ---
const rawLancer = (queriedAt) => ({
  pair: 'BTC-USD',
  matches: [
    { similarity: 0.5, outcome_pct: 0.8, outcome_max_pct: 1.2 },
    { similarity: 0.4, outcome_pct: 0.6, outcome_max_pct: 1.0 },
  ],
  queried_at: queriedAt,
});

test('normalizeSignal: lancer {matches} 形状 → 聚合（similarity=matches[0]，outcome 取均值）', () => {
  const s = normalizeSignal(rawLancer(sig.queried_at));
  assert.equal(s.similarity, 0.5);
  assert.ok(Math.abs(s.avg_outcome - 0.7) < 1e-9);
  assert.ok(Math.abs(s.avg_max_outcome - 1.1) < 1e-9);
  assert.equal(s.queried_at, sig.queried_at);
});

test('normalizeSignal: 空 matches / error 对象 / null → null（HOLD signal_unavailable）', () => {
  assert.equal(normalizeSignal({ matches: [], queried_at: 'x' }), null);
  assert.equal(normalizeSignal({ error: 'timeout' }), null);
  assert.equal(normalizeSignal(null), null);
});

test('normalizeSignal: 扁平形状原样通过（向后兼容）', () => {
  assert.equal(normalizeSignal(sig).similarity, 0.5);
});

test('decide 直接吃 lancer 原始响应（C2 端到端）', () => {
  const r = decide(DEFAULTS, cycle, st0(), 'BTC-USD', rawLancer(sig.queried_at), px, 'Bullish');
  assert.equal(r.action, 'BUY');
  assert.equal(r.order.signal_similarity, 0.5);
});

// --- C4 修复：订单 time 必须为 6 位小数（clearing-house TO_TIMESTAMP SSSSSS 严格匹配）---
test('order.time 格式 = yyyy-MM-ddTHH:mm:ss.SSSSSSZ（6 位微秒）', () => {
  const r = decide(DEFAULTS, cycle, st0(), 'BTC-USD', sig, px, 'Bullish');
  assert.match(r.order.time, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/);
});

// --- I2 修复：price<=0 时任何动作都不发生（consensus 早退语义）---
test('price<=0 → HOLD no_price（持仓也不强平，等价 consensus 早退）', () => {
  const st = st0();
  st.positions['BTC-USD'] = { quantity: 0.5, entry_price: 100 };
  const r = decide(DEFAULTS, cycle, st, 'BTC-USD', { ...sig, similarity: 0.05 }, { price: 0, ts: 0 }, 'Bearish');
  assert.equal(r.action, 'HOLD');
  assert.equal(r.reason, 'no_price');
});

test('updateEquity: 回撤>2% 熔断；UTC 日切重置', () => {
  const st = st0();
  updateEquity(st, { 'BTC-USD': 50 }, DEFAULTS, '2026-10-04');
  assert.equal(st.circuit_broken, false);
  st.cash = 900; // 10% 回撤
  updateEquity(st, {}, DEFAULTS, '2026-10-04');
  assert.equal(st.circuit_broken, true);
  updateEquity(st, {}, DEFAULTS, '2026-10-05');
  assert.equal(st.circuit_broken, false);
  assert.equal(st.day_baseline.date, '2026-10-05');
});

test('updateEquity: 持仓估值优先用现价，缺价回退 entry_price', () => {
  const st = st0(); st.cash = 500;
  st.positions['BTC-USD'] = { quantity: 2, entry_price: 100 };
  assert.equal(updateEquity(st, { 'BTC-USD': 150 }, DEFAULTS, '2026-10-04'), 800);
  assert.equal(updateEquity(st, {}, DEFAULTS, '2026-10-04'), 700);
});
