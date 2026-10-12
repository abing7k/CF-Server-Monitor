import assert from 'node:assert/strict';
import test from 'node:test';
import { saveMetricsHistory } from '../src/database/schema.js';
import { HISTORY_INSERT_COLUMNS } from '../src/utils/historyFields.js';

const SERVER_COLUMNS = [
  'id',
  'name',
  'traffic_rx_total',
  'traffic_tx_total',
  'traffic_last_rx',
  'traffic_last_tx'
];

const RX_TOTAL_INDEX = HISTORY_INSERT_COLUMNS.indexOf('net_rx_total');
const TX_TOTAL_INDEX = HISTORY_INSERT_COLUMNS.indexOf('net_tx_total');

// 用一个内存里的 servers 表模拟 D1：累计状态只存在 servers 行上
function makeDb() {
  const servers = new Map();
  const statements = [];

  return {
    servers,
    statements,
    prepare(sql) {
      const record = { sql, args: [] };
      return {
        bind(...args) {
          record.args = args;
          return this;
        },
        async first() {
          statements.push(record);
          if (/SELECT traffic_rx_total/.test(sql)) {
            const row = servers.get(record.args[0]);
            return row ? { ...row } : null;
          }
          return null;
        },
        async all() {
          statements.push(record);
          if (/PRAGMA table_info\(servers\)/.test(sql)) {
            return { results: SERVER_COLUMNS.map(name => ({ name })) };
          }
          return { results: [] };
        },
        async run() {
          statements.push(record);
          if (/UPDATE servers SET traffic_rx_total/.test(sql)) {
            const [rxTotal, txTotal, lastRx, lastTx, id] = record.args;
            servers.set(id, {
              traffic_rx_total: rxTotal,
              traffic_tx_total: txTotal,
              traffic_last_rx: lastRx,
              traffic_last_tx: lastTx
            });
          }
          return { success: true, meta: { changes: 1 } };
        }
      };
    }
  };
}

function totalsFromLastInsert(db) {
  const insert = [...db.statements]
    .reverse()
    .find(s => /INSERT INTO metrics_history/.test(s.sql));
  assert.ok(insert, '应该有 metrics_history 写入');
  return {
    rx: insert.args[RX_TOTAL_INDEX],
    tx: insert.args[TX_TOTAL_INDEX]
  };
}

async function report(db, serverId, metrics) {
  await saveMetricsHistory(db, serverId, 1, metrics, 'US', 1735689600000, 'test');
  return totalsFromLastInsert(db);
}

test('累计流量按网卡计数增量累加，重启回退时不倒退', async () => {
  const db = makeDb();
  const serverId = 'srv-1';

  // 首次上报：用 max(网卡计数, 本月计数) 作为下界
  const first = await report(db, serverId, {
    net_rx: 1000,
    net_tx: 2000,
    net_rx_monthly: 800,
    net_tx_monthly: 1500
  });
  assert.deepEqual(first, { rx: 1000, tx: 2000 });

  const second = await report(db, serverId, {
    net_rx: 1500,
    net_tx: 2600,
    net_rx_monthly: 1300,
    net_tx_monthly: 2100
  });
  assert.deepEqual(second, { rx: 1500, tx: 2600 });

  // 机器重启：网卡计数器归零，这次增量记 0，累计值保持
  const afterRestart = await report(db, serverId, {
    net_rx: 200,
    net_tx: 100,
    net_rx_monthly: 1400,
    net_tx_monthly: 2200
  });
  assert.deepEqual(afterRestart, { rx: 1500, tx: 2600 });

  // 重启后继续累计
  const resumed = await report(db, serverId, {
    net_rx: 500,
    net_tx: 400,
    net_rx_monthly: 1700,
    net_tx_monthly: 2500
  });
  assert.deepEqual(resumed, { rx: 1800, tx: 2900 });
});

test('累计流量只读写 servers 主键，不回查 metrics_history', async () => {
  const db = makeDb();

  await report(db, 'srv-2', { net_rx: 10, net_tx: 20 });

  const stateReads = db.statements.filter(s => /SELECT traffic_rx_total/.test(s.sql));
  assert.equal(stateReads.length, 1);
  assert.ok(/WHERE id = \?/.test(stateReads[0].sql), '累计状态必须按主键读取');

  const scans = db.statements.filter(s =>
    /FROM metrics_history/.test(s.sql) && /ORDER BY timestamp/.test(s.sql)
  );
  assert.equal(
    scans.length,
    0,
    'metrics_history 没有 (server_id, timestamp) 索引，按时间倒序查会全表扫描并打爆 D1 读额度'
  );
});
