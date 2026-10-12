import assert from 'node:assert/strict';
import test from 'node:test';
import { initDatabase, saveMetricsHistory } from '../src/database/schema.js';
import { HISTORY_INSERT_COLUMNS, createHistoryTableSql } from '../src/utils/historyFields.js';

let DatabaseSync = null;
try {
  ({ DatabaseSync } = await import('node:sqlite'));
} catch {
  DatabaseSync = null;
}

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
function makeDb({ historyRow = null } = {}) {
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
          if (/FROM metrics_history/.test(sql)) {
            return historyRow;
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

// 本地升级后补列的效果：servers 行存在，但累计状态还是默认 0
function seedUpgradedServer(db, serverId) {
  db.servers.set(serverId, {
    traffic_rx_total: 0,
    traffic_tx_total: 0,
    traffic_last_rx: 0,
    traffic_last_tx: 0
  });
}

test('累计流量按网卡计数增量累加，重启回退时不倒退', async () => {
  const db = makeDb();
  const serverId = 'srv-1';
  seedUpgradedServer(db, serverId);

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
  seedUpgradedServer(db, 'srv-2');

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

test('升级后第一次上报承接历史累计值，同时不重复累加本次开机流量', async () => {
  const db = makeDb({ historyRow: { net_rx_total: 5000, net_tx_total: 7000 } });
  const serverId = 'srv-3';
  seedUpgradedServer(db, serverId);

  // servers 表还没有累计状态：用历史累计值作为起点，本次上报不加增量
  const first = await report(db, serverId, {
    net_rx: 1000,
    net_tx: 2000,
    net_rx_monthly: 800,
    net_tx_monthly: 1500
  });
  assert.deepEqual(first, { rx: 5000, tx: 7000 });

  // 之后按网卡计数增量继续累加
  const second = await report(db, serverId, {
    net_rx: 1200,
    net_tx: 2100,
    net_rx_monthly: 950,
    net_tx_monthly: 1600
  });
  assert.deepEqual(second, { rx: 5200, tx: 7100 });

  const carryReads = db.statements.filter(s => /FROM metrics_history/.test(s.sql));
  assert.equal(carryReads.length, 1, '承接历史只在首次上报查一次');
  assert.ok(/WHERE id >= \?/.test(carryReads[0].sql), '承接历史必须走主键区间');
});

// 用内存 SQLite 跑一遍真实建表 + 上报流程，确认补列和累计状态落库
test('真实 SQLite 上建表、补列并累计流量', { skip: !DatabaseSync && 'node:sqlite 不可用' }, async () => {
  const sqlite = new DatabaseSync(':memory:');
  const db = makeSqliteDb(sqlite);

  await initDatabase(db);
  await db.prepare(
    "INSERT INTO servers (id, name, server_group, history_partition_id) VALUES ('srv-sqlite', 'Box', 'Default', 1)"
  ).run();

  await saveMetricsHistory(db, 'srv-sqlite', 1, {
    net_rx: 1000,
    net_tx: 2000,
    net_rx_monthly: 800,
    net_tx_monthly: 1500
  }, 'US', Date.UTC(2026, 0, 1, 0, 0, 0), 'test');

  const firstState = sqlite
    .prepare('SELECT traffic_rx_total, traffic_tx_total, traffic_last_rx, traffic_last_tx FROM servers WHERE id = ?')
    .get('srv-sqlite');
  assert.equal(firstState.traffic_rx_total, 1000);
  assert.equal(firstState.traffic_tx_total, 2000);
  assert.equal(firstState.traffic_last_rx, 1000);
  assert.equal(firstState.traffic_last_tx, 2000);

  await saveMetricsHistory(db, 'srv-sqlite', 1, {
    net_rx: 1300,
    net_tx: 2400,
    net_rx_monthly: 1100,
    net_tx_monthly: 1900
  }, 'US', Date.UTC(2026, 0, 1, 0, 1, 0), 'test');

  const lastRow = sqlite
    .prepare('SELECT net_rx_total, net_tx_total FROM metrics_history ORDER BY id DESC LIMIT 1')
    .get();
  assert.equal(lastRow.net_rx_total, 1300);
  assert.equal(lastRow.net_tx_total, 2400);

  const secondState = sqlite
    .prepare('SELECT traffic_rx_total, traffic_tx_total FROM servers WHERE id = ?')
    .get('srv-sqlite');
  assert.equal(secondState.traffic_rx_total, 1300);
  assert.equal(secondState.traffic_tx_total, 2400);

  sqlite.close();
});

test('老库没有累计流量列时自动补列', { skip: !DatabaseSync && 'node:sqlite 不可用' }, async () => {
  const sqlite = new DatabaseSync(':memory:');
  const db = makeSqliteDb(sqlite);

  // 模拟升级前的库：servers 表没有 traffic_* 列
  sqlite.exec("CREATE TABLE servers (id TEXT PRIMARY KEY, name TEXT, server_group TEXT DEFAULT 'Default')");
  sqlite.exec("CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT)");
  sqlite.exec(createHistoryTableSql('metrics_history'));
  sqlite.prepare("INSERT INTO servers (id, name) VALUES ('legacy', 'Old')").run();

  await saveMetricsHistory(db, 'legacy', 1, {
    net_rx: 10,
    net_tx: 20,
    net_rx_monthly: 5,
    net_tx_monthly: 8
  }, 'US', Date.UTC(2026, 0, 2, 0, 0, 0), 'test');

  const columns = sqlite.prepare('PRAGMA table_info(servers)').all().map(c => c.name);
  for (const name of ['traffic_rx_total', 'traffic_tx_total', 'traffic_last_rx', 'traffic_last_tx']) {
    assert.ok(columns.includes(name), `老库应自动补上 ${name}`);
  }

  const state = sqlite
    .prepare('SELECT traffic_rx_total, traffic_tx_total FROM servers WHERE id = ?')
    .get('legacy');
  assert.equal(state.traffic_rx_total, 10);
  assert.equal(state.traffic_tx_total, 20);

  sqlite.close();
});
// 把 node:sqlite 包装成 Worker 里用到的 D1 接口子集
function makeSqliteDb(sqlite) {
  return {
    prepare(sql) {
      const exec = (args, method) => {
        const stmt = sqlite.prepare(sql);
        if (method === 'first') return stmt.get(...args) ?? null;
        if (method === 'all') return { results: stmt.all(...args) };
        return stmt.run(...args);
      };
      return {
        bind: (...args) => ({
          first: async () => exec(args, 'first'),
          all: async () => exec(args, 'all'),
          run: async () => exec(args, 'run')
        }),
        first: async () => exec([], 'first'),
        all: async () => exec([], 'all'),
        run: async () => exec([], 'run')
      };
    }
  };
}
