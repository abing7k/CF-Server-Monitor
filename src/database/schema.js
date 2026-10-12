import { getAllServers, getLatestMetricsCache, setLatestMetricsCache, getMetricsHistoryCache, setMetricsHistoryCache, getCacheDuration, clearAllCaches } from '../utils/cache.js';
import { saveSiteOptions, debug, getSettingByKey, normalizeLongHistoryPoints, DEFAULT_LONG_HISTORY_POINTS } from '../utils/settings.js';
import { attachDiskMetricsObject, flattenDiskMetrics, isDisabledProbeMetric, normalizeProbeMetricRow } from '../utils/metrics.js';
import { ensureServerOptimization, buildHistoryId, getServerHistoryInfo, getHistoryIdRange } from './indexOptimization.js';
import { addHistoryColumns, ensureHistoryIndex, isHistoryOptimized } from './updateDatabase.js';
import {
  buildSparseHistoryQuery,
  shouldUseSparseHistorySampling
} from './historySampling.js';
import {
  createHistoryTableSql,
  HISTORY_INSERT_COLUMNS
} from '../utils/historyFields.js';
import {
  DASHBOARD_LATENCY_WINDOW_CACHE_MAX_SERVERS,
  DASHBOARD_LATENCY_WINDOW_CACHE_TTL_MS,
  DASHBOARD_LATENCY_WINDOW_HOURS,
  DASHBOARD_LATENCY_WINDOW_POINTS,
  DASHBOARD_LATENCY_WINDOW_QUERY_CONCURRENCY
} from '../utils/config.js';

let dbInitialized = false;

const LOSS_AGG_COLUMNS = new Set(['loss_ct', 'loss_cu', 'loss_cm', 'loss_bd', 'loss_node_1', 'loss_node_2', 'loss_node_3', 'loss_node_4']);
const DEFAULT_HISTORY_MAX_POINTS = 160;
const LATENCY_NODE_FIELDS = ['ct', 'cu', 'cm', 'bd', 'node_1', 'node_2', 'node_3', 'node_4'];
const DASHBOARD_LATENCY_COLUMNS = LATENCY_NODE_FIELDS
  .flatMap(field => [`ping_${field}`, `loss_${field}`]);
const LEGACY_DASHBOARD_LATENCY_COLUMNS = ['ct', 'cu', 'cm', 'bd']
  .flatMap(field => [`ping_${field}`, `loss_${field}`]);
const dashboardLatencyHistoryCache = new Map();

function pruneDashboardLatencyHistoryCache(now = Date.now()) {
  for (const [serverId, entry] of dashboardLatencyHistoryCache) {
    if (!entry || now - entry.cachedAt > DASHBOARD_LATENCY_WINDOW_CACHE_TTL_MS) {
      dashboardLatencyHistoryCache.delete(serverId);
    }
  }

  while (dashboardLatencyHistoryCache.size > DASHBOARD_LATENCY_WINDOW_CACHE_MAX_SERVERS) {
    const oldestServerId = dashboardLatencyHistoryCache.keys().next().value;
    if (oldestServerId === undefined) break;
    dashboardLatencyHistoryCache.delete(oldestServerId);
  }
}

export function clearDashboardLatencyHistoryCache() {
  dashboardLatencyHistoryCache.clear();
}

export async function initDatabase(db) {
  if (dbInitialized) return;

  debug('初始化数据库');
  
  try {
    const SettingTableExists = await db.prepare(`
      SELECT name FROM sqlite_master WHERE type='table' AND name='settings'
    `).first();
    if (!SettingTableExists) {
      await db.prepare(`
        CREATE TABLE IF NOT EXISTS settings (
          key TEXT PRIMARY KEY, 
          value TEXT
        )
      `).run();
      await saveSiteOptions(db, { servers_optimized: 'true' });
      await saveSiteOptions(db, { history_id_optimized: 'true' });
    }

    // 判断servers表是否存在
    const ServerTableExists = await db.prepare(`
      SELECT name FROM sqlite_master WHERE type='table' AND name='servers'
    `).first();
    if (!ServerTableExists) {
      await db.prepare(`
        CREATE TABLE IF NOT EXISTS servers (
          id TEXT PRIMARY KEY,
          name TEXT,
          server_group TEXT DEFAULT 'Default',
          region TEXT DEFAULT '',
          tags TEXT DEFAULT '',
          note TEXT DEFAULT '',
          price TEXT DEFAULT '',
          billing_cycle TEXT DEFAULT 'month',
          auto_renewal TEXT DEFAULT '0',
          currency TEXT DEFAULT '¥',
          expire_date TEXT DEFAULT '',
          traffic_limit TEXT DEFAULT '',
          traffic_calc_type TEXT DEFAULT 'total',
          traffic_alert_percent INTEGER DEFAULT NULL,
          traffic_alert_state TEXT,
          "interface" TEXT DEFAULT '',
          reset_day INTEGER DEFAULT 1,
          collect_interval INTEGER DEFAULT 0,
          report_interval INTEGER DEFAULT 60,
          wss_report_interval INTEGER DEFAULT 2,
          connection_mode TEXT DEFAULT 'auto',
          ping_mode TEXT DEFAULT 'tcp',
          auto_update TEXT DEFAULT '0',
          custom_ct TEXT DEFAULT '',
          custom_cu TEXT DEFAULT '',
          custom_cm TEXT DEFAULT '',
          custom_bd TEXT DEFAULT '',
          node_1 TEXT DEFAULT '',
          node_2 TEXT DEFAULT '',
          node_3 TEXT DEFAULT '',
          node_4 TEXT DEFAULT '',
          rx_correction REAL DEFAULT NULL,
          tx_correction REAL DEFAULT NULL,
          offline_notify_disabled TEXT DEFAULT '0',
          is_hidden TEXT DEFAULT '0',
          sort_order INTEGER DEFAULT 0,
          traffic_rx_total REAL DEFAULT 0,
          traffic_tx_total REAL DEFAULT 0,
          traffic_last_rx REAL DEFAULT 0,
          traffic_last_tx REAL DEFAULT 0,
          history_partition_id INTEGER DEFAULT 0,
          timestamp INTEGER DEFAULT 0
        )
      `).run();
    } else {
      debug('检查servers表优化状态');
      await ensureServerOptimization(db);
    }

    // 判断metrics_history表是否存在
    const historyTableExists = await db.prepare(`
      SELECT name FROM sqlite_master WHERE type='table' AND name='metrics_history'
    `).first();
    if (!historyTableExists) {
      await db.prepare(createHistoryTableSql('metrics_history')).run();
    }else{
      await ensureHistoryIndex(db);
    }

    debug('✅ 数据库初始化完成');
    dbInitialized = true;
  } catch (e) {
    console.error('❌ 数据库初始化失败:', e);
  }
}

export async function clearHistory(db) {
  debug('开始清空历史数据...');
  
  try {
    await db.prepare(`DROP TABLE IF EXISTS metrics_history`).run();
    debug('✅ 已删除 metrics_history 表');

    await db.prepare(`DROP TABLE IF EXISTS metrics_history_old`).run();
    debug('✅ 已删除 metrics_history_old 表');
    
    dbInitialized = false;
    
    await initDatabase(db);

    await saveSiteOptions(db, { history_id_optimized: 'true' });

    await clearAllCaches(db);
    clearDashboardLatencyHistoryCache();
    
    debug('✅ 数据库重建完成');
    
    return {
      success: true,
      message: 'databaseRebuiltSuccess'
    };
  } catch (e) {
    console.error('❌ 数据库清理失败:', e);
    return {
      success: false,
      message: 'databaseRebuiltFailed',
      error: e.message
    };
  }
}

async function hasHistoryServerTimeIndex(db, tableName) {
  const index = await db.prepare(`
    SELECT name
    FROM sqlite_master
    WHERE type = 'index'
      AND tbl_name = ?
      AND sql IS NOT NULL
      AND LOWER(sql) LIKE '%server_id%'
      AND LOWER(sql) LIKE '%timestamp%'
    LIMIT 1
  `).bind(tableName).first();

  return !!index;
}

function buildHistorySourceQuery(tableName, useIdRange, columns) {
  if (useIdRange) {
    return `
      SELECT timestamp, ${columns} FROM ${tableName}
      WHERE id >= ?
        AND id <= ?
    `;
  }

  return `
    SELECT timestamp, ${columns} FROM ${tableName}
    WHERE server_id = ?
      AND typeof(timestamp) = 'integer'
      AND timestamp >= ?
  `;
}

export async function getMetricsHistory(
  db,
  serverId,
  hours,
  columns,
  server = null,
  longHistoryPoints = DEFAULT_LONG_HISTORY_POINTS
) {
  const now = Date.now();
  const cacheDuration = getCacheDuration(hours);
  const queryHours = Math.min(hours, 168);
  const configuredLongHistoryPoints = queryHours > 1
    ? Number(normalizeLongHistoryPoints(longHistoryPoints))
    : null;
  
  const cached = getMetricsHistoryCache(serverId, hours, columns, configuredLongHistoryPoints);
  if (cached && now - cached.timestamp < cacheDuration) {
    debug(`[History] CACHE HIT: ${serverId}, hours: ${hours}`);
    return cached.data;
  }
  
  const totalMs = queryHours * 60 * 60 * 1000;

  const cutoff = now - queryHours * 60 * 60 * 1000;
  const historyInfo = await getServerHistoryInfo(db, serverId, server);
  const queryStart = Math.max(cutoff, historyInfo.startTimestamp);

  // 判断是否需要查询 metrics_history_old 表
  // 如果实际查询起点早于本周日 00:00 UTC（表轮换时间），说明需要查旧表
  const nowDate = new Date(now);
  const day = nowDate.getUTCDay();
  const thisSunday = new Date(Date.UTC(nowDate.getUTCFullYear(), nowDate.getUTCMonth(), nowDate.getUTCDate() - day));
  const needOldTable = queryStart < thisSunday.getTime();
  
  const oldTableExists = needOldTable && !!await db.prepare(
    `SELECT name FROM sqlite_master WHERE type='table' AND name='metrics_history_old'`
  ).first();

  const history_id_optimized = await getSettingByKey(db, 'history_id_optimized', true);
  const currentHasServerTimeIndex = history_id_optimized
    ? false
    : await hasHistoryServerTimeIndex(db, 'metrics_history');
  const currentUsesIdRange = history_id_optimized || !currentHasServerTimeIndex;
  const oldUsesIdRange = oldTableExists
    ? history_id_optimized || !await hasHistoryServerTimeIndex(db, 'metrics_history_old')
    : false;
  const needsIdRange = currentUsesIdRange || oldUsesIdRange;

  let idRange = null;
  if (needsIdRange) {
    if (!historyInfo.partitionId) {
      throw new Error('Invalid history partition id');
    }

    idRange = getHistoryIdRange(historyInfo.partitionId, queryStart);
  }

  const columnList = columns.split(',').map(c => c.trim()).filter(c => c && c !== 'timestamp');
  const sourceColumns = columnList.join(', ');
  const lossColumns = columnList.filter(col => LOSS_AGG_COLUMNS.has(col));
  const lossWindowExpressions = lossColumns.map(col =>
    `MAX(${col}) OVER (PARTITION BY bucket) AS ${col}_bucket_max`
  );
  const selectColumns = columnList.map(col =>
    LOSS_AGG_COLUMNS.has(col) ? `${col}_bucket_max AS ${col}` : col
  );

  const useSparseSampling = shouldUseSparseHistorySampling(
    queryHours,
    currentUsesIdRange,
    oldTableExists,
    oldUsesIdRange
  );
  const sparseQueryEnd = Math.floor(now / 1000) * 1000 + 1000;
  const maxPoints = queryHours > 1
    ? configuredLongHistoryPoints
    : DEFAULT_HISTORY_MAX_POINTS;
  const samplingDurationMs = useSparseSampling
    ? Math.max(0, sparseQueryEnd - queryStart)
    : totalMs;
  const intervalMs = Math.max(10_000, Math.ceil(samplingDurationMs / maxPoints));

  debug(
    '[History]',
    'server:', serverId,
    'hours:', hours,
    'queryHours:', queryHours,
    'maxPoints:', maxPoints,
    'interval:', intervalMs,
    'cutoff:', new Date(cutoff).toISOString(),
    'start:', new Date(queryStart).toISOString()
  );

  let rawResult;

  if (useSparseSampling) {
    // Long-range queries sample one complete row per time bucket via primary-key seeks.
    // Computing an exact maximum for loss columns would require scanning every row in the bucket.
    const queryEnd = sparseQueryEnd;
    const firstRangeEnd = Math.min(
      queryEnd,
      queryStart + intervalMs
    );
    const idPrefix = getHistoryIdRange(historyInfo.partitionId).startId;
    const sparseQuery = buildSparseHistoryQuery({
      columns: sourceColumns,
      queryStart,
      queryEnd,
      firstRangeEnd,
      intervalMs,
      idPrefix,
      oldTableExists,
      tableBoundary: thisSunday.getTime()
    });

    debug('[History] SPARSE ID SAMPLING:', sparseQuery.bindValues.length, 'bind values');
    const sparseResult = await db.prepare(sparseQuery.sql).bind(...sparseQuery.bindValues).all();
    rawResult = {
      ...sparseResult,
      results: sparseResult.results
        .filter(row => row.sample_json)
        .map(row => JSON.parse(row.sample_json))
    };
  } else {
    const sourceQueries = [];
    const bindValues = [];

    sourceQueries.push(buildHistorySourceQuery('metrics_history', currentUsesIdRange, sourceColumns));
    if (currentUsesIdRange) {
      bindValues.push(idRange.startId, idRange.endId);
    } else {
      bindValues.push(serverId, queryStart);
    }

    if (oldTableExists) {
      debug('[History] 跨周查询，合并 metrics_history 和 metrics_history_old');
      sourceQueries.push(buildHistorySourceQuery('metrics_history_old', oldUsesIdRange, sourceColumns));
      if (oldUsesIdRange) {
        bindValues.push(idRange.startId, idRange.endId);
      } else {
        bindValues.push(serverId, queryStart);
      }
    }

    bindValues.push(intervalMs);

    rawResult = await db.prepare(`
      WITH history_rows AS (
        ${sourceQueries.join('\n        UNION ALL\n')}
      ),
      bucketed AS (
        SELECT
          timestamp,
          ${sourceColumns},
          CAST(timestamp / ? AS INTEGER) AS bucket
        FROM history_rows
      ),
      sampled AS (
        SELECT
          timestamp,
          ${sourceColumns},
          ROW_NUMBER() OVER (
            PARTITION BY bucket
            ORDER BY timestamp
          ) AS rn
          ${lossWindowExpressions.length ? `,\n          ${lossWindowExpressions.join(',\n          ')}` : ''}
        FROM bucketed
      )
      SELECT timestamp, ${selectColumns.join(', ')}
      FROM sampled
      WHERE rn = 1
    `).bind(...bindValues).all();
  }

  const result = rawResult.results.map(row => attachDiskMetricsObject(normalizeProbeMetricRow({
    ...row,
    timestamp: Number(row.timestamp)
  })));

  result.sort((a, b) => a.timestamp - b.timestamp);

  setMetricsHistoryCache(serverId, hours, columns, result, configuredLongHistoryPoints);

  debug(`[History] FINAL: ${result.length}, interval: ${intervalMs}ms`);

  return result;
}

function normalizeLatencyHistoryValue(value, metricType) {
  if (isDisabledProbeMetric(value)) return false;
  if (value === null || value === undefined || value === '') return null;

  const number = Number(value);
  if (!Number.isFinite(number)) return null;

  if (metricType === 'loss') {
    return Math.max(0, Math.min(100, Math.round(number)));
  }
  return number > 0 ? Math.round(number) : null;
}

function buildLatencyHistoryPoint(row, metricType, { includeEmpty = false } = {}) {
  const timestamp = Number(row?.timestamp);
  if (!Number.isFinite(timestamp) || timestamp <= 0) return null;

  const point = { ts: timestamp };
  for (const field of LATENCY_NODE_FIELDS) {
    const column = `${metricType}_${field}`;
    if (!Object.prototype.hasOwnProperty.call(row, column)) continue;
    const value = normalizeLatencyHistoryValue(row[column], metricType);
    if (value !== null) point[field] = value;
    else if (includeEmpty) point[field] = null;
  }

  return includeEmpty || Object.keys(point).length > 1 ? point : null;
}

function normalizeDashboardLatencyRows(rows, options = {}) {
  const ping = [];
  const loss = [];

  for (const row of rows || []) {
    const pingPoint = buildLatencyHistoryPoint(row, 'ping', options);
    if (pingPoint) ping.push(pingPoint);

    const lossPoint = buildLatencyHistoryPoint(row, 'loss', options);
    if (lossPoint) loss.push(lossPoint);
  }

  ping.sort((a, b) => a.ts - b.ts);
  loss.sort((a, b) => a.ts - b.ts);
  return { ping, loss };
}

function parseDashboardLatencySample(row) {
  if (!row?.sample_json) return null;
  try {
    return JSON.parse(row.sample_json);
  } catch (_) {
    return null;
  }
}

function normalizeDashboardLatencyWindow(sampleRows, { queryStart, intervalMs, points }) {
  const rows = Array.from({ length: points }, (_, index) => {
    const bucketTimestamp = queryStart + index * intervalMs;
    const sample = parseDashboardLatencySample(sampleRows?.[index]);
    if (!sample || typeof sample !== 'object') {
      return { timestamp: bucketTimestamp };
    }

    return {
      ...sample,
      sample_timestamp: Number(sample.timestamp),
      timestamp: bucketTimestamp
    };
  });

  return normalizeDashboardLatencyRows(rows, { includeEmpty: true });
}

export async function getDashboardLatencyHistory(db, servers, options = {}) {
  const serverList = Array.isArray(servers) ? servers : [];
  const serverIds = serverList.map(server => String(server?.id || '').trim()).filter(Boolean);
  if (serverIds.length === 0) return new Map();

  const now = Number.isFinite(Number(options.now)) ? Number(options.now) : Date.now();
  const useCache = options.cache !== false;
  const result = new Map();
  const serversToFetch = [];

  if (useCache) {
    pruneDashboardLatencyHistoryCache(now);
  }

  for (const server of serverList) {
    const serverId = String(server?.id || '').trim();
    if (!serverId) continue;

    const cached = dashboardLatencyHistoryCache.get(serverId);
    if (useCache && cached && now - cached.cachedAt < DASHBOARD_LATENCY_WINDOW_CACHE_TTL_MS) {
      result.set(serverId, cached.window);
      continue;
    }
    serversToFetch.push(server);
  }

  if (serversToFetch.length === 0) return result;

  const points = Number.isInteger(options.points) && options.points > 0
    ? options.points
    : DASHBOARD_LATENCY_WINDOW_POINTS;
  const queryEnd = Math.floor(now / 1000) * 1000 + 1000;
  const cutoff = now - DASHBOARD_LATENCY_WINDOW_HOURS * 60 * 60 * 1000;
  const columns = DASHBOARD_LATENCY_COLUMNS.join(', ');

  const nowDate = new Date(now);
  const day = nowDate.getUTCDay();
  const thisSunday = new Date(Date.UTC(nowDate.getUTCFullYear(), nowDate.getUTCMonth(), nowDate.getUTCDate() - day));
  const oldTableExists = cutoff < thisSunday.getTime() && !!await db.prepare(
    `SELECT name FROM sqlite_master WHERE type='table' AND name='metrics_history_old'`
  ).first();

  const fetchServerLatency = async server => {
    const serverId = String(server?.id || '').trim();
    if (!serverId) return;

    try {
      const historyInfo = await getServerHistoryInfo(db, serverId, server);
      if (!historyInfo.partitionId) {
        result.set(serverId, { ping: [], loss: [] });
        return;
      }

      const queryStart = cutoff;
      if (queryStart >= queryEnd) {
        result.set(serverId, { ping: [], loss: [] });
        return;
      }

      const intervalMs = Math.max(10_000, Math.ceil((queryEnd - queryStart) / points));
      const idPrefix = getHistoryIdRange(historyInfo.partitionId).startId;
      const querySparseHistory = async queryColumns => {
        const sparseQuery = buildSparseHistoryQuery({
          columns: queryColumns,
          queryStart,
          queryEnd,
          firstRangeEnd: Math.min(queryEnd, queryStart + intervalMs),
          intervalMs,
          idPrefix,
          oldTableExists,
          tableBoundary: thisSunday.getTime(),
          sampleOrder: 'DESC'
        });
        return db.prepare(sparseQuery.sql).bind(...sparseQuery.bindValues).all();
      };

      let rawResult;
      try {
        rawResult = await querySparseHistory(columns);
      } catch (error) {
        // Existing deployments may serve history before the new probe columns are migrated.
        if (!/no such column/i.test(String(error?.message || error))) throw error;
        rawResult = await querySparseHistory(LEGACY_DASHBOARD_LATENCY_COLUMNS.join(', '));
      }
      const window = normalizeDashboardLatencyWindow(rawResult.results, { queryStart, intervalMs, points });
      result.set(serverId, window);
      if (useCache) {
        dashboardLatencyHistoryCache.set(serverId, { cachedAt: now, window });
      }
    } catch (e) {
      debug('[DashboardLatency] query failed:', serverId, e?.message || e);
      const empty = { ping: [], loss: [] };
      result.set(serverId, empty);
      if (useCache) {
        dashboardLatencyHistoryCache.set(serverId, { cachedAt: now, window: empty });
      }
    }
  };

  for (let offset = 0; offset < serversToFetch.length; offset += DASHBOARD_LATENCY_WINDOW_QUERY_CONCURRENCY) {
    await Promise.all(
      serversToFetch
        .slice(offset, offset + DASHBOARD_LATENCY_WINDOW_QUERY_CONCURRENCY)
        .map(fetchServerLatency)
    );
  }

  return result;
}


export async function weeklyCleanup(db) {
  try {
    debug('[Cleanup] 开始执行表轮换操作...');
    
    // 判断metrics_history有无索引
    const index = await db.prepare(
      `SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='metrics_history'`
    ).first();
    if(!index){
      await saveSiteOptions(db, { history_id_optimized: 'true' });
      debug('✅ 切换到优化模式');
    }else{
      debug('✅ 继续兼容模式');
    }
    
    // 1. 删除旧的 metrics_history_old 表（如果存在）
    await db.prepare(`DROP TABLE IF EXISTS metrics_history_old`).run();
    debug('[Cleanup] 已删除旧的 metrics_history_old 表');
    
    // 2. 将 metrics_history 重命名为 metrics_history_old
    const currentTable = await db.prepare(
      `SELECT name FROM sqlite_master WHERE type='table' AND name='metrics_history'`
    ).first();
    
    if (currentTable) {
      await db.prepare(`ALTER TABLE metrics_history RENAME TO metrics_history_old`).run();
      debug('[Cleanup] 已将 metrics_history 重命名为 metrics_history_old');
    }
  
    // 3. 重新初始化数据库以创建新的 metrics_history 表
    dbInitialized = false;
    await initDatabase(db);

    debug('[Cleanup] 已创建新的 metrics_history 表');
    
    return {
      success: true,
      message: '表轮换成功'
    };
  } catch (e) {
    console.error('[Cleanup] 表轮换失败:', e);
    return { success: false, error: e.message };
  }
}

/*
 * 跨重启的累计流量。
 *
 * 探针只报告两个口径：网卡计数（net_rx/net_tx，重启后从 0 重新开始）与本月计数
 * （net_rx_monthly/net_tx_monthly，只有账单周期切换才会归零）。因此「累计」必须由
 * 服务端维护：按网卡计数增量累加，计数器回退（重启或网卡重建）时把该次增量记 0，
 * 已有累计值保持不变。
 *
 * 状态存在 servers 表，而不是 metrics_history：后者每周会被 weeklyCleanup 轮换清空，
 * 放进去每周都会从头再来。
 *
 * 性能上必须走主键：servers 表按 id 查是 O(1)，一次读取一行。
 * 千万不要改成去 metrics_history 里「查上一条记录」——那张表走 id 前缀优化、
 * 没有 (server_id, timestamp) 索引，这种查询会全表扫描，把 D1 每天 500 万行的
 * 读取额度打爆（2026-10-12 就是这么出事的）。
 *
 * 首次累计用 max(网卡计数, 本月计数) 作为下界：累计流量不可能小于本月流量，
 * 也不小于本次开机以来跑掉的量，升级后不会出现「累计 < 本月」的倒挂。
 */
const TRAFFIC_STATE_COLUMNS = {
  traffic_rx_total: 'REAL DEFAULT 0',
  traffic_tx_total: 'REAL DEFAULT 0',
  traffic_last_rx: 'REAL DEFAULT 0',
  traffic_last_tx: 'REAL DEFAULT 0'
};

// 每个 D1 绑定只检查一次（用 WeakMap 而不是单个变量，避免同一个 isolate 上多份 db 互相影响）
const trafficStateColumnsReady = new WeakMap();

// 老库补列：新建的库建表时已带这些列，历史库在首次用到时补上
function ensureTrafficStateColumns(db) {
  const cached = trafficStateColumnsReady.get(db);
  if (cached) return cached;

  const ready = (async () => {
    const { results: columns = [] } = await db.prepare('PRAGMA table_info(servers)').all();
    const existing = new Set(columns.map(c => c.name));
    for (const [name, def] of Object.entries(TRAFFIC_STATE_COLUMNS)) {
      if (existing.has(name)) continue;
      try {
        await db.prepare(`ALTER TABLE servers ADD COLUMN ${name} ${def}`).run();
        debug(`servers 表补列 ${name}`);
      } catch (e) {
        // 多个 isolate 同时补列时，后到的会撞上 duplicate column，忽略即可
        if (!/duplicate column/i.test(String(e?.message || e))) throw e;
      }
    }
  })().catch(e => {
    // 失败（例如 D1 读额度耗尽）时清掉缓存，下次上报再重试
    trafficStateColumnsReady.delete(db);
    throw e;
  });

  trafficStateColumnsReady.set(db, ready);
  return ready;
}

// 升级后的第一次上报：servers 表里还没有累计状态，尽量承接历史记录里的累计值，
// 这样「总流量」不会因为换了存储位置而从当月流量重新开始。
// 查询走 history_partition_id 拼出的主键区间，是有界的主键范围扫描，代价极低；
// 这里也不能写成按 timestamp 倒序的 server_id 过滤（那会退化成全表扫描）。
async function readHistoryCarry(db, historyPartitionId) {
  const empty = { rx: 0, tx: 0 };
  const partitionId = Number(historyPartitionId);
  if (!Number.isInteger(partitionId) || partitionId <= 0) return empty;

  try {
    const { startId, endId } = getHistoryIdRange(partitionId);
    const row = await db.prepare(`
      SELECT net_rx_total, net_tx_total
      FROM metrics_history
      WHERE id >= ?
        AND id <= ?
      ORDER BY id DESC
      LIMIT 1
    `).bind(startId, endId).first();

    if (!row) return empty;
    return {
      rx: parseFloat(row.net_rx_total) || 0,
      tx: parseFloat(row.net_tx_total) || 0
    };
  } catch (e) {
    debug('承接历史累计流量失败，改用下界值:', e?.message || e);
    return empty;
  }
}

async function resolveTrafficTotals(db, serverId, historyPartitionId, metrics) {
  const currentRx = parseFloat(metrics.net_rx) || 0;
  const currentTx = parseFloat(metrics.net_tx) || 0;
  const seedRx = Math.max(currentRx, parseFloat(metrics.net_rx_monthly) || 0);
  const seedTx = Math.max(currentTx, parseFloat(metrics.net_tx_monthly) || 0);

  let totalRx = seedRx;
  let totalTx = seedTx;

  try {
    await ensureTrafficStateColumns(db);

    const state = await db.prepare(
      'SELECT traffic_rx_total, traffic_tx_total, traffic_last_rx, traffic_last_tx FROM servers WHERE id = ?'
    ).bind(serverId).first();

    if (state) {
      const storedRx = parseFloat(state.traffic_rx_total) || 0;
      const storedTx = parseFloat(state.traffic_tx_total) || 0;
      const lastRx = parseFloat(state.traffic_last_rx) || 0;
      const lastTx = parseFloat(state.traffic_last_tx) || 0;

      let baseRx = storedRx > 0 ? storedRx : seedRx;
      let baseTx = storedTx > 0 ? storedTx : seedTx;

      // 还没有累计状态时先承接历史值（升级后第一次上报会走到这里）
      if (storedRx <= 0 || storedTx <= 0) {
        const carry = await readHistoryCarry(db, historyPartitionId);
        if (storedRx <= 0) baseRx = Math.max(baseRx, carry.rx);
        if (storedTx <= 0) baseTx = Math.max(baseTx, carry.tx);
      }

      // 只有存过累计值才按增量累加；计数器回退（重启）时该次增量记 0。
      const deltaRx = storedRx > 0 && currentRx >= lastRx ? currentRx - lastRx : 0;
      const deltaTx = storedTx > 0 && currentTx >= lastTx ? currentTx - lastTx : 0;

      totalRx = baseRx + deltaRx;
      totalTx = baseTx + deltaTx;
    }

    await db.prepare(
      'UPDATE servers SET traffic_rx_total = ?, traffic_tx_total = ?, traffic_last_rx = ?, traffic_last_tx = ? WHERE id = ?'
    ).bind(totalRx, totalTx, currentRx, currentTx, serverId).run();
  } catch (e) {
    console.warn('累计流量更新失败，沿用下界值:', e?.message || e);
  }

  return { net_rx_total: totalRx, net_tx_total: totalTx };
}

export async function saveMetricsHistory(db, serverId, historyPartitionId, metrics, regionCode = '', timestamp = null, agentVersion = '') {
  const historyId = buildHistoryId(historyPartitionId, timestamp);
  const rawTimestamp = Number(timestamp);
  const now = Number.isFinite(rawTimestamp) && rawTimestamp > 0
    ? (rawTimestamp < 10000000000 ? rawTimestamp * 1000 : rawTimestamp)
    : Date.now();

  const trafficTotals = await resolveTrafficTotals(db, serverId, historyPartitionId, metrics);

  const DISABLED_PROBE_VALUE = 'false';

  const parsePing = (val) => {
    if (isDisabledProbeMetric(val)) return DISABLED_PROBE_VALUE;
    const num = parseInt(val);
    return (num > 0) ? num : null;
  };

  const parseLoss = (val) => {
    if (isDisabledProbeMetric(val)) return DISABLED_PROBE_VALUE;
    const num = parseInt(val);
    if (Number.isNaN(num)) return null;
    return Math.max(0, Math.min(100, num));
  };

  const insertHistoryRow = async () => {
    const diskMetrics = flattenDiskMetrics(metrics);

    await db.prepare(`
    INSERT INTO metrics_history (
      ${HISTORY_INSERT_COLUMNS.join(', ')}
    ) VALUES (
      ${HISTORY_INSERT_COLUMNS.map(() => '?').join(', ')}
    )
  `).bind(
    historyId,
    serverId,
    now,
    agentVersion || '',
    parseFloat(metrics.cpu) || 0,
    metrics.load || metrics.load_avg || '0 0 0',
    parseFloat(metrics.net_in_speed) || 0,
    parseFloat(metrics.net_out_speed) || 0,
    parseFloat(metrics.net_rx) || 0,
    parseFloat(metrics.net_tx) || 0,
    parseInt(metrics.processes) || 0,
    parseInt(metrics.tcp_conn) || 0,
    parseInt(metrics.udp_conn) || 0,
    parsePing(metrics.ping_ct),
    parsePing(metrics.ping_cu),
    parsePing(metrics.ping_cm),
    parsePing(metrics.ping_bd),
    parsePing(metrics.ping_node_1),
    parsePing(metrics.ping_node_2),
    parsePing(metrics.ping_node_3),
    parsePing(metrics.ping_node_4),
    parseLoss(metrics.loss_ct),
    parseLoss(metrics.loss_cu),
    parseLoss(metrics.loss_cm),
    parseLoss(metrics.loss_bd),
    parseLoss(metrics.loss_node_1),
    parseLoss(metrics.loss_node_2),
    parseLoss(metrics.loss_node_3),
    parseLoss(metrics.loss_node_4),
    parseFloat(metrics.ram_total) || 0,
    parseFloat(metrics.ram_used) || 0,
    parseFloat(metrics.swap_total) || 0,
    parseFloat(metrics.swap_used) || 0,
    parseFloat(metrics.disk_total) || 0,
    parseFloat(metrics.disk_used) || 0,
    diskMetrics.disk_read_bps,
    diskMetrics.disk_write_bps,
    diskMetrics.disk_read_iops,
    diskMetrics.disk_write_iops,
    diskMetrics.disk_await_ms,
    diskMetrics.disk_util,
    parseInt(metrics.cpu_cores) || 0,
    metrics.cpu_info || '',
    Array.isArray(metrics.gpu_info) ? JSON.stringify(metrics.gpu_info) : (metrics.gpu_info || ''),
    metrics.arch || '',
    metrics.os || '',
    metrics.kernel_version || '',
    regionCode,
    metrics.ip_v4 || '0',
    metrics.ip_v6 || '0',
    metrics.boot_time || '',
    parseFloat(metrics.net_rx_monthly) || 0,
    parseFloat(metrics.net_tx_monthly) || 0,
    trafficTotals.net_rx_total,
    trafficTotals.net_tx_total
    ).run();
  };

  try {
    await insertHistoryRow();
  } catch (e) {
    if (e?.message && /has no column/i.test(e.message)) {
      console.warn('检测到数据库字段缺失，尝试添加缺失字段...');
      await addHistoryColumns(db);
      try {
        await insertHistoryRow();
      } catch (retryError) {
        console.error('保存历史数据失败:', retryError);
      }
      return;
    }
    console.error('保存历史数据失败:', e);
  }
}

export async function getLatestMetrics(db, serverId, server = null) {
  try {
    const historyInfo = await getServerHistoryInfo(db, serverId, server);
    if (!historyInfo.partitionId) {
      throw new Error('Invalid history partition id');
    }

    const useIdFilter = await isHistoryOptimized(db);

    const rangeStart = historyInfo.startTimestamp > 0 ? historyInfo.startTimestamp : null;
    const { startId, endId } = getHistoryIdRange(historyInfo.partitionId, rangeStart);
    debug(`Server ${serverId} history_id_range: ${startId} - ${endId}`);
  
    const result = useIdFilter ? await db.prepare(`
      SELECT * FROM metrics_history
      WHERE id >= ?
        AND id <= ?
      ORDER BY id DESC
      LIMIT 1
    `).bind(startId, endId).first()
    :await db.prepare(`
      SELECT * FROM metrics_history
      WHERE server_id = ?
      ORDER BY timestamp DESC
      LIMIT 1
    `).bind(serverId).first();
    return result ? normalizeProbeMetricRow(result) : null;
  } catch (e) {
    console.error('获取最新指标数据失败:', e);
    return null;
  }
}

export async function getLatestMetricsForAllServers(db) {
  const now = Date.now();
  const cacheInfo = getLatestMetricsCache();
  if (cacheInfo.cache && now - cacheInfo.time < cacheInfo.ttl) {
    return cacheInfo.cache;
  }

  // 确保 metrics_history 表有 idx_history_server_time 索引
  await ensureHistoryIndex(db);

  try {
    const servers = await getAllServers(db);

    const entries = await Promise.all(
      servers.map(s =>
        getLatestMetrics(db, s.id, s).then(metrics => [s.id, metrics])
      )
    );

    const result = new Map(entries.filter(([, m]) => m !== null));
    setLatestMetricsCache(result);
    return result;
  } catch (e) {
    console.error('获取所有服务器最新指标数据失败:', e);
    const cacheInfo = getLatestMetricsCache();
    return cacheInfo.cache || new Map();
  }
}
