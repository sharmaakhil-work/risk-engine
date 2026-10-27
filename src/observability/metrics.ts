// A deliberately small Prometheus text-format registry: counters and gauges
// with fixed label sets, no client library. /metrics renders it; the worker
// updates it. Every metric is declared up front with help text so scrapes are
// self-describing and typos fail loudly at runtime instead of silently
// creating orphan series.

type MetricType = 'counter' | 'gauge';

interface Metric {
  name: string;
  help: string;
  type: MetricType;
  labels: Record<string, string>;
  value: number;
}

const registry = new Map<string, Metric>();
const declared = new Map<string, { help: string; type: MetricType }>();

export function declareMetric(name: string, help: string, type: MetricType): void {
  if (declared.has(name)) throw new Error(`metric ${name} declared twice`);
  declared.set(name, { help, type });
}

function seriesKey(name: string, labels: Record<string, string>): string {
  const parts = Object.keys(labels)
    .sort()
    .map(k => `${k}="${String(labels[k]).replace(/"/g, '\\"')}"`);
  return parts.length === 0 ? name : `${name}{${parts.join(',')}}`;
}

export function incMetric(
  name: string,
  labelsOrDelta: Record<string, string> | number = {},
  delta = 1,
): void {
  const decl = declared.get(name);
  if (!decl) throw new Error(`undeclared metric ${name}`);
  if (decl.type !== 'counter') throw new Error(`${name} is a ${decl.type}, not a counter`);
  const labels = typeof labelsOrDelta === 'number' ? {} : labelsOrDelta;
  const d = typeof labelsOrDelta === 'number' ? labelsOrDelta : delta;
  const key = seriesKey(name, labels);
  let metric = registry.get(key);
  if (!metric) {
    metric = { name, help: decl.help, type: 'counter', labels, value: 0 };
    registry.set(key, metric);
  }
  metric.value += d;
}

export function setMetric(name: string, value: number, labels: Record<string, string> = {}): void {
  const decl = declared.get(name);
  if (!decl) throw new Error(`undeclared metric ${name}`);
  if (decl.type !== 'gauge') throw new Error(`${name} is a ${decl.type}, not a gauge`);
  const key = seriesKey(name, labels);
  let metric = registry.get(key);
  if (!metric) {
    metric = { name, help: decl.help, type: 'gauge', labels, value: 0 };
    registry.set(key, metric);
  }
  metric.value = value;
}

export function renderMetrics(extra: Record<string, number> = {}): string {
  const lines: string[] = [];
  const seenTypes = new Set<string>();
  const all: Metric[] = [
    ...registry.values(),
    ...Object.entries(extra).map(([name, value]) => {
      const decl = declared.get(name);
      return {
        name,
        help: decl?.help ?? '',
        type: (decl?.type ?? 'gauge') as MetricType,
        labels: {},
        value,
      };
    }),
  ];
  all.sort((a, b) => seriesKey(a.name, a.labels).localeCompare(seriesKey(b.name, b.labels)));
  for (const m of all) {
    if (!seenTypes.has(m.name)) {
      seenTypes.add(m.name);
      if (m.help) lines.push(`# HELP ${m.name} ${m.help}`);
      lines.push(`# TYPE ${m.name} ${m.type}`);
    }
    lines.push(`${seriesKey(m.name, m.labels)} ${m.value}`);
  }
  return `${lines.join('\n')}\n`;
}

export function declareStandardMetrics(): void {
  declareMetric('risk_engine_settles_total', 'Fill settlements by outcome', 'counter');
  declareMetric('risk_engine_settlement_errors_total', 'Settlement attempts that ended in a transient error', 'counter');
  declareMetric('risk_engine_orders_placed_total', 'Orders accepted into the two-phase lifecycle', 'counter');
  declareMetric('risk_engine_fills_published_total', 'Fills published by the exchange pump', 'counter');
  declareMetric('risk_engine_risk_evals_total', 'Risk evaluations executed', 'counter');
  declareMetric('risk_engine_risk_eval_duration_ms', 'Duration of the last risk evaluation', 'gauge');
  declareMetric('risk_engine_equity_usd', 'Portfolio equity at last evaluation', 'gauge');
  declareMetric('risk_engine_maintenance_required_usd', 'Maintenance requirement at last evaluation', 'gauge');
  declareMetric('risk_engine_margin_ratio', 'Equity / maintenance requirement at last evaluation', 'gauge');
  declareMetric('risk_engine_marks_stale', 'Symbols with stale marks at last evaluation', 'gauge');
  declareMetric('risk_engine_liquidation_rounds_total', 'Liquidation rounds submitted', 'counter');
  declareMetric('risk_engine_margin_warnings_total', 'Margin warning events published', 'counter');
  declareMetric('risk_engine_reclaimed_total', 'Stream messages reclaimed from dead consumers', 'counter');
  declareMetric('risk_engine_recovered_fills_total', 'Lost fills recovered by the CDR sweep', 'counter');
  declareMetric('risk_engine_stuck_orders_fixed_total', 'Stuck orders completed or cancelled by the sweep', 'counter');
  declareMetric('risk_engine_invariant_failures_total', 'Book invariant check failures', 'counter');
  declareMetric('risk_engine_snapshot_rows_written_total', 'Risk snapshots persisted', 'counter');
  declareMetric('risk_engine_process_uptime_seconds', 'Worker process uptime', 'gauge');
}

// Declared at module load so any import path that records a metric (worker,
// API, or a script invoking placeOrder) finds the registry fully declared —
// an undeclared metric fails loudly instead of silently no-oping.
declareStandardMetrics();
