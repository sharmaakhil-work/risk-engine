import { redis, STREAMS } from '../redis.js';
import { config } from '../config.js';

export type RiskEventKind =
  | 'order_placed'
  | 'margin_warning'
  | 'margin_recovered'
  | 'liquidation'
  | 'price_stale'
  | 'price_fresh'
  | 'price_divergence'
  | 'bankruptcy'
  | 'recon_recovered'
  | 'order_lost_recovered'
  | 'scenario_set'
  | 'rate_limited';

// Risk events are published to a Redis Stream rather than pub/sub: streams are
// durable, replayable and capped (MAXLEN ~), so a consumer that was down can
// still read what it missed and the API can serve recent history.
export async function publishEvent(kind: RiskEventKind, payload: Record<string, unknown>): Promise<void> {
  await redis.xadd(
    STREAMS.riskEvents,
    'MAXLEN',
    '~',
    String(config.streamMaxLen),
    '*',
    'kind',
    kind,
    'ts',
    String(Date.now()),
    'payload',
    JSON.stringify(payload),
  );
}

export interface RiskEvent {
  id: string;
  kind: string;
  ts: number;
  payload: Record<string, unknown>;
}

export function parseEvent(id: string, fields: string[]): RiskEvent | null {
  const map = new Map<string, string>();
  for (let i = 0; i + 1 < fields.length; i += 2) map.set(fields[i]!, fields[i + 1]!);
  const kind = map.get('kind');
  if (!kind) return null;
  let payload: Record<string, unknown> = {};
  try {
    payload = JSON.parse(map.get('payload') ?? '{}') as Record<string, unknown>;
  } catch {
    payload = {};
  }
  return { id, kind, ts: Number(map.get('ts') ?? 0), payload };
}

export async function recentEvents(limit = 50): Promise<RiskEvent[]> {
  const res = (await redis.xrevrange(STREAMS.riskEvents, '+', '-', 'COUNT', String(limit))) as [string, string[]][];
  return res.map(([id, fields]) => parseEvent(id, fields)).filter((e): e is RiskEvent => e !== null);
}
