import WebSocket from 'ws';
import { Decimal } from '../precision/decimal.js';

export interface FeedTick {
  venue: string;
  symbol: string;
  price: Decimal;
  size: Decimal | null;
  ts: number;
}

export interface FeedStatusInfo {
  venue: string;
  connected: boolean;
  lastTickAt: number | null;
}

export interface FeedHandle {
  venue: string;
  stop(): void;
  status(): FeedStatusInfo;
}

export interface FeedOptions {
  symbols: readonly string[];
  onTick: (tick: FeedTick) => void;
}

// Real public market data (no account credentials, no trading). The feed is
// allowed to fail silently: the price store's synthetic fallback keeps the
// demo runnable when exchanges are unreachable, and marks carry their true
// source so nothing simulated is ever presented as a real print.
export function startBinanceFeed({ symbols, onTick }: FeedOptions): FeedHandle {
  const streams = symbols.map(s => `${s.toLowerCase()}usdt@trade`).join('/');
  const url = `wss://stream.binance.com:9443/stream?streams=${streams}`;

  let ws: WebSocket | null = null;
  let stopped = false;
  let connected = false;
  let attempt = 0;
  let lastTickAt: number | null = null;

  const connect = () => {
    if (stopped) return;
    ws = new WebSocket(url);
    ws.on('open', () => {
      connected = true;
      attempt = 0;
    });
    ws.on('message', raw => {
      try {
        const msg = JSON.parse(String(raw)) as {
          data?: { e?: string; E?: number; s?: string; p?: string; q?: string };
        };
        const d = msg.data;
        if (d?.e !== 'trade' || !d.s || !d.p) return;
        onTick({
          venue: 'binance',
          symbol: d.s.replace(/USDT$/, ''),
          price: new Decimal(d.p),
          size: d.q ? new Decimal(d.q) : null,
          ts: d.E ?? Date.now(),
        });
        lastTickAt = Date.now();
      } catch {
        // malformed frame
      }
    });
    ws.on('error', () => {
      connected = false;
      ws?.terminate();
    });
    ws.on('close', () => {
      connected = false;
      if (!stopped) {
        const delay = Math.min(30_000, 1_000 * 2 ** attempt++);
        setTimeout(connect, delay);
      }
    });
  };

  connect();
  return {
    venue: 'binance',
    stop: () => {
      stopped = true;
      ws?.terminate();
    },
    status: () => ({ venue: 'binance', connected, lastTickAt }),
  };
}
