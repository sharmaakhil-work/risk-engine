import WebSocket from 'ws';
import { Decimal } from '../precision/decimal.js';
import type { FeedHandle, FeedOptions } from './binance-ws.js';

// Real public market data from Coinbase's ticker channel (no credentials, no
// trading). Same failure policy as the Binance feed: silent degradation with
// the price store's clearly-tagged synthetic fallback.
export function startCoinbaseFeed({ symbols, onTick }: FeedOptions): FeedHandle {
  let ws: WebSocket | null = null;
  let stopped = false;
  let connected = false;
  let attempt = 0;
  let lastTickAt: number | null = null;

  const connect = () => {
    if (stopped) return;
    ws = new WebSocket('wss://ws-feed.exchange.coinbase.com');
    ws.on('open', () => {
      connected = true;
      attempt = 0;
      ws?.send(
        JSON.stringify({
          type: 'subscribe',
          product_ids: symbols.map(s => `${s}-USD`),
          channels: ['ticker'],
        }),
      );
    });
    ws.on('message', raw => {
      try {
        const msg = JSON.parse(String(raw)) as {
          type?: string;
          product_id?: string;
          price?: string;
          last_size?: string;
          time?: string;
        };
        if (msg.type !== 'ticker' || !msg.product_id || !msg.price) return;
        onTick({
          venue: 'coinbase',
          symbol: msg.product_id.replace(/-USD$/, ''),
          price: new Decimal(msg.price),
          size: msg.last_size ? new Decimal(msg.last_size) : null,
          ts: msg.time ? Date.parse(msg.time) : Date.now(),
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
    venue: 'coinbase',
    stop: () => {
      stopped = true;
      ws?.terminate();
    },
    status: () => ({ venue: 'coinbase', connected, lastTickAt }),
  };
}
