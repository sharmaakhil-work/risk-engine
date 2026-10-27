import pino from 'pino';

// Structured JSON logs (one event per line) for every runtime component.
// CLI scripts (seed, simulate, db-up) intentionally keep plain console output
// — they are human-facing tools, not services.
export const log = pino({
  level: process.env.LOG_LEVEL ?? 'info',
  base: { service: 'risk-engine' },
});

export function component(name: string) {
  return log.child({ component: name });
}
