import { createHash, timingSafeEqual } from 'node:crypto';
import type { FastifyInstance } from 'fastify';
import { config } from '../config.js';
import { component } from '../observability/logger.js';

const log = component('api');

// Liveness and readiness stay open so orchestrators can probe them without
// credentials; everything else (including /metrics) requires the key.
const EXEMPT = new Set(['/health', '/ready']);

// Bearer-token gate, enabled by setting API_KEY. Both sides are hashed before
// the constant-time compare so token length never leaks through timing.
export function registerAuth(app: FastifyInstance): void {
  app.addHook('preHandler', async (request, reply) => {
    if (!config.apiKey) return;
    const route = request.routeOptions?.url ?? '';
    if (EXEMPT.has(route)) return;
    const header = request.headers.authorization ?? '';
    const token = header.startsWith('Bearer ') ? header.slice(7) : '';
    const given = createHash('sha256').update(token).digest();
    const expected = createHash('sha256').update(config.apiKey).digest();
    if (!timingSafeEqual(given, expected)) {
      await reply.code(401).send({ error: 'unauthorized' });
    }
  });
  app.addHook('onSend', async (_request, reply) => {
    reply.header('x-content-type-options', 'nosniff');
    reply.header('x-frame-options', 'DENY');
    reply.header('referrer-policy', 'no-referrer');
  });
  if (!config.apiKey) {
    log.warn('API_KEY not set — authentication disabled (fine locally, not for anything exposed)');
  }
}
