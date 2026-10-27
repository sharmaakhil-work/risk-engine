import { execFileSync, spawnSync } from 'node:child_process';
import { TEST_DATABASE_URL } from '../test/test-env.js';
import { redis } from '../src/redis.js';

const mode = process.argv[2] ?? 'setup';

const url = new URL(TEST_DATABASE_URL);
const dbName = url.pathname.replace(/^\//, '') || 'risk_engine_test';
const host = url.hostname || 'localhost';
const port = url.port || '5432';
const user = decodeURIComponent(url.username || '');

const dirs = [
  process.env.PSQL_BIN_DIR ?? '',
  '/opt/homebrew/opt/postgresql@16/bin',
  '/usr/local/homebrew/opt/postgresql@16/bin',
].filter(d => d !== '');

function runBestEffort(bin: 'createdb' | 'dropdb'): 'ok' | 'missing' | 'noop' {
  const candidates = [bin, ...dirs.map(d => `${d}/${bin}`)];
  let sawENOENT = 0;
  for (const cmd of candidates) {
    try {
      execFileSync(cmd, ['-h', host, '-p', port, ...(user ? ['-U', user] : []), dbName], { stdio: 'pipe' });
      return 'ok';
    } catch (err) {
      const e = err as NodeJS.ErrnoException & { stderr?: Buffer | string };
      if (e.code === 'ENOENT') {
        sawENOENT++;
        continue;
      }
      const stderr = String(e.stderr ?? '');
      if (stderr.includes('does not exist') || stderr.includes('already exists')) return 'noop';
      console.error(`[test-db] ${cmd} failed: ${stderr.trim()}`);
      return 'noop';
    }
  }
  return sawENOENT === candidates.length ? 'missing' : 'noop';
}

async function main(): Promise<void> {
  if (mode === 'teardown') {
    const dropped = runBestEffort('dropdb');
    console.log(
      `[test-db] teardown: ${
        dropped === 'ok' ? `dropped ${dbName}` : 'skipped drop (no local psql or pre-provisioned DB — fine for CI)'
      }`,
    );
    await redis.flushdb().catch(() => {});
    redis.disconnect();
    return;
  }

  // Clean slate every run: drop any leftover test database, create fresh,
  // migrate. In CI the database is pre-provisioned by the service container,
  // and no psql binaries exist — that is fine, migrate will verify it.
  runBestEffort('dropdb');
  const created = runBestEffort('createdb');
  if (created === 'missing') {
    console.log('[test-db] createdb unavailable — assuming pre-provisioned database (CI service container)');
  }

  const res = spawnSync('npx', ['prisma', 'migrate', 'deploy'], {
    env: { ...process.env, DATABASE_URL: TEST_DATABASE_URL },
    stdio: 'inherit',
  });
  if (res.status !== 0) {
    console.error('[test-db] prisma migrate deploy failed — is postgres running?');
    process.exit(1);
  }

  await redis.flushdb().catch(() => {});
  redis.disconnect();
  console.log(`[test-db] ready: ${dbName} migrated, test redis flushed`);
}

main().catch(err => {
  console.error('[test-db] fatal:', err);
  process.exit(1);
});
