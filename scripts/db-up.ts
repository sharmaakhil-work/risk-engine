import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import { config } from '../src/config.js';

const url = new URL(config.databaseUrl);
const dbName = url.pathname.replace(/^\//, '') || 'risk_engine';
const host = url.hostname || 'localhost';
const port = url.port || '5432';
const user = decodeURIComponent(url.username || '');

if (!fs.existsSync('.env')) {
  fs.writeFileSync('.env', `DATABASE_URL="${config.databaseUrl}"\nREDIS_URL="${config.redisUrl}"\n`);
  console.log('created .env with local defaults (prisma CLI reads it)');
}

const candidates = [
  process.env.PSQL_BIN_DIR ? `${process.env.PSQL_BIN_DIR}/createdb` : '',
  'createdb',
  '/opt/homebrew/opt/postgresql@16/bin/createdb',
  '/usr/local/homebrew/opt/postgresql@16/bin/createdb',
].filter((c): c is string => Boolean(c));

let done = false;
for (const cmd of candidates) {
  try {
    execFileSync(cmd, ['-h', host, '-p', port, ...(user ? ['-U', user] : []), dbName], { stdio: 'pipe' });
    console.log(`created database ${dbName} (via ${cmd})`);
    done = true;
    break;
  } catch (err) {
    const stderr = String((err as { stderr?: Buffer | string }).stderr ?? '');
    if (stderr.includes('already exists')) {
      console.log(`database ${dbName} already exists`);
      done = true;
      break;
    }
  }
}

if (!done) {
  console.error('could not create the database — is postgresql@16 running? (brew services start postgresql@16)');
  process.exit(1);
}
