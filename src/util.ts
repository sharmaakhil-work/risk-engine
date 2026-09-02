export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export function jitterBackoff(attempt: number, baseMs = 60, maxMs = 1_500): number {
  const exp = Math.min(baseMs * 2 ** attempt, maxMs);
  return exp / 2 + Math.random() * exp / 2;
}

export function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function hashSeed(input: string): number {
  let h = 2166136261;
  for (let i = 0; i < input.length; i++) {
    h ^= input.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}

let uidCounter = 0;

export function uid(prefix: string): string {
  uidCounter = (uidCounter + 1) % 1_000_000;
  return `${prefix}-${process.pid.toString(36)}${Date.now().toString(36)}${uidCounter.toString(36)}`;
}
