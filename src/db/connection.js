import { PGlite } from '@electric-sql/pglite';
import pg from 'pg';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { config } from '../config/index.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/**
 * Enterprise Connection Pool Manager
 * Implements Lecture 17 Slide 5: "Guarding the Gates: Database Connection Pooling"
 * Maintains a queue of active and leased connections, enforcing a hard upper limit (max).
 */
class ConnectionPoolManager {
  constructor(options = {}) {
    this.max = options.max || 20;
    this.idleTimeoutMillis = options.idleTimeoutMillis || 30000;
    this.connectionTimeoutMillis = options.connectionTimeoutMillis || 5000;
    this.url = options.url || null;
    this.dataDir = options.dataDir || './data/pgdata';

    this.activeCount = 0;
    this.waitingQueue = [];
    this.totalAcquired = 0;
    this.totalReleased = 0;
    this.totalQueued = 0;

    this.pgliteInstance = null;
    this.pgPool = null;
    this.initialized = false;
  }

  async initialize() {
    if (this.initialized) return;

    if (this.url) {
      // Connect to external PostgreSQL (e.g. Supabase, Neon, AWS RDS, local PG)
      console.log(`[ConnectionPool] Initializing pg.Pool for external DB with max=${this.max}`);
      this.pgPool = new pg.Pool({
        connectionString: this.url,
        max: this.max,
        idleTimeoutMillis: this.idleTimeoutMillis,
        connectionTimeoutMillis: this.connectionTimeoutMillis
      });
      this.isExternal = true;
    } else {
      // Initialize Embedded Real PostgreSQL Engine via PGlite
      const absoluteDataDir = path.resolve(process.cwd(), this.dataDir);
      if (!fs.existsSync(absoluteDataDir)) {
        fs.mkdirSync(absoluteDataDir, { recursive: true });
      }
      console.log(`[ConnectionPool] Initializing Embedded PostgreSQL (PGlite) at ${absoluteDataDir} with pool capacity=${this.max}`);
      this.pgliteInstance = new PGlite(absoluteDataDir);
      this.isExternal = false;
    }

    // Execute schema migration
    await this.runMigrations();
    this.initialized = true;
  }

  async runMigrations() {
    const schemaPath = path.join(__dirname, 'schema.sql');
    const ddl = fs.readFileSync(schemaPath, 'utf-8');
    if (this.isExternal) {
      await this.pgPool.query(ddl);
    } else {
      await this.pgliteInstance.exec(ddl);
    }
    console.log('[ConnectionPool] Multi-tenant database schema migrations applied successfully.');
  }

  /**
   * Acquire a connection lease from the pool
   */
  async acquire() {
    if (this.activeCount < this.max) {
      this.activeCount++;
      this.totalAcquired++;
      return;
    }

    // Active limit reached -> queue the request (Queue Manager pattern)
    this.totalQueued++;
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        const index = this.waitingQueue.findIndex(item => item.resolve === resolve);
        if (index !== -1) {
          this.waitingQueue.splice(index, 1);
        }
        reject(new Error(`Connection timeout: pool exhausted (max=${this.max})`));
      }, this.connectionTimeoutMillis);

      this.waitingQueue.push({ resolve, timeout });
    });
  }

  /**
   * Release connection lease back to the pool
   */
  release() {
    this.totalReleased++;
    if (this.waitingQueue.length > 0) {
      const nextInLine = this.waitingQueue.shift();
      clearTimeout(nextInLine.timeout);
      nextInLine.resolve();
    } else {
      this.activeCount = Math.max(0, this.activeCount - 1);
    }
  }

  /**
   * Execute query with pool lease management and metrics
   */
  async query(text, params = []) {
    if (!this.initialized) {
      await this.initialize();
    }

    await this.acquire();
    const startTime = performance.now();

    try {
      let result;
      if (this.isExternal) {
        const res = await this.pgPool.query(text, params);
        result = { rows: res.rows, rowCount: res.rowCount };
      } else {
        const res = await this.pgliteInstance.query(text, params);
        result = { rows: res.rows, rowCount: res.rows.length };
      }
      return result;
    } finally {
      this.release();
    }
  }

  /**
   * Execute raw statements (e.g. DDL / scripts)
   */
  async exec(sql) {
    if (!this.initialized) {
      await this.initialize();
    }
    if (this.isExternal) {
      return await this.pgPool.query(sql);
    } else {
      return await this.pgliteInstance.exec(sql);
    }
  }

  /**
   * Returns live connection pool telemetry (Lecture 17 Slide 5)
   */
  getStats() {
    return {
      maxCapacity: this.max,
      activeConnections: this.activeCount,
      idleCapacity: Math.max(0, this.max - this.activeCount),
      waitingInQueue: this.waitingQueue.length,
      lifetimeAcquired: this.totalAcquired,
      lifetimeReleased: this.totalReleased,
      lifetimeQueued: this.totalQueued,
      engine: this.isExternal ? 'PostgreSQL (External pg.Pool)' : 'PostgreSQL (Embedded PGlite Engine)'
    };
  }

  async close() {
    if (this.pgPool) {
      await this.pgPool.end();
    }
    if (this.pgliteInstance) {
      await this.pgliteInstance.close();
    }
    this.initialized = false;
  }
}

export const db = new ConnectionPoolManager({
  max: config.db.pool.max,
  idleTimeoutMillis: config.db.pool.idleTimeoutMillis,
  connectionTimeoutMillis: config.db.pool.connectionTimeoutMillis,
  url: config.db.url,
  dataDir: config.db.dataDir
});
