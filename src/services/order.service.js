import { db } from '../db/connection.js';
import { assertTenantBoundary } from '../middleware/tenant.middleware.js';

/**
 * Order Service - Core B2B SaaS Business Logic
 * Implements Lecture 17: Database Performance Tuning & Optimization
 * - N+1 Query Bottleneck vs Optimized Single-Trip JOIN
 * - Composite Index Optimization verified via EXPLAIN ANALYZE
 * - Strict Tenant Boundary Scoping
 */
export class OrderService {
  /**
   * Un-Optimized Query Pattern (Classic N+1 Problem - Lecture 17 Slide 4)
   * Fetches 1 list of orders, then fires N individual queries in a loop for child items.
   * Total Queries = 1 + N (Severe network & connection pool exhaustion)
   */
  static async getOrdersUnoptimized(tenantId, limit = 50) {
    const startTime = performance.now();
    let queryCount = 0;

    // 1st Query: Fetch orders for tenant
    const ordersResult = await db.query(
      `SELECT id, tenant_id, customer_name, customer_email, total_amount, status, created_at
       FROM orders
       WHERE tenant_id = $1
       ORDER BY created_at DESC
       LIMIT $2`,
      [tenantId, limit]
    );
    queryCount++;

    const orders = ordersResult.rows;

    // N Queries: Loop over every order and execute an independent query
    for (const order of orders) {
      assertTenantBoundary({ tenantId }, order.tenant_id);

      const itemsResult = await db.query(
        `SELECT id, product_id, product_name, quantity, unit_price, subtotal
         FROM order_items
         WHERE order_id = $1 AND tenant_id = $2`,
        [order.id, tenantId]
      );
      queryCount++;
      order.items = itemsResult.rows;
    }

    const durationMs = performance.now() - startTime;

    return {
      orders,
      benchmark: {
        strategy: 'UNOPTIMIZED_N_PLUS_ONE',
        ordersFetched: orders.length,
        totalQueriesExecuted: queryCount, // 1 + N queries
        latencyMs: parseFloat(durationMs.toFixed(3)),
        description: `Executed 1 query for orders + ${orders.length} queries for items (${queryCount} total round-trips).`
      }
    };
  }

  /**
   * Optimized Query Pattern (Single Round-Trip with JOIN & JSON Aggregation)
   * Fetches orders and all child items in exactly 1 single SQL query.
   * Total Queries = 1 (Zero N+1 round-trips, minimal latency)
   */
  static async getOrdersOptimized(tenantId, limit = 50) {
    const startTime = performance.now();

    const result = await db.query(
      `SELECT 
         o.id, 
         o.tenant_id, 
         o.customer_name, 
         o.customer_email, 
         o.total_amount, 
         o.status, 
         o.created_at,
         COALESCE(
           json_agg(
             json_build_object(
               'id', oi.id,
               'product_id', oi.product_id,
               'product_name', oi.product_name,
               'quantity', oi.quantity,
               'unit_price', oi.unit_price,
               'subtotal', oi.subtotal
             )
           ) FILTER (WHERE oi.id IS NOT NULL), '[]'::json
         ) AS items
       FROM orders o
       LEFT JOIN order_items oi ON o.id = oi.order_id AND oi.tenant_id = o.tenant_id
       WHERE o.tenant_id = $1
       GROUP BY o.id
       ORDER BY o.created_at DESC
       LIMIT $2`,
      [tenantId, limit]
    );

    const durationMs = performance.now() - startTime;
    const orders = result.rows;

    // Defensive check
    for (const order of orders) {
      assertTenantBoundary({ tenantId }, order.tenant_id);
    }

    return {
      orders,
      benchmark: {
        strategy: 'OPTIMIZED_SINGLE_JOIN',
        ordersFetched: orders.length,
        totalQueriesExecuted: 1, // Exactly 1 round-trip
        latencyMs: parseFloat(durationMs.toFixed(3)),
        description: `Fetched ${orders.length} orders with all child items in a single grouped JOIN query.`
      }
    };
  }

  /**
   * Runs EXPLAIN (ANALYZE, BUFFERS) before and after composite indexing
   * Implements Lecture 17 Slide 3: "Database Monitoring & EXPLAIN / ANALYZE"
   */
  static async compareExplainAnalyze(tenantId) {
    // 1. Ensure Index is DROPPED to measure unindexed execution plan
    await db.exec('DROP INDEX IF EXISTS idx_orders_tenant_created;');

    const beforeRaw = await db.query(
      `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
       SELECT id, customer_name, total_amount, created_at
       FROM orders
       WHERE tenant_id = $1
       ORDER BY created_at DESC;`,
      [tenantId]
    );

    const beforePlan = beforeRaw.rows[0]['QUERY PLAN'] || beforeRaw.rows[0];

    // 2. CREATE Composite Index on [tenant_id, created_at DESC]
    await db.exec('CREATE INDEX IF NOT EXISTS idx_orders_tenant_created ON orders (tenant_id, created_at DESC);');
    // Also index foreign key on order_items for optimal JOIN performance
    await db.exec('CREATE INDEX IF NOT EXISTS idx_order_items_tenant_order ON order_items (tenant_id, order_id);');

    // 3. Run EXPLAIN ANALYZE on indexed table
    const afterRaw = await db.query(
      `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
       SELECT id, customer_name, total_amount, created_at
       FROM orders
       WHERE tenant_id = $1
       ORDER BY created_at DESC;`,
      [tenantId]
    );

    const afterPlan = afterRaw.rows[0]['QUERY PLAN'] || afterRaw.rows[0];

    // Extract key metrics from JSON plans
    const extractMetrics = (planObj) => {
      const planRoot = Array.isArray(planObj) ? planObj[0]?.Plan : planObj?.Plan;
      const executionTime = Array.isArray(planObj) ? planObj[0]?.['Execution Time'] : planObj?.['Execution Time'];
      const planningTime = Array.isArray(planObj) ? planObj[0]?.['Planning Time'] : planObj?.['Planning Time'];
      return {
        nodeType: planRoot?.['Node Type'] || 'Scan',
        indexName: planRoot?.['Index Name'] || null,
        startupCost: planRoot?.['Startup Cost'],
        totalCost: planRoot?.['Total Cost'],
        actualRows: planRoot?.['Actual Rows'],
        actualTotalTime: planRoot?.['Actual Total Time'],
        planningTimeMs: planningTime,
        executionTimeMs: executionTime
      };
    };

    const beforeMetrics = extractMetrics(beforePlan);
    const afterMetrics = extractMetrics(afterPlan);

    const speedup = beforeMetrics.executionTimeMs && afterMetrics.executionTimeMs
      ? parseFloat((beforeMetrics.executionTimeMs / Math.max(0.001, afterMetrics.executionTimeMs)).toFixed(2))
      : 'N/A';

    return {
      query: 'SELECT id, customer_name, total_amount, created_at FROM orders WHERE tenant_id = $1 ORDER BY created_at DESC',
      tenantId,
      beforeIndex: {
        status: 'Without Composite Index',
        plan: beforePlan,
        summary: beforeMetrics,
        scanType: beforeMetrics.nodeType // e.g. "Seq Scan" or "Sort"
      },
      afterIndex: {
        status: 'With Composite Index idx_orders_tenant_created (tenant_id, created_at DESC)',
        plan: afterPlan,
        summary: afterMetrics,
        scanType: afterMetrics.nodeType // e.g. "Index Scan"
      },
      performanceGain: {
        speedupMultiplier: `${speedup}x faster`,
        latencyReductionMs: parseFloat((beforeMetrics.executionTimeMs - afterMetrics.executionTimeMs).toFixed(3))
      }
    };
  }

  /**
   * Create an order directly in DB (used by consumer or transactional path)
   */
  static async persistOrder(orderData) {
    const { id, tenant_id, user_id, customer_name, customer_email, total_amount, status, items } = orderData;
    
    // Insert order record
    await db.query(
      `INSERT INTO orders (id, tenant_id, user_id, customer_name, customer_email, total_amount, status, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)
       ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, updated_at = CURRENT_TIMESTAMP`,
      [id, tenant_id, user_id || null, customer_name, customer_email, total_amount, status || 'PENDING']
    );

    // Insert order items
    if (items && Array.isArray(items)) {
      for (const item of items) {
        await db.query(
          `INSERT INTO order_items (id, tenant_id, order_id, product_id, product_name, quantity, unit_price, subtotal)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
           ON CONFLICT (id) DO NOTHING`,
          [
            item.id,
            tenant_id,
            id,
            item.product_id,
            item.product_name,
            item.quantity,
            item.unit_price,
            item.subtotal
          ]
        );
      }
    }

    return orderData;
  }
}
