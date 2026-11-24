import { db } from '../src/db/connection.js';
import { OrderService } from '../src/services/order.service.js';

/**
 * EXPLAIN ANALYZE Execution Plan Benchmark Script
 * Implements Lecture 17 Slide 3:
 * "Reading the Blueprint: Database Monitoring & EXPLAIN / ANALYZE"
 * "EXPLAIN aur ANALYZE database ka X-Ray hain. Yeh tools batate hain ke
 *  ek simple query andar ja kar table mein kitna lamba safar tay kar rahi hai."
 */
async function runExplainBenchmark() {
  console.log('================================================================');
  console.log('🔍 BENCHMARK: EXPLAIN ANALYZE Execution Plans Before & After Indexing');
  console.log('================================================================');

  await db.initialize();
  const tenantId = 'tenant-alpha';

  console.log(`Target Query:`);
  console.log(`SELECT id, customer_name, total_amount, created_at FROM orders WHERE tenant_id = '${tenantId}' ORDER BY created_at DESC;\n`);

  const comparison = await OrderService.compareExplainAnalyze(tenantId);

  console.log('================================================================');
  console.log('🔴 BEFORE COMPOSITE INDEX (Un-indexed Table):');
  console.log('----------------------------------------------------------------');
  console.log(`Scan Mechanism:      ${comparison.beforeIndex.scanType}`);
  console.log(`Estimated Cost:      ${comparison.beforeIndex.summary.startupCost} .. ${comparison.beforeIndex.summary.totalCost}`);
  console.log(`Actual Rows:         ${comparison.beforeIndex.summary.actualRows}`);
  console.log(`Planning Time:       ${comparison.beforeIndex.summary.planningTimeMs} ms`);
  console.log(`Execution Time:      ${comparison.beforeIndex.summary.executionTimeMs} ms`);
  console.log('\nFull Execution Plan (Raw JSON Tree):');
  console.log(JSON.stringify(comparison.beforeIndex.plan, null, 2));

  console.log('\n================================================================');
  console.log('🟢 AFTER COMPOSITE INDEX [idx_orders_tenant_created (tenant_id, created_at DESC)]:');
  console.log('----------------------------------------------------------------');
  console.log(`Scan Mechanism:      ${comparison.afterIndex.scanType}`);
  console.log(`Index Utilized:      ${comparison.afterIndex.summary.indexName || 'idx_orders_tenant_created'}`);
  console.log(`Estimated Cost:      ${comparison.afterIndex.summary.startupCost} .. ${comparison.afterIndex.summary.totalCost}`);
  console.log(`Actual Rows:         ${comparison.afterIndex.summary.actualRows}`);
  console.log(`Planning Time:       ${comparison.afterIndex.summary.planningTimeMs} ms`);
  console.log(`Execution Time:      ${comparison.afterIndex.summary.executionTimeMs} ms`);
  console.log('\nFull Execution Plan (Raw JSON Tree):');
  console.log(JSON.stringify(comparison.afterIndex.plan, null, 2));

  console.log('\n================================================================');
  console.log('📈 ARCHITECTURAL PERFORMANCE ANALYSIS:');
  console.log('----------------------------------------------------------------');
  console.log(`- Query Speedup:             ${comparison.performanceGain.speedupMultiplier}`);
  console.log(`- Execution Latency Gain:    ${comparison.performanceGain.latencyReductionMs} ms faster`);
  console.log(`- Database Mechanism Shift:  Sequential Scan (O(N) full table scan) ➡️ B-Tree Index Scan (O(log N))`);
  console.log(`- Tenant Isolation Benefit:  Composite index ensures Postgres only reads pages for [${tenantId}],`);
  console.log(`                             ignoring all other tenants without table-wide locking.`);
  console.log('================================================================');

  await db.close();
}

runExplainBenchmark().catch((err) => {
  console.error('Error running EXPLAIN benchmark:', err);
  process.exit(1);
});
