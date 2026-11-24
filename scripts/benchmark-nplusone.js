import { db } from '../src/db/connection.js';
import { OrderService } from '../src/services/order.service.js';

/**
 * N+1 Problem vs Optimized JOIN Benchmark Script
 * Implements Lecture 17 Slide 4:
 * "The Root Cause: The N+1 Query Problem"
 * "100 orders ke liye Foodpanda rider 100 dafa restaurant nahi jata.
 *  Code mein array map ke andar query likhna (N+1) sab se bari bewakoofi hai."
 */
async function runNPlusOneBenchmark() {
  console.log('================================================================');
  console.log('⚡ BENCHMARK: Unoptimized N+1 Queries vs Single Grouped JOIN');
  console.log('================================================================');

  await db.initialize();
  const tenantId = 'tenant-alpha';
  const testLimits = [10, 50, 100];

  console.log(`Testing with Tenant Context: [${tenantId}]`);
  console.log('----------------------------------------------------------------');

  for (const limit of testLimits) {
    console.log(`\n📊 Running Benchmark for ${limit} Orders:`);
    console.log('----------------------------------------------------------------');

    // 1. Run Unoptimized N+1
    const unopt = await OrderService.getOrdersUnoptimized(tenantId, limit);
    console.log(`🔴 UNOPTIMIZED (N+1):`);
    console.log(`   - Orders Fetched:        ${unopt.benchmark.ordersFetched}`);
    console.log(`   - Total DB Round-trips:  ${unopt.benchmark.totalQueriesExecuted} queries (1 + ${unopt.benchmark.ordersFetched})`);
    console.log(`   - Latency:               ${unopt.benchmark.latencyMs} ms`);

    // 2. Run Optimized Single JOIN
    const opt = await OrderService.getOrdersOptimized(tenantId, limit);
    console.log(`🟢 OPTIMIZED (Single JOIN):`);
    console.log(`   - Orders Fetched:        ${opt.benchmark.ordersFetched}`);
    console.log(`   - Total DB Round-trips:  ${opt.benchmark.totalQueriesExecuted} query (1 single network trip)`);
    console.log(`   - Latency:               ${opt.benchmark.latencyMs} ms`);

    // Comparison Analysis
    const queriesSaved = unopt.benchmark.totalQueriesExecuted - opt.benchmark.totalQueriesExecuted;
    const latencySaved = (unopt.benchmark.latencyMs - opt.benchmark.latencyMs).toFixed(2);
    const speedup = (unopt.benchmark.latencyMs / Math.max(0.1, opt.benchmark.latencyMs)).toFixed(1);

    console.log(`✨ IMPACT FOR ${limit} ORDERS:`);
    console.log(`   - Database Round-Trips Eliminated: ${queriesSaved} queries saved (${((queriesSaved / unopt.benchmark.totalQueriesExecuted) * 100).toFixed(1)}% reduction)`);
    console.log(`   - Latency Reduced by:             ${latencySaved} ms (${speedup}x speedup)`);
    console.log(`   - Foodpanda Analogy:              Rider took 1 trip instead of ${unopt.benchmark.totalQueriesExecuted} trips!`);
  }

  console.log('\n================================================================');
  console.log('✅ N+1 Benchmark Completed Successfully.');
  console.log('================================================================');
  await db.close();
}

runNPlusOneBenchmark().catch((err) => {
  console.error('Error running benchmark:', err);
  process.exit(1);
});
