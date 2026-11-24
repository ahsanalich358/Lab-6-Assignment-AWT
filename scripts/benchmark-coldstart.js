import { db } from '../src/db/connection.js';
import { processInvoiceGeneration, forceColdStartReset } from '../src/serverless/invoice-worker.js';

/**
 * Serverless Cold-Start vs Warm-Start Benchmark
 * Implements Lecture 17 Slide 8 & 9:
 * "The Serverless Trade-off: The 'Cold Start'"
 * "WAPDA ki light jane par backup generator on hone mein thora time lagta hai.
 *  Serverless function jab zero se wake-up hota hai, toh pehli request hamesha slow hoti hai."
 */
async function runColdStartBenchmark() {
  console.log('================================================================');
  console.log('❄️ BENCHMARK: Serverless Worker Cold Start vs Warm Start Latency');
  console.log('================================================================');

  await db.initialize();

  // Fetch an existing order from the database to satisfy the foreign key constraint
  const orderRes = await db.query(
    `SELECT o.*, 
       COALESCE(
         json_agg(
           json_build_object(
             'product_name', oi.product_name,
             'quantity', oi.quantity,
             'unit_price', oi.unit_price,
             'subtotal', oi.subtotal
           )
         ) FILTER (WHERE oi.id IS NOT NULL), '[]'::json
       ) AS items
     FROM orders o
     LEFT JOIN order_items oi ON o.id = oi.order_id
     WHERE o.tenant_id = 'tenant-alpha'
     GROUP BY o.id
     LIMIT 1`
  );

  const testOrder = orderRes.rows[0] || {
    id: 'ord_fallback',
    customer_name: 'Imran Ashraf',
    customer_email: 'imran.ashraf@enterprise.pk',
    total_amount: 1250.00,
    items: [
      { product_name: 'Enterprise Cloud Server', quantity: 1, unit_price: 850.00, subtotal: 850.00 }
    ]
  };

  // 1. Force container reset to guarantee Cold Start on Invocation #1
  forceColdStartReset();

  const results = [];
  const totalInvocations = 5;

  console.log(`Executing ${totalInvocations} sequential invocations on Serverless Worker...\n`);

  for (let i = 1; i <= totalInvocations; i++) {
    const isColdExpected = (i === 1);
    console.log(`➡️  Invocation #${i} (${isColdExpected ? 'COLD START' : 'WARM START'})...`);

    const invoice = await processInvoiceGeneration({
      tenantId: 'tenant-alpha',
      order: testOrder
    });

    results.push({
      invocation: i,
      state: invoice.telemetry.coldStart ? 'COLD START ❄️' : 'WARM START 🔥',
      containerId: invoice.containerId,
      initializationMs: invoice.telemetry.initializationMs,
      executionMs: invoice.telemetry.executionMs,
      totalLatencyMs: invoice.telemetry.totalLatencyMs
    });

    // Small delay between warm calls
    await new Promise((r) => setTimeout(r, 100));
  }

  console.log('\n================================================================');
  console.log('📊 SERVERLESS EXECUTION LATENCY RESULTS:');
  console.log('================================================================');
  console.table(results);

  const coldLatency = results[0].totalLatencyMs;
  const warmLatencies = results.slice(1).map(r => r.totalLatencyMs);
  const avgWarmLatency = parseFloat((warmLatencies.reduce((a, b) => a + b, 0) / warmLatencies.length).toFixed(2));
  const latencyPenalty = (coldLatency / avgWarmLatency).toFixed(1);

  console.log('================================================================');
  console.log('🧠 ARCHITECTURAL ANALYSIS & TRADE-OFF:');
  console.log('----------------------------------------------------------------');
  console.log(`- Cold Start Latency:        ${coldLatency} ms`);
  console.log(`- Average Warm Latency:      ${avgWarmLatency} ms`);
  console.log(`- Cold Start Penalty:        ${latencyPenalty}x slower than warm requests!`);
  console.log(`- Root Cause (Slide 9):      When scaled to 0, cloud runtime must spin up container,`);
  console.log(`                             load Node.js runtime, import PDFKit fonts and libraries.`);
  console.log(`- Architectural Verdict:     Serverless is ideal for asynchronous background jobs`);
  console.log(`                             (PDF generation, email dispatch), but UNFIT for`);
  console.log(`                             hyper-low-latency real-time trading or gaming.`);
  console.log('================================================================');

  await db.close();
}

runColdStartBenchmark().catch((err) => {
  console.error('Error running cold-start benchmark:', err);
  process.exit(1);
});
