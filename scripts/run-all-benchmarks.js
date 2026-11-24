import { spawn } from 'child_process';
import path from 'path';

/**
 * Master Verification & Benchmark Runner
 * Sequentially runs database seeding, tenant isolation tests, Kafka tests,
 * N+1 benchmarks, EXPLAIN ANALYZE comparison, and Serverless Cold-Start analysis.
 */
async function runScript(scriptPath, title) {
  return new Promise((resolve, reject) => {
    console.log('\n================================================================');
    console.log(`🚀 RUNNING: ${title} (${scriptPath})`);
    console.log('================================================================');

    const proc = spawn('node', [scriptPath], {
      stdio: 'inherit',
      cwd: process.cwd()
    });

    proc.on('close', (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`Script ${scriptPath} exited with code ${code}`));
      }
    });
  });
}

async function runAll() {
  console.log('================================================================');
  console.log('🏆 CSC337: ADVANCED WEB TECHNOLOGIES - LAB ASSIGNMENT 06');
  console.log('COMPREHENSIVE VERIFICATION & BENCHMARK SUITE');
  console.log('================================================================');

  try {
    await runScript('scripts/seed-db.js', 'Step 1: Multi-Tenant Database Seeding (2,400 Orders)');
    await runScript('tests/tenant-isolation.test.js', 'Step 2: Strict Tenant Isolation & Data Boundary Tests');
    await runScript('tests/kafka-pipeline.test.js', 'Step 3: Apache Kafka 202 Accepted & Idempotent Consumer Pipeline');
    await runScript('scripts/benchmark-nplusone.js', 'Step 4: N+1 Query Resolution vs Single Grouped JOIN Benchmark');
    await runScript('scripts/benchmark-explain.js', 'Step 5: PostgreSQL EXPLAIN ANALYZE Before vs After Composite Index');
    await runScript('scripts/benchmark-coldstart.js', 'Step 6: Serverless Background Worker Cold Start vs Warm Start Latency');

    console.log('\n================================================================');
    console.log('✨ ALL LAB ASSIGNMENT 06 BENCHMARKS & VERIFICATIONS PASSED 100%!');
    console.log('================================================================');
  } catch (err) {
    console.error('\n❌ Benchmark Suite Failed:', err.message);
    process.exit(1);
  }
}

runAll();
