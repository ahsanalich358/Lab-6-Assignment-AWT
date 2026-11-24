import express from 'express';
import { db } from '../db/connection.js';
import { localBroker } from '../kafka/client.js';
import { config } from '../config/index.js';

const router = express.Router();

/**
 * GET /api/v1/analytics/pool-status
 * Live Connection Pool Monitoring
 * Implements Lecture 17 Slide 5: "Guarding the Gates: Database Connection Pooling"
 */
router.get('/pool-status', (req, res) => {
  const stats = db.getStats();
  return res.status(200).json({
    timestamp: new Date().toISOString(),
    connectionPool: stats,
    health: stats.activeConnections < stats.maxCapacity ? 'HEALTHY' : 'SATURATED'
  });
});

/**
 * GET /api/v1/analytics/kafka-status
 * Real-time Kafka Topic, Partition & Offset metrics
 * Implements Lecture 18 Slide 12: "Anatomy of a Stream: Topics, Partitions & Offsets"
 */
router.get('/kafka-status', (req, res) => {
  const metadata = localBroker.getTopicMetadata(config.kafka.topicOrders);
  return res.status(200).json({
    timestamp: new Date().toISOString(),
    topic: metadata.topic,
    totalPartitions: metadata.partitions.length,
    partitions: metadata.partitions
  });
});

/**
 * GET /api/v1/analytics/invoices
 * Invoices & Cold Start Telemetry for current tenant
 * Implements Lecture 17 Slide 8 & 9: Serverless Cold vs Warm Latency
 */
router.get('/invoices', async (req, res) => {
  try {
    const result = await db.query(
      `SELECT id, order_id, invoice_number, cold_start, init_duration_ms, execution_duration_ms, total_latency_ms, created_at
       FROM invoices
       WHERE tenant_id = $1
       ORDER BY created_at DESC
       LIMIT 50`,
      [req.tenantId]
    );

    const invoices = result.rows;
    const coldStarts = invoices.filter(i => i.cold_start);
    const warmStarts = invoices.filter(i => !i.cold_start);

    const avgCold = coldStarts.length > 0 
      ? parseFloat((coldStarts.reduce((acc, i) => acc + parseFloat(i.total_latency_ms), 0) / coldStarts.length).toFixed(2))
      : 0;
    const avgWarm = warmStarts.length > 0
      ? parseFloat((warmStarts.reduce((acc, i) => acc + parseFloat(i.total_latency_ms), 0) / warmStarts.length).toFixed(2))
      : 0;

    return res.status(200).json({
      tenantId: req.tenantId,
      totalInvoices: invoices.length,
      telemetrySummary: {
        coldStartCount: coldStarts.length,
        warmStartCount: warmStarts.length,
        averageColdLatencyMs: avgCold,
        averageWarmLatencyMs: avgWarm,
        latencyPenaltyFactor: avgWarm > 0 ? `${(avgCold / avgWarm).toFixed(1)}x slower on Cold Start` : 'N/A'
      },
      invoices
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

export default router;
