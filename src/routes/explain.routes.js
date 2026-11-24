import express from 'express';
import { OrderService } from '../services/order.service.js';

const router = express.Router();

/**
 * GET /api/v1/explain/analyze
 * Generates Query Execution Plans Before and After Composite Indexing
 * Implements Lecture 17 Slide 3: EXPLAIN / ANALYZE
 */
router.get('/analyze', async (req, res) => {
  try {
    const report = await OrderService.compareExplainAnalyze(req.tenantId);
    return res.status(200).json(report);
  } catch (error) {
    console.error('[ExplainRoutes] Error running EXPLAIN ANALYZE:', error);
    return res.status(500).json({ error: 'EXPLAIN_ANALYZE_FAILED', message: error.message });
  }
});

export default router;
