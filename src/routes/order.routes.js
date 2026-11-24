import express from 'express';
import { v4 as uuidv4 } from 'uuid';
import { OrderService } from '../services/order.service.js';
import { OrderProducer } from '../kafka/producer.js';
import { db } from '../db/connection.js';
import { assertTenantBoundary } from '../middleware/tenant.middleware.js';

const router = express.Router();

/**
 * POST /api/v1/orders
 * Implements Lecture 18 Slide 10 & 11:
 * - Asynchronous Order Acceptance: Returns HTTP 202 Accepted immediately
 * - Publishes 'OrderCreated' event to Kafka topic 'tenant.orders'
 * - Partition Key: tenant_id (Guarantees strict in-order processing per tenant)
 */
router.post('/', async (req, res) => {
  try {
    const { customer_name, customer_email, items } = req.body;
    const tenantId = req.tenantId;

    if (!customer_name || !customer_email || !items || !Array.isArray(items) || items.length === 0) {
      return res.status(400).json({
        error: 'INVALID_ORDER_PAYLOAD',
        message: 'Order requires customer_name, customer_email, and at least one item.'
      });
    }

    // Verify all requested products belong strictly to this tenant (Tenant Isolation Check)
    let totalAmount = 0;
    const enrichedItems = [];

    for (const item of items) {
      const productRes = await db.query(
        'SELECT * FROM products WHERE id = $1 AND tenant_id = $2',
        [item.product_id, tenantId]
      );

      if (productRes.rows.length === 0) {
        return res.status(400).json({
          error: 'PRODUCT_NOT_FOUND_OR_CROSS_TENANT',
          message: `Product [${item.product_id}] does not exist for tenant [${tenantId}]. Cross-tenant order violation prevented.`
        });
      }

      const product = productRes.rows[0];
      const quantity = parseInt(item.quantity || 1, 10);
      const unitPrice = parseFloat(product.price);
      const subtotal = parseFloat((unitPrice * quantity).toFixed(2));
      totalAmount += subtotal;

      enrichedItems.push({
        id: `item_${uuidv4().substring(0, 8)}`,
        product_id: product.id,
        product_name: product.name,
        quantity,
        unit_price: unitPrice,
        subtotal
      });
    }

    const orderId = `ord_${uuidv4().substring(0, 10)}`;

    const orderPayload = {
      id: orderId,
      tenant_id: tenantId,
      customer_name,
      customer_email,
      total_amount: parseFloat(totalAmount.toFixed(2)),
      status: 'ACCEPTED',
      items: enrichedItems,
      created_at: new Date().toISOString()
    };

    // Immediately persist initial state so it is queryable
    await OrderService.persistOrder(orderPayload);

    // Publish to Apache Kafka (Asynchronous Decoupling)
    const kafkaResult = await OrderProducer.publishOrderCreated(orderPayload);

    // Return 202 Accepted IMMEDIATELY (Pizza Token Pattern - Slide 10)
    return res.status(202).json({
      status: 'ACCEPTED',
      message: 'Order accepted and dispatched to Apache Kafka for asynchronous processing.',
      order_id: orderId,
      tenant_id: tenantId,
      total_amount: orderPayload.total_amount,
      kafka: {
        topic: kafkaResult.topic,
        partition_key: kafkaResult.partitionKey,
        event_id: kafkaResult.eventId,
        correlation_id: kafkaResult.correlationId
      }
    });
  } catch (error) {
    console.error('[OrderRoutes] Error creating order:', error);
    return res.status(500).json({ error: 'ORDER_CREATION_FAILED', message: error.message });
  }
});

/**
 * GET /api/v1/orders
 * Returns tenant orders (optimized by default)
 */
router.get('/', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit || '50', 10);
    const result = await OrderService.getOrdersOptimized(req.tenantId, limit);
    return res.status(200).json(result);
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

/**
 * GET /api/v1/orders/unoptimized
 * Benchmark Endpoint: Demonstrates the N+1 Query Bottleneck (Lecture 17 Slide 4)
 */
router.get('/unoptimized', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit || '50', 10);
    const result = await OrderService.getOrdersUnoptimized(req.tenantId, limit);
    return res.status(200).json(result);
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

/**
 * GET /api/v1/orders/optimized
 * Benchmark Endpoint: Demonstrates Single-Trip JOIN Optimization (Lecture 17 Slide 4)
 */
router.get('/optimized', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit || '50', 10);
    const result = await OrderService.getOrdersOptimized(req.tenantId, limit);
    return res.status(200).json(result);
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

/**
 * GET /api/v1/orders/:id
 * Fetches single order with strict tenant boundary enforcement
 */
router.get('/:id', async (req, res) => {
  try {
    const orderId = req.params.id;
    const result = await db.query(
      `SELECT o.*, 
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
       LEFT JOIN order_items oi ON o.id = oi.order_id
       WHERE o.id = $1
       GROUP BY o.id`,
      [orderId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'ORDER_NOT_FOUND', message: `Order ${orderId} does not exist.` });
    }

    const order = result.rows[0];

    // CRITICAL: Tenant Isolation Enforcement (Lecture 18 Slide 14)
    assertTenantBoundary(req, order.tenant_id);

    return res.status(200).json({ order });
  } catch (error) {
    if (error.statusCode === 403) {
      return res.status(403).json({ error: error.code, message: error.message });
    }
    return res.status(500).json({ error: error.message });
  }
});

export default router;
