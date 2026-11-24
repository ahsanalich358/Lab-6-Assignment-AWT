import { kafkaClient } from './client.js';
import { db } from '../db/connection.js';
import { config } from '../config/index.js';
import { OrderService } from '../services/order.service.js';
import { processInvoiceGeneration } from '../serverless/invoice-worker.js';

/**
 * Kafka Consumer Group Service
 * Implements Lecture 18 Slide 11, 12, 13:
 * - Consumer Group ('order-processing-group')
 * - Idempotent Consumer Pattern (Lecture 18 Slide 13: Exactly-Once safety via deduplication)
 * - Sequential processing per partition
 * - Asynchronous Inventory Deduction & Offset Commit
 */
export class OrderConsumerService {
  static consumer = null;
  static isRunning = false;

  static async start() {
    if (this.isRunning) return;

    this.consumer = await kafkaClient.createConsumer(config.kafka.groupId);
    await this.consumer.connect();
    await this.consumer.subscribe({
      topic: config.kafka.topicOrders,
      fromBeginning: false
    });

    console.log(`[KafkaConsumerGroup] Consumer joined group '${config.kafka.groupId}' on topic '${config.kafka.topicOrders}'.`);

    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const offset = message.offset.toString();
        const rawValue = message.value.toString();
        const eventData = JSON.parse(rawValue);

        const { eventId, tenantId, orderId, payload } = eventData;

        console.log(`[KafkaConsumerGroup] Received event | Topic: ${topic} | Partition: ${partition} | Offset: ${offset} | Tenant: ${tenantId} | EventID: ${eventId}`);

        try {
          // 1. Idempotency Check (Lecture 18 Slide 13: "Idempotent consumer kya hota hai?")
          // Prevent double inventory deduction if network retry delivers duplicate message
          const existingEvent = await db.query(
            'SELECT event_id FROM processed_events WHERE event_id = $1',
            [eventId]
          );

          if (existingEvent.rows.length > 0) {
            console.log(`[KafkaConsumerGroup] IDEMPOTENCY HIT: Event [${eventId}] was already processed! Skipping execution.`);
            return;
          }

          // 2. Persist or Update Order in Database with Tenant Scoping
          await OrderService.persistOrder({
            ...payload,
            status: 'PROCESSING'
          });

          // 3. Asynchronous Inventory Deduction (Tenant-isolated)
          if (payload.items && Array.isArray(payload.items)) {
            for (const item of payload.items) {
              await db.query(
                `UPDATE products 
                 SET inventory_count = GREATEST(0, inventory_count - $1)
                 WHERE id = $2 AND tenant_id = $3`,
                [item.quantity, item.product_id, tenantId]
              );
            }
          }

          // 4. Trigger Serverless Background Task: PDF Invoice Generation (Lecture 17)
          try {
            const invoiceResult = await processInvoiceGeneration({
              tenantId,
              order: payload
            });
            console.log(`[KafkaConsumerGroup] Serverless Invoice generated: ${invoiceResult.invoiceNumber} | ColdStart: ${invoiceResult.telemetry.coldStart} | Latency: ${invoiceResult.telemetry.totalLatencyMs}ms`);
          } catch (invErr) {
            console.error(`[KafkaConsumerGroup] Invoice generation failed for order ${orderId}:`, invErr.message);
          }

          // 5. Update Order Status to COMPLETED
          await db.query(
            `UPDATE orders SET status = 'COMPLETED', updated_at = CURRENT_TIMESTAMP WHERE id = $1 AND tenant_id = $2`,
            [orderId, tenantId]
          );

          // 6. Record Processed Event (Immutable Offset & Audit Trail)
          await db.query(
            `INSERT INTO processed_events (event_id, tenant_id, topic, partition_id, offset_val, status)
             VALUES ($1, $2, $3, $4, $5, 'PROCESSED')
             ON CONFLICT (event_id) DO NOTHING`,
            [eventId, tenantId, topic, partition, parseInt(offset, 10)]
          );

          console.log(`[KafkaConsumerGroup] SUCCESS: Offset committed | Partition: ${partition} | Offset: ${offset} | Order: ${orderId} | Status: COMPLETED`);
        } catch (error) {
          console.error(`[KafkaConsumerGroup] ERROR processing order event ${eventId}:`, error);
        }
      }
    });

    this.isRunning = true;
  }
}
