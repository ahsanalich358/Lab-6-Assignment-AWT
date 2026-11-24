import { kafkaClient, localBroker } from './client.js';
import { config } from '../config/index.js';
import { v4 as uuidv4 } from 'uuid';

/**
 * Order Kafka Producer
 * Implements Lecture 18 Slide 11 & 12:
 * - Asynchronous Order Acceptance: Returns 202 Accepted immediately
 * - Emits 'OrderCreated' event to topic 'tenant.orders'
 * - Message Key: tenant_id (Guarantees all orders for a tenant land in the same partition)
 */
export class OrderProducer {
  static producer = null;

  static async init() {
    if (!this.producer) {
      this.producer = await kafkaClient.getProducer();
      console.log('[OrderProducer] Connected and ready to publish high-throughput orders.');
    }
    return this.producer;
  }

  /**
   * Publishes OrderCreated event with partition key
   */
  static async publishOrderCreated(orderPayload) {
    await this.init();

    const eventId = `evt_${uuidv4()}`;
    const correlationId = `corr_${uuidv4()}`;

    const eventMessage = {
      eventId,
      eventType: 'OrderCreated',
      tenantId: orderPayload.tenant_id,
      orderId: orderPayload.id,
      timestamp: new Date().toISOString(),
      payload: orderPayload
    };

    const topic = config.kafka.topicOrders;
    
    // Crucial Requirement: Use tenant_id as Kafka Message Key to guarantee sequential order delivery within partitions
    const messageKey = orderPayload.tenant_id;

    const result = await this.producer.send({
      topic,
      messages: [
        {
          key: messageKey,
          value: JSON.stringify(eventMessage),
          headers: {
            'event-id': eventId,
            'correlation-id': correlationId,
            'tenant-id': orderPayload.tenant_id,
            'event-type': 'OrderCreated'
          }
        }
      ]
    });

    return {
      eventId,
      correlationId,
      topic,
      partitionKey: messageKey,
      result
    };
  }

  static getTopicStatus() {
    return localBroker.getTopicMetadata(config.kafka.topicOrders);
  }
}
