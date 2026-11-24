import { createApp } from './app.js';
import { config } from './config/index.js';
import { db } from './db/connection.js';
import { autoSeedIfEmpty } from './db/auto-seed.js';
import { OrderProducer } from './kafka/producer.js';
import { OrderConsumerService } from './kafka/consumer.js';

async function bootstrap() {
  console.log('================================================================');
  console.log('🚀 CSC337: Advanced Web Technologies - Lab Assignment 06');
  console.log('🏢 Multi-Tenant B2B SaaS Platform');
  console.log('================================================================');

  try {
    // 1. Initialize Database & Connection Pool
    console.log('[Bootstrap] Initializing Database & Connection Pool...');
    await db.initialize();

    // 2. Auto-seed if database is fresh
    await autoSeedIfEmpty();

    // 3. Initialize Kafka Producer & Consumer Group
    console.log('[Bootstrap] Initializing Apache Kafka Event-Driven Subsystem...');
    await OrderProducer.init();
    await OrderConsumerService.start();

    // 4. Start Express HTTP Server
    const app = createApp();
    const server = app.listen(config.port, () => {
      console.log(`[Bootstrap] HTTP Server running on http://localhost:${config.port}`);
      console.log(`[Bootstrap] Environment: ${config.nodeEnv}`);
      console.log(`[Bootstrap] Connection Pool Size: ${config.db.pool.max}`);
      console.log(`[Bootstrap] Kafka Topic: ${config.kafka.topicOrders}`);
      console.log('================================================================');
    });

    // Graceful Shutdown
    const shutdown = async (signal) => {
      console.log(`\n[Bootstrap] Received ${signal}. Initiating graceful shutdown...`);
      server.close(async () => {
        await db.close();
        console.log('[Bootstrap] Database connections drained and closed.');
        process.exit(0);
      });
    };

    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));

  } catch (error) {
    console.error('[Bootstrap] Fatal startup error:', error);
    process.exit(1);
  }
}

bootstrap();
