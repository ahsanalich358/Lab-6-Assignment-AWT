import { createApp } from '../src/app.js';
import { db } from '../src/db/connection.js';
import { OrderProducer } from '../src/kafka/producer.js';
import { OrderConsumerService } from '../src/kafka/consumer.js';

const app = createApp();
let isInitialized = false;

async function ensureServices() {
  if (!isInitialized) {
    await db.initialize();
    await OrderProducer.init();
    await OrderConsumerService.start();
    isInitialized = true;
  }
}

export default async function handler(req, res) {
  await ensureServices();
  return app(req, res);
}
