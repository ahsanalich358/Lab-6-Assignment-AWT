import dotenv from 'dotenv';
dotenv.config();

export const config = {
  port: parseInt(process.env.PORT || '4000', 10),
  nodeEnv: process.env.NODE_ENV || 'development',
  jwtSecret: process.env.JWT_SECRET || 'csc337_super_secret_jwt_key_2026',

  db: {
    dataDir: process.env.DB_DATA_DIR || './data/pgdata',
    url: process.env.DATABASE_URL || null,
    pool: {
      max: parseInt(process.env.DB_POOL_MAX || '20', 10),
      idleTimeoutMillis: parseInt(process.env.DB_POOL_IDLE_TIMEOUT_MS || '30000', 10),
      connectionTimeoutMillis: parseInt(process.env.DB_POOL_CONNECTION_TIMEOUT_MS || '5000', 10)
    }
  },

  kafka: {
    clientId: process.env.KAFKA_CLIENT_ID || 'csc337-saas-order-service',
    groupId: process.env.KAFKA_GROUP_ID || 'order-processing-group',
    topicOrders: process.env.KAFKA_TOPIC_ORDERS || 'tenant.orders',
    brokers: process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : null,
    username: process.env.KAFKA_USERNAME || null,
    password: process.env.KAFKA_PASSWORD || null,
    sasl: process.env.KAFKA_SASL === 'true',
    ssl: process.env.KAFKA_SSL === 'true'
  },

  serverless: {
    coldStartBaseDelayMs: parseInt(process.env.COLD_START_BASE_DELAY_MS || '1200', 10),
    warmStartExecutionMs: parseInt(process.env.WARM_START_EXECUTION_MS || '35', 10)
  }
};
