import express from 'express';
import cors from 'cors';
import { tenantMiddleware } from './middleware/tenant.middleware.js';
import orderRoutes from './routes/order.routes.js';
import explainRoutes from './routes/explain.routes.js';
import analyticsRoutes from './routes/analytics.routes.js';
import tenantRoutes from './routes/tenant.routes.js';

export function createApp() {
  const app = express();

  app.use(cors());
  app.use(express.json());
  app.use(express.static('public'));

  // Health and System Info
  app.get('/health', (req, res) => {
    res.status(200).json({
      status: 'UP',
      timestamp: new Date().toISOString(),
      service: 'CSC337 Multi-Tenant SaaS Platform',
      version: '1.0.0'
    });
  });

  // Public Tenant Routes (Registry & Token Issuance)
  app.use('/api/v1/tenants', tenantRoutes);

  // Scoped Tenant Routes (Protected by Tenant Resolution Middleware)
  // Enforces Lecture 18: Tenant Data Boundary
  app.use('/api/v1/orders', tenantMiddleware, orderRoutes);
  app.use('/api/v1/explain', tenantMiddleware, explainRoutes);
  app.use('/api/v1/analytics', tenantMiddleware, analyticsRoutes);

  // 404 Handler
  app.use((req, res) => {
    res.status(404).json({
      error: 'NOT_FOUND',
      message: `The endpoint ${req.method} ${req.url} does not exist.`
    });
  });

  // Global Error Handler
  app.use((err, req, res, next) => {
    console.error('[GlobalErrorHandler]', err);
    const statusCode = err.statusCode || 500;
    res.status(statusCode).json({
      error: err.code || 'INTERNAL_SERVER_ERROR',
      message: err.message || 'An unexpected error occurred.'
    });
  });

  return app;
}
