import express from 'express';
import { db } from '../db/connection.js';
import { generateTenantToken } from '../middleware/auth.middleware.js';

const router = express.Router();

/**
 * GET /api/v1/tenants
 * Lists registered tenants (public registry)
 */
router.get('/', async (req, res) => {
  try {
    const result = await db.query('SELECT id, name, subdomain, plan, status, created_at FROM tenants ORDER BY id ASC');
    return res.status(200).json({ tenants: result.rows });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

/**
 * POST /api/v1/tenants/token
 * Issues a JWT token with embedded tenant_id claim
 */
router.post('/token', async (req, res) => {
  try {
    const { tenant_id, email } = req.body;
    if (!tenant_id) {
      return res.status(400).json({ error: 'tenant_id is required' });
    }

    const tenantRes = await db.query('SELECT * FROM tenants WHERE id = $1', [tenant_id]);
    if (tenantRes.rows.length === 0) {
      return res.status(404).json({ error: 'Tenant not found' });
    }

    const tenant = tenantRes.rows[0];
    const userRes = await db.query('SELECT * FROM users WHERE tenant_id = $1 LIMIT 1', [tenant_id]);
    const user = userRes.rows[0] || {
      id: `usr_${tenant_id}_admin`,
      email: email || `admin@${tenant.subdomain}.com`,
      name: `${tenant.name} Administrator`,
      role: 'admin'
    };

    const token = generateTenantToken(user, tenant.id);

    return res.status(200).json({
      message: 'Token issued successfully for tenant authentication',
      tenant_id: tenant.id,
      tenant_name: tenant.name,
      token,
      user
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

/**
 * GET /api/v1/tenants/products
 * Returns product catalog scoped to active tenant
 */
router.get('/products', async (req, res) => {
  try {
    const tenantId = req.tenantId; // Injected by tenantMiddleware
    const result = await db.query(
      'SELECT id, name, sku, price, inventory_count FROM products WHERE tenant_id = $1 ORDER BY name ASC',
      [tenantId]
    );
    return res.status(200).json({
      tenantId,
      count: result.rows.length,
      products: result.rows
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

export default router;
