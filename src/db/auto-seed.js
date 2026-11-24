import { db } from './connection.js';
import { v4 as uuidv4 } from 'uuid';

/**
 * Auto-Seed Service
 * Automatically populates tenants, users, products, and realistic sample orders
 * on first startup if database is empty. Ensures 100% turnkey deployment on Render/Vercel.
 */
export async function autoSeedIfEmpty() {
  try {
    const check = await db.query('SELECT count(*) FROM tenants');
    const tenantCount = parseInt(check.rows[0].count, 10);

    if (tenantCount > 0) {
      console.log(`[AutoSeed] Database already populated with ${tenantCount} tenants. Skipping seed.`);
      return;
    }

    console.log('[AutoSeed] Database is uninitialized. Auto-seeding initial multi-tenant dataset...');

    // 1. Tenants
    const tenants = [
      { id: 'tenant-alpha', name: 'Alpha Retail Technologies', subdomain: 'alpha', plan: 'enterprise' },
      { id: 'tenant-beta', name: 'Beta Global Logistics', subdomain: 'beta', plan: 'pro' },
      { id: 'tenant-gamma', name: 'Gamma Omnichannel Commerce', subdomain: 'gamma', plan: 'enterprise' }
    ];

    for (const t of tenants) {
      await db.query(
        `INSERT INTO tenants (id, name, subdomain, plan, status)
         VALUES ($1, $2, $3, $4, 'active')
         ON CONFLICT (id) DO NOTHING`,
        [t.id, t.name, t.subdomain, t.plan]
      );
    }

    // 2. Users
    for (const t of tenants) {
      await db.query(
        `INSERT INTO users (id, tenant_id, email, name, role)
         VALUES ($1, $2, $3, $4, 'admin')
         ON CONFLICT (id) DO NOTHING`,
        [`usr_${t.id}_admin`, t.id, `admin@${t.subdomain}.com`, `${t.name} Administrator`]
      );
    }

    // 3. Products
    const products = [
      { id: 'prod-alpha-101', tenant_id: 'tenant-alpha', name: 'Enterprise Cloud Server', sku: 'ALPH-SRV-01', price: 850.00, count: 500 },
      { id: 'prod-alpha-102', tenant_id: 'tenant-alpha', name: 'High-Speed NVMe Storage Array', sku: 'ALPH-NVME-02', price: 320.00, count: 750 },
      { id: 'prod-alpha-103', tenant_id: 'tenant-alpha', name: 'Managed Kubernetes Cluster Node', sku: 'ALPH-K8S-03', price: 450.00, count: 600 },
      { id: 'prod-beta-201', tenant_id: 'tenant-beta', name: 'GPS Fleet Tracking Unit', sku: 'BETA-GPS-01', price: 120.00, count: 1200 },
      { id: 'prod-beta-202', tenant_id: 'tenant-beta', name: 'Telemetry Gateway Sensor', sku: 'BETA-TEL-02', price: 85.00, count: 2000 },
      { id: 'prod-beta-203', tenant_id: 'tenant-beta', name: 'Automated Dispatch Tablet', sku: 'BETA-TAB-03', price: 290.00, count: 400 }
    ];

    for (const p of products) {
      await db.query(
        `INSERT INTO products (id, tenant_id, name, sku, price, inventory_count)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (id) DO NOTHING`,
        [p.id, p.tenant_id, p.name, p.sku, p.price, p.count]
      );
    }

    // 4. Sample Orders
    const sampleCustomers = ['Ayesha Khan', 'Bilal Ahmed', 'Fatima Zahra', 'Hamza Tariq'];
    for (let i = 1; i <= 200; i++) {
      const tenantId = i % 2 === 0 ? 'tenant-alpha' : 'tenant-beta';
      const orderId = `ord_seed_${tenantId.split('-')[1]}_${i.toString().padStart(4, '0')}`;
      const customer = sampleCustomers[i % sampleCustomers.length];
      const prod = tenantId === 'tenant-alpha' ? products[0] : products[3];

      await db.query(
        `INSERT INTO orders (id, tenant_id, customer_name, customer_email, total_amount, status)
         VALUES ($1, $2, $3, $4, $5, 'COMPLETED')
         ON CONFLICT (id) DO NOTHING`,
        [orderId, tenantId, customer, `${customer.toLowerCase().replace(' ', '.')}@domain.pk`, prod.price]
      );

      await db.query(
        `INSERT INTO order_items (id, tenant_id, order_id, product_id, product_name, quantity, unit_price, subtotal)
         VALUES ($1, $2, $3, $4, $5, 1, $6, $6)
         ON CONFLICT (id) DO NOTHING`,
        [`item_${uuidv4().substring(0, 8)}`, tenantId, orderId, prod.id, prod.name, prod.price]
      );
    }

    console.log('[AutoSeed] Successfully seeded tenants, products, and 200 initial orders.');
  } catch (error) {
    console.error('[AutoSeed] Warning during auto-seed:', error.message);
  }
}
