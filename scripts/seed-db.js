import { db } from '../src/db/connection.js';
import { v4 as uuidv4 } from 'uuid';

/**
 * Enterprise Database Seeder
 * Populates realistic multi-tenant data:
 * - 3 Enterprise Tenants (Alpha, Beta, Gamma)
 * - Users & Tenant-specific product catalogs
 * - 2,500 realistic orders with 5,000+ order_items for query optimization benchmarks
 */
async function seedDatabase() {
  console.log('================================================================');
  console.log('🌱 Starting Multi-Tenant Database Seeding...');
  console.log('================================================================');

  await db.initialize();

  // 1. Seed Tenants
  const tenants = [
    { id: 'tenant-alpha', name: 'Alpha Retail Technologies', subdomain: 'alpha', plan: 'enterprise' },
    { id: 'tenant-beta', name: 'Beta Global Logistics', subdomain: 'beta', plan: 'pro' },
    { id: 'tenant-gamma', name: 'Gamma Omnichannel Commerce', subdomain: 'gamma', plan: 'enterprise' }
  ];

  for (const t of tenants) {
    await db.query(
      `INSERT INTO tenants (id, name, subdomain, plan, status)
       VALUES ($1, $2, $3, $4, 'active')
       ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`,
      [t.id, t.name, t.subdomain, t.plan]
    );
    console.log(`[Seed] Seeded Tenant: ${t.name} (${t.id})`);
  }

  // 2. Seed Users
  for (const t of tenants) {
    const userId = `usr_${t.id}_admin`;
    await db.query(
      `INSERT INTO users (id, tenant_id, email, name, role)
       VALUES ($1, $2, $3, $4, 'admin')
       ON CONFLICT (id) DO NOTHING`,
      [userId, t.id, `admin@${t.subdomain}.com`, `${t.name} Administrator`]
    );
  }

  // 3. Seed Products (Distinct catalog per tenant to ensure strict data boundary)
  const productCatalog = {
    'tenant-alpha': [
      { id: 'prod-alpha-101', name: 'Enterprise Cloud Server', sku: 'ALPH-SRV-01', price: 850.00, inventory: 500 },
      { id: 'prod-alpha-102', name: 'High-Speed NVMe Storage Array', sku: 'ALPH-NVME-02', price: 320.00, inventory: 750 },
      { id: 'prod-alpha-103', name: 'Managed Kubernetes Cluster Node', sku: 'ALPH-K8S-03', price: 450.00, inventory: 600 },
      { id: 'prod-alpha-104', name: 'Dedicated Network Load Balancer', sku: 'ALPH-LB-04', price: 180.00, inventory: 900 }
    ],
    'tenant-beta': [
      { id: 'prod-beta-201', name: 'GPS Fleet Tracking Unit', sku: 'BETA-GPS-01', price: 120.00, inventory: 1200 },
      { id: 'prod-beta-202', name: 'Telemetry Gateway Sensor', sku: 'BETA-TEL-02', price: 85.00, inventory: 2000 },
      { id: 'prod-beta-203', name: 'Automated Dispatch Tablet', sku: 'BETA-TAB-03', price: 290.00, inventory: 400 }
    ],
    'tenant-gamma': [
      { id: 'prod-gamma-301', name: 'Cold-Chain Temperature Monitor', sku: 'GAM-TEMP-01', price: 75.00, inventory: 1500 },
      { id: 'prod-gamma-302', name: 'Smart Warehouse Barcode Scanner', sku: 'GAM-SCN-02', price: 140.00, inventory: 800 }
    ]
  };

  for (const [tenantId, items] of Object.entries(productCatalog)) {
    for (const p of items) {
      await db.query(
        `INSERT INTO products (id, tenant_id, name, sku, price, inventory_count)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (id) DO UPDATE SET price = EXCLUDED.price, inventory_count = EXCLUDED.inventory_count`,
        [p.id, tenantId, p.name, p.sku, p.price, p.inventory]
      );
    }
    console.log(`[Seed] Seeded ${items.length} products for ${tenantId}`);
  }

  // 4. Seed High-Volume Realistic Orders & Order Items
  console.log('[Seed] Generating 2,400 orders across tenants for realistic EXPLAIN ANALYZE indexing...');
  const customerNames = ['Ayesha Khan', 'Bilal Ahmed', 'Fatima Zahra', 'Hamza Tariq', 'Usman Ali', 'Zainab Bibi', 'Ali Raza', 'Mariam Siddiqui'];
  const totalOrdersToGenerate = 2400;

  let insertedOrders = 0;
  let insertedItems = 0;

  const now = Date.now();
  const dayMs = 24 * 60 * 60 * 1000;

  for (let i = 1; i <= totalOrdersToGenerate; i++) {
    // 60% Alpha, 25% Beta, 15% Gamma
    const rand = Math.random();
    let tenantId = 'tenant-alpha';
    if (rand > 0.85) tenantId = 'tenant-gamma';
    else if (rand > 0.60) tenantId = 'tenant-beta';

    const orderId = `ord_seed_${tenantId.split('-')[1]}_${i.toString().padStart(5, '0')}`;
    const customer = customerNames[i % customerNames.length];
    const customerEmail = `${customer.toLowerCase().replace(' ', '.')}${i % 100}@customer.org`;
    const orderDate = new Date(now - (Math.random() * 60 * dayMs)).toISOString();

    const catalog = productCatalog[tenantId];
    const item1 = catalog[i % catalog.length];
    const qty1 = 1 + (i % 3);
    const subtotal1 = parseFloat((item1.price * qty1).toFixed(2));

    let orderTotal = subtotal1;

    // Insert Order
    await db.query(
      `INSERT INTO orders (id, tenant_id, customer_name, customer_email, total_amount, status, created_at)
       VALUES ($1, $2, $3, $4, $5, 'COMPLETED', $6)
       ON CONFLICT (id) DO NOTHING`,
      [orderId, tenantId, customer, customerEmail, orderTotal, orderDate]
    );
    insertedOrders++;

    // Insert Order Item 1
    await db.query(
      `INSERT INTO order_items (id, tenant_id, order_id, product_id, product_name, quantity, unit_price, subtotal, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
       ON CONFLICT (id) DO NOTHING`,
      [`item_${uuidv4().substring(0, 8)}`, tenantId, orderId, item1.id, item1.name, qty1, item1.price, subtotal1, orderDate]
    );
    insertedItems++;

    // Add 2nd item to every alternate order
    if (i % 2 === 0) {
      const item2 = catalog[(i + 1) % catalog.length];
      const qty2 = 1;
      const subtotal2 = parseFloat(item2.price.toFixed(2));
      await db.query(
        `INSERT INTO order_items (id, tenant_id, order_id, product_id, product_name, quantity, unit_price, subtotal, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         ON CONFLICT (id) DO NOTHING`,
        [`item_${uuidv4().substring(0, 8)}`, tenantId, orderId, item2.id, item2.name, qty2, item2.price, subtotal2, orderDate]
      );
      insertedItems++;
    }

    if (i % 600 === 0) {
      console.log(`[Seed Progress] ${i} / ${totalOrdersToGenerate} orders seeded...`);
    }
  }

  console.log(`[Seed] Finished seeding: ${insertedOrders} orders and ${insertedItems} order items created.`);
  console.log('================================================================');
  console.log('✅ Multi-Tenant Database Seeding Completed Successfully.');
  console.log('================================================================');
  await db.close();
}

seedDatabase().catch((err) => {
  console.error('[Seed] Error seeding database:', err);
  process.exit(1);
});
