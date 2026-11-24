import { createApp } from '../src/app.js';
import { db } from '../src/db/connection.js';

/**
 * Tenant Isolation Automated Verification Test Suite
 * Implements Lecture 18 Slide 14 & 16:
 * - Tenant A must NEVER access Tenant B's data (Violations = 0 marks).
 * - Automatic query scoping with Tenant Context Discriminator.
 * - Cross-tenant read, write, and reference blocking.
 */
async function runTenantIsolationTests() {
  console.log('================================================================');
  console.log('🛡️ RUNNING TENANT ISOLATION VERIFICATION TESTS');
  console.log('================================================================');

  await db.initialize();
  const app = createApp();

  let passedTests = 0;
  let totalTests = 0;

  function assert(condition, message) {
    totalTests++;
    if (condition) {
      console.log(`  ✅ PASS: ${message}`);
      passedTests++;
    } else {
      console.error(`  ❌ FAIL: ${message}`);
      throw new Error(`Test assertion failed: ${message}`);
    }
  }

  // Helper function to simulate HTTP requests without external server
  async function testRequest({ method, path, headers = {}, body = null }) {
    return new Promise((resolve) => {
      const req = {
        method,
        url: path,
        headers: {
          'content-type': 'application/json',
          ...headers
        },
        query: {},
        body: body || {},
        params: {}
      };

      // Extract query params if present
      if (path.includes('?')) {
        const [urlPath, queryString] = path.split('?');
        req.url = urlPath;
        const searchParams = new URLSearchParams(queryString);
        for (const [k, v] of searchParams.entries()) {
          req.query[k] = v;
        }
      }

      const res = {
        statusCode: 200,
        headers: {},
        setHeader(k, v) { this.headers[k.toLowerCase()] = v; },
        status(code) { this.statusCode = code; return this; },
        json(data) {
          this.body = data;
          resolve({ status: this.statusCode, body: data, headers: this.headers });
        },
        send(data) {
          this.body = data;
          resolve({ status: this.statusCode, body: data, headers: this.headers });
        }
      };

      app.handle(req, res, (err) => {
        if (err) {
          const code = err.statusCode || 500;
          resolve({ status: code, body: { error: err.code, message: err.message } });
        } else {
          resolve({ status: 404, body: { error: 'NOT_FOUND' } });
        }
      });
    });
  }

  try {
    // TEST 1: Request with NO Tenant identifier must be rejected with 400
    console.log('\n[Test 1] Missing Tenant Identification Enforcement:');
    const resNoTenant = await testRequest({
      method: 'GET',
      path: '/api/v1/orders'
    });
    assert(resNoTenant.status === 400, 'Request without tenant identifier rejected with HTTP 400');
    assert(resNoTenant.body.error === 'TENANT_IDENTIFICATION_REQUIRED', 'Error code specifies TENANT_IDENTIFICATION_REQUIRED');

    // TEST 2: Tenant Alpha querying orders only sees Tenant Alpha orders
    console.log('\n[Test 2] Automatic Tenant Data Scoping (Tenant Alpha):');
    const resAlphaOrders = await testRequest({
      method: 'GET',
      path: '/api/v1/orders?limit=10',
      headers: { 'x-tenant-id': 'tenant-alpha' }
    });
    assert(resAlphaOrders.status === 200, 'Tenant Alpha orders request returns HTTP 200');
    const alphaOrders = resAlphaOrders.body.orders || [];
    assert(alphaOrders.length > 0, `Tenant Alpha retrieved ${alphaOrders.length} orders`);
    const crossTenantLeaked = alphaOrders.filter(o => o.tenant_id !== 'tenant-alpha');
    assert(crossTenantLeaked.length === 0, 'Zero orders belong to other tenants (100% scoped to tenant-alpha)');

    // TEST 3: Tenant Beta querying orders only sees Tenant Beta orders
    console.log('\n[Test 3] Automatic Tenant Data Scoping (Tenant Beta):');
    const resBetaOrders = await testRequest({
      method: 'GET',
      path: '/api/v1/orders?limit=10',
      headers: { 'x-tenant-id': 'tenant-beta' }
    });
    assert(resBetaOrders.status === 200, 'Tenant Beta orders request returns HTTP 200');
    const betaOrders = resBetaOrders.body.orders || [];
    assert(betaOrders.length > 0, `Tenant Beta retrieved ${betaOrders.length} orders`);
    const betaLeaked = betaOrders.filter(o => o.tenant_id !== 'tenant-beta');
    assert(betaLeaked.length === 0, 'Zero orders belong to other tenants (100% scoped to tenant-beta)');

    // TEST 4: Cross-Tenant Boundary Violation: Tenant A attempts to fetch Tenant B specific order by ID
    console.log('\n[Test 4] Direct Cross-Tenant ID Tampering (Tenant A accesses Tenant B Order):');
    const targetBetaOrder = betaOrders[0];
    const resCrossTenantAttempt = await testRequest({
      method: 'GET',
      path: `/api/v1/orders/${targetBetaOrder.id}`,
      headers: { 'x-tenant-id': 'tenant-alpha' } // Authenticated as Alpha, requesting Beta's order!
    });
    assert(resCrossTenantAttempt.status === 403, 'Cross-tenant access attempt strictly BLOCKED with HTTP 403 Forbidden');
    assert(resCrossTenantAttempt.body.error === 'CROSS_TENANT_VIOLATION', 'Violation explicitly flagged as CROSS_TENANT_VIOLATION');

    // TEST 5: Cross-Tenant Product Injection: Tenant A attempts to order Tenant B product
    console.log('\n[Test 5] Cross-Tenant Resource Ordering Prevention:');
    const resCrossProduct = await testRequest({
      method: 'POST',
      path: '/api/v1/orders',
      headers: { 'x-tenant-id': 'tenant-alpha' },
      body: {
        customer_name: 'Attacker Alpha',
        customer_email: 'attacker@alpha.com',
        items: [
          { product_id: 'prod-beta-201', quantity: 2 } // Belongs to Tenant Beta!
        ]
      }
    });
    assert(resCrossProduct.status === 400, 'Order with cross-tenant product rejected with HTTP 400 Bad Request');
    assert(resCrossProduct.body.error === 'PRODUCT_NOT_FOUND_OR_CROSS_TENANT', 'Cross-tenant product reference detected and stopped');

    // TEST 6: Subdomain Tenant Resolution Check
    console.log('\n[Test 6] Subdomain-based Tenant Context Extraction:');
    const resSubdomain = await testRequest({
      method: 'GET',
      path: '/api/v1/orders?limit=5',
      headers: { 'host': 'alpha.localhost:4000' }
    });
    assert(resSubdomain.status === 200, 'Subdomain host [alpha.localhost] resolved successfully');
    assert(resSubdomain.headers['x-tenant-context'] === 'tenant-alpha', 'Tenant context bound to tenant-alpha via subdomain');

    console.log('\n================================================================');
    console.log(`🎉 ALL ${passedTests}/${totalTests} TENANT ISOLATION TESTS PASSED!`);
    console.log('🛡️ Data Boundary Verification: 100% Compliant (Zero Cross-Tenant Leaks).');
    console.log('================================================================');

  } finally {
    await db.close();
  }
}

runTenantIsolationTests().catch((err) => {
  console.error('[TenantIsolationTest] Error:', err);
  process.exit(1);
});
