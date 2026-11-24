import jwt from 'jsonwebtoken';
import { config } from '../config/index.js';
import { db } from '../db/connection.js';

// In-memory cache for fast tenant lookup to avoid repeating DB lookups on every single request
const tenantCache = new Map();

/**
 * Tenant Resolution Middleware
 * Implements Lecture 18 Slide 14 & 16: Multi-Tenant Architecture & Tenant Isolation
 * Resolution Hierarchy:
 * 1. Custom HTTP Header ('x-tenant-id')
 * 2. JWT Claim ('tenant_id' inside Bearer token)
 * 3. Subdomain extraction (e.g. 'alpha.saas.local' -> tenant with subdomain 'alpha')
 * 4. Query string ('?tenant_id=...')
 */
export async function tenantMiddleware(req, res, next) {
  try {
    let rawTenantId = null;

    // Strategy 1: Custom Header (x-tenant-id)
    if (req.headers['x-tenant-id']) {
      rawTenantId = req.headers['x-tenant-id'].toString().trim();
    }

    // Strategy 2: JWT Claim (Authorization header)
    if (!rawTenantId && req.headers.authorization && req.headers.authorization.startsWith('Bearer ')) {
      const token = req.headers.authorization.split(' ')[1];
      try {
        const decoded = jwt.verify(token, config.jwtSecret);
        if (decoded && decoded.tenant_id) {
          rawTenantId = decoded.tenant_id;
        }
      } catch (err) {
        // Token invalid or expired - skip to next strategy
      }
    }

    // Strategy 3: Subdomain Extraction
    if (!rawTenantId && req.headers.host) {
      const host = req.headers.host.split(':')[0]; // strip port
      const parts = host.split('.');
      if (parts.length > 2 || (parts.length === 2 && parts[1] === 'localhost')) {
        const subdomain = parts[0].toLowerCase();
        if (subdomain !== 'www' && subdomain !== 'api') {
          // Check if subdomain matches a tenant in cache or DB
          const tenantBySubdomain = await resolveTenantBySubdomain(subdomain);
          if (tenantBySubdomain) {
            rawTenantId = tenantBySubdomain.id;
          }
        }
      }
    }

    // Strategy 4: Fallback Query Param
    if (!rawTenantId && req.query.tenant_id) {
      rawTenantId = req.query.tenant_id.toString().trim();
    }

    // Enforce Tenant Boundary: If no tenant is resolved, reject immediately
    if (!rawTenantId) {
      return res.status(400).json({
        error: 'TENANT_IDENTIFICATION_REQUIRED',
        message: 'A valid tenant must be identified via X-Tenant-ID header, JWT claim, or subdomain.',
        documentation: 'Violations of tenant boundary result in zero access. Provide X-Tenant-ID header.'
      });
    }

    // Validate tenant existence
    const tenant = await resolveTenantById(rawTenantId);
    if (!tenant) {
      return res.status(404).json({
        error: 'TENANT_NOT_FOUND',
        message: `Tenant '${rawTenantId}' is not registered or is inactive.`
      });
    }

    // Bind tenant context to request
    req.tenant = tenant;
    req.tenantId = tenant.id;

    // Set response header for audit tracing
    res.setHeader('X-Tenant-Context', tenant.id);

    next();
  } catch (error) {
    console.error('[TenantMiddleware] Error during tenant resolution:', error);
    return res.status(500).json({
      error: 'TENANT_RESOLUTION_ERROR',
      message: 'Internal server error resolving tenant context.'
    });
  }
}

/**
 * Strict Tenant Boundary Enforcer
 * Guarantees that any record accessed belongs explicitly to req.tenantId.
 * If another tenant's ID is detected, halts execution and returns 403 Forbidden.
 */
export function assertTenantBoundary(req, targetTenantId) {
  if (!targetTenantId || targetTenantId !== req.tenantId) {
    const error = new Error(`CROSS_TENANT_VIOLATION: Active Tenant [${req.tenantId}] attempted to access data belonging to Tenant [${targetTenantId}]. Access Denied.`);
    error.statusCode = 403;
    error.code = 'CROSS_TENANT_VIOLATION';
    throw error;
  }
}

async function resolveTenantById(id) {
  if (tenantCache.has(id)) {
    return tenantCache.get(id);
  }
  const result = await db.query('SELECT * FROM tenants WHERE id = $1 AND status = $2', [id, 'active']);
  if (result.rows.length > 0) {
    const tenant = result.rows[0];
    tenantCache.set(tenant.id, tenant);
    return tenant;
  }
  return null;
}

async function resolveTenantBySubdomain(subdomain) {
  for (const [, tenant] of tenantCache.entries()) {
    if (tenant.subdomain === subdomain) return tenant;
  }
  const result = await db.query('SELECT * FROM tenants WHERE subdomain = $1 AND status = $2', [subdomain, 'active']);
  if (result.rows.length > 0) {
    const tenant = result.rows[0];
    tenantCache.set(tenant.id, tenant);
    return tenant;
  }
  return null;
}

export function clearTenantCache() {
  tenantCache.clear();
}
