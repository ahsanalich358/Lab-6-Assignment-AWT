import jwt from 'jsonwebtoken';
import { config } from '../config/index.js';

/**
 * JWT Authentication Middleware
 * Issues and validates tokens bearing tenant_id claims for enterprise users.
 */
export function generateTenantToken(user, tenantId) {
  return jwt.sign(
    {
      sub: user.id,
      email: user.email,
      name: user.name,
      tenant_id: tenantId,
      role: user.role || 'member'
    },
    config.jwtSecret,
    { expiresIn: '24h' }
  );
}

export function authenticateToken(req, res, next) {
  const authHeader = req.headers.authorization;
  const token = authHeader && authHeader.split(' ')[1];

  if (!token) {
    return res.status(401).json({
      error: 'AUTH_TOKEN_REQUIRED',
      message: 'Bearer token required for authenticated routes.'
    });
  }

  jwt.verify(token, config.jwtSecret, (err, user) => {
    if (err) {
      return res.status(403).json({
        error: 'INVALID_OR_EXPIRED_TOKEN',
        message: 'The provided authentication token is invalid or expired.'
      });
    }
    req.user = user;
    next();
  });
}
