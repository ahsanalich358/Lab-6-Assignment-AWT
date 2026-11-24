import PDFDocument from 'pdfkit';
import fs from 'fs';
import path from 'path';
import { db } from '../db/connection.js';
import { config } from '../config/index.js';

/**
 * Serverless PDF Invoice Generation Worker
 * Implements Lecture 17 Slide 8 & 9: Serverless Paradigm & Cold Start Analysis
 * Compatible with Vercel Serverless Functions and AWS Lambda handler signatures.
 * 
 * Container Lifecycle Instrumentation:
 * - Cold Start: Container allocation, Node.js runtime boot, asset loading (~1200ms)
 * - Warm Start: Reused in-memory execution context (~30-50ms)
 */

// Module-level global container cache to track warm container reuse
let containerInstance = null;

export async function processInvoiceGeneration(invoiceRequest) {
  const requestStartTime = performance.now();
  let isColdStart = false;
  let initDurationMs = 0;

  // 1. Cold Start Detection & Container Boot Simulation
  if (!containerInstance) {
    isColdStart = true;
    const initStart = performance.now();
    
    // Simulate container boot, heavy PDF engine loading, and TLS handshake (Lecture 17 Slide 9)
    await new Promise((resolve) => setTimeout(resolve, config.serverless.coldStartBaseDelayMs));

    containerInstance = {
      containerId: `lambda_${Math.random().toString(36).substring(2, 10)}`,
      bootTime: new Date().toISOString(),
      invocationCount: 0
    };
    initDurationMs = performance.now() - initStart;
  }

  containerInstance.invocationCount++;

  // 2. Execution Phase (Warm Workload)
  const execStart = performance.now();

  const { tenantId, order } = invoiceRequest;
  const invoicesDir = path.resolve(process.cwd(), 'data', 'invoices');
  if (!fs.existsSync(invoicesDir)) {
    fs.mkdirSync(invoicesDir, { recursive: true });
  }

  const invoiceNumber = `INV-${order.id.substring(0, 8).toUpperCase()}`;
  const pdfFileName = `${tenantId}_${invoiceNumber}.pdf`;
  const pdfFilePath = path.join(invoicesDir, pdfFileName);

  // Generate Actual PDF using PDFKit
  await new Promise((resolve, reject) => {
    try {
      const doc = new PDFDocument({ margin: 40 });
      const writeStream = fs.createWriteStream(pdfFilePath);
      doc.pipe(writeStream);

      // Header Banner
      doc.rect(0, 0, doc.page.width, 60).fill('#1E293B');
      doc.fillColor('#FFFFFF').fontSize(20).text('TAX INVOICE', 40, 20);
      doc.fontSize(10).text(`Tenant: ${tenantId.toUpperCase()}`, doc.page.width - 200, 25);

      // Order Information
      doc.moveDown(3);
      doc.fillColor('#0F172A').fontSize(12).text(`Invoice Number: ${invoiceNumber}`, 40, 80);
      doc.text(`Order ID: ${order.id}`);
      doc.text(`Date: ${new Date().toLocaleDateString()}`);
      doc.text(`Billed To: ${order.customer_name} (${order.customer_email})`);
      doc.text(`Status: COMPLETED (Paid via SaaS Escrow)`);

      doc.moveDown(1.5);
      doc.fontSize(14).text('Purchased Items', { underline: true });
      doc.moveDown(0.5);

      // Table Header
      let y = doc.y;
      doc.fontSize(10).fillColor('#475569');
      doc.text('Item Description', 40, y);
      doc.text('Qty', 320, y);
      doc.text('Unit Price', 380, y);
      doc.text('Total', 480, y);
      doc.moveTo(40, y + 15).lineTo(550, y + 15).stroke('#CBD5E1');

      y += 25;
      doc.fillColor('#0F172A');
      const items = order.items || [];
      for (const item of items) {
        doc.text(item.product_name || 'Standard Product', 40, y);
        doc.text(String(item.quantity || 1), 320, y);
        doc.text(`$${parseFloat(item.unit_price || 0).toFixed(2)}`, 380, y);
        doc.text(`$${parseFloat(item.subtotal || 0).toFixed(2)}`, 480, y);
        y += 20;
      }

      // Summary
      doc.moveTo(40, y + 5).lineTo(550, y + 5).stroke('#CBD5E1');
      y += 15;
      doc.fontSize(12).text(`Grand Total: $${parseFloat(order.total_amount || 0).toFixed(2)}`, 380, y, { bold: true });

      // Cold Start Telemetry Stamp
      doc.moveDown(3);
      doc.fontSize(8).fillColor('#64748B').text(
        `Serverless Execution Telemetry: Container [${containerInstance.containerId}] | Invocations: [${containerInstance.invocationCount}] | Cold Start: [${isColdStart}] | Execution Time: [${parseFloat((performance.now() - execStart).toFixed(2))}ms]`,
        40,
        doc.page.height - 50
      );

      doc.end();
      writeStream.on('finish', resolve);
      writeStream.on('error', reject);
    } catch (err) {
      reject(err);
    }
  });

  const executionDurationMs = performance.now() - execStart;
  const totalLatencyMs = performance.now() - requestStartTime;

  // Persist invoice record in database
  const invoiceId = `inv_${Math.random().toString(36).substring(2, 12)}`;
  await db.query(
    `INSERT INTO invoices (id, tenant_id, order_id, invoice_number, pdf_path, cold_start, init_duration_ms, execution_duration_ms, total_latency_ms)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
     ON CONFLICT (id) DO NOTHING`,
    [
      invoiceId,
      tenantId,
      order.id,
      invoiceNumber,
      pdfFilePath,
      isColdStart,
      parseFloat(initDurationMs.toFixed(2)),
      parseFloat(executionDurationMs.toFixed(2)),
      parseFloat(totalLatencyMs.toFixed(2))
    ]
  );

  return {
    invoiceId,
    invoiceNumber,
    pdfFilePath,
    containerId: containerInstance.containerId,
    invocationCount: containerInstance.invocationCount,
    telemetry: {
      coldStart: isColdStart,
      initializationMs: parseFloat(initDurationMs.toFixed(2)),
      executionMs: parseFloat(executionDurationMs.toFixed(2)),
      totalLatencyMs: parseFloat(totalLatencyMs.toFixed(2)),
      phaseBreakdown: isColdStart 
        ? `Cold Start: ~${Math.round(initDurationMs)}ms boot + ~${Math.round(executionDurationMs)}ms execution`
        : `Warm Start: 0ms boot + ~${Math.round(executionDurationMs)}ms execution`
    }
  };
}

/**
 * Resets container state to deliberately trigger a Cold Start (for benchmarking)
 */
export function forceColdStartReset() {
  containerInstance = null;
  console.log('[ServerlessWorker] Container purged from memory. Next invocation will experience a COLD START.');
}

/**
 * Vercel Serverless Function Export Handler
 */
export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(455).json({ error: 'Method Not Allowed' });
  }
  try {
    const { tenantId, order } = req.body;
    const result = await processInvoiceGeneration({ tenantId, order });
    return res.status(200).json(result);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
}
