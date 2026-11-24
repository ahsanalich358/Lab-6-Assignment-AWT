import { db } from '../src/db/connection.js';
import { OrderProducer } from '../src/kafka/producer.js';
import { OrderConsumerService } from '../src/kafka/consumer.js';
import { localBroker } from '../src/kafka/client.js';
import { v4 as uuidv4 } from 'uuid';

/**
 * Apache Kafka Event-Driven Pipeline Automated Verification Test
 * Implements Lecture 18 Slide 11, 12, 13:
 * - Asynchronous Order Producer (202 Accepted)
 * - Key-based Partitioning (tenant_id as key guarantees FIFO order within partitions)
 * - Consumer Group with Offset Tracking
 * - At-Least-Once Delivery with Idempotent Consumer (Slide 13)
 */
async function runKafkaPipelineTests() {
  console.log('================================================================');
  console.log('⚡ RUNNING KAFKA EVENT-DRIVEN PIPELINE VERIFICATION TESTS');
  console.log('================================================================');

  await db.initialize();
  await OrderProducer.init();
  await OrderConsumerService.start();

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

  try {
    const tenantId = 'tenant-alpha';
    const testOrderId = `ord_test_k_${uuidv4().substring(0, 8)}`;

    // Get initial product inventory
    const initialProdRes = await db.query('SELECT inventory_count FROM products WHERE id = $1', ['prod-alpha-101']);
    const initialInventory = initialProdRes.rows[0].inventory_count;

    console.log(`[Setup] Initial inventory for prod-alpha-101: ${initialInventory}`);

    // TEST 1: Key Partitioning Consistency
    console.log('\n[Test 1] Key-Based Partitioning (tenant_id routing):');
    const part1 = localBroker.calculatePartition('tenant-alpha', 4);
    const part2 = localBroker.calculatePartition('tenant-alpha', 4);
    const part3 = localBroker.calculatePartition('tenant-beta', 4);

    assert(part1 === part2, `Tenant 'tenant-alpha' consistently hashes to partition ${part1}`);
    console.log(`  ℹ️  Tenant 'tenant-alpha' -> Partition ${part1} | Tenant 'tenant-beta' -> Partition ${part3}`);

    // TEST 2: Order Publication & Immediate Decoupling
    console.log('\n[Test 2] Order Producer Decoupling & 202 Event Dispatch:');
    const orderPayload = {
      id: testOrderId,
      tenant_id: tenantId,
      customer_name: 'Zeeshan Malik',
      customer_email: 'zeeshan.malik@cloud.pk',
      total_amount: 850.00,
      status: 'ACCEPTED',
      items: [
        {
          id: `item_${uuidv4().substring(0, 8)}`,
          product_id: 'prod-alpha-101',
          product_name: 'Enterprise Cloud Server',
          quantity: 2,
          unit_price: 850.00,
          subtotal: 1700.00
        }
      ]
    };

    const pubResult = await OrderProducer.publishOrderCreated(orderPayload);
    assert(pubResult.topic === 'tenant.orders', 'Event published to topic [tenant.orders]');
    assert(pubResult.partitionKey === tenantId, 'Event message key equals tenant_id');
    assert(Boolean(pubResult.eventId), `Generated unique event ID: ${pubResult.eventId}`);

    // TEST 3: Asynchronous Consumer Group Processing
    console.log('\n[Test 3] Consumer Group Execution & Inventory Update:');
    // Poll for consumer background execution (accommodating ~1200ms serverless cold-start)
    let orderCheck;
    const maxWaitMs = 5000;
    const pollIntervalMs = 250;
    let waitedMs = 0;

    while (waitedMs < maxWaitMs) {
      await new Promise((r) => setTimeout(r, pollIntervalMs));
      waitedMs += pollIntervalMs;
      orderCheck = await db.query('SELECT * FROM orders WHERE id = $1', [testOrderId]);
      if (orderCheck.rows.length > 0 && orderCheck.rows[0].status === 'COMPLETED') {
        break;
      }
    }

    assert(orderCheck.rows.length > 0, 'Order persisted in database');
    assert(orderCheck.rows[0].status === 'COMPLETED', `Order status updated to COMPLETED (took ${waitedMs}ms through Kafka & Serverless Cold-Start)`);

    // Verify inventory deducted
    const updatedProdRes = await db.query('SELECT inventory_count FROM products WHERE id = $1', ['prod-alpha-101']);
    const updatedInventory = updatedProdRes.rows[0].inventory_count;
    assert(updatedInventory === initialInventory - 2, `Product inventory deducted by 2 (from ${initialInventory} to ${updatedInventory})`);

    // TEST 4: Offset Tracking and Audit Recording
    console.log('\n[Test 4] Kafka Offset Tracking & Event Audit Logging:');
    const eventCheck = await db.query('SELECT * FROM processed_events WHERE event_id = $1', [pubResult.eventId]);
    assert(eventCheck.rows.length > 0, `Event [${pubResult.eventId}] recorded in processed_events table`);
    assert(eventCheck.rows[0].status === 'PROCESSED', 'Event state is PROCESSED');
    console.log(`  ℹ️  Committed Offset: ${eventCheck.rows[0].offset_val} on Partition: ${eventCheck.rows[0].partition_id}`);

    // TEST 5: Idempotency Protection (Duplicate Delivery Prevention - Lecture 18 Slide 13)
    console.log('\n[Test 5] Idempotent Consumer Protection Against Duplicate Deliveries:');
    const inventoryBeforeDuplicate = updatedInventory;

    // Simulate Kafka re-delivering the exact same event
    localBroker.emit('message:tenant.orders', {
      topic: 'tenant.orders',
      partition: eventCheck.rows[0].partition_id,
      message: {
        offset: (parseInt(eventCheck.rows[0].offset_val, 10) + 1).toString(),
        value: Buffer.from(JSON.stringify({
          eventId: pubResult.eventId, // SAME ID
          tenantId: tenantId,
          orderId: testOrderId,
          payload: orderPayload
        }))
      }
    });

    await new Promise((r) => setTimeout(r, 400));

    const postDuplicateProdRes = await db.query('SELECT inventory_count FROM products WHERE id = $1', ['prod-alpha-101']);
    const inventoryAfterDuplicate = postDuplicateProdRes.rows[0].inventory_count;
    assert(
      inventoryAfterDuplicate === inventoryBeforeDuplicate,
      `Idempotent guard succeeded: Inventory remained unchanged at ${inventoryAfterDuplicate} (No duplicate deduction occurred)`
    );

    console.log('\n================================================================');
    console.log(`🎉 ALL ${passedTests}/${totalTests} KAFKA PIPELINE TESTS PASSED!`);
    console.log('⚡ Event-Driven Pipeline & Idempotency: 100% Verified.');
    console.log('================================================================');

  } finally {
    await db.close();
  }
}

runKafkaPipelineTests().catch((err) => {
  console.error('[KafkaPipelineTest] Error:', err);
  process.exit(1);
});
