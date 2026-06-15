// inventory-service/config/kafka.js
const { Kafka } = require('kafkajs');
const logger = require('./logger');

const KAFKA_URL = 'localhost:9092';

const kafka = new Kafka({ clientId: 'inventory-service', brokers: [KAFKA_URL] });

let producer;
let orderEventsConsumer;
let paymentEventsConsumer;

// =========================================================================
// Topics mà inventory-service cần tồn tại trước khi subscribe
//
// Tại sao cần làm điều này?
//   Kafka consumer.subscribe() trên một topic CHƯA TỒN TẠI → crash:
//   "This server does not host this topic-partition"
//
//   Nếu chỉ dựa vào Kafka auto-create: topic chỉ được tạo khi PRODUCER
//   gửi message lần đầu tiên. Nếu inventory-service start trước order-service
//   → topic 'order-events' chưa có → lỗi.
//
//   Fix: Dùng Admin API để assertTopics (tạo nếu chưa có, bỏ qua nếu đã có)
//   → Bất kỳ service nào start trước cũng được
// =========================================================================
async function ensureTopicsExist() {
    const admin = kafka.admin();
    await admin.connect();

    // Inventory-service subscribe 2 topic này — phải tồn tại trước khi subscribe
    // Inventory-service cũng publish 'inventory-events' — tạo luôn cho đồng bộ
    const topics = [
        { topic: 'order-events',     numPartitions: 1, replicationFactor: 1 },
        { topic: 'payment-events',   numPartitions: 1, replicationFactor: 1 },
        { topic: 'inventory-events', numPartitions: 1, replicationFactor: 1 },
    ];

    await admin.createTopics({
        topics,
        waitForLeaders: true, // chờ partition có leader trước khi return
    });

    await admin.disconnect();
    logger.info({ trace_id: 'SYSTEM', message: 'Kafka: topics ensured (order-events, payment-events, inventory-events).' });
}

async function connectKafka() {
    try {
        // Đảm bảo tất cả topics tồn tại TRƯỚC khi connect consumer
        await ensureTopicsExist();

        producer = kafka.producer();

        // 2 consumer groups riêng biệt để subscribe 2 topic khác nhau
        orderEventsConsumer   = kafka.consumer({ groupId: 'inventory-order-events-group' });
        paymentEventsConsumer = kafka.consumer({ groupId: 'inventory-payment-events-group' });

        await producer.connect();
        await orderEventsConsumer.connect();
        await paymentEventsConsumer.connect();

        logger.info({ trace_id: 'SYSTEM', message: 'Kafka infrastructure ready (inventory config).' });
    } catch (error) {
        logger.error({ trace_id: 'SYSTEM', message: `Kafka connection error: ${error.message}` });
        throw error;
    }
}

module.exports = {
    connectKafka,
    getKafkaProducer:          () => producer,
    getOrderEventsConsumer:    () => orderEventsConsumer,
    getPaymentEventsConsumer:  () => paymentEventsConsumer,
};
