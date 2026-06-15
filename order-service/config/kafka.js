// config/kafka.js
const { Kafka } = require('kafkajs');
const logger = require('./logger');

const KAFKA_URL = 'localhost:9092';

const kafka = new Kafka({ clientId: 'order-service', brokers: [KAFKA_URL] });
let producer;

async function ensureTopicsExist() {
    const admin = kafka.admin();
    await admin.connect();

    await admin.createTopics({
        topics: [
            { topic: 'order-events',       numPartitions: 1, replicationFactor: 1 },
            { topic: 'user-behavior-logs', numPartitions: 1, replicationFactor: 1 },
        ],
        waitForLeaders: true,
    });

    await admin.disconnect();
    logger.info({ trace_id: 'SYSTEM', message: 'Kafka: topics ensured (order-events, user-behavior-logs).' });
}

async function connectKafka() {
    try {
        await ensureTopicsExist();

        producer = kafka.producer();
        await producer.connect();

        logger.info({ trace_id: 'SYSTEM', message: 'Kafka infrastructure ready (order config).' });
        return producer;
    } catch (error) {
        logger.error({ trace_id: 'SYSTEM', message: `Kafka connection error: ${error.message}` });
        throw error;
    }
}

// Export một function để lấy producer đã connect
module.exports = { connectKafka, getKafkaProducer: () => producer };