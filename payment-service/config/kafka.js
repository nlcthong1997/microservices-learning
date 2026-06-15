// payment-service/config/kafka.js
const { Kafka } = require('kafkajs');
const logger = require('./logger');

const KAFKA_URL = 'localhost:9092';

const kafka = new Kafka({ clientId: 'payment-service', brokers: [KAFKA_URL] });

let producer;
let consumer;

async function ensureTopicsExist() {
    const admin = kafka.admin();
    await admin.connect();

    await admin.createTopics({
        topics: [
            { topic: 'inventory-events', numPartitions: 1, replicationFactor: 1 },
            { topic: 'payment-events',   numPartitions: 1, replicationFactor: 1 },
        ],
        waitForLeaders: true,
    });

    await admin.disconnect();
    logger.info({ trace_id: 'SYSTEM', message: 'Kafka: topics ensured (inventory-events, payment-events).' });
}

async function connectKafka() {
    try {
        await ensureTopicsExist();

        producer = kafka.producer();
        consumer = kafka.consumer({ groupId: 'payment-service-group' });

        await producer.connect();
        await consumer.connect();

        logger.info({ trace_id: 'SYSTEM', message: 'Kafka infrastructure ready (payment config).' });
    } catch (error) {
        logger.error({ trace_id: 'SYSTEM', message: `Kafka connection error: ${error.message}` });
        throw error;
    }
}

module.exports = {
    connectKafka,
    getKafkaProducer: () => producer,
    getKafkaConsumer: () => consumer,
};
