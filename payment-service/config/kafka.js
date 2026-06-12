// payment-service/config/kafka.js
const { Kafka } = require('kafkajs');
const logger = require('./logger');

const KAFKA_URL = 'localhost:9092';

const kafka = new Kafka({ clientId: 'payment-service', brokers: [KAFKA_URL] });

let producer;
let consumer;

async function connectKafka() {
    try {
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
