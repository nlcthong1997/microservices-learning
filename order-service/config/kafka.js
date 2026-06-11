// config/kafka.js
const { Kafka } = require('kafkajs');
const logger = require('./logger');

const KAFKA_URL = 'localhost:9092';
let producer;

async function connectKafka() {
    try {
        const kafka = new Kafka({ clientId: 'order-service', brokers: [KAFKA_URL] });
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