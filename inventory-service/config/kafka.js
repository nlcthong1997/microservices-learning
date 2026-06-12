// inventory-service/config/kafka.js
const { Kafka } = require('kafkajs');
const logger = require('./logger');

const KAFKA_URL = 'localhost:9092';

const kafka = new Kafka({ clientId: 'inventory-service', brokers: [KAFKA_URL] });

let producer;
let orderEventsConsumer;
let paymentEventsConsumer;

async function connectKafka() {
    try {
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
