import { Kafka } from 'kafkajs';
import { topic } from './config';

const kafka = new Kafka({
  clientId: 'kafka-playground-producer-client',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'kafka' });

(async () => {
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  console.log('Subscribed and consumed!');
  await consumer.run({
    eachMessage: async ({ message }) => {
      console.log(`Received!`);
      console.log({
        value: message?.value?.toString()
      });
    }
  });
})();
