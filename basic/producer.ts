import { Kafka } from 'kafkajs';
import { partition, topic } from './config';
import inputEventEmitter from './inputEventEmitter';
import { readInput } from './util';

const kafka = new Kafka({
  clientId: 'kafka-playground-producer-client',
  brokers: ['localhost:9092']
});

const producer = kafka.producer();

(async () => {
  console.log('Loading Kafka producer...');
  await producer.connect();
  console.log(`Producer is ready`);
  // Trigger a new event to read input
  inputEventEmitter.emit('canInput');
})();

// Read user input
inputEventEmitter.on('canInput', async () => {
  const input = await readInput('Message: ');

  if (input === 'exit') {
    console.log(`Terminating...`);
    process.exit();
  } else {
    try {
      const metadata = await producer.send({
        topic,
        messages: [{ value: Buffer.from(input) }]
      });
      console.log(metadata);
    } catch (error) {
      console.error(error);
    } finally {
      inputEventEmitter.emit('canInput');
    }
  }
});
