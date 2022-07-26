import { Producer } from 'node-rdkafka';
import { partition, topic } from './config';
import inputEventEmitter from './inputEventEmitter';
import { readInput } from './util';

const producer = new Producer({
  'metadata.broker.list': 'localhost:29092',
  dr_cb: true,
  dr_msg_cb: true
});

producer.connect();

console.log('Loading Kafka producer...');

producer
  .on('ready', async () => {
    console.log(`Producer is ready`);

    // Trigger a new event to read input
    inputEventEmitter.emit('canInput');
  })
  .on('delivery-report', (error, report) => {
    if (error) {
      console.error('Error delivering message');
      console.error(error);
    } else {
      console.log(`Delivery-report: ${JSON.stringify(report)}`);
    }

    // Trigger a new event to read input
    inputEventEmitter.emit('canInput');
  });

producer.setPollInterval(1000);

// Read user input
inputEventEmitter.on('canInput', async () => {
  const input = await readInput('Message: ');

  if (input === 'exit') {
    console.log(`Terminating...`);
    process.exit();
  } else {
    producer.produce(topic, partition, Buffer.from(input));
  }
});
