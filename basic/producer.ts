import { Producer } from 'node-rdkafka';
import { v4 } from 'uuid';
import { partition, topic } from './config';

const producer = new Producer({
  'metadata.broker.list': 'localhost:9092',
  dr_cb: true,
  dr_msg_cb: true
});

producer.connect();

producer
  .on('ready', () => {
    try {
      const message = `Hello world with ${v4()}`;
      producer.produce(topic, partition, Buffer.from(message));
      console.log(message);
    } catch (error) {
      console.error(error);
    }
  })
  .on('delivery-report', (error, report) => {
    if (error) {
      console.error('Error delivering message');
      console.error(error);
    } else {
      console.log(`Delivery-report: ${JSON.stringify(report)}`);
    }

    process.exit();
  });

producer.setPollInterval(1000);
