import { KafkaConsumer } from 'node-rdkafka';
import { topic } from './config';

const consumer = new KafkaConsumer(
  {
    'group.id': 'kafka',
    'metadata.broker.list': 'localhost:9092'
  },
  {}
);

consumer.connect();

consumer
  .on('ready', () => {
    consumer.subscribe([topic]);
    consumer.consume();
    console.log('Subscribed and consumed!');
  })
  .on('data', (data) => {
    console.log(`Received!`);
    console.log(data.value?.toString());
  });
