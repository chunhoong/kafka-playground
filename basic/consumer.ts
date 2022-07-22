import { KafkaConsumer, LibrdKafkaError, Message } from 'node-rdkafka';
import { partition, topic } from './config';

const consumer = new KafkaConsumer(
  {
    'group.id': 'kafka',
    'metadata.broker.list': 'localhost:9092'
  },
  {}
);

consumer.connect();

consumer.on('ready', async () => {
  consumer.subscribe([topic]);
  console.log('Subscribed and consumed!');
  consumer.consume(onReceived);
});

const onReceived = (error: LibrdKafkaError, messages: Message[] | Message) => {
  console.log(`Received from consume callback!`);
  if (error) {
    console.error(error);
  } else {
    if (Array.isArray(messages)) {
      messages.forEach((message) => {
        console.log({ ...message, value: message.value?.toString() });
      });
    } else {
      console.log(`Single message found!`);
      const message = messages as Message;
      console.log({ ...message, value: message.value?.toString() });
    }
    consumer.commit(messages);
  }
};
