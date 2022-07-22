import { KafkaConsumer, LibrdKafkaError, Message, TopicPartition, TopicPartitionOffset } from 'node-rdkafka';
import { partition, topic } from './config';

const consumer = new KafkaConsumer(
  {
    'group.id': 'kafka',
    'metadata.broker.list': 'localhost:9092',
    offset_commit_cb: true
  },
  { 'auto.offset.reset': 'earliest' }
);

consumer.connect();

consumer
  .on('ready', async () => {
    consumer.subscribe([topic]);
    console.log('Subscribed and consumed!');
    consumer.consume(onReceived);
  })
  .on('offset.commit', (error, topicPartitions: TopicPartitionOffset[]) => {
    if (error) {
      console.error('Hmmmm');
      console.error(error);
    } else {
      console.log(`Commited`);
      console.log(topicPartitions);
    }
  })
  .on('event.error', (error: LibrdKafkaError) => {
    console.error('Oops');
    console.error(error);
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
