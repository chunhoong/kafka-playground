import { KafkaConsumer, LibrdKafkaError, Message, Producer, TopicPartitionOffset } from 'node-rdkafka';
import { v4 as uuidV4 } from 'uuid';
import { broker, partition, topic } from './config';

jest.setTimeout(20000);

let consumer: KafkaConsumer;
let producer: Producer;

const message = `Hello world!: ${uuidV4()}`;

const consumedMessages: Array<string | undefined> = [];

const waitFor = (delayInMilliseconds: number) => {
  return new Promise((resolve) => {
    setTimeout(resolve, delayInMilliseconds);
  });
};

const isMessageConsumed = async (predicate: () => boolean, checkOnceEvery: number, checkFor: number) => {
  let elapsed = 0;
  let received = undefined;
  while (!received && elapsed < checkFor) {
    received = predicate();
    await waitFor(checkOnceEvery);
    elapsed += checkOnceEvery;
  }
  return received;
};

const initConsumer = () => {
  consumer = new KafkaConsumer(
    {
      'group.id': 'kafka',
      'metadata.broker.list': broker
    },
    { 'auto.offset.reset': 'earliest' }
  );

  consumer.connect();

  consumer
    .on('ready', async () => {
      consumer.subscribe([topic]);
      console.log('Subscribed and consumed!');
      consumer.consume((error: LibrdKafkaError, messages: Message[] | Message) => {
        console.log(`Received from consume callback!`);
        if (error) {
          console.error(error);
        } else {
          if (Array.isArray(messages)) {
            messages.forEach((message) => {
              console.log({ ...message, value: message.value?.toString() });
              consumedMessages.push(message.value?.toString());
            });
          } else {
            console.log(`Single message found!`);
            const message = messages as Message;
            console.log({ ...message, value: message.value?.toString() });
            consumedMessages.push(message.value?.toString());
          }
          consumer.commit(messages);
        }
      });
    })
    .on('event.error', (error: LibrdKafkaError) => {
      console.error('Oops consumer error');
      console.error(error);
    });
};

const initProducer = () => {
  producer = new Producer({
    'metadata.broker.list': broker
  });

  producer.connect();

  console.log('Loading Kafka producer...');

  producer
    .on('ready', async () => {
      console.log(`Producer is ready`);
      producer.produce(topic, partition, Buffer.from(message));
    })
    .on('event.error', (error: LibrdKafkaError) => {
      console.error('Oops producer error');
      console.error(error);
    });

  producer.setPollInterval(1000);
};

afterAll(() => {
  producer.disconnect();
  consumer.unsubscribe().disconnect();
});

describe('ProduceAndConsume', () => {
  it('should publish and consume', async () => {
    initConsumer();
    initProducer();
    await expect(isMessageConsumed(() => consumedMessages.includes(message), 300, 15000)).resolves.toBeTruthy();
  });
});
