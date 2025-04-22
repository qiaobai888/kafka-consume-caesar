import { Kafka } from 'kafkajs';

const mechanism = 'scram-sha-512';

const url = import.meta.env.KAFKA_URL;
if (!url) {
  throw new Error('KAFKA_URL is not set');
}
const username = import.meta.env.KAFKA_USERNAME;
if (!username) {
  throw new Error('KAFKA_USERNAME is not set');
}
const password = import.meta.env.KAFKA_PASSWORD;
if (!password) {
  throw new Error('KAFKA_PASSWORD is not set');
}

const kafka = new Kafka({
  clientId: 'edwardb-debugger',
  brokers: [url],
  ssl: true,
  sasl: {
    mechanism,
    username,
    password,
  },
});

const topic = '<topic_name_here>';

const consumer = kafka.consumer({ groupId: 'caesar-debugger' });

await consumer.connect();

await consumer.subscribe({ topic, fromBeginning: true });

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

consumer.run({
  autoCommit: false,
  eachMessage: async ({ message }) => {
    const cmd = JSON.parse(message.value?.toString() || '{}');
    const conditions = true; // change condition here
    if (conditions) {
      console.log(JSON.stringify({
        timestamp: new Date(Number(message.timestamp)).toLocaleString(),
        offset: message.offset,
        cmd,
      }));
    }
  },
});
consumer.seek({ topic, partition: 0, offset: '6942002496' });
