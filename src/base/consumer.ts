import {
  Consumer,
  KafkaClient,
  ConsumerOptions,
  OffsetFetchRequest,
  KafkaClientOptions,
  Message,
} from 'kafka-node';
import { AsyncClientAdapter, FixedContext } from '@vodyani/core';

export class ConsumerAdapter implements AsyncClientAdapter {
  public instance: Consumer;

  constructor(
    options: KafkaClientOptions,
    offsetFetchRequest: Array<OffsetFetchRequest | string>,
    consumerOptions: ConsumerOptions,
  ) {
    const kafkaClient = new KafkaClient(options);
    this.instance = new Consumer(kafkaClient, offsetFetchRequest, consumerOptions);
  }

  @FixedContext
  public async close() {
    await new Promise((resolve) => {
      this.instance.close(() => resolve(null));
    });
  }

  @FixedContext
  public subscribe(topic: string, callback: (payload: Message) => Promise<any>) {
    this.instance.on('message', (payload: Message) => {
      if (payload.topic === topic) {
        callback(payload);
      }
    });
  }
}
