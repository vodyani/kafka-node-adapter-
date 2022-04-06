import { AsyncClientAdapter, FixedContext } from '@vodyani/core';
import { ConsumerGroup, ConsumerGroupOptions, Message } from 'kafka-node';

export class ConsumerGroupAdapter implements AsyncClientAdapter {
  public instance: ConsumerGroup;

  constructor(
    options: ConsumerGroupOptions,
    topics: string[],
  ) {
    this.instance = new ConsumerGroup(options, topics);
  }

  @FixedContext
  public async close() {
    await new Promise((resolve, reject) => {
      this.instance.close((error: Error) => (error ? reject(error) : resolve(null)));
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
