import {
  Producer,
  KafkaClient,
  ProduceRequest,
  ProducerOptions,
  CustomPartitioner,
  KafkaClientOptions,
} from 'kafka-node';
import { AsyncClientAdapter, FixedContext, getDefaultNumber, isValid } from '@vodyani/core';

export class ProducerAdapter implements AsyncClientAdapter<Producer> {
  public instance: Producer;

  constructor(
    options?: KafkaClientOptions,
    produceOptions?: ProducerOptions,
    customPartitioner?: CustomPartitioner,
  ) {
    const kafkaClient = new KafkaClient(options);
    this.instance = new Producer(kafkaClient, produceOptions, customPartitioner);
  }

  @FixedContext
  public async close() {
    await new Promise((resolve) => {
      this.instance.close(() => resolve(null));
    });
  }

  @FixedContext
  public async publish(payload: ProduceRequest, partition?: number): Promise<any> {
    const client = this.instance;

    if (!isValid(client)) {
      return null;
    }

    const size = getDefaultNumber(partition);

    payload.partition = Math.floor(Math.random() * size);

    const result = await new Promise((resolve, reject) => {
      client.send(
        [payload],
        (error: Error, data: any) => (error ? reject(error) : resolve(data)),
      );
    });

    return result;
  }
}
