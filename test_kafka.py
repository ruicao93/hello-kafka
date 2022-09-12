import sys

import confluent_kafka
from confluent_kafka import Consumer


c = Consumer({
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'testgroup',
    'enable.auto.commit': 'false',
    'auto.offset.reset': 'earliest',
    'debug': 'topic,metadata,protocol,cgrp'
})

topic = 'quickstart'
partition = 0
c.subscribe([topic])


def poll_message(csm):
    msg = csm.poll(2.0)

    if msg is None:
        return
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        return

    print('Received message: {}, offset: {}, partition: {}'.format(msg.value().decode('utf-8'), msg.offset(),
                                                                   msg.partition()))
    last_offset = csm.position([confluent_kafka.TopicPartition(topic=topic, partition=partition)])[0]
    print('Position: {}'.format(last_offset))


def poll():
    # c.assign([confluent_kafka.TopicPartition(topic=topic, partition=partition)])
    last_offset = c.position([confluent_kafka.TopicPartition(topic=topic, partition=partition)])[0]
    print('Position: {}'.format(last_offset))
    low_offset, high_offset = c.get_watermark_offsets(confluent_kafka.TopicPartition(topic=topic, partition=partition))
    print('Watermark: {} ==> {}'.format(low_offset, high_offset))
    print('================='.format(last_offset))
    while len(c.assignment()) == 0:
        msg = c.poll(100)
        print('Received message1: {}, offset: {}, partition: {}'.format(msg.value().decode('utf-8'), msg.offset(),
                                                                       msg.partition()))
    print('Partition: {}'.format(c.assignment()[0]))
    tpc = [confluent_kafka.TopicPartition(topic=topic, partition=partition)]
    c.pause(tpc)
    c.seek(confluent_kafka.TopicPartition(topic=topic, partition=partition, offset=5))
    # c.seek(confluent_kafka.TopicPartition(topic=topic, partition=partition, offset=5))
    c.resume(tpc)
    while True:
        poll_message(c)
    # poll_message()
    # poll_message()
    # c.seek(confluent_kafka.TopicPartition(topic=topic, partition=partition, offset=5))


if __name__ == '__main__':
    try:
        poll()
    except KeyboardInterrupt:
        print('Interrupted')
        c.close()
        sys.exit(0)
