import signal
import sys

import confluent_kafka
from confluent_kafka import Consumer


def on_assign(consumer, partitions):
    print("on_assign get called")


c = Consumer({
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'testgroup',
    'enable.auto.commit': 'false',
    'auto.offset.reset': 'earliest',
    'debug': 'topic,metadata,protocol,cgrp,consumer,broker'
})

topic = 'quickstart'
partition = 0
#c.subscribe([topic], on_assign=on_assign)
c.subscribe([topic])


def poll_message(csm):
    msg = csm.poll(2.0)

    if msg is None:
        return True
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        return True
    msg_val = msg.value().decode('utf-8')
    print('Received message: {}, offset: {}, partition: {}'.format(msg_val, msg.offset(),
                                                                   msg.partition()))
    last_offset = csm.position([confluent_kafka.TopicPartition(topic=topic, partition=partition)])[0]
    print('Position: {}'.format(last_offset))
    if msg_val == "exit":
        input("Press any key to commit the last message: ")
        csm.commit(msg)
        return False
    return True

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
    # c.seek(confluent_kafka.TopicPartition(topic=topic, partition=partition, offset=5))
    # c.seek(confluent_kafka.TopicPartition(topic=topic, partition=partition, offset=5))
    c.resume(tpc)
    run = True
    while run:
        run = poll_message(c)
    # poll_message()
    # poll_message()
    # c.seek(confluent_kafka.TopicPartition(topic=topic, partition=partition, offset=5))


def signal_handler(signum, frame):
    print('Signal handler called with signal', signum)
    if c:
        c.close()
    sys.exit(1)


if __name__ == '__main__':
    try:
        # Set the signal handler and a 5-second alarm
        signal.signal(signal.SIGINT, signal_handler)
        poll()
        input("Press any key to close the consumer: ")
        c.close()
        c = None
    except KeyboardInterrupt:
        print('Interrupted')
        if c:
            c.close()
        sys.exit(0)
