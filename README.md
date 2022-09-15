

# Termination requirements

```angular2html
For C, make sure the following objects are destroyed prior to calling rd_kafka_consumer_close() and rd_kafka_destroy():

rd_kafka_message_t
rd_kafka_topic_t
rd_kafka_topic_partition_t
rd_kafka_topic_partition_list_t
rd_kafka_event_t
rd_kafka_queue_t
For C++ make sure the following objects are deleted prior to calling KafkaConsumer::close() and delete on the Consumer, KafkaConsumer or Producer handle:

Message
Topic
TopicPartition
Event
Queue
```

