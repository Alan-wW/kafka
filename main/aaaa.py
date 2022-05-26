#!/bin/env python
from kafka import KafkaConsumer
import msgpack

# connect to Kafka server and pass the topic we want to consume
consumer = KafkaConsumer('xingbin_test',  bootstrap_servers=['42.193.99.39:9092'])
try:
    for msg in consumer:
        aa = msgpack.loads(msg.value)
        print(aa)
        # print("%s:%d:%d: key=%s value=%s" % (msg.topic, msg.partition, msg.offset, msg.key, msg.value))
except KeyboardInterrupt as e:
    print(e)
