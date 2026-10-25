from event_streaming import Consumer

with Consumer(topic="orders", group_id="analytics", from_beginning=True) as consumer:
    for record in consumer:
        print(record.partition, record.offset, record.value)
        consumer.commit(record)
