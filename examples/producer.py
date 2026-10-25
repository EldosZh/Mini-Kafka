from event_streaming import Producer

with Producer("127.0.0.1", 9092) as producer:
    metadata = producer.send(
        "orders", b'{"order_id":123}', key=b"user-42", acks="fsync"
    )
    print(metadata)
