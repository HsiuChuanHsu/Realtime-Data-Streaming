from confluent_kafka import Consumer

class KafkaConsumer():
    def __init__(self, bootstrap_server, group_id, topic):
        self.bootstrap_server = bootstrap_server
        self.group_id = group_id
        self.topic = topic
        self.consumer = Consumer({
                'bootstrap.servers': self.bootstrap_server
                , 'group.id': self.group_id
                # , 'auto.offset.reset': 'earliest'
            })
        
    def consumer_message(self):
        c = self.consumer
        c.subscribe([self.topic])
        try:
            while True:
                print("LISTENING.......")
                msg = c.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    print("Consumer error: {}".format(msg.error()))
                    continue

                print('Received message: {}'.format(msg.value().decode('utf-8')))
        except:
            pass
        c.close()

if __name__ == "__main__":
    bootstrap_server = "localhost:9092"
    group_id = "mygroup"
    topic = "users_created"

    consumer = KafkaConsumer(bootstrap_server, group_id, topic)
    consumer.consumer_message()    
