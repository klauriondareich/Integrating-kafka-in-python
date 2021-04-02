# This Class allow to use these Producer and Consumer in another project
# To work both Producer and Consumer you need to install and import the Py-kafka Library

# Producer imports
from time import sleep
from json import dumps
from kafka import KafkaProducer

# Consumer imports
from kafka import KafkaConsumer
from json import loads

class ProducerAndConsumer:

    def __init__(self, kafka_topic="my_topic", bootstrap_servers=['localhost:9092']):
        self.topic = kafka_topic
        self.bootstrap_servers = bootstrap_servers
        
        # Producer instance config
        self.__producer_init =  KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda x: dumps(x).encode('utf-8'))

        # Consumer instance config
        self.__consumer_init = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='group_one',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
        )

    # Run the Producer
    def producer_run(self):
        producer = self.__producer_init
        message = "start"
        print("Producer is running ...")
        while message != "stop":
            message = input("test me : ")
            producer.send(self.topic, value=message)

    # Run the Consumer
    def consumer_run(self):
        consumer = self.__consumer_init
        print("Consumer is running ...")
        for message in consumer:
            message = message.value
            print('new message: ', message)    
        






 