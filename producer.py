from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from schema import Schema
from logger import logger
import json


class Producer(object):
    def __init__(self, bootstrap_servers, schema_registry_url, topic, partitions=10, replication=3):
        self.servers = bootstrap_servers
        self.registry_url = schema_registry_url
        self.topic = topic
#         self.partitions = partitions
#         self.replication = replication
        logger.info("Initialized producer object with params:")
        logger.info("servers:\t{}".format(self.servers))
        logger.info("registry url:\t{}".format(self.registry_url))
        logger.info("topic:\t{}".format(topic))
        
        
    def producer(self):
        """
            Create and return avro producer object.
        """
        try:
            schema = Schema(self.registry_url, self.topic)
            schema_json = schema.get_latest_schema()
            avroProducer = AvroProducer({ \
            'bootstrap.servers': '{}'.format(self.servers), \
            'schema.registry.url': 'http://{}'.format(self.registry_url) \
            }, default_value_schema=avro.loads(schema_json['schema']))
            return avroProducer
        except Exception as e:
            return format(e)
        

        
    def publish(self,key,values):
        """
            This method produces kafka messages.
            Serializes only the values.
            value are python dict()
            
        """
        try:
            avroProducer = self.producer()
            logger.info("\tPublishing data..............................................")
            avroProducer.produce(topic=self.topic, value=values)
            logger.info('\tData published successfully!')
            avroProducer.poll(10)
            return "Success"  
        except Exception as e:
            return format(e)

        
if __name__ == '__main__':
    bootstrap_servers = '10.128.0.8,10.128.0.9,10.128.0.10'
    topic = 'redshift'
    schema_registry_url = '10.128.0.9:8081'
    producer = Producer(bootstrap_servers, schema_registry_url, topic)
    value = {"name": "Value", "contact":3,"favorite_color":"green","address":"Pune"}
    key = {"name":"chutiya","favorite_number":3,"favorite_color":"green","address":"Singapore"}
    print(producer.publish(key,value))

# from time import sleep

# producer = KafkaProducer(bootstrap_servers=['10.128.0.8:9092','10.128.0.9:9092','10.128.0.10:9092'])

# for i in range(0,100):
#     producer.send(topic='redshift',key=bytes('name',encoding='utf-8'),value=bytes(str(i),encoding='utf-8'))
#     print('sending {}th record to topic redshift'.format(i))
#     sleep(3)


    
# def connect(bootstrap_servers=['localhost:9092']):
#     try:
#         producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
#         if producer.bootstrap_servers():
#             return producer
#     except Exception as e:
#         return format(e)
    
# def publish_message(producer_instance, topic, key, value):
#     try:
#         key_bytes = bytes(key, encoding='utf-8')
#         value_bytes = bytes(value, encoding='utf-8')
#         producer.send(topic=topic, key=key_bytes, value=value_bytes)
#     except Exception as e:
#         return format(e)
    
    