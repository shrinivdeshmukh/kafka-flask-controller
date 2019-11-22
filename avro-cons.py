from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
import os
import json
import requests

def check_dir(dir_path):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
        return True
    else:
        return True
    
# def check_schema_format(schema_id):

# def check_schema_format(schema_id):
#     try:
       
def dump(obj):
  for attr in dir(obj):
    print("obj.%s = %r" % (attr, getattr(obj, attr)))
    
def get_schema_id(topic):
    response = requests.get('http://35.223.91.93:8081/subjects/{}/versions/latest'.format(topic))
    return json.loads(response.text)
    

c = AvroConsumer({
    'bootstrap.servers': '10.128.0.8,10.128.0.9,10.128.0.10',
    'schema.registry.url': 'http://35.223.91.93:8081',
    'group.id': 'groupid'
    })

c.subscribe(['redshift'])

while True:
    try:
        msg = c.poll(1)

    except SerializerError as e:
        print("Message deserialization failed for {}: {}".format(msg, e))
        break

    if msg is None:
        continue

    if msg.error():
        print("AvroConsumer error: {}".format(msg.error()))
        continue

    print(get_schema_id('redshift'))
    if check_dir('consumers'):
        with open('consumers/groupid','a') as f:
            json.dump(msg.value(),f)
            f.write('\n')
            
#         with open("data.json") as file:
#             data = msg.value()

#         with open("consumers/groupid", "a") as file:
#             csv_file = csv.writer(file)
#             for item in data:
#                 fields = list(msg.value().values())
#                 csv_file.writerow([item['pk'], item['model']] + fields)
c.close()
