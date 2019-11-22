from schema import Schema
import json
import os

# class Models(object):
    
#     def __init__(self, topic):
#         self.topic = topic
        
#     def map

def check_dir(dir_path):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
        return True
    else:
        return True

def map(value_json: dict, mapping: dict):
#     schema = Schema()
    try:
#         latest_schema = schema.get_
        schema_id = value_json['id']
        topic = value_json['subject']
        if check_dir(topic):
            with open("{}/{}.json".format(topic,schema_id), 'w') as f:
                json.dump(mapping,f)
        return "Filepath:\t{}/{}.json".format(topic,schema_id)
    except Exception as e:
        return format(e)
#     finally:
#         del schema
    
value_json = {'subject': 'redshift', 'version': 2, 'id': 25, 'schema': '{"type":"record","name":"User","namespace":"example.avro","fields":[{"name":"name","type":"string"},{"name":"favorite_number","type":["int","null"]},{"name":"favorite_color","type":["string","null"]},{"name":"address","type":"string"}]}'}

mapping = {"name":"name", "number":"favourite_number","color":"favourite_color","address":"address"}

map(value_json,mapping)