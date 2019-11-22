import requests
import json
import ast
from logger import logger


class Schema(object):
    
    def __init__(self, url, topic):
        self.url = url
        self.topic = topic
        self.headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
        self.register_url = 'subjects/{}/versions/'.format(topic)
        self.change_compatibility_url = 'config'
        self.get_all_schema_url = 'subjects/{}/versions'.format(self.topic)
        self.get_latest_schema_url = 'subjects/{}/versions/latest'.format(self.topic)
        logger.info("Initialized Schema Objects with params:")
        logger.info("Topic:\t{}".format(self.topic))
        logger.info("url:\t{}".format(self.url))
        

    def format_schema(self, schema):
        try:
            payload = "{ \"schema\": \"" \
              + schema.replace("\"", "\\\"").replace("\t", "").replace("\n", "") \
              + "\" }"
            return payload
        except Exception as e:
            return format(e)

    def register_schema(self, schema):
        try:
#             payload = self.format_schema(schema)
            payload = {}
            payload['schema'] = json.dumps(schema)
            response = requests.post(url="http://{}/{}".format(self.url,self.register_url), data=json.dumps(payload), headers=self.headers)
            print("response text is:\t{}".format(response.text))
            return response.text
        except Exception as e:
            return format(e)
    
    def get_all_schema(self):
        try:
            url = 'http://{}/{}'.format(self.url,self.get_all_schema_url)
            response = requests.get(url, headers=self.headers)
            formatted_response = eval(response.text)
            schema_json = dict({})
            
            for element in formatted_response:
                print("element",element)
                response = requests.get(url+'/{}'.format(element), headers=self.headers)
                data = response.text
                schema_json[str(element)] = json.loads(data)             
            return schema_json
        except Exception as e:
            return format(e)
        
    def get_latest_schema(self):
        try:
            url = 'http://{}/{}'.format(self.url,self.get_latest_schema_url)
            response = requests.get(url, headers=self.headers)
            formatted_response = eval(response.text)
            schema_json = dict({})
            logger.info("\tget_latest_schema:\t{}".format(response.text))
#             response = requests.get(url+('/{}').format(formatted_response[-1]),headers=self.headers)
#             schema_json[str(formatted_response[-1])] = json.loads(response.text)
            return json.loads(response.text)
        except Exception as e:
            return format(e)
            
        
        
    def alter_compatibility(self, compatibility):
        try:
            url = 'http://{}/{}'.format(self.url, self.change_compatibility)
            data = json.dumps({"compatibility":compatibility})
            response = requests.put(url,data=data, headers=self.headers)
            return response.text
        except Exception as e:
            return format(e)

# if __name__ == '__main__':
#     url = '10.128.0.9:8081'
#     topic = 'redshift'
#     schema_obj = Schema(url,topic)
#     print(schema_obj.get_all_schema())
# #     with open("user.avsc", 'r') as content_file:
# #         schema = content_file.read()
# #     
# #     print(schema_obj.register_schema(schema))