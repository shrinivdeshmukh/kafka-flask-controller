from flask import Flask, request
import json
from schema import Schema
from producer import Producer
import models

app = Flask(__name__)
url = '10.128.0.9:8081'
topic = 'redshift'
bootstrap_servers = '10.128.0.8,10.128.0.9,10.128.0.10'


@app.route('/getSchema/')
def get_schema():
    try:
        schema = Schema(url,topic)
        response = schema.get_latest_schema()
        del schema
        return response
    except Exception as e:
        return e
    
@app.route('/publish', methods=["POST"])
def publish():
    try:
        producer = Producer(bootstrap_servers, url, topic)
        values = request.json["val"]
        response = producer.publish(key=values,values=values)
        del producer
        return response
    except Exception as e:
        return format(e)
        
@app.route('/map',methods=['POST'])
def map_values():
    try:
        map_json = request.json['mapping']
        schema = Schema(url,topic)
        response = schema.get_latest_schema()
        map_response = models.map(response,map_json)
        return map_response
    except Exception as e:
        return format(e)
    finally:
        del schema
    
@app.route('/register/schema', methods=['POST'])
def register_schema():
    schema = Schema(url,topic)
    try:
        schema_json = request.json['schema']
        register_schema_response = schema.register_schema(schema_json)
        return register_schema_response
    except Exception as e:
        return "Error while registering schema:\t{}".format(e)
    finally:
        del schema
        
if __name__ == '__main__':
    app.run(host='10.128.0.9', debug=True)