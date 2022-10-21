import time
import requests
from kafka import KafkaProducer
from json import dumps
import json
from urllib.request import urlretrieve
import datetime as dt
import pandas as pd
import sys

meetup_dot_com_rsvp_stream_api_url = "http://stream.meetup.com/2/rsvps"
kafka_topic_name = "meetuprsvptopic"
kafka_bootstrap_servers = "localhost:9092"

if __name__ == "__main__":
    print("kafka Producer Application Started...")


    kafka_producer_obj = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))

    while True:
        try:


            stream_api_response = requests.get(meetup_dot_com_rsvp_stream_api_url,stream=True)
            print(stream_api_response.json())
            if stream_api_response == 200:
                for api_response_message in stream_api_response.iter_lines():
                    print("Message received")
                    #print(api_response_message)
                    print(type(api_response_message))

                    api_response_message1 = json.load(api_response_message)

                    print("message to set 1")
                    #print(api_response_message1)
                    print(type(api_response_message1))
                    kafka_producer_obj.send(kafka_topic_name, api_response_message1)
                    time.sleep(1)
        except Exception as ex:
            print('conection to meetuo stram api could not established')

    print('while loop complete')