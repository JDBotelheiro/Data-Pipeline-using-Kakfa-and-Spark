# push data to kafka
import time
import requests
import datetime
import os
from kafka import KafkaProducer

# Where do I leave the kafka pipelines running? Docker?


API_URL = os.getenv("URL", default='http://0.0.0.0:3030/FoodOrderingApp')

def appDataStream(url):
    """Collect the logs from API service

    Returns:
        str: logs data
    """
    try:
        r = requests.get(url)
        return r.text
    except:
        return "Error in Connection"

def dataToKafka(url):
    """Main function to run the kafka pipeline to collect the raw data
    """
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    while True:
        msg =  appDataStream(url)
        producer.send("RawData", msg.encode('utf-8'))
        time.sleep(1)
        
if __name__ == "__main__":
    dataToKafka(url=API_URL)