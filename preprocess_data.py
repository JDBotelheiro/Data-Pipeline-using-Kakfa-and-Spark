import json
import findspark
findspark.init("/Users/Joao/Downloads/spark-3.3.1-bin-hadoop3")

from bson import json_util
from dateutil import parser
from pyspark.sql import SparkSession
from kafka import KafkaConsumer, KafkaProducer
#Mongo DB
from pymongo import MongoClient
from feature_preprocess import FeaturePreprocess


def featureStore(localhost=27017):
    """Load connection to feature store"""
    client = MongoClient('localhost', localhost) #MongoClient('localhost', 27017)
    db = client['RealTimeDB']
    collection = db['RealTimeCollection']
    return collection

def timestamp_exist(TimeStamp, collection):
    if collection.find(
                        #Find documents matching any of these values
                        {"$and":[
                            {"TimeStamp": {"$eq": TimeStamp}},
                            {"TimeStamp": {"$eq": TimeStamp}}
                        ]}):
        return True
    else:
        return False
    
def main():
    # Start Spark context - adicionar mais alguma info aqui!!!
    spark_session = SparkSession.builder.getOrCreate()
    spark_context = spark_session._sc
    spark_context.setLogLevel("WARN")
    # Start Kafka consumer to retrive the raw data
    consumer = KafkaConsumer('RawData', auto_offset_reset='earliest',bootstrap_servers=['localhost:9092'], consumer_timeout_ms=1000)
    # Start kafka producer to send the preprocessed data
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    # Load feature store
    collection = featureStore()
    for msg in consumer:
        if msg.value.decode("utf-8")!="Error in Connection":
            # Define data structure and peform some preprocessing
            msg = msg.value.decode("utf-8").split(",")
            data = FeaturePreprocess().structureData(sc=spark_context, msg=msg)
            if timestamp_exist(data['TimeStamp'], collection) == False:            
                #push data to feature store
                collection.insert(data)
                producer.send("CleanData", json.dumps(data, default=json_util.default).encode('utf-8'))
            
            print(data)

if __name__ == "__main__":
    main()