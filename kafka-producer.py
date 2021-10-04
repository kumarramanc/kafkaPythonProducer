# Import KafkaConsumer from Kafka library
from kafka import KafkaProducer

# import json
import json

# Define server with port
bootstrap_servers = ['b-1.kafka-cluster-data-in.jyds4m.c17.kafka.us-east-1.amazonaws.com:9092','b-2.kafka-cluster-data-in.jyds4m.c17.kafka.us-east-1.amazonaws.com:9092']

# Define topic name to which the message will be sent 
topicName = 'Products'

# Function to serialize json and encoding as utf-8
def jsonSerializer(data):
    return json.dumps(data).encode("utf-8")

# Initialize instance of a producer and using the above defined json serializer function
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=jsonSerializer)

# Sending the data to the broker & Topic  
def inputData(x):
    producer.send('Products', x)
    producer.flush()
    return x


# Testing the Data Send 
inputData({"Name":"Lamuk","Address":"1514 Main Street"})