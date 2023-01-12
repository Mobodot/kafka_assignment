from confluent_kafka import Consumer
from confluent_kafka.serialization import MessageField
from confluent_kafka.schema_registry import  SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import SerializationContext
from restaurant import *
from config import config_details
   

def get_schema_reg_details(schema_reg_config: SchemaRegistryClient,
                           schema_id_key: int,
                           schema_id_value: int) -> tuple:
    schema_reg_client = SchemaRegistryClient(schema_reg_config)
    schema_key = schema_reg_client.get_schema(schema_id_key)
    schema_value = schema_reg_client.get_schema(schema_id_value)
    return (schema_key.schema_str, 
            schema_value.schema_str, 
            schema_reg_client)

schema_reg_config = config_details(config_key="schema_registry_config")
schema_id_key, schema_id_value = 100002, 100005
schema_details = get_schema_reg_details(schema_reg_config, 
                                        schema_id_key, 
                                        schema_id_value)

# counter = 0
# def consumer_counter():
#     global counter
#     counter += 1 

        
def main(topic):
    deserializer_context = SerializationContext(topic, MessageField.VALUE)
    json_deserializer = JSONDeserializer(schema_details[1], 
                                         RestaurantRecord.dict_to_restaurant)
    
    consumer_config = config_details(config_key="consumer")
    cluster_config = config_details(config_key="cluster_config")
    consumer_config.update(cluster_config)
    consumer = Consumer(consumer_config)
    consumer.subscribe([topic])
    
    counter = 0
    while True:
        try:
            msg = consumer.poll(1.0)
            if msg is None: continue
            
            rest_record = json_deserializer(msg.value(), deserializer_context)
            
            if rest_record:
                print("restaurant record key {}: \n"
                    "restaurant record value {}".format(
                        msg.key(), rest_record
                    ))
            counter += 1
        except KeyboardInterrupt:
            break
    
    consumer.close()
    print(counter)
    
topic = "restuarant-take-away-data"
main(topic)