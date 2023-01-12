from uuid import uuid4
from confluent_kafka import Producer
from confluent_kafka.serialization import MessageField
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import  SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import SerializationContext
from restaurant import *
from config import config_details


# need to do: get_latest_version of schemas
def delivery_callback(err, msg):
    """Optional per-message delivery callback (triggered by poll() or flush())
    when a message has been successfully delivered or permanently
    failed delivery (after retries).
    """
    if err:
        print(f"ERROR: Message failed delivery: {err}")
    else:
        print("Produced event to topic {topic}: key = {key:12} partition = [{partition}] offset = [{offset}]".format(
            topic=msg.topic(), key=msg.key().decode('utf-8'), partition=msg.partition(), offset=msg.offset()
        ))
        

def get_schema_reg_details(schema_reg_config: SchemaRegistryClient,
                           schema_id_key: int,
                           schema_id_value: int) -> tuple:

    schema_registry_client = SchemaRegistryClient(schema_reg_config)
    schema_key = schema_registry_client.get_schema(schema_id_key)
    schema_value = schema_registry_client.get_schema(schema_id_value)
    return (schema_key.schema_str, 
            schema_value.schema_str, 
            schema_registry_client)
        
schema_reg_config = config_details(config_key="schema_registry_config")
schema_id_key, schema_id_value = 100002, 100005
schema_details = get_schema_reg_details(schema_reg_config, 
                                        schema_id_key, 
                                        schema_id_value)


def main(topic):
    schema_registry_client, schema_str = schema_details[2], schema_details[1]
    json_serialize_context = SerializationContext(topic, MessageField.VALUE)
    string_serialize_context = SerializationContext(topic, MessageField.KEY)

    string_serializer = StringSerializer()
    jsonserializer = JSONSerializer(schema_str, 
                                    schema_registry_client,
                                    to_dict=RestaurantRecord.restaurant_to_dict)
    
    print(f"Producing user records to topic {topic}. ^C to exit.")
    producer = Producer(config_details(config_key="cluster_config"))
    producer.poll(0.0)
    
    counter = 0
    try:
        for rest_record in get_restaurant_record_instance("restaurant.csv"):
            
            if counter <= 10:
                producer.produce(topic,
                                key=string_serializer(str((uuid4())), string_serialize_context),
                                value=jsonserializer(rest_record, json_serialize_context),
                                on_delivery=delivery_callback)
            else:
                break
            counter += 1
    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input. discarding record...")
        
    print("\nFlushing records...")
    producer.flush()
    
    
topic = "restuarant-take-away-data"
main(topic)
