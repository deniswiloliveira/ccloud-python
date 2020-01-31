from confluent_kafka import Producer, KafkaError
from confluent_kafka.avro import AvroProducer
import json
import ccloud_lib


if __name__ == '__main__':
    # Initialization
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create AvroProducer instance
    p = AvroProducer({
           'bootstrap.servers': conf['bootstrap.servers'],
           'sasl.mechanisms': 'PLAIN',
           'security.protocol': 'SASL_SSL',
           'sasl.username': conf['sasl.username'],
           'sasl.password': conf['sasl.password'],
           'schema.registry.url': conf['schema.registry.url'],
           'schema.registry.basic.auth.credentials.source': conf['basic.auth.credentials.source'],
           'schema.registry.basic.auth.user.info': conf['schema.registry.basic.auth.user.info']
    }, default_key_schema=ccloud_lib.schema_key, default_value_schema=ccloud_lib.schema_value)

    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)

    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def acked(err, msg):
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))

    for n in range(10):
        name_object = ccloud_lib.Name()
        name_object.name = "alice"
        record_key = name_object.to_dict()
        count_object = ccloud_lib.Count()
        count_object.count = n
        record_value = count_object.to_dict()
        print("Producing Avro record: {}\t{}".format(name_object.name, count_object.count))
        p.produce(topic=topic, key=record_key, value=record_value, on_delivery=acked)
        # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls.
        p.poll(0)

    p.flush(10)

    print("10 messages were produced to topic {}!".format(topic))