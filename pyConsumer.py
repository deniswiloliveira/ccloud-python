from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
import json
import ccloud_lib


if __name__ == '__main__':

    # Initialization
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Avro Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    c = AvroConsumer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        'schema.registry.url': conf['schema.registry.url'],
        'schema.registry.basic.auth.credentials.source': conf['basic.auth.credentials.source'],
        'schema.registry.basic.auth.user.info': conf['schema.registry.basic.auth.user.info'],
        'group.id': 'python_example_group_2',
        'auto.offset.reset': 'earliest'
    })

    # Subscribe to topic
    c.subscribe([topic])

    # Process messages
    total_count = 0
    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = ccloud_lib.Name(msg.key())
                name_object = record_key.name
                name = name_object['name']
                record_value = ccloud_lib.Count(msg.value())
                count_object = record_value.count
                count = count_object['count']
                total_count += count
                print("Consumed record with key {} and value {}, \
                      and updated total count to {}"
                      .format(name, count, total_count))
    except SerializerError as e:
        # Report malformed record, discard results, continue polling
        print("Message deserialization failed {}".format(e))
        pass
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        c.close()