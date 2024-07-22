#a.kafka

sink = KafkaSink.builder() \
    .set_bootstrap_servers(brokers) \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
            .set_topic("topic-name")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
    ) \
    .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
    .build()

stream.sink_to(sink)


#b.cassandra
#c.Dynamo DB

#d.ElasticSearch

#1.Elasticsearch 6 static index :

from pyflink.datastream.connectors.elasticsearch import Elasticsearch6SinkBuilder, ElasticsearchEmitter

env = StreamExecutionEnvironment.get_execution_environment()
env.add_jars(ELASTICSEARCH_SQL_CONNECTOR_PATH)

input = ...

# The set_bulk_flush_max_actions instructs the sink to emit after every element, otherwise they would be buffered
es6_sink = Elasticsearch6SinkBuilder() \
    .set_bulk_flush_max_actions(1) \
    .set_emitter(ElasticsearchEmitter.static_index('foo', 'id', 'bar')) \
    .set_hosts(['localhost:9200']) \
    .build()

input.sink_to(es6_sink).name('es6 sink')

#--------------------
#Elasticsearch 6 dynamic index:
from pyflink.datastream.connectors.elasticsearch import Elasticsearch6SinkBuilder, ElasticsearchEmitter

env = StreamExecutionEnvironment.get_execution_environment()
env.add_jars(ELASTICSEARCH_SQL_CONNECTOR_PATH)

input = ...

es_sink = Elasticsearch6SinkBuilder() \
    .set_emitter(ElasticsearchEmitter.dynamic_index('name', 'id', 'bar')) \
    .set_hosts(['localhost:9200']) \
    .build()

input.sink_to(es6_sink).name('es6 dynamic index sink')

#----------------------------------

#2.Elasticsearch 7 static index:

from pyflink.datastream.connectors.elasticsearch import Elasticsearch7SinkBuilder, ElasticsearchEmitter

env = StreamExecutionEnvironment.get_execution_environment()
env.add_jars(ELASTICSEARCH_SQL_CONNECTOR_PATH)

input = ...

# The set_bulk_flush_max_actions instructs the sink to emit after every element, otherwise they would be buffered
es7_sink = Elasticsearch7SinkBuilder() \
    .set_bulk_flush_max_actions(1) \
    .set_emitter(ElasticsearchEmitter.static('foo', 'id')) \
    .set_hosts(['localhost:9200']) \
    .build()

input.sink_to(es7_sink).name('es7 sink')
--------------
#Elasticsearch 7 dynamic index:

from pyflink.datastream.connectors.elasticsearch import Elasticsearch7SinkBuilder, ElasticsearchEmitter

env = StreamExecutionEnvironment.get_execution_environment()
env.add_jars(ELASTICSEARCH_SQL_CONNECTOR_PATH)

input = ...

es7_sink = Elasticsearch7SinkBuilder() \
    .set_emitter(ElasticsearchEmitter.dynamic_index('name', 'id')) \
    .set_hosts(['localhost:9200']) \
    .build()

input.sink_to(es7_sink).name('es7 dynamic index sink')

#======================

#e.Amazon Kinesis Data Firehose Sink
#Kinesis Endpoints
sink_properties = {
    # Required
    'aws.region': 'eu-west-1',
    # Optional, provide via alternative routes e.g. environment variables
    'aws.credentials.provider.basic.accesskeyid': 'aws_access_key_id',
    'aws.credentials.provider.basic.secretkey': 'aws_secret_access_key',
    'aws.endpoint': 'http://localhost:4566'
}

kdf_sink = KinesisFirehoseSink.builder() \
    .set_firehose_client_properties(sink_properties) \     # Required
    .set_serialization_schema(SimpleStringSchema()) \      # Required
    .set_delivery_stream_name('your-stream-name') \        # Required
    .set_fail_on_error(False) \                            # Optional
    .set_max_batch_size(500) \                             # Optional
    .set_max_in_flight_requests(50) \                      # Optional
    .set_max_buffered_requests(10000) \                    # Optional
    .set_max_batch_size_in_bytes(5 * 1024 * 1024) \        # Optional
    .set_max_time_in_buffer_ms(5000) \                     # Optional
    .set_max_record_size_in_bytes(1 * 1024 * 1024) \       # Optional
    .build()


#f.Amazon Kinesis Data Streams Connector
#Kinesis Endpoints
consumer_config = {
    'aws.region': 'us-east-1',
    'aws.credentials.provider.basic.accesskeyid': 'aws_access_key_id',
    'aws.credentials.provider.basic.secretkey': 'aws_secret_access_key',
    'flink.stream.initpos': 'LATEST'
}

env = StreamExecutionEnvironment.get_execution_environment()

kinesis = env.add_source(FlinkKinesisConsumer("stream-1", SimpleStringSchema(), consumer_config))
#----------------------------

sink_properties = {
    # Required
    'aws.region': 'us-east-1',
    # Optional, provide via alternative routes e.g. environment variables
    'aws.credentials.provider.basic.accesskeyid': 'aws_access_key_id',
    'aws.credentials.provider.basic.secretkey': 'aws_secret_access_key',
    'aws.endpoint': 'http://localhost:4567'
}

kds_sink = KinesisStreamsSink.builder() \
    .set_kinesis_client_properties(sink_properties) \                      # Required
    .set_serialization_schema(SimpleStringSchema()) \                      # Required
    .set_partition_key_generator(PartitionKeyGenerator.fixed()) \          # Required
    .set_stream_name("your-stream-name") \                                 # Required
    .set_fail_on_error(False) \                                            # Optional
    .set_max_batch_size(500) \                                             # Optional
    .set_max_in_flight_requests(50) \                                      # Optional
    .set_max_buffered_requests(10000) \                                    # Optional
    .set_max_batch_size_in_bytes(5 * 1024 * 1024) \                        # Optional
    .set_max_time_in_buffer_ms(5000) \                                     # Optional
    .set_max_record_size_in_bytes(1 * 1024 * 1024) \                       # Optional
    .build()

simple_string_stream = ...
simple_string_stream.sink_to(kds_sink)


