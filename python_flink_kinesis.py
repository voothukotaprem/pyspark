consumer_config = {
    'aws.region': 'us-east-1',
    'aws.credentials.provider.basic.accesskeyid': 'aws_access_key_id',
    'aws.credentials.provider.basic.secretkey': 'aws_secret_access_key',
    'flink.stream.initpos': 'LATEST'
}

env = StreamExecutionEnvironment.get_execution_environment()
kinesis = env.add_source(FlinkKinesisConsumer("stream-1", SimpleStringSchema(), consumer_config))



env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(5000) # checkpoint every 5000 msecs
#------------------------

consumer_config = {
    'aws.region': 'us-east-1',
    'flink.stream.initpos': 'LATEST',
    'flink.stream.recordpublisher':  'EFO',
    'flink.stream.efo.consumername': 'my-flink-efo-consumer'
}

env = StreamExecutionEnvironment.get_execution_environment()

kinesis = env.add_source(FlinkKinesisConsumer(
    "kinesis_stream_name", SimpleStringSchema(), consumer_config))

#=======================================-----------------------===============================

# Required
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