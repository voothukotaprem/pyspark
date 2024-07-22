source = KafkaSource.builder() \
    .set_bootstrap_servers(brokers) \
    .set_topics("input-topic") \
    .set_group_id("my-group") \
    .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

KafkaSource.builder().set_value_only_deserializer(SimpleStringSchema())


KafkaSource.builder() \
    # Start from committed offset of the consuming group, without reset strategy
    .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets()) \
    # Start from committed offset, also use EARLIEST as reset strategy if committed offset doesn't exist
    .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
    # Start from the first record whose timestamp is greater than or equals a timestamp (milliseconds)
    .set_starting_offsets(KafkaOffsetsInitializer.timestamp(1657256176000)) \
    # Start from the earliest offset
    .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
    # Start from the latest offset
    .set_starting_offsets(KafkaOffsetsInitializer.latest())



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


