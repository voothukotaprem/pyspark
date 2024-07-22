Dependency #
Apache Flink ships with a universal Kafka connector which attempts to track the latest version of the Kafka client.
The version of the client it uses may change between Flink releases. Modern Kafka clients are backwards compatible with
broker versions 0.10.0 or later. For details on Kafka compatibility, please refer to the official Kafka documentation.

Only available for stable versions.
Flink’s streaming connectors are not part of the binary distribution. See how to link with them for cluster execution here.

In order to use the Kafka connector in PyFlink jobs, the following dependencies are required:


Kafka Source #
This part describes the Kafka source based on the new data source API.


Usage #
Kafka source provides a builder class for constructing instance of KafkaSource.
The code snippet below shows how to build a KafkaSource to consume messages
from the earliest offset of topic “input-topic”, with consumer group “my-group” and deserialize only the value of message as string.


source = KafkaSource.builder() \
    .set_bootstrap_servers(brokers) \
    .set_topics("input-topic") \
    .set_group_id("my-group") \
    .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")


The following properties are required for building a KafkaSource:
Bootstrap servers, configured by setBootstrapServers(String)
Topics / partitions to subscribe, see the following Topic-partition subscription for more details.
Deserializer to parse Kafka messages, see the following Deserializer for more details.
Topic-partition Subscription #


#Kafka source provide 3 ways of topic-partition subscription:

Topic list, subscribing messages from all partitions in a list of topics. For example:
KafkaSource.builder().set_topics("topic-a", "topic-b")

Topic pattern, subscribing messages from all topics whose name matches the provided regular expression. For example:
KafkaSource.builder().set_topic_pattern("topic.*")


Partition set, subscribing partitions in the provided partition set. For example:

partition_set = {
    KafkaTopicPartition("topic-a", 0),
    KafkaTopicPartition("topic-b", 5)
}
KafkaSource.builder().set_partitions(partition_set)
Deserializer #
A deserializer is required for parsing Kafka messages. Deserializer (Deserialization schema) can be configured by setDeserializer(KafkaRecordDeserializationSchema), where KafkaRecordDeserializationSchema defines how to deserialize a Kafka ConsumerRecord.

If only the value of Kafka ConsumerRecord is needed, you can use setValueOnlyDeserializer(DeserializationSchema) in the builder, where DeserializationSchema defines how to deserialize binaries of Kafka message value.

You can also use a Kafka Deserializer for deserializing Kafka message value. For example using StringDeserializer for deserializing Kafka message value as string:

import org.apache.kafka.common.serialization.StringDeserializer;

KafkaSource.<String>builder()
        .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class));


Currently, PyFlink only supports set_value_only_deserializer to customize deserialization of the value of a Kafka record.
KafkaSource.builder().set_value_only_deserializer(SimpleStringSchema())

Starting Offset #
Kafka source is able to consume messages starting from different offsets by specifying OffsetsInitializer. Built-in initializers include:

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
You can also implement a custom offsets initializer if built-in initializers above cannot fulfill your requirement. (Not supported in PyFlink)

If offsets initializer is not specified, OffsetsInitializer.earliest() will be used by default.

Boundedness #
Kafka source is designed to support both streaming and batch running mode. By default, the KafkaSource is set to run in streaming manner, thus never stops until Flink job fails or is cancelled. You can use setBounded(OffsetsInitializer) to specify stopping offsets and set the source running in batch mode. When all partitions have reached their stopping offsets, the source will exit.

You can also set KafkaSource running in streaming mode, but still stop at the stopping offset by using setUnbounded(OffsetsInitializer). The source will exit when all partitions reach their specified stopping offset.

Additional Properties #
In addition to properties described above, you can set arbitrary properties for KafkaSource and KafkaConsumer by using setProperties(Properties) and setProperty(String, String). KafkaSource has following options for configuration:

client.id.prefix defines the prefix to use for Kafka consumer’s client ID
partition.discovery.interval.ms defines the interval im milliseconds for Kafka source to discover new partitions. See Dynamic Partition Discovery below for more details.
register.consumer.metrics specifies whether to register metrics of KafkaConsumer in Flink metric group
commit.offsets.on.checkpoint specifies whether to commit consuming offsets to Kafka brokers on checkpoint
For configurations of KafkaConsumer, you can refer to Apache Kafka documentation for more details.

Please note that the following keys will be overridden by the builder even if it is configured:

key.deserializer is always set to ByteArrayDeserializer
value.deserializer is always set to ByteArrayDeserializer
auto.offset.reset.strategy is overridden by OffsetsInitializer#getAutoOffsetResetStrategy() for the starting offsets
partition.discovery.interval.ms is overridden to -1 when setBounded(OffsetsInitializer) has been invoked
Dynamic Partition Discovery #
In order to handle scenarios like topic scaling-out or topic creation without restarting the Flink job, Kafka source can be configured to periodically discover new partitions under provided topic-partition subscribing pattern. To enable partition discovery, set a non-negative value for property partition.discovery.interval.ms:

Java
Python
KafkaSource.builder() \
    .set_property("partition.discovery.interval.ms", "10000")  # discover new partitions per 10 seconds
Partition discovery is disabled by default. You need to explicitly set the partition discovery interval to enable this feature.
Event Time and Watermarks #
By default, the record will use the timestamp embedded in Kafka ConsumerRecord as the event time. You can define your own WatermarkStrategy for extract event time from the record itself, and emit watermark downstream:

env.fromSource(kafkaSource, new CustomWatermarkStrategy(), "Kafka Source With Custom Watermark Strategy");
This documentation describes details about how to define a WatermarkStrategy. (Not supported in PyFlink)

Idleness #
The Kafka Source does not go automatically in an idle state if the parallelism is higher than the number of partitions. You will either need to lower the parallelism or add an idle timeout to the watermark strategy. If no records flow in a partition of a stream for that amount of time, then that partition is considered “idle” and will not hold back the progress of watermarks in downstream operators.

This documentation describes details about how to define a WatermarkStrategy#withIdleness.

Consumer Offset Committing #
Kafka source commits the current consuming offset when checkpoints are completed, for ensuring the consistency between Flink’s checkpoint state and committed offsets on Kafka brokers.

If checkpointing is not enabled, Kafka source relies on Kafka consumer’s internal automatic periodic offset committing logic, configured by enable.auto.commit and auto.commit.interval.ms in the properties of Kafka consumer.

Note that Kafka source does NOT rely on committed offsets for fault tolerance. Committing offset is only for exposing the progress of consumer and consuming group for monitoring.

Monitoring #
Kafka source exposes the following metrics in the respective scope.

Scope of Metric #
Scope	Metrics	User Variables	Description	Type
Operator	currentEmitEventTimeLag	n/a	The time span from the record event timestamp to the time the record is emitted by the source connector¹: currentEmitEventTimeLag = EmitTime - EventTime.	Gauge
watermarkLag	n/a	The time span that the watermark lags behind the wall clock time: watermarkLag = CurrentTime - Watermark	Gauge
sourceIdleTime	n/a	The time span that the source has not processed any record: sourceIdleTime = CurrentTime - LastRecordProcessTime	Gauge
pendingRecords	n/a	The number of records that have not been fetched by the source. e.g. the available records after the consumer offset in a Kafka partition.	Gauge
KafkaSourceReader.commitsSucceeded	n/a	The total number of successful offset commits to Kafka, if offset committing is turned on and checkpointing is enabled.	Counter
KafkaSourceReader.commitsFailed	n/a	The total number of offset commit failures to Kafka, if offset committing is turned on and checkpointing is enabled. Note that committing offsets back to Kafka is only a means to expose consumer progress, so a commit failure does not affect the integrity of Flink's checkpointed partition offsets.	Counter
KafkaSourceReader.committedOffsets	topic, partition	The last successfully committed offsets to Kafka, for each partition. A particular partition's metric can be specified by topic name and partition id.	Gauge
KafkaSourceReader.currentOffsets	topic, partition	The consumer's current read offset, for each partition. A particular partition's metric can be specified by topic name and partition id.	Gauge
¹ This metric is an instantaneous value recorded for the last processed record. This metric is provided because latency histogram could be expensive. The instantaneous latency value is usually a good enough indication of the latency.

Kafka Consumer Metrics #
All metrics of Kafka consumer are also registered under group KafkaSourceReader.KafkaConsumer. For example, Kafka consumer metric “records-consumed-total” will be reported in metric: <some_parent_groups>.operator.KafkaSourceReader.KafkaConsumer.records-consumed-total .

You can configure whether to register Kafka consumer’s metric by configuring option register.consumer.metrics. This option will be set as true by default.

For metrics of Kafka consumer, you can refer to Apache Kafka Documentation for more details.

In case you experience a warning with a stack trace containing javax.management.InstanceAlreadyExistsException: kafka.consumer:[...], you are probably trying to register multiple KafkaConsumers with the same client.id. The warning indicates that not all available metrics are correctly forwarded to the metrics system. You must ensure that a different client.id.prefix for every KafkaSource is configured and that no other KafkaConsumer in your job uses the same client.id.

Security #
In order to enable security configurations including encryption and authentication, you just need to setup security configurations as additional properties to the Kafka source. The code snippet below shows configuring Kafka source to use PLAIN as SASL mechanism and provide JAAS configuration:


KafkaSource.builder() \
    .set_property("security.protocol", "SASL_PLAINTEXT") \
    .set_property("sasl.mechanism", "PLAIN") \
    .set_property("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"username\" password=\"password\";")
For a more complex example, use SASL_SSL as the security protocol and use SCRAM-SHA-256 as SASL mechanism:
KafkaSource.builder() \
    .set_property("security.protocol", "SASL_SSL") \
    # SSL configurations
    # Configure the path of truststore (CA) provided by the server
    .set_property("ssl.truststore.location", "/path/to/kafka.client.truststore.jks") \
    .set_property("ssl.truststore.password", "test1234") \
    # Configure the path of keystore (private key) if client authentication is required
    .set_property("ssl.keystore.location", "/path/to/kafka.client.keystore.jks") \
    .set_property("ssl.keystore.password", "test1234") \
    # SASL configurations
    # Set SASL mechanism as SCRAM-SHA-256
    .set_property("sasl.mechanism", "SCRAM-SHA-256") \
    # Set JAAS configurations
    .set_property("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"username\" password=\"password\";")
Please note that the class path of the login module in sasl.jaas.config might be different if you relocate Kafka client dependencies in the job JAR, so you may need to rewrite it with the actual class path of the module in the JAR.

For detailed explanations of security configurations, please refer to the “Security” section in Apache Kafka documentation.

Behind the Scene #
If you are interested in how Kafka source works under the design of new data source API, you may want to read this part as a reference. For details about the new data source API, documentation of data source and FLIP-27 provide more descriptive discussions.
Under the abstraction of the new data source API, Kafka source consists of the following components:

Source Split #
A source split in Kafka source represents a partition of Kafka topic. A Kafka source split consists of:

TopicPartition the split representing
Starting offset of the partition
Stopping offset of the partition, only available when the source is running in bounded mode
The state of Kafka source split also stores current consuming offset of the partition, and the state will be converted to immutable split when Kafka source reader is snapshot, assigning current offset to the starting offset of the immutable split.

You can check class KafkaPartitionSplit and KafkaPartitionSplitState for more details.

Split Enumerator #
The split enumerator of Kafka is responsible for discovering new splits (partitions) under the provided topic partition subscription pattern, and assigning splits to readers, uniformly distributed across subtasks, in round-robin style. Note that the split enumerator of Kafka source pushes splits eagerly to source readers, so it won’t need to handle split requests from source reader.

Source Reader #
The source reader of Kafka source extends the provided SourceReaderBase, and use single-thread-multiplexed thread model, which read multiple assigned splits (partitions) with one KafkaConsumer driven by one SplitReader. Messages are deserialized right after they are fetched from Kafka in SplitReader. The state of split, or current progress of message consuming is updated by KafkaRecordEmitter , which is also responsible for assigning event time when the record is emitted downstream.

Kafka SourceFunction #
FlinkKafkaConsumer is deprecated and will be removed with Flink 1.17, please use KafkaSource instead.
For older references you can look at the Flink 1.13 documentation.

Kafka Sink #
KafkaSink allows writing a stream of records to one or more Kafka topics.

Usage #
Kafka sink provides a builder class to construct an instance of a KafkaSink. The code snippet below shows how to write String records to a Kafka topic with a delivery guarantee of at least once.

Java
Python
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
The following properties are required to build a KafkaSink:

Bootstrap servers, setBootstrapServers(String)
Record serializer, setRecordSerializer(KafkaRecordSerializationSchema)
If you configure the delivery guarantee with DeliveryGuarantee.EXACTLY_ONCE you also have use setTransactionalIdPrefix(String)
Serializer #
You always need to supply a KafkaRecordSerializationSchema to transform incoming elements from the data stream to Kafka producer records. Flink offers a schema builder to provide some common building blocks i.e. key/value serialization, topic selection, partitioning. You can also implement the interface on your own to exert more control.

Java
Python
KafkaRecordSerializationSchema.builder() \
    .set_topic_selector(lambda element: <your-topic-selection-logic>) \
    .set_value_serialization_schema(SimpleStringSchema()) \
    .set_key_serialization_schema(SimpleStringSchema()) \
    # set partitioner is not supported in PyFlink
    .build()
It is required to always set a value serialization method and a topic (selection method). Moreover, it is also possible to use Kafka serializers instead of Flink serializer by using setKafkaKeySerializer(Serializer) or setKafkaValueSerializer(Serializer).

Fault Tolerance #
Overall the KafkaSink supports three different DeliveryGuarantees. For DeliveryGuarantee.AT_LEAST_ONCE and DeliveryGuarantee.EXACTLY_ONCE Flink’s checkpointing must be enabled. By default the KafkaSink uses DeliveryGuarantee.NONE. Below you can find an explanation of the different guarantees.

DeliveryGuarantee.NONE does not provide any guarantees: messages may be lost in case of issues on the Kafka broker and messages may be duplicated in case of a Flink failure.
DeliveryGuarantee.AT_LEAST_ONCE: The sink will wait for all outstanding records in the Kafka buffers to be acknowledged by the Kafka producer on a checkpoint. No messages will be lost in case of any issue with the Kafka brokers but messages may be duplicated when Flink restarts because Flink reprocesses old input records.
DeliveryGuarantee.EXACTLY_ONCE: In this mode, the KafkaSink will write all messages in a Kafka transaction that will be committed to Kafka on a checkpoint. Thus, if the consumer reads only committed data (see Kafka consumer config isolation.level), no duplicates will be seen in case of a Flink restart. However, this delays record visibility effectively until a checkpoint is written, so adjust the checkpoint duration accordingly. Please ensure that you use unique transactionalIdPrefix across your applications running on the same Kafka cluster such that multiple running jobs do not interfere in their transactions! Additionally, it is highly recommended to tweak Kafka transaction timeout (see Kafka producer transaction.timeout.ms)» maximum checkpoint duration + maximum restart duration or data loss may happen when Kafka expires an uncommitted transaction.
Monitoring #
Kafka sink exposes the following metrics in the respective scope.

Scope	Metrics	User Variables	Description	Type
Operator	currentSendTime	n/a	The time it takes to send the last record. This metric is an instantaneous value recorded for the last processed record.	Gauge
Kafka Producer #
FlinkKafkaProducer is deprecated and will be removed with Flink 1.15, please use KafkaSink instead.
For older references you can look at the Flink 1.13 documentation.

Kafka Connector Metrics #
Flink’s Kafka connectors provide some metrics through Flink’s metrics system to analyze the behavior of the connector. The producers and consumers export Kafka’s internal metrics through Flink’s metric system for all supported versions. The Kafka documentation lists all exported metrics in its documentation.

It is also possible to disable the forwarding of the Kafka metrics by either configuring register.consumer.metrics outlined by this section for the KafkaSource or when using the KafkaSink you can set the configuration register.producer.metrics to false via the producer properties.

Enabling Kerberos Authentication #
Flink provides first-class support through the Kafka connector to authenticate to a Kafka installation configured for Kerberos. Simply configure Flink in flink-conf.yaml to enable Kerberos authentication for Kafka like so:

Configure Kerberos credentials by setting the following -
security.kerberos.login.use-ticket-cache: By default, this is true and Flink will attempt to use Kerberos credentials in ticket caches managed by kinit. Note that when using the Kafka connector in Flink jobs deployed on YARN, Kerberos authorization using ticket caches will not work.
security.kerberos.login.keytab and security.kerberos.login.principal: To use Kerberos keytabs instead, set values for both of these properties.
Append KafkaClient to security.kerberos.login.contexts: This tells Flink to provide the configured Kerberos credentials to the Kafka login context to be used for Kafka authentication.
Once Kerberos-based Flink security is enabled, you can authenticate to Kafka with either the Flink Kafka Consumer or Producer by simply including the following two settings in the provided properties configuration that is passed to the internal Kafka client:

Set security.protocol to SASL_PLAINTEXT (default NONE): The protocol used to communicate to Kafka brokers. When using standalone Flink deployment, you can also use SASL_SSL; please see how to configure the Kafka client for SSL here.
Set sasl.kerberos.service.name to kafka (default kafka): The value for this should match the sasl.kerberos.service.name used for Kafka broker configurations. A mismatch in service name between client and server configuration will cause the authentication to fail.
For more information on Flink configuration for Kerberos security, please see here. You can also find here further details on how Flink internally setups Kerberos-based security.

Upgrading to the Latest Connector Version #
The generic upgrade steps are outlined in upgrading jobs and Flink versions guide. For Kafka, you additionally need to follow these steps:

Do not upgrade Flink and the Kafka Connector version at the same time.
Make sure you have a group.id configured for your Consumer.
Set setCommitOffsetsOnCheckpoints(true) on the consumer so that read offsets are committed to Kafka. It’s important to do this before stopping and taking the savepoint. You might have to do a stop/restart cycle on the old connector version to enable this setting.
Set setStartFromGroupOffsets(true) on the consumer so that we get read offsets from Kafka. This will only take effect when there is no read offset in Flink state, which is why the next step is very important.
Change the assigned uid of your source/sink. This makes sure the new source/sink doesn’t read state from the old source/sink operators.
Start the new job with --allow-non-restored-state because we still have the state of the previous connector version in the savepoint.
Troubleshooting #
If you have a problem with Kafka when using Flink, keep in mind that Flink only wraps KafkaConsumer or KafkaProducer and your problem might be independent of Flink and sometimes can be solved by upgrading Kafka brokers, reconfiguring Kafka brokers or reconfiguring KafkaConsumer or KafkaProducer in Flink. Some examples of common problems are listed below.
Data loss #
Depending on your Kafka configuration, even after Kafka acknowledges writes you can still experience data loss. In particular keep in mind about the following properties in Kafka config:

acks
log.flush.interval.messages
log.flush.interval.ms
log.flush.*
Default values for the above options can easily lead to data loss. Please refer to the Kafka documentation for more explanation.

UnknownTopicOrPartitionException #
One possible cause of this error is when a new leader election is taking place, for example after or during restarting a Kafka broker. This is a retriable exception, so Flink job should be able to restart and resume normal operation. It also can be circumvented by changing retries property in the producer settings. However this might cause reordering of messages, which in turn if undesired can be circumvented by setting max.in.flight.requests.per.connection to 1.

ProducerFencedException #
The reason for this exception is most likely a transaction timeout on the broker side. With the implementation of KAFKA-6119, the (producerId, epoch) will be fenced off after a transaction timeout and all of its pending transactions are aborted (each transactional.id is mapped to a single producerId; this is described in more detail in the following blog post).