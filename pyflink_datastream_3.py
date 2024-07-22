#1

#Create from a list object #
#You can create a DataStream from a list object:

from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
ds = env.from_collection(
    collection=[(1, 'aaa|bb'), (2, 'bb|a'), (3, 'aaa|a')],
    type_info=Types.ROW([Types.INT(), Types.STRING()]))


#2

#Create using DataStream connectors #
#You can also create a DataStream using DataStream connectors with method add_source as following:

from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema

env = StreamExecutionEnvironment.get_execution_environment()
# the sql connector for kafka is used here as it's a fat jar and could avoid dependency issues
env.add_jars("file:///path/to/flink-sql-connector-kafka.jar")

deserialization_schema = JsonRowDeserializationSchema.builder() \
    .type_info(type_info=Types.ROW([Types.INT(), Types.STRING()])).build()

kafka_consumer = FlinkKafkaConsumer(
    topics='test_source_topic',
    deserialization_schema=deserialization_schema,
    properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'})

ds = env.add_source(kafka_consumer)



#3
#You can also create a DataStream using for unified DataStream connectors with method from_source .

#Note Currently, it only supports NumberSequenceSource and FileSource as unified DataStream source connectors.


#a.NumberSequenceSource

from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.number_seq import NumberSequenceSource

env = StreamExecutionEnvironment.get_execution_environment()
seq_num_source = NumberSequenceSource(1, 1000)
ds = env.from_source(
    source=seq_num_source,
    watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
    source_name='seq_num_source',
    type_info=Types.LONG())


#b.FileSource

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import (FileSource, StreamFormat, FileSink,
                                                       OutputFileConfig, RollingPolicy, BucketAssigner)
from pyflink.common import WatermarkStrategy, Encoder, Types

from azure.storage.filedatalake import FileSystemClient

file_system = FileSystemClient.from_connection_string(connection_str, file_system_name="my_fs")
# setting the stream environment object
env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
env.add_jars("file:///opt/flink/plugins/azure/flink-azure-fs-hadoop-1.16.0.jar")
env.add_classpaths("file:///opt/flink/plugins/azure/flink-azure-fs-hadoop-1.16.0.jar")

file_client = file_system.get_file_client(my_file)
input_path = 'abfss://' + file_client.url[8:]
print('URL is  ===== >>>>',file_client.url)

# Source
ds = env.from_source(
            source=FileSource.for_record_stream_format(StreamFormat.text_line_format(),
                                                       input_path)
                             .process_static_file_set().build(),
            watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
            source_name="file_source"
 )

ds.sink_to(
            sink=FileSink.for_row_format(
                base_path=output_path,
                encoder=Encoder.simple_string_encoder())
            .with_bucket_assigner(BucketAssigner.base_path_bucket_assigner())
                .build())
ds.print()
env.execute()


#4.Create using Table & SQL connectors

from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(stream_execution_environment=env)

t_env.execute_sql("""
        CREATE TABLE my_source (
          a INT,
          b VARCHAR
        ) WITH (
          'connector' = 'datagen',
          'number-of-rows' = '10'
        )
    """)

ds = t_env.to_append_stream(
    t_env.from_path('my_source'),
    Types.ROW([Types.INT(), Types.STRING()]))
#converting table to stream

#Note The StreamExecutionEnvironment env should be specified when creating the TableEnvironment t_env.
 #As all the Java Table & SQL connectors could be used in PyFlink Table API, this means that all of them could also
# be used in PyFlink DataStream API.

#5.converison

# convert a DataStream to a Table
table = t_env.from_data_stream(ds, 'a, b, c')

# convert a Table to a DataStream
ds = t_env.to_append_stream(table, Types.ROW([Types.INT(), Types.STRING()]))
# or
ds = t_env.to_retract_stream(table, Types.ROW([Types.INT(), Types.STRING()])


#6.h