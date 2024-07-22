#1.Formats

#a.avro

schema = AvroSchema.parse_string("""
{
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "favoriteNumber",  "type": ["int", "null"]},
        {"name": "favoriteColor", "type": ["string", "null"]}
    ]
}
""")

env = StreamExecutionEnvironment.get_execution_environment()
ds = env.create_input(AvroInputFormat(AVRO_FILE_PATH, schema))

def json_dumps(record):
    import json
    return json.dumps(record)

ds.map(json_dumps).print()

#b.CSV

#I.for_record_stream_format
# For PyFlink users, a csv schema can be defined by manually adding columns, and the output type of the csv
# source will be a Row with each column mapped to a field.

-schema = CsvSchema.builder() \
    .add_number_column('id', number_type=DataTypes.BIGINT()) \
    .add_array_column('array', separator='#', element_type=DataTypes.INT()) \
    .add_boolean_column('salaried',type=DataTypes.BOOLEAN()) \
    .add_string_column('name') \
    .set_column_separator(',') \
    .build()

source = FileSource.for_record_stream_format(CsvReaderFormat.for_schema(schema), CSV_FILE_PATH).build()

# the type of record will be Types.ROW_NAMED(['id', 'array'], [Types.LONG(), Types.LIST(Types.INT())])
ds = env.from_source(source, WatermarkStrategy.no_watermarks(), 'csv-surce')

def split(line):
    yield from line.split()


ds = ds.flat_map(split) \
       .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
       .key_by(lambda i: i[0]) \
       .reduce(lambda i, j: (i[0], i[1] + j[1]))



#Similarly to the TextLineInputFormat, CsvReaderFormat can be used in both continues and batch modes

#II.for_bulk_format
#For PyFlink users, CsvBulkWriters could be used to create BulkWriterFactory to write records to files in CSV format.

schema = CsvSchema.builder() \
    .add_number_column('id', number_type=DataTypes.INT()) \
    .add_array_column('list', separator='#', element_type=DataTypes.STRING()) \
    .set_column_separator('|') \
    .build()

sink = FileSink.for_bulk_format(
    OUTPUT_DIR, CsvBulkWriters.for_schema(schema)).build()

ds.sink_to(sink)

#c.JSON

#In PyFlink, JsonRowSerializationSchema and JsonRowDeserializationSchema are built-in support for Row type.
# For example to use it in KafkaSource and KafkaSink:

row_type_info = Types.ROW_NAMED(['name', 'age'], [Types.STRING(), Types.INT()])
json_format = JsonRowDeserializationSchema.builder().type_info(row_type_info).build()

source = KafkaSource.builder() \
    .set_value_only_deserializer(json_format) \
    .build()


row_type_info = Types.ROW_NAMED(['name', 'age'], [Types.STRING(), Types.INT()])
json_format = JsonRowSerializationSchema.builder().with_type_info(row_type_info).build()

sink = KafkaSink.builder() \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
            .set_topic('test')
            .set_value_serialization_schema(json_format)
            .build()
    ) \
    .build()


#d.parquet

#1.Vectorized reader

# Parquet rows are decoded in batches
FileSource.for_bulk_file_format(BulkFormat, Path...)

# Monitor the Paths to read data as unbounded data
FileSource.for_bulk_file_format(BulkFormat, Path...) \
          .monitor_continuously(Duration.of_millis(5)) \
          .build()

#2.Avro Parquet reader (To read Avro records, you will need to add the parquet-avro dependency)

# Parquet rows are decoded in batches
FileSource.for_record_stream_format(StreamFormat, Path...)

# Monitor the Paths to read data as unbounded data
FileSource.for_record_stream_format(StreamFormat, Path...) \
          .monitor_continuously(Duration.of_millis(5)) \
          .build()

#3.Flink RowData #

#In this example, you will create a DataStream containing Parquet records as Flink RowDatas. The schema is projected to
# read only the specified fields (“f7”, “f4” and “f99”).
#Flink will read records in batches of 500 records. The first boolean parameter specifies that timestamp columns will be
# interpreted as UTC. The second boolean instructs the application that the projected Parquet fields names are case-sensitive.
# There is no watermark strategy defined as records do not contain event timestamps.
    row_type = DataTypes.ROW([
    DataTypes.FIELD('f7', DataTypes.DOUBLE()),
    DataTypes.FIELD('f4', DataTypes.INT()),
    DataTypes.FIELD('f99', DataTypes.VARCHAR()),
])
source = FileSource.for_bulk_file_format(ParquetColumnarRowInputFormat(
    row_type=row_type,
    hadoop_config=Configuration(),
    batch_size=500,
    is_utc_timestamp=False,
    is_case_sensitive=True,
), PARQUET_FILE_PATH).build()
ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "file-source")

#4.Avro Parquet Records #
#Flink supports producing three types of Avro records by reading Parquet files (Only Generic record is supported in PyFlink):

#Generic record
#Specific record
#Reflect record

#Generic record #
#Avro schemas are defined using JSON. You can get more information about Avro schemas and types from the Avro specification.
# This example uses an Avro schema example similar to the one described in the official Avro tutorial:


# parsing avro schema
schema = AvroSchema.parse_string("""
{
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "favoriteNumber",  "type": ["int", "null"]},
        {"name": "favoriteColor", "type": ["string", "null"]}
    ]
}
""")

source = FileSource.for_record_stream_format(
    AvroParquetReaders.for_generic_record(schema), # file paths
).build()

env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(10)

stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "file-source")


#E.textfile

#1.Bounded read example:

#In this example we create a DataStream containing the lines of a text file as Strings.
# There is no need for a watermark strategy as records do not contain event timestamps.
source = FileSource.for_record_stream_format(StreamFormat.text_line_format(), *path).build()
stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "file-source")


#2.Continuous read example: In this example, we create a DataStream containing the lines of text files as Strings
# that will infinitely grow as new files are added to the directory. We monitor for new files each second.
# There is no need for a watermark strategy as records do not contain event timestamps.


source = FileSource \
    .for_record_stream_format(StreamFormat.text_line_format(), *path) \
    .monitor_continously(Duration.of_seconds(1)) \
    .build()
stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "file-source")

#F.Amazon Kinesis Data Streams Connector
consumer_config = {
    'aws.region': 'us-east-1',
    'aws.credentials.provider.basic.accesskeyid': 'aws_access_key_id',
    'aws.credentials.provider.basic.secretkey': 'aws_secret_access_key',
    'flink.stream.initpos': 'LATEST'
}

env = StreamExecutionEnvironment.get_execution_environment()

kinesis = env.add_source(FlinkKinesisConsumer("stream-1", SimpleStringSchema(), consumer_config))