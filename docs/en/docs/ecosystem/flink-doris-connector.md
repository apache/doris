---
{
    "title": "Flink Doris Connector",
    "language": "en"
}
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Flink Doris Connector

> This document applies to flink-doris-connector versions after 1.1.0, for versions before 1.1.0 refer to [here](https://doris.apache.org/docs/0.15/extending-doris/flink-doris-connector)



The Flink Doris Connector can support operations (read, insert, modify, delete) data stored in Doris through Flink.

Github: https://github.com/apache/doris-flink-connector

* `Doris` table can be mapped to `DataStream` or `Table`.

>**Note:**
>
>1. Modification and deletion are only supported on the Unique Key model
>2. The current deletion is to support Flink CDC to access data to achieve automatic deletion. If it is to delete other data access methods, you need to implement it yourself. For the data deletion usage of Flink CDC, please refer to the last section of this document

## Version Compatibility

| Connector Version | Flink Version | Doris Version | Java Version | Scala Version |
| --------- | ----- | ------ | ---- | ----- |
| 1.0.3     | 1.11+ | 0.15+  | 8    | 2.11,2.12 |
| 1.1.0     | 1.14  | 1.0+   | 8    | 2.11,2.12 |
| 1.2.0     | 1.15  | 1.0+   | 8    | -         |
| 1.3.0     | 1.16  | 1.0+   | 8    | -         |

## Build and Install

Ready to work

1.Modify the `custom_env.sh.tpl` file and rename it to `custom_env.sh`

2.Specify the thrift installation directory

```bash
##source file content
#export THRIFT_BIN=
#export MVN_BIN=
#export JAVA_HOME=

##amend as below,MacOS as an example
export THRIFT_BIN=/opt/homebrew/Cellar/thrift@0.13.0/0.13.0/bin/thrift
#export MVN_BIN=
#export JAVA_HOME=
```

Install `thrift` 0.13.0 (Note: `Doris` 0.15 and the latest builds are based on `thrift` 0.13.0, previous versions are still built with `thrift` 0.9.3)
Windows:
  1. Download: `http://archive.apache.org/dist/thrift/0.13.0/thrift-0.13.0.exe`
  2. Modify thrift-0.13.0.exe to thrift 
 
MacOS:
  1. Download: `brew install thrift@0.13.0`
  2. default address: /opt/homebrew/Cellar/thrift@0.13.0/0.13.0/bin/thrift

Note: Executing `brew install thrift@0.13.0` on MacOS may report an error that the version cannot be found. The solution is as follows, execute it in the terminal:
    1. `brew tap-new $USER/local-tap`
    2. `brew extract --version='0.13.0' thrift $USER/local-tap`
    3. `brew install thrift@0.13.0`
 Reference link: `https://gist.github.com/tonydeng/02e571f273d6cce4230dc8d5f394493c`
 
Linux:
   ```bash
    1. wget https://archive.apache.org/dist/thrift/0.13.0/thrift-0.13.0.tar.gz  # Download source package
    2. yum install -y autoconf automake libtool cmake ncurses-devel openssl-devel lzo-devel zlib-devel gcc gcc-c++  # Install dependencies
    3. tar zxvf thrift-0.13.0.tar.gz
    4. cd thrift-0.13.0
    5. ./configure --without-tests
    6. make
    7. make install
    8. thrift --version  # Check the version after installation is complete
   ```
   Note: If you have compiled Doris, you do not need to install thrift, you can directly use `$DORIS_HOME/thirdparty/installed/bin/thrift`


Execute following command in source dir:

```bash
sh build.sh

  Usage:
    build.sh --flink version # specify flink version (after flink-doris-connector v1.2 and flink-1.15, there is no need to provide scala version)
    build.sh --tag           # this is a build from tag
  e.g.:
    build.sh --flink 1.16.0
    build.sh --tag
```
Then, for example, execute the command to compile according to the version you need:
`sh build.sh --flink 1.16.0`


After successful compilation, the file `flink-doris-connector-1.16-1.3.0-SNAPSHOT.jar` will be generated in the `target/` directory. Copy this file to `classpath` in `Flink` to use `Flink-Doris-Connector`. For example, `Flink` running in `Local` mode, put this file in the `lib/` folder. `Flink` running in `Yarn` cluster mode, put this file in the pre-deployment package.

**Remarks:** 

1. Doris FE should be configured to enable http v2 in the configuration

   conf/fe.conf

```
enable_http_server_v2 = true
```
## Using Maven

Add flink-doris-connector Maven dependencies

```
<!-- flink-doris-connector -->
<dependency>
  <groupId>org.apache.doris</groupId>
  <artifactId>flink-doris-connector-1.16</artifactId>
  <version>1.3.0</version>
</dependency>  
```

**Notes**

1. Please replace the corresponding Connector and Flink dependency versions according to different Flink versions. Version 1.3.0 only supports Flink1.16

2. You can also download the relevant version jar package from [here](https://repo.maven.apache.org/maven2/org/apache/doris/).

## How to use

There are three ways to use Flink Doris Connector. 

* SQL
* DataStream

### Parameters Configuration

Flink Doris Connector Sink writes data to Doris by the `Stream load`, and also supports the configurations of `Stream load`, For specific parameters, please refer to [here](../data-operate/import/import-way/stream-load-manual.md).

* SQL  configured by `sink.properties.` in the `WITH`
* DataStream configured by `DorisExecutionOptions.builder().setStreamLoadProp(Properties)`


### SQL

* Source

```sql
CREATE TABLE flink_doris_source (
    name STRING,
    age INT,
    price DECIMAL(5,2),
    sale DOUBLE
    ) 
    WITH (
      'connector' = 'doris',
      'fenodes' = 'FE_IP:8030',
      'table.identifier' = 'database.table',
      'username' = 'root',
      'password' = 'password'
);
```

* Sink

```sql
-- enable checkpoint
SET 'execution.checkpointing.interval' = '10s';
CREATE TABLE flink_doris_sink (
    name STRING,
    age INT,
    price DECIMAL(5,2),
    sale DOUBLE
    ) 
    WITH (
      'connector' = 'doris',
      'fenodes' = 'FE_IP:8030',
      'table.identifier' = 'db.table',
      'username' = 'root',
      'password' = 'password',
      'sink.label-prefix' = 'doris_label'
);
```

* Insert

```sql
INSERT INTO flink_doris_sink select name,age,price,sale from flink_doris_source
```

### DataStream

* Source

```java
DorisOptions.Builder builder = DorisOptions.builder()
        .setFenodes("FE_IP:8030")
        .setTableIdentifier("db.table")
        .setUsername("root")
        .setPassword("password");

DorisSource<List<?>> dorisSource = DorisSourceBuilder.<List<?>>builder()
        .setDorisOptions(builder.build())
        .setDorisReadOptions(DorisReadOptions.builder().build())
        .setDeserializer(new SimpleListDeserializationSchema())
        .build();

env.fromSource(dorisSource, WatermarkStrategy.noWatermarks(), "doris source").print();
```

* Sink

**String Stream**

```java
// enable checkpoint
env.enableCheckpointing(10000);
// using batch mode for bounded data
env.setRuntimeMode(RuntimeExecutionMode.BATCH);

DorisSink.Builder<String> builder = DorisSink.builder();
DorisOptions.Builder dorisBuilder = DorisOptions.builder();
dorisBuilder.setFenodes("FE_IP:8030")
        .setTableIdentifier("db.table")
        .setUsername("root")
        .setPassword("password");

Properties properties = new Properties();
/**
json format to streamload
properties.setProperty("format", "json");
properties.setProperty("read_json_by_line", "true");
**/

DorisExecutionOptions.Builder  executionBuilder = DorisExecutionOptions.builder();
executionBuilder.setLabelPrefix("label-doris") //streamload label prefix
                .setStreamLoadProp(properties); 

builder.setDorisReadOptions(DorisReadOptions.builder().build())
        .setDorisExecutionOptions(executionBuilder.build())
        .setSerializer(new SimpleStringSerializer()) //serialize according to string 
        .setDorisOptions(dorisBuilder.build());


//mock string source
List<Tuple2<String, Integer>> data = new ArrayList<>();
data.add(new Tuple2<>("doris",1));
DataStreamSource<Tuple2<String, Integer>> source = env.fromCollection(data);

source.map((MapFunction<Tuple2<String, Integer>, String>) t -> t.f0 + "\t" + t.f1)
      .sinkTo(builder.build());
```

**RowData Stream**

```java
// enable checkpoint
env.enableCheckpointing(10000);
// using batch mode for bounded data
env.setRuntimeMode(RuntimeExecutionMode.BATCH);

//doris sink option
DorisSink.Builder<RowData> builder = DorisSink.builder();
DorisOptions.Builder dorisBuilder = DorisOptions.builder();
dorisBuilder.setFenodes("FE_IP:8030")
        .setTableIdentifier("db.table")
        .setUsername("root")
        .setPassword("password");

// json format to streamload
Properties properties = new Properties();
properties.setProperty("format", "json");
properties.setProperty("read_json_by_line", "true");
DorisExecutionOptions.Builder  executionBuilder = DorisExecutionOptions.builder();
executionBuilder.setLabelPrefix("label-doris") //streamload label prefix
                .setStreamLoadProp(properties); //streamload params

//flink rowdata‘s schema
String[] fields = {"city", "longitude", "latitude", "destroy_date"};
DataType[] types = {DataTypes.VARCHAR(256), DataTypes.DOUBLE(), DataTypes.DOUBLE(), DataTypes.DATE()};

builder.setDorisReadOptions(DorisReadOptions.builder().build())
        .setDorisExecutionOptions(executionBuilder.build())
        .setSerializer(RowDataSerializer.builder()    //serialize according to rowdata 
                           .setFieldNames(fields)
                           .setType("json")           //json format
                           .setFieldType(types).build())
        .setDorisOptions(dorisBuilder.build());

//mock rowdata source
DataStream<RowData> source = env.fromElements("")
    .map(new MapFunction<String, RowData>() {
        @Override
        public RowData map(String value) throws Exception {
            GenericRowData genericRowData = new GenericRowData(4);
            genericRowData.setField(0, StringData.fromString("beijing"));
            genericRowData.setField(1, 116.405419);
            genericRowData.setField(2, 39.916927);
            genericRowData.setField(3, LocalDate.now().toEpochDay());
            return genericRowData;
        }
    });

source.sinkTo(builder.build());
```

**SchemaChange Stream**
```java
// enable checkpoint
env.enableCheckpointing(10000);

Properties props = new Properties();
props.setProperty("format", "json");
props.setProperty("read_json_by_line", "true");
DorisOptions dorisOptions = DorisOptions.builder()
        .setFenodes("127.0.0.1:8030")
        .setTableIdentifier("test.t1")
        .setUsername("root")
        .setPassword("").build();

DorisExecutionOptions.Builder  executionBuilder = DorisExecutionOptions.builder();
executionBuilder.setLabelPrefix("label-doris" + UUID.randomUUID())
        .setStreamLoadProp(props).setDeletable(true);

DorisSink.Builder<String> builder = DorisSink.builder();
builder.setDorisReadOptions(DorisReadOptions.builder().build())
        .setDorisExecutionOptions(executionBuilder.build())
        .setDorisOptions(dorisOptions)
        .setSerializer(JsonDebeziumSchemaSerializer.builder().setDorisOptions(dorisOptions).build());

env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")//.print();
        .sinkTo(builder.build());
```
refer: [CDCSchemaChangeExample](https://github.com/apache/doris-flink-connector/blob/master/flink-doris-connector/src/test/java/org/apache/doris/flink/CDCSchemaChangeExample.java)


### General

| Key                              | Default Value     | Required | Comment                                                      |
| -------------------------------- | ----------------- | ------------------------------------------------------------ | -------------------------------- |
| fenodes                    | --                | Y               | Doris FE http address, support multiple addresses, separated by commas            |
| table.identifier           | --                | Y               | Doris table identifier, eg, db1.tbl1                                 |
| username                            | --            | Y           | Doris username                                            |
| password                        | --            | Y           | Doris password                                              |
| doris.request.retries            | 3                 | N                | Number of retries to send requests to Doris                                    |
| doris.request.connect.timeout.ms | 30000             | N            | Connection timeout for sending requests to Doris                                |
| doris.request.read.timeout.ms    | 30000             | N            | Read timeout for sending request to Doris                                |
| doris.request.query.timeout.s    | 3600              | N             | Query the timeout time of doris, the default is 1 hour, -1 means no timeout limit             |
| doris.request.tablet.size        | Integer.MAX_VALUE | N | The number of Doris Tablets corresponding to an Partition. The smaller this value is set, the more partitions will be generated. This will increase the parallelism on the flink side, but at the same time will cause greater pressure on Doris. |
| doris.batch.size                 | 1024              | N             | The maximum number of rows to read data from BE at one time. Increasing this value can reduce the number of connections between Flink and Doris. Thereby reducing the extra time overhead caused by network delay. |
| doris.exec.mem.limit             | 2147483648        | N       | Memory limit for a single query. The default is 2GB, in bytes.                     |
| doris.deserialize.arrow.async    | false             | N            | Whether to support asynchronous conversion of Arrow format to RowBatch required for flink-doris-connector iteration           |
| doris.deserialize.queue.size     | 64                | N               | Asynchronous conversion of the internal processing queue in Arrow format takes effect when doris.deserialize.arrow.async is true        |
| doris.read.field            | --            | N           | List of column names in the Doris table, separated by commas                  |
| doris.filter.query          | --            | N           | Filter expression of the query, which is transparently transmitted to Doris. Doris uses this expression to complete source-side data filtering. |
| sink.label-prefix | -- | Y | The label prefix used by stream load imports. In the 2pc scenario, global uniqueness is required to ensure the EOS semantics of Flink. |
| sink.properties.*     | --               | N              | The stream load parameters.<br /> <br /> eg:<br /> sink.properties.column_separator' = ','<br /> <br /> Setting 'sink.properties.escape_delimiters' = 'true' if you want to use a control char as a separator, so that such as '\\x01' will translate to binary 0x01<br /><br />Support JSON format import, you need to enable both 'sink.properties.format' ='json' and 'sink.properties.strip_outer_array' ='true' |
| sink.enable-delete     | true               | N              | Whether to enable deletion. This option requires Doris table to enable batch delete function (0.15+ version is enabled by default), and only supports Uniq model.|
| sink.enable-2pc                  | true              | N        | Whether to enable two-phase commit (2pc), the default is true, to ensure Exactly-Once semantics. For two-phase commit, please refer to [here](../data-operate/import/import-way/stream-load-manual.md). |
| sink.max-retries                 | 1                  | N        | In the 2pc scenario, the number of retries after the commit phase fails.                                                                                                                                                                                                                                         |
| sink.buffer-size                 | 1048576(1MB)       | N        | Write data cache buffer size, in bytes. It is not recommended to modify, the default configuration is sufficient.                                                                                                                                                                                                                                 |
| sink.buffer-count                | 3                  | N        | The number of write data cache buffers, it is not recommended to modify, the default configuration is sufficient.                                                                                                                               



## Doris & Flink Column Type Mapping

| Doris Type | Flink Type                       |
| ---------- | -------------------------------- |
| NULL_TYPE  | NULL              |
| BOOLEAN    | BOOLEAN       |
| TINYINT    | TINYINT              |
| SMALLINT   | SMALLINT              |
| INT        | INT            |
| BIGINT     | BIGINT               |
| FLOAT      | FLOAT              |
| DOUBLE     | DOUBLE            |
| DATE       | DATE |
| DATETIME   | TIMESTAMP |
| DECIMAL    | DECIMAL                      |
| CHAR       | STRING             |
| LARGEINT   | STRING             |
| VARCHAR    | STRING            |
| DECIMALV2  | DECIMAL                      |
| TIME       | DOUBLE             |
| HLL        | Unsupported datatype             |

## An example of using Flink CDC to access Doris (supports insert/update/delete events)
```sql
SET 'execution.checkpointing.interval' = '10s';
CREATE TABLE cdc_mysql_source (
  id int
  ,name VARCHAR
  ,PRIMARY KEY (id) NOT ENFORCED
) WITH (
 'connector' = 'mysql-cdc',
 'hostname' = '127.0.0.1',
 'port' = '3306',
 'username' = 'root',
 'password' = 'password',
 'database-name' = 'database',
 'table-name' = 'table'
);

-- Support delete event synchronization (sink.enable-delete='true'), requires Doris table to enable batch delete function
CREATE TABLE doris_sink (
id INT,
name STRING
) 
WITH (
  'connector' = 'doris',
  'fenodes' = '127.0.0.1:8030',
  'table.identifier' = 'database.table',
  'username' = 'root',
  'password' = '',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.enable-delete' = 'true',
  'sink.label-prefix' = 'doris_label'
);

insert into doris_sink select id,name from cdc_mysql_source;
```

## Use FlinkCDC to update Key column
Generally, in a business database, the number is used as the primary key of the table, such as the Student table, the number (id) is used as the primary key, but with the development of the business, the number corresponding to the data may change.
In this scenario, using FlinkCDC + Doris Connector to synchronize data can automatically update the data in the Doris primary key column.
### Principle
The underlying collection tool of Flink CDC is Debezium. Debezium internally uses the op field to identify the corresponding operation: the values of the op field are c, u, d, and r, corresponding to create, update, delete, and read.
For the update of the primary key column, FlinkCDC will send DELETE and INSERT events downstream, and after the data is synchronized to Doris, it will automatically update the data of the primary key column.

### Example
The Flink program can refer to the CDC synchronization example above. After the task is successfully submitted, execute the Update primary key column statement (`update student set id = '1002' where id = '1001'`) on the MySQL side to modify the data in Doris .

## Java example

`samples/doris-demo/`  An example of the Java version is provided below for reference, see [here](https://github.com/apache/doris/tree/master/samples/doris-demo/)

## Best Practices

### Application scenarios

The most suitable scenario for using Flink Doris Connector is to synchronize source data to Doris (Mysql, Oracle, PostgreSQL) in real time/batch, etc., and use Flink to perform joint analysis on data in Doris and other data sources. You can also use Flink Doris Connector

### Other

1. The Flink Doris Connector mainly relies on Checkpoint for streaming writing, so the interval between Checkpoints is the visible delay time of the data.
2. To ensure the Exactly Once semantics of Flink, the Flink Doris Connector enables two-phase commit by default, and Doris enables two-phase commit by default after version 1.1. 1.0 can be enabled by modifying the BE parameters, please refer to [two_phase_commit](../data-operate/import/import-way/stream-load-manual.md).

## FAQ

1. **After Doris Source finishes reading data, why does the stream end?**

Currently Doris Source is a bounded stream and does not support CDC reading.

2. **Can Flink read Doris and perform conditional pushdown?**

By configuring the doris.filter.query parameter, refer to the configuration section for details.

3. **How to write Bitmap type?**

```sql
CREATE TABLE bitmap_sink (
dt int,
page string,
user_id int
)
WITH (
   'connector' = 'doris',
   'fenodes' = '127.0.0.1:8030',
   'table.identifier' = 'test.bitmap_test',
   'username' = 'root',
   'password' = '',
   'sink.label-prefix' = 'doris_label',
   'sink.properties.columns' = 'dt,page,user_id,user_id=to_bitmap(user_id)'
)
````
4. **errCode = 2, detailMessage = Label [label_0_1] has already been used, relate to txn [19650]**

In the Exactly-Once scenario, the Flink Job must be restarted from the latest Checkpoint/Savepoint, otherwise the above error will be reported.
When Exactly-Once is not required, it can also be solved by turning off 2PC commits (sink.enable-2pc=false) or changing to a different sink.label-prefix.

5. **errCode = 2, detailMessage = transaction [19650] not found**

Occurred in the Commit phase, the transaction ID recorded in the checkpoint has expired on the FE side, and the above error will occur when committing again at this time.
At this time, it cannot be started from the checkpoint, and the expiration time can be extended by modifying the streaming_label_keep_max_second configuration in fe.conf, which defaults to 12 hours.

6. **errCode = 2, detailMessage = current running txns on db 10006 is 100, larger than limit 100**

This is because the concurrent import of the same library exceeds 100, which can be solved by adjusting the parameter `max_running_txn_num_per_db` of fe.conf. For details, please refer to [max_running_txn_num_per_db](https://doris.apache.org/zh-CN/docs/dev/admin-manual/config/fe-config/#max_running_txn_num_per_db)

7. **How to ensure the order of a batch of data when Flink writes to the Uniq model?**

You can add sequence column configuration to ensure that, for details, please refer to [sequence](https://doris.apache.org/zh-CN/docs/dev/data-operate/update-delete/sequence-column-manual)

8. **The Flink task does not report an error, but the data cannot be synchronized? **

Before Connector1.1.0, it was written in batches, and the writing was driven by data. It was necessary to determine whether there was data written upstream. After 1.1.0, it depends on Checkpoint, and Checkpoint must be enabled to write.

9. **tablet writer write failed, tablet_id=190958, txn_id=3505530, err=-235**

It usually occurs before Connector1.1.0, because the writing frequency is too fast, resulting in too many versions. The frequency of Streamload can be reduced by setting the sink.batch.size and sink.batch.interval parameters.

10. **Flink imports dirty data, how to skip it? **

When Flink imports data, if there is dirty data, such as field format, length, etc., it will cause StreamLoad to report an error, and Flink will continue to retry at this time. If you need to skip, you can disable the strict mode of StreamLoad (strict_mode=false, max_filter_ratio=1) or filter the data before the Sink operator.

11. **How should the source table and Doris table correspond?**
When using Flink Connector to import data, pay attention to two aspects. The first is that the columns and types of the source table correspond to the columns and types in flink sql; the second is that the columns and types in flink sql must match those of the doris table For the correspondence between columns and types, please refer to the above "Doris & Flink Column Type Mapping" for details
