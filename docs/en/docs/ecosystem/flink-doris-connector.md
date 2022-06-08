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

- The Flink Doris Connector can support operations (read, insert, modify, delete) data stored in Doris through Flink.

Github: https://github.com/apache/incubator-doris-flink-connector

* `Doris` table can be mapped to `DataStream` or `Table`.

>**Note:**
>
>1. Modification and deletion are only supported on the Unique Key model
>2. The current deletion is to support Flink CDC to access data to achieve automatic deletion. If it is to delete other data access methods, you need to implement it yourself. For the data deletion usage of Flink CDC, please refer to the last section of this document

## Version Compatibility

| Connector | Flink | Doris  | Java | Scala |
| --------- | ----- | ------ | ---- | ----- |
| 1.11.6-2.12-xx | 1.11.x | 0.13+  | 8    | 2.12  |
| 1.12.7-2.12-xx | 1.12.x | 0.13.+ | 8 | 2.12 |
| 1.13.5-2.12-xx | 1.13.x | 0.13.+ | 8 | 2.12 |
| 1.14.4-2.12-xx | 1.14.x | 0.13.+ | 8 | 2.12 |

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
    1.Download source package：`wget https://archive.apache.org/dist/thrift/0.13.0/thrift-0.13.0.tar.gz`
    2.Install dependencies：`yum install -y autoconf automake libtool cmake ncurses-devel openssl-devel lzo-devel zlib-devel gcc gcc-c++`
    3.`tar zxvf thrift-0.13.0.tar.gz`
    4.`cd thrift-0.13.0`
    5.`./configure --without-tests`
    6.`make`
    7.`make install`
   Check the version after installation is complete：thrift --version
   Note: If you have compiled Doris, you do not need to install thrift, you can directly use $DORIS_HOME/thirdparty/installed/bin/thrift
```

Execute following command in source dir:

```bash
sh build.sh

  Usage:
    build.sh --flink version --scala version # specify flink and scala version
    build.sh --tag                           # this is a build from tag
  e.g.:
    build.sh --flink 1.14.3 --scala 2.12
    build.sh --tag
Then, for example, execute the command to compile according to the version you need:
sh build.sh --flink 1.14.3 --scala 2.12
```

> Note: If you check out the source code from tag, you can just run `sh build.sh --tag` without specifying the flink and scala versions. This is because the version in the tag source code is fixed. For example, `1.13.5_2.12-1.0.1` means flink version 1.13.5, scala version 2.12, and connector version 1.0.1.

After successful compilation, the file `flink-doris-connector-1.14_2.12-1.0.0-SNAPSHOT.jar` will be generated in the `output/` directory. Copy this file to `ClassPath` in `Flink` to use `Flink-Doris-Connector`. For example, `Flink` running in `Local` mode, put this file in the `lib/` folder. `Flink` running in `Yarn` cluster mode, put this file in the pre-deployment package.

**Remarks:** 

1. Doris FE should be configured to enable http v2 in the configuration
2. Scala version currently supports 2.12 and 2.11

conf/fe.conf

```
enable_http_server_v2 = true
```
## Using Maven

```
<dependency>
  <groupId>org.apache.doris</groupId>
  <artifactId>flink-doris-connector-1.14_2.12</artifactId>
  <!--artifactId>flink-doris-connector-1.13_2.12</artifactId-->
  <!--artifactId>flink-doris-connector-1.12_2.12</artifactId-->
  <!--artifactId>flink-doris-connector-1.11_2.12</artifactId-->
  <version>1.0.3</version>
</dependency>
```

**Notes**

1.Please replace the Connector version according to the different Flink and Scala versions.
2.At present, only the scala2.12 version of the package is provided in maven. The 2.11 version of the package needs to be compiled by itself. Please refer to the compilation and installation section above.

## How to use

There are three ways to use Flink Doris Connector. 

* SQL
* DataStream
* DataSet

### Parameters Configuration

Flink Doris Connector Sink writes data to Doris by the `Stream load`, and also supports the configurations of `Stream load`

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
      'fenodes' = '$YOUR_DORIS_FE_HOSTNAME:$YOUR_DORIS_FE_RESFUL_PORT',
      'table.identifier' = '$YOUR_DORIS_DATABASE_NAME.$YOUR_DORIS_TABLE_NAME',
      'username' = '$YOUR_DORIS_USERNAME',
      'password' = '$YOUR_DORIS_PASSWORD'
);
```

* Sink

```sql
CREATE TABLE flink_doris_sink (
    name STRING,
    age INT,
    price DECIMAL(5,2),
    sale DOUBLE
    ) 
    WITH (
      'connector' = 'doris',
      'fenodes' = '$YOUR_DORIS_FE_HOSTNAME:$YOUR_DORIS_FE_RESFUL_PORT',
      'table.identifier' = '$YOUR_DORIS_DATABASE_NAME.$YOUR_DORIS_TABLE_NAME',
      'username' = '$YOUR_DORIS_USERNAME',
      'password' = '$YOUR_DORIS_PASSWORD'
);
```

* Insert

```sql
INSERT INTO flink_doris_sink select name,age,price,sale from flink_doris_source
```

### DataStream

* Source

```java
 Properties properties = new Properties();
 properties.put("fenodes","FE_IP:8030");
 properties.put("username","root");
 properties.put("password","");
 properties.put("table.identifier","db.table");
 env.addSource(new DorisSourceFunction(
                        new DorisStreamOptions(properties), 
                        new SimpleListDeserializationSchema()
                )
        ).print(); 
```

* Sink

Json Stream

```java
Properties pro = new Properties();
pro.setProperty("format", "json");
pro.setProperty("strip_outer_array", "true");
env.fromElements(
    "{\"longitude\": \"116.405419\", \"city\": \"北京\", \"latitude\": \"39.916927\"}"
    )
     .addSink(
     	DorisSink.sink(
            DorisReadOptions.builder().build(),
         	DorisExecutionOptions.builder()
                    .setBatchSize(3)
                    .setBatchIntervalMs(0l)
                    .setMaxRetries(3)
                    .setStreamLoadProp(pro).build(),
         	DorisOptions.builder()
                    .setFenodes("FE_IP:8030")
                    .setTableIdentifier("db.table")
                    .setUsername("root")
                    .setPassword("").build()
     	));
```

Json Stream

```java
env.fromElements(
    "{\"longitude\": \"116.405419\", \"city\": \"北京\", \"latitude\": \"39.916927\"}"
    )
    .addSink(
    	DorisSink.sink(
        	DorisOptions.builder()
                    .setFenodes("FE_IP:8030")
                    .setTableIdentifier("db.table")
                    .setUsername("root")
                    .setPassword("").build()
    	));
```

RowData Stream

```java
DataStream<RowData> source = env.fromElements("")
    .map(new MapFunction<String, RowData>() {
        @Override
        public RowData map(String value) throws Exception {
            GenericRowData genericRowData = new GenericRowData(3);
            genericRowData.setField(0, StringData.fromString("北京"));
            genericRowData.setField(1, 116.405419);
            genericRowData.setField(2, 39.916927);
            return genericRowData;
        }
    });

String[] fields = {"city", "longitude", "latitude"};
LogicalType[] types = {new VarCharType(), new DoubleType(), new DoubleType()};

source.addSink(
    DorisSink.sink(
        fields,
        types,
        DorisReadOptions.builder().build(),
        DorisExecutionOptions.builder()
            .setBatchSize(3)
            .setBatchIntervalMs(0L)
            .setMaxRetries(3)
            .build(),
        DorisOptions.builder()
            .setFenodes("FE_IP:8030")
            .setTableIdentifier("db.table")
            .setUsername("root")
            .setPassword("").build()
    ));
```

### DataSet

* Sink

```java
MapOperator<String, RowData> data = env.fromElements("")
    .map(new MapFunction<String, RowData>() {
        @Override
        public RowData map(String value) throws Exception {
            GenericRowData genericRowData = new GenericRowData(3);
            genericRowData.setField(0, StringData.fromString("北京"));
            genericRowData.setField(1, 116.405419);
            genericRowData.setField(2, 39.916927);
            return genericRowData;
        }
    });

DorisOptions dorisOptions = DorisOptions.builder()
    .setFenodes("FE_IP:8030")
    .setTableIdentifier("db.table")
    .setUsername("root")
    .setPassword("").build();
DorisReadOptions readOptions = DorisReadOptions.defaults();
DorisExecutionOptions executionOptions = DorisExecutionOptions.defaults();

LogicalType[] types = {new VarCharType(), new DoubleType(), new DoubleType()};
String[] fields = {"city", "longitude", "latitude"};

DorisDynamicOutputFormat outputFormat = new DorisDynamicOutputFormat(
    dorisOptions, readOptions, executionOptions, types, fields
    );

outputFormat.open(0, 1);
data.output(outputFormat);
outputFormat.close();
```



### General

| Key                              | Default Value     | Comment                                                      |
| -------------------------------- | ----------------- | ------------------------------------------------------------ |
| fenodes                    | --                | Doris FE http address, support multiple addresses, separated by commas            |
| table.identifier           | --                | Doris table identifier, eg, db1.tbl1                                 |
| username                            | --            | Doris username                                            |
| password                        | --            | Doris password                                              |
| doris.request.retries            | 3                 | Number of retries to send requests to Doris                                    |
| doris.request.connect.timeout.ms | 30000             | Connection timeout for sending requests to Doris                                |
| doris.request.read.timeout.ms    | 30000             | Read timeout for sending request to Doris                                |
| doris.request.query.timeout.s    | 3600              | Query the timeout time of doris, the default is 1 hour, -1 means no timeout limit             |
| doris.request.tablet.size        | Integer.MAX_VALUE | The number of Doris Tablets corresponding to an Partition. The smaller this value is set, the more partitions will be generated. This will increase the parallelism on the flink side, but at the same time will cause greater pressure on Doris. |
| doris.batch.size                 | 1024              | The maximum number of rows to read data from BE at one time. Increasing this value can reduce the number of connections between Flink and Doris. Thereby reducing the extra time overhead caused by network delay. |
| doris.exec.mem.limit             | 2147483648        | Memory limit for a single query. The default is 2GB, in bytes.                     |
| doris.deserialize.arrow.async    | false             | Whether to support asynchronous conversion of Arrow format to RowBatch required for flink-doris-connector iteration           |
| doris.deserialize.queue.size     | 64                | Asynchronous conversion of the internal processing queue in Arrow format takes effect when doris.deserialize.arrow.async is true        |
| doris.read.field            | --            | List of column names in the Doris table, separated by commas                  |
| doris.filter.query          | --            | Filter expression of the query, which is transparently transmitted to Doris. Doris uses this expression to complete source-side data filtering. |
| sink.batch.size                        | 10000          | Maximum number of lines in a single write BE                                             |
| sink.max-retries                        | 1          | Number of retries after writing BE failed                                              |
| sink.batch.interval                         | 10s           | The flush interval, after which the asynchronous thread will write the data in the cache to BE. The default value is 10 second, and the time units are ms, s, min, h, and d. Set to 0 to turn off periodic writing. |
| sink.properties.*     | --               | The stream load parameters.<br /> <br /> eg:<br /> sink.properties.column_separator' = ','<br /> <br />  Setting 'sink.properties.escape_delimiters' = 'true' if you want to use a control char as a separator, so that such as '\\x01' will translate to binary 0x01<br /><br />  Support JSON format import, you need to enable both 'sink.properties.format' ='json' and 'sink.properties.strip_outer_array' ='true'|
| sink.enable-delete     | true               | Whether to enable deletion. This option requires Doris table to enable batch delete function (0.15+ version is enabled by default), and only supports Uniq model.|
| sink.batch.bytes                        | 10485760          | Maximum bytes of batch in a single write to BE. When the data size in batch exceeds this threshold, cache data is written to BE. The default value is 10MB |

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
| DATE       | STRING |
| DATETIME   | STRING |
| DECIMAL    | DECIMAL                      |
| CHAR       | STRING             |
| LARGEINT   | STRING             |
| VARCHAR    | STRING            |
| DECIMALV2  | DECIMAL                      |
| TIME       | DOUBLE             |
| HLL        | Unsupported datatype             |

## An example of using Flink CDC to access Doris (supports insert/update/delete events)
```sql
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
  'sink.properties.strip_outer_array' = 'true',
  'sink.enable-delete' = 'true'
);

insert into doris_sink select id,name from cdc_mysql_source;
```

## Java example

`samples/doris-demo/fink-demo/`  An example of the Java version is provided below for reference, see [here](https://github.com/apache/incubator-doris/tree/master/samples/doris-demo/flink-demo)

## Best Practices

### Application scenarios

The most suitable scenario for using Flink Doris Connector is to synchronize source data to Doris (Mysql, Oracle, PostgreSQL) in real time/batch, etc., and use Flink to perform joint analysis on data in Doris and other data sources. You can also use Flink Doris Connector

### The amount of data

The writing frequency of Flink Doris Connector is mainly controlled by sink.batch.size, sink.batch.interval and sink.batch.bytes

These three parameters are used to control the execution time of a single task. When any one of the thresholds is reached, the task ends. where `sink.batch.size` is used to record the number of lines of data written by the word. `sink.batch.interval` indicates the interval to start writing data, `sink.batch.bytes`, the amount of data to be written at a time, in bytes. The current consumption rate of a task is about 5-10MB/s.

Then suppose a row of data is 500B, and the user wants every 100MB or 10 seconds to be a task. The expected processing time for 100MB is 10-20 seconds, which corresponds to about 200,000 rows. Then a reasonable configuration is:

```
"sink.batch.interval" = "10",
"sink.batch.size" = "200000",
"sink.batch.bytes" = "104857600"
```

### common problem

1. Could not execute SQL statement. Reason：java.lang.IllegalAraumenException: Row parity: 32，but serializer rarity：31

Because Doris has a hidden column, you need to manually add a column `__DORIS_DELETE_SIGN__` in Flink Schema Type: TINYINT

2. Bitmap type write
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
   'sink.properties.columns' = 'dt,page,user_id,user_id=to_bitmap(user_id)'
)
````
