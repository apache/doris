---
{
    "title": "Flink doris connector Design",
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
under 

# Flink doris connector Design



First of all, thanks to the author of the community Spark Doris Connector

From the perspective of Doris, by introducing its data into Flink, Flink can use a series of rich ecological products, which broadens the imagination of the product and also makes it possible to query Doris and other data sources jointly.

Starting from our business architecture and business needs, we chose Flink as part of our architecture, the ETL and real-time computing framework for data. The community currently supports Spark doris connector, so we designed and developed Flink doris Connector with reference to Spark doris connector.

##Technical Choice

When the model was originally selected, it was the same as the Spark Doris connector, so we started to consider the JDBC method, but, as described in the Spark doris connector article, this method has advantages, but the disadvantages are more obvious. Later, we read and tested the Spark code and decided to implement it on the shoulders of giants (note: copy the code and modify it directly).

The following content is from the Spark Doris Connector blog, directly copied

```
Therefore, we developed a new data source Spark-Doris-Connector for Doris. Under this scheme, Doris can publish Doris data and distribute it to Spark. The Spark driver accesses Doris's FE to obtain the Doris table architecture and basic data distribution. After that, according to this data distribution, the data query task is reasonably allocated to the executors. Finally, Spark's execution program accesses different BEs for querying. Greatly improve query efficiency
```

## 1. Instructions

Compile and generate doris-flink-1.0.0-SNAPSHOT.jar in the extension/flink-doris-connector/ directory of the Doris code base, add this jar package to the ClausPath of flink, and then you can use Flink-on -Doris function

## 2. how to use

Compile and generate doris-flink-1.0.0-SNAPSHOT.jar in the extension/flink-doris-connector/ directory of the Doris code library, add this jar package to the ClassPath of flink, and then use the Flink-on-Doris function

#### 2.1 SQL way

Support function:

1. Supports reading data in Doris data warehouse tables through Flink SQL to Flink for calculations
2. Support inserting data into the corresponding table of the data warehouse through Flink SQL. The back-end implementation is to communicate directly with BE through Stream Load to complete the data insertion operation
3. You can use Flink to associate non-Doris external data source tables for association analysis

example:



```java
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(
                "CREATE TABLE test_aggregation01 (" +
                        "user_id STRING," +
                        "user_city STRING," +
                        "age INT," +
                        "last_visit_date STRING" +
                        ") " +
                        "WITH (\n" +
                        "  'connector' = 'doris',\n" +
                        "  'fenodes' = 'doris01:8030',\n" +
                        "  'table.identifier' = 'demo.test_aggregation',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = ''\n" +
                        ")");
        tEnv.executeSql(
                "CREATE TABLE test_aggregation02 (" +
                        "user_id STRING," +
                        "user_city STRING," +
                        "age INT," +
                        "last_visit_date STRING" +
                        ") " +
                        "WITH (\n" +
                        "  'connector' = 'doris',\n" +
                        "  'fenodes' = 'doris01:8030',\n" +
                        "  'table.identifier' = 'demo.test_aggregation_01',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = ''\n" +
                        ")");

        tEnv.executeSql("INSERT INTO test_aggregation02 select * from test_aggregation01");
        tEnv.executeSql("select count(1) from test_aggregation01");
```

#### 2.2 DataStream way:

```java
DorisOptions.Builder options = DorisOptions.builder()
                .setFenodes("$YOUR_DORIS_FE_HOSTNAME:$YOUR_DORIS_FE_RESFUL_PORT")
                .setUsername("$YOUR_DORIS_USERNAME")
                .setPassword("$YOUR_DORIS_PASSWORD")
                .setTableIdentifier("$YOUR_DORIS_DATABASE_NAME.$YOUR_DORIS_TABLE_NAME");
env.addSource(new DorisSourceFunction<>(options.build(),new SimpleListDeserializationSchema())).print();
```

## 3. Applicable scene

![1616987965864](/images/Flink-doris-connector.png)

#### 3.1. Use Flink to perform joint analysis on data in Doris and other data sources

Many business departments place their data on different storage systems, such as some online analysis and report data in Doris, some structured retrieval data in Elasticsearch, and some data used for transaction processing in MySQL, and so on. It is often necessary to analyze the business across multiple storage sources. After connecting Flink and Doris through the Flink Doris connector, companies can directly use Flink to perform joint query calculations on the data in Doris and multiple external data sources.

#### 3.2 Real-time data access

Before Flink Doris Connector: For business irregular data, it is usually necessary to perform standardized processing on messages, and write null value filtering into new topics, and then start regular loading to write Doris.

![1616988281677](/images/Flink-doris-connector1.png)

After Flink Doris Connector: flink reads kafka and writes doris directly.

![1616988514873](/images/Flink-doris-connector2.png)

## 4.Technical realization

### 4.1 Architecture diagram

![1616997396610](/images/Flink-doris-connector-architecture.png)

### 4.2 Doris provides more external capabilities

#### 4.2.1 Doris FE

The interface for obtaining metadata information of internal tables, single-table query planning and some statistical information has been opened to the outside world.

All Rest API interfaces require HttpBasic authentication. The user name and password are the user name and password for logging in to the database. Pay attention to the correct assignment of permissions.

```
// Get table schema meta information
GET api/{database}/{table}/_schema

// Get the query plan template for a single table
POST api/{database}/{table}/_query_plan
{
"sql": "select k1, k2 from {database}.{table}"
}

// Get the table size
GET api/{database}/{table}/_count
```

#### 4.2.2 Doris BE

Through the Thrift protocol, data filtering, scanning and cropping capabilities are directly provided to the outside world.

```
service TDorisExternalService {
     // Initialize the query executor
TScanOpenResult open_scanner(1: TScanOpenParams params);

// Streaming batch to get data, Apache Arrow data format
     TScanBatchResult get_next(1: TScanNextBatchParams params);

// end scan
     TScanCloseResult close_scanner(1: TScanCloseParams params);
}
```

For definitions of Thrift related structures, please refer to:

https://github.com/apache/incubator-doris/blob/master/gensrc/thrift/DorisExternalService.thrift

### 4.3 Implement DataStream

Inherit org.apache.flink.streaming.api.functions.source.RichSourceFunction and customize DorisSourceFunction. During initialization, get the execution plan of the related table and get the corresponding partition.

Rewrite the run method to read data from the partition in a loop.

```
public void run(SourceContext sourceContext){
       //Cycle through the partitions
        for(PartitionDefinition partitions : dorisPartitions){
            scalaValueReader = new ScalaValueReader(partitions, settings);
            while (scalaValueReader.hasNext()){
                Object next = scalaValueReader.next();
                sourceContext.collect(next);
            }
        }
}
```

### 4.4 Implement Flink SQL on Doris

Refer to [Flink Custom Source&Sink](https://ci.apache.org/projects/flink/flink-docs-stable/zh/dev/table/sourceSinks.html) and Flink-jdbc-connector to implement the following As a result, Flink SQL can be used to directly manipulate Doris tables, including reading and writing.

#### 4.4.1 Implementation details

1. Realize DynamicTableSourceFactory and DynamicTableSinkFactory register doris connector
2. Customize DynamicTableSource and DynamicTableSink to generate logical plans
3. After DorisRowDataInputFormat and DorisDynamicOutputFormat obtain the logical plan, start execution

![1616747472136](/images/table_connectors.svg)

The most important implementation is DorisRowDataInputFormat and DorisDynamicOutputFormat customized based on RichInputFormat and RichOutputFormat.

In DorisRowDataInputFormat, the obtained dorisPartitions are divided into multiple shards in createInputSplits for parallel computing.

```java
public DorisTableInputSplit[] createInputSplits(int minNumSplits) {
		List<DorisTableInputSplit> dorisSplits = new ArrayList<>();
		int splitNum = 0;
		for (PartitionDefinition partition : dorisPartitions) {
			dorisSplits.add(new DorisTableInputSplit(splitNum++,partition));
		}
		return dorisSplits.toArray(new DorisTableInputSplit[0]);
}

public RowData nextRecord(RowData reuse)  {
		if (!hasNext) {
            //After reading the data, return null
			return null;
		}
		List next = (List)scalaValueReader.next();
		GenericRowData genericRowData = new GenericRowData(next.size());
		for(int i =0;i<next.size();i++){
			genericRowData.setField(i, next.get(i));
		}
		//Determine if there is still data
		hasNext = scalaValueReader.hasNext();
		return genericRowData;
}
```

In DorisRowDataOutputFormat, write data to doris through streamload. Refer to org.apache.doris.plugin.audit.DorisStreamLoader for streamload program

```java
public  void writeRecord(RowData row) throws IOException {
       //streamload Default delimiter \t
        StringJoiner value = new StringJoiner("\t");
        GenericRowData rowData = (GenericRowData) row;
        for(int i = 0; i < row.getArity(); ++i) {
            value.add(rowData.getField(i).toString());
        }
        //streamload write data
        DorisStreamLoad.LoadResponse loadResponse = dorisStreamLoad.loadBatch(value.toString());
        System.out.println(loadResponse);
}
```

