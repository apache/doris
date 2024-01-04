---
{
    "title": "Spark Load",
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

# Implementation Design for Spark Load

## Background

Doris supports various data ingestion methods, including Broker Load, Routine Load, Stream Load, and Mini Batch Load.
Spark Load is the optimal choice for first-time data ingestion into Doris. It is for moving large volumes of data and can speed up data ingestion.

## Basic Concepts

* FE: Frontend, the frontend nodes of Doris. They process request from clients, manage metadata and clusters, and generate query execution plans.
* BE: Backend, the backend nodes of Doris. They store and manage data, and execute query plans.
* Tablet:  A table in Doris is divided into tablets.
* DPP: Data preprocessing. To transfer, clean, partition, sort, and aggregate data using external computing resources (Hadoop, Spark)

## Design

### Objective

Doris itself is relatively slow in batch import of 100GB data or larger. As it hasn't realized read/writing separation. Such batch import can lead to high CPU usage. For users who need to migrate large data volumes, Doris provides the Spark Load option. It leverages the data loading and concurrency capabilities of Spark clusters to perform ETL, sorting, and aggregation upon data ingestion. In this way, users can migrate huge datasets faster and less costly.

The Spark Load method must be compatible with the multiple deployment modes of Spark. The first step is to support YARN clusters. Meanwhile, considering the variety of data formats, it needs to support multiple kinds of data files, including CSV, Parquet, and ORC.  

### Implementation Plans

When designing the implementation of Spark Load, we take into account the our existing data loading framework.

#### Plan A

For the ease of development, we hope to reuse the existing data loading framework as much as possible, so this is the overall plan for implementing Spark Load:

The Spark Load statement input by the user will be parsed to produce LoadStmt. An identifier field `isSparkLoad` will be added to the LoadStmt. If it is true, the system will create a SparkLoadJob  (similar to how a BrokerLoadJob is created). The job will be executed by a state machine mechanism. In PENDING stage, a SparkLoadPendingTask will be created; in LOADING stage, a LoadLoadingTask will be created for data loading. Doris reuses its existing data loading framework and executes the loading plan in the BE.

A few considerations for this plan:

##### Syntax

The Spark Load statement is similar to that of Broker Load. This is an example:

```
		LOAD LABEL example_db.label1
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
		NEGATIVE
        INTO TABLE `my_table`
		PARTITION (p1, p2)
		COLUMNS TERMINATED BY ","
		columns(k1,k2,k3,v1,v2)
		set (
			v3 = v1 + v2,
			k4 = hll_hash(k2)
		)
		where k1 > 20
        )
		with spark.cluster_name
        PROPERTIES
        (
        "spark.master" = "yarn",
		"spark.executor.cores" = "5",
		"spark.executor.memory" = "10g",
		"yarn.resourcemanager.address" = "xxx.tc:8032",
        "max_filter_ratio" = "0.1",
        );
```

`spark.cluster_name` is the name of the Spark cluster to be loaded. It can be set via SET PROPERTY and users may refer to the configurations of the original Hadoop cluster. The Spark cluster configurations in the PROPERTIES will overwrite those in `spark.cluster_name`.

Explanation of the properties:

- spark.master: Spark cluster deployment mode. Supported modes include yarn/standalone/local/k8s. We will prioritize the support for yarn and implement the yarn-cluster mode. (The yarn-client mode is mainly used in interactive scenarios.)
- spark.executor.cores: the number of CPUs in the executor
- spark.executor.memory: the memory size of the executor
- yarn.resourcemanager.address: address of the resourcemanager of the specified YARN
- max_filter_ratio: the upper limit of the filter ratio

##### SparkLoadJob

After a user sends a Spark Load statement, the statement will be parsed and a SparkLoadJob will be created.

```
SparkLoadJob:
         +-------+-------+
         |    PENDING    |-----------------|
         +-------+-------+                 |
				 | SparkLoadPendingTask    |
                 v                         |
         +-------+-------+                 |
         |    LOADING    |-----------------|
         +-------+-------+                 |
				 | LoadLoadingTask         |
                 v                         |
         +-------+-------+                 |
         |  COMMITTED    |-----------------|
         +-------+-------+                 |
				 |                         |
                 v                         v  
         +-------+-------+         +-------+-------+     
         |   FINISHED    |         |   CANCELLED   |
         +-------+-------+         +-------+-------+
				 |                         Λ
                 +-------------------------+
```

The above is the execution process of a SparkLoadJob.

##### SparkLoadPendingTask

A SparkLoadPendingTask is to commit a Spark ETL job to the Spark cluster. Since Spark supports various deployment modes (localhost, standalone, yarn, k8s), we need an abstraction of a generic interface for Spark ETL. The main tasks include:

- Commit a Spark ETL job
- Cancel a Spark ETL job
- Obtain status of the Spark ETL job

A prototype of the interface:

```
class SparkEtlJob {
	// Commit a Spark ETL job
	// Return JobId
	String submitJob(TBrokerScanRangeParams params);

	// Cancel a Spark ETL job
	bool cancelJob(String jobId);

	// Obtain status of the Spark ETL job
	JobStatus getJobStatus(String jobId);
private:
	std::list<DataDescription> data_descriptions;
};
```

We hope to support various sub-classes to support different cluster deployment modes. And we hope to implement SparkEtlJobForYarn to support Spark Load for YARN clusters. The JobId in the above snippet is the aphid of the YARN cluster. One way to obtain the appid is to commit a Spark Job via the spark-submit client, analyze the standard error output, and retrieve the appid by text matching.

However, based on lessons drawn from Hadoop DPP jobs, we need to take care of job queuing issues: due to data volume or cluster queue reasons, the number of concurrent loading jobs might hit the limit and subsequent job commits might fail. One solution is to separately set a limit on the number of concurrent Spark Load jobs and a per-user concurrency limit, so as to avoid avoid interference between jobs of different users. 



Additionally, implementing a job scheduling system that prioritizes jobs based on their importance and allocates resources accordingly can ensure that critical jobs are processed first and that resources are used efficiently.



Key steps of executing a Spark ETL job include:

1. Type conversion（extraction/transformation）

   Transfer the fields in the source file into column types (check if the fields are legal for function computation, etc.)

2. Function computation (transformation), including negative computation

   Complete computation of the specified function. Function list: "strftime", "time_format", "alignment_timestamp", "default_value", "md5sum", "replace_value", "now", "hll_hash", "substitute".

3.  "columns from path" extraction

4. "where" filtering

5. Data partitioning and bucketing

6. Data sorting and pre-aggregation

   

   The OlapTableSink includes data sorting and aggregation, so logically we don't need to implement them separately, **but we decide to include them in the Spark ETL job in order to  improve BE efficiency by skipping these steps during loading in BE.** We might change that based on future test performance.  

   

   Another nut to crack to support global dictionary for bitmap since the bitmap columns of string type require global dictionary.

   

   To indicate whether the data sorting and aggregation have been completed, we plan to generate a a job.json description file upon job completion, which includes the following properties:

   ```
   {
   	"is_segment_file" : "false",
   	"is_sort" : "true",
   	"is_agg" : "true",
   }
   ```

   Explanation: 
   	is_sort: if the data is sorted
   	is_agg: if the data is aggregated
   	is_segment_file: if the generated files are segment files

7. Since the computation of Rollup is based on the base table, to optimize the generation of Rollups, we need to look into the index hierarchy.

The tricky part is to support expression calculation for columns.

Lastly, for the storage format of the output file after a Spark Load job is finished, we plan to support CSV, Parquet, and ORC, and make Parquet the default option for higher storage efficiency.

##### LoadLoadingTask

For the LoadLoadingTask, we can reuse our existing logic. What's different from BrokerLoadJob is, LoadLoadingTask doesn't need to include column mapping, function, computation, negative import, filtering, or aggregation, because they have all been done in SparkEtlTask. A LoadLoadingTask can just include simple column mapping and type conversion.

##### Data loading to BE

For this part, we can fully reuse the existing framework.

#### Plan B

Plan A can reuse our data loading framework to a great extend and realize support for large data import as soon as possible. But the downside is, data processed by the Spark ETL job is already partitioned into tablets, but our existing Broker Load framework will still partition and bucket the data, serialize it, and then send  data to the target BE nodes via RPC. The process generates extra overheads. 

So we have a second plan. Plan B is to generate Segment files of the Doris storage format after the SparkEtlJob. And then the three replicas should complete file loading via the add_rowset interface under a clone-like mechanism. The difference between Plan B and Plan A is that, in Plan B, you need to:

1. Add a tabletid suffix to the generated files
2. Add an interface to SparkLoadPendingTask class: Map<long, Pair<String, Long>> getFilePathMap(). The interface returns the mapping relations between tabletid and files.
3. Add a "spark_push" interface to BE RPC service, in order to pull the transformed files after ETL to local machines (read via Broker) and then load data via the "add_rowset" interface using clone-like logic.
4. Generate a new loading task: SparkLoadLoadingTask. The task is to read job.json files, parse the properties, and use the properties as RPC parameters. Then it calls the "spark_push" interface and sends a data loading request to the BE where the tablet is. The "spark_push" interface in BE will decide what to do based on the "is_segment_file" parameter. If it is true, it will download the segment file and add rowset; if it is false, it will follow the pusher logic and execute data loading.

Plan B also moves the segment file generation workload to the Spark cluster, which can greatly reduce the burden on the Doris cluster and thus improve efficiency. However, in Plan B, the underlying rowset and the segment v2 interface will be packed into individual SO files, and we need to use Spark to call that interface to transfer data into segment files. 

 relies on packaging the underlying rowset and segment v2 interfaces into independent shared object (SO) files, and using Spark to call these interfaces to transform data into segment files.

## Conclusion

As we compare two plans, Plan A allows for the least development work but the BE will undertake unnecessary workloads, while for Plan B, we can make use of our existing Hadoop data loading framework. Therefore, we plan to implement Spark Load in two steps.

Step 1: Go with Plan B and complete data partitioning, sorting, and aggregation in Spark, and generate Parquet files. Then we follow the Hadoop pusher process and transfer data formats in the BE.

Step 2: Encapsulate the database of segment writing, generate Doris formats directly, and add an RPC interface to implement clone-like data loading logic.
