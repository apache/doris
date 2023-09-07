---
{
    "title": "Doris支持spark导入设计文档",
    "language": "zh-CN"
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

# Doris支持spark导入设计文档

## 背景

Doris现在支持Broker load/routine load/stream load/mini batch load等多种导入方式。
spark load主要用于解决初次迁移，大量数据迁移doris的场景，用于提升数据导入的速度。

## 名词解释

* FE：Frontend，即 Palo 的前端节点。主要负责接收和返回客户端请求、元数据以及集群管理、查询计划生成等工作。
* BE：Backend，即 Palo 的后端节点。主要负责数据存储与管理、查询计划执行等工作。
* Tablet： 一个palo table的水平分片称为tablet。
* Dpp：Data preprocessing，数据预处理模块，通过外部计算资源（Hadoop、Spark）完成对导入数据预处理，包括转化、清洗、分区、排序和聚合等。

## 设计

### 目标

Doris中现有的导入方式中，针对百G级别以上的数据的批量导入支持不是很好，功能上需要修改很多配置，而且可能无法完成导入，性能上会比较慢，并且由于没有读写分离，需要占用较多的cpu等资源。而这种大数据量导入会在用户迁移的时候遇到，所以需要实现基于spark集群的导入功能，利用spark集群的并发能力，完成导入时的ETL计算，排序、聚合等等，满足用户大数据量导入需求，降低用户导入时间和迁移成本。

在Spark导入中，需要考虑支持多种spark部署模式，设计上需要兼容多种部署方式，可以考虑先实现yarn集群的部署模式；同时，由于用户数据格式多种多样，需要支持包括csv、parquet、orc等多种格式的数据文件。

### 实现方案

在将spark导入的设计实现的时候，有必要讲一下现有的导入框架。现在有的导入框架，可以参考《Doris Broker导入实现解析》。

#### 方案1

参考现有的导入框架和原有适用于百度内部hadoop集群的hadoop导入方式的实现，为了最大程度复用现有的导入框架，降低开发的难度，整体的方案如下：

用户的导入语句经过语法和语意分析之后，生成LoadStmt，LoadStmt中增加一个isSparkLoad标识字段，如果为true，就会创建出SparkLoadJob，跟BrokerLoadJob类似，会通过状态机机制，实现Job的执行，在PENDING，会创建SparkLoadPendingTask，然后在LOADING阶段还是创建LoadLoadingTask，进行数据导入。在BE中，复用现有的计划执行框架，执行导入计划。

实现Spark导入主要需要考虑以下几点：

##### 语法	
	这块主要考虑用户习惯，导入语句格式上尽量保持跟broker导入语句相似。下面是一个方案：

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
其中spark.cluster_name为用户导入使用的Spark集群名，可以通过SET PROPERTY来设置，可参考原来Hadoop集群的设置。
property中的Spark集群设置会覆盖spark.cluster_name中对应的内容。
各个property的含义如下:
- spark.master是表示spark集群部署模式，支持包括yarn/standalone/local/k8s，预计先实现yarn的支持，并且使用yarn-cluster模式（yarn-client模式一般用于交互式的场景）。
- spark.executor.cores: executor的cpu个数
- spark.executor.memory: executor的内存大小
- yarn.resourcemanager.address：指定yarn的resourcemanager地址
- max_filter_ratio：指定最大过滤比例阈值

##### SparkLoadJob

用户发送spark load语句，经过parse之后，会创建SparkLoadJob，

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
上图为SparkLoadJob的执行流程。

##### SparkLoadPendingTask
SparkLoadPendingTask主要用来提交spark etl作业到spark集群中。由于spark支持不同部署模型（localhost, standalone, yarn, k8s），所以需要抽象一个通用的接口SparkEtlJob，实现SparkEtl的功能，主要接口包括：
- 提交spark etl任务
- 取消spark etl的任务
- 获取spark etl任务状态的接口

大体接口如下：
```
class SparkEtlJob {
	// 提交spark etl作业
	// 返回JobId
	String submitJob(TBrokerScanRangeParams params);

	// 取消作业，用于支持用户cancel导入作业
	bool cancelJob(String jobId);

	// 获取作业状态，用于判断是否已经完成
	JobStatus getJobStatus(String jobId);
private:
	std::list<DataDescription> data_descriptions;
};
```
可以实现不同的子类，来实现对不同集群部署模式的支持。可以实现SparkEtlJobForYarn用于支持yarn集群的spark导入作业。具体来说上述接口中JobId就是Yarn集群的appid，如何获取appid？一个方案是通过spark-submit客户端提交spark job，然后分析标准错误中的输出，通过文本匹配获取appid。

这里需要参考hadoop dpp作业的经验，就是需要考虑任务运行可能因为数据量、集群队列等原因，会达到并发导入作业个数限制，导致后续任务提交失败，这块需要考虑一下任务堆积的问题。一个方案是可以单独设置spark load job并发数限制，并且针对每个用户提供一个并发数的限制，这样各个用户之间的作业可以不用相互干扰，提升用户体验。

spark任务执行的事情，包括以下几个关键点：
1. 类型转化（extraction/Transformation）

	将源文件字段转成具体列类型（判断字段是否合法，进行函数计算等等）
2. 函数计算（Transformation），包括negative计算
	
	完成用户指定的列函数的计算。函数列表："strftime","time_format","alignment_timestamp","default_value","md5sum","replace_value","now","hll_hash","substitute"
3. Columns from path的提取
4. 进行where条件的过滤
5. 进行分区和分桶
6. 排序和预聚合

	因为在OlapTableSink过程中会进行排序和聚合，逻辑上可以不需要进行排序和聚合，但是因为排序和预聚合可以提升在BE端执行导入的效率。**如果在spark etl作业中进行排序和聚合，那么在BE执行导入的时候可以省略这个步骤。**这块可以依据后续测试的情况进行调整。目前看，可以先在etl作业中进行排序。
	还有一个需要考虑的就是如何支持bitmap类型中的全局字典，string类型的bitmap列需要依赖全局字典。
	为了告诉下游etl作业是否已经完成已经完成排序和聚合，可以在作业完成的时候生成一个job.json的描述文件，里面包含如下属性：

	```
	{
		"is_segment_file" : "false",
		"is_sort" : "true",
		"is_agg" : "true",
	}
	```
	其中：
		is_sort表示是否排序
		is_agg表示是否聚合
		is_segment_file表示是否生成的是segment文件

7. 现在rollup数据的计算都是基于base表，需要考虑能够根据index之间的层级关系，优化rollup数据的生成。

这里面相对比较复杂一点就是列的表达式计算的支持。

最后，spark load作业完成之后，产出的文件存储格式可以支持csv、parquet、orc，从存储效率上来说，建议默认为parquet。

##### LoadLoadingTask
	
LoadLoadingTask可以复现现在的逻辑，但是，有一个地方跟BrokerLoadJob不一样的地址就是，经过SparkEtlTask处理过后的数据文件已经完成列映射、函数计算、负导入、过滤、聚合等操作，这个时候LoadLoadingTask就不用进行这些操作，只需要进行简单的列映射和类型转化。

##### BE导入任务执行

这块可以完全复用现有的导入框架，应该不需要做改动。

#### 方案2

方案1可以最大限度的复用现有的导入框架，能够快速实现支持大数据量导入的功能。但是存在以下问题，就是经过spark etl处理之后的数据其实已经按照tablet划分好了，但是现有的Broker导入框架还是会对流式读取的数据进行分区和bucket计算，然后经过序列化通过rpc发送到对应的目标BE的机器，有一次序列化和网络IO的开销。 方案2是在SparkEtlJob生成数据的时候，直接生成doris的存储格式Segment文件，然后三个副本需要通过类似clone机制的方式，通过add_rowset接口，进行文件的导入。这种方案具体不一样的地方如下：

1. 需要在生成的文件中添加tabletid后缀
2. 在SparkLoadPendingTask类中增加一个接口protected Map<long, Pair<String, Long>> getFilePathMap()用于返回tabletid和文件之间的映射关系，
3. 在BE rpc服务中增加一个spark_push接口，实现拉取源端etl转化之后的文件到本地（可以通过broker读取），然后通过add_rowset接口完成数据的导入，类似克隆的逻辑
4. 生成新的导入任务SparkLoadLoadingTask,该SparkLoadLoadingTask主要功能就是读取job.json文件，解析其中的属性并且，将属性作为rpc参数，调用spark_push接口，向tablet所在的后端BE发送导入请求，进行数据的导入。BE中spark_push根据is_segment_file来决定如何处理，如果为true，则直接下载segment文件，进行add rowset；如果为false，则走pusher逻辑，实现数据导入。

该方案将segment文件的生成也统一放到了spark集群中进行，能够极大的降低doris集群的负载，效率应该会比较高。但是方案2需要依赖于将底层rowset和segment v2的接口打包成独立的so文件，并且通过spark调用该接口来将数据转化成segment文件。

## 总结

综合以上两种方案，第一种方案的改动量比较小，但是BE做了重复的工作。第二种方案可以参考原有的Hadoop导入框架。所以，计划分两步完成spark load的工作。

第一步，按照方案2，实现通过Spark完成导入数据的分区排序聚合，生成parquet格式文件。然后走Hadoop pusher的流程由BE转化格式。

第二步，封装segment写入的库，直接生成Doris底层的格式，并且增加一个rpc接口，实现类似clone的导入逻辑。
