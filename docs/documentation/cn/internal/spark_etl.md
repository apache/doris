# Doris spark导入ETL逻辑设计

## 背景

Doris为了解决初次迁移，大量数据迁移doris的问题，引入了spark导入，用于提升数据导入的速度。在spark导入中，需要利用spark进行ETL计算、分区、分桶、文件格式生成等逻辑。下面分别讲述具体的实现设计。

## 名词解释

* FE：Frontend，即 Palo 的前端节点。主要负责接收和返回客户端请求、元数据以及集群管理、查询计划生成等工作。
* BE：Backend，即 Palo 的后端节点。主要负责数据存储与管理、查询计划执行等工作。

## 设计

### 目标

在Spark导入中，需要达到以下目标：

1. 需要考虑支持多种spark部署模式，设计上需要兼容多种部署方式，可以考虑先实现yarn集群的部署模式；
2. 需要支持包括csv、parquet、orc等多种格式的数据文件。
3. 能够支持doris中所有的类型，其中包括hll和bitmap类型。同时，bitmap类型需要考虑支持全局字典，以实现string类型的精确去重
4. 能够支持排序和预聚合
5. 支持分区分桶逻辑
6. 支持生成base表和rollup表的数据
7. 能够支持生成doris的存储格式

### 实现方案

参考[pr-2865](https://github.com/apache/incubator-doris/pull/2856), 整的方案将按照如下的框架实现：

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
				 | SparkLoadLodingTas      |
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

整个流程大体如下：

1. 用户的sql语句会被解析成LoadStmt，并且里面带一个is_spark_load的属性，为true
2. LoadStmt会生成SparkLoadJob进行执行
3. 在SparkLoadJob阶段会生成SparkLoadPendingTask，完成spark etl作业的提交
4. 在SparkLoadLodingTask会向table涉及到的BE发送TPushReq
5. 在BE中需要基于EngineBatchLoadTask完成数据的下载和导入

spark导入的语句：
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
		with spark
        PROPERTIES
        (
        "cluster_type" = "yarn",
        "yarn_resourcemanager_address" = "xxx.tc:8032",
        "max_filter_ratio" = "0.1"
        );
```

#### SparkLoadPendingTask

该Task主要实现spark任务的提交。 

1. 作业提交

为此实现一个SparkApplication类，用于完成spark作业的提交.

```
class SparkApplication {
public:
	// 提交spark etl作业
	// 返回appId
	String submitApp(TBrokerScanRangeParams params);

	// 取消作业，用于支持用户cancel导入作业
	bool cancelApp(String appId);

	// 获取作业状态，用于判断是否已经完成
	JobStatus getJobStatus(String jobId);
};

```

SparkApplication采用基于InProcessLauncher，并且采用cluster模式，来实现在FE中提交spark etl app。主要逻辑如下：
```
AbstractLauncher launcher = new InProcessLauncher();
launcher.setAppResource(appResource)
        .setMainClass(mainClass) // 这个地方使用导入作业的label
        .setMaster(master) // 这个地方使用cluster
CountDownLatch countDownLatch;
Listener handleListeners = new SparkAppHandle.Listener() {
    @Override
    public void stateChanged(SparkAppHandle handle) {
        if (handle.getState().isFinal()) {
            countDownLatch.countDown();
        }

        @Override
        public void infoChanged(SparkAppHandle handle) {}
    };

countDownLatch = new CountDownLatch(1);

SparkAppHandle handle = launcher.startApplication(handleListeners);
boolean regularExit = countDownLatch.await(20, TimeUnit.MINUTES);
if (!regularExit)
    handle.kill();
```


其中SparkAppHandle可以用来操控Spark App作业，其接口主要如下：
```
void	addListener(SparkAppHandle.Listener l);
void	disconnect();
String	getAppId();
SparkAppHandle.State	getState();
void	kill();
void	stop();
```

2. 参数

	参数分为两大类：

- spark相关的参数，列表可以参考：http://spark.apache.org/docs/latest/running-on-yarn.html。最主要的参数罗列如下：
	```
		spark.yarn.am.memory
		spark.yarn.am.cores
		spark.executor.instances
		spark.executor.memory
		spark.executor.cores
		spark.yarn.stagingDir
		spark.yarn.queue
	```
- 业务参数，包括表的schema信息、表的rollup信息、用户导入语句的信息，参考broker load，主要参数结构如下：

```
DataDescriptions：用户导入语句信息，用于获取列映射关系、column from path、where predicate、文件类型等等参数。需要对DataDescriptions分析之后，得到类似TBrokerScanRangeParams的结构体，传给spark etl作业。

Partition信息和bucket信息：包括分区和分桶列和现有的partition信息。具体就是：TOlapTablePartitionParam、TOlapTableIndexSchema、TOlapTableSchemaParam等

base表和rollup表的树形层级关系：优化现有的rollup数据生成方案，实现基于最小覆盖parent rollup来计算rollup的方式，优化rollup计算。 
class IndexTree {
	int indexId;
	int parentIndexId;
	List<int> childIndexId;
};
```
针对精确去重场景下的bitmap类型的列需要特别处理，因为bitmap类型的列可能需要构建全局词典和利用全局词典进行数据转化的步骤，因此业务相关的参数中还需要提供一个bitmap列的信息List<BitmapArg>，定义如下：

```
class BitmapArg {
String columnName; // bitmap列名
String globalDictPath; // 全局字典路径
};

```
现在，全局字典构建方案还没有确定，一种方案https://github.com/wangbo/open_mark/issues/2。不管怎么样，这里主要关心产生的全局字典的路径。对于具体的构建步骤可以等实现全局字典的时候再设计(如果采用基于hive的方案，可以在提交spark etl app之前，进行词典构建任务)。


参考：
- http://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/launcher/package-summary.html
- http://spark.apache.org/docs/latest/api/java/org/apache/spark/launcher/SparkLauncher.html#startApplication-org.apache.spark.launcher.SparkAppHandle.Listener...-
- http://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/launcher/package-summary.html
- https://www.cnblogs.com/yy3b2007com/p/10247239.html

#### spark etl作业

spark etl中主要考虑实现以下逻辑：

1. doris类型和spark类型的映射

spark支持的类型：https://spark.apache.org/docs/latest/sql-reference.html#data-types

除了hll类型和bitmap列类型之外，spark支持所有的doris类型。在spark 2.x中不支持用户自定义类型（UDT），为了实现hll和bitmap类型的支持，这两种类型，可以使用Spark的StringType来进行存储，并且实现对应的udf和udaf函数。

注意：需要主要列的精度、char的长度等等问题

2. 支持etl函数计算
数据源的数据通过dataframe加载，然后进行类型转化和函数计算进行对应的ETL。

ColumnsFromPath可以使用spark中：spark.sql.sources.partitionColumnTypeInference.enabled的功能。

需要支持的函数包括："strftime","time_format","alignment_timestamp","default_value","md5sum","replace_value","now","hll_hash","substitute"。

目前spark中已经实现了一写函数，需要实现对应的映射逻辑。不支持的函数包括，hll_hash/hll_empty/to_bitmap/bitmap_hash。

这些不支持的函数，需要通过spark的udf/udaf的框架，进行实现。一个基于spark UserDefinedAggregateFunction实现的bitmap_count的例子：https://blog.csdn.net/xiongbingcool/article/details/81282118。

udf的一个例子：

```
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;

UserDefinedFunction mode = udf(
  (Seq<String> ss) -> ss.headOption(), DataTypes.StringType
);

df.select(mode.apply(col("vs"))).show();
```

3. 条件过滤

使用dataframe的filter接口，过滤where条件。

4. 预聚合
需要基于key列进行groupBy之后，进行value列的预聚合计算，需要依赖spark的udaf和自定义实现的udaf。

5. 分区和分桶

spark dataframe中提供partitionBy和bucketBy的接口用于分区和分桶，所以，只要dataframe中有对应的列数据，就可以比较方便的进行分区和分桶。为此，需要在dataframe中额外的保存partition和bucket列。如果需要排序，可以通过指定sortBy来实现。

6. Doris存储格式生成

在经过分区和分桶之后，就会得到tablet的数据，然后通过save接口保存。这个时候需要实现自定义的存储格式。

7. 生成作业的执行结果文件

参考parquet文件的实现，需要继承FileFormat，实现DorisFileFormat，完成dataframe数据转化成doris存储格式文件。这个需要依赖于将BE中存储格式写入的模块打包成jar，可以通过JNI实现。

8. rollup表计算

由于采用基于层次的rollup计算方式，所以采用广度优先遍历的方式计算每一个层上的rollup，在计算第i层的rollup数据，只需要保留i-1层的数据。这样子避免所有的rollup表都从base表中计算的问题。

9. string类型的bitmap列基于全局词典的id计算

如果是string类型的bitmap列，需要基于用户提供的全局词典路径，加载对应的词典（可以按照约定的格式，实现对应的数据读取），进行id的映射。

10. 将处理结果写入job.json文件

主要写入如下结构：

```
class EtlResult {
Boolean isSuccess;
String failReason; // 存储失败原因，比如异常数据
Boolean isAgg;
Boolean isSorted;
Boolean isDorisFile;
};
```

所以，整个etl的过程如下：

```
         +-------+-------+
         |bitmap全局ID计算|
         +-------+-------+
				 |
                 v
         +-------+-------+
         |     列计算     |
         +-------+-------+
				 |
                 v
         +-------+-------+
         |  where过滤     |
         +-------+-------+
				 |
                 v
         +-------+-------+    子rollup计算
         |     预聚合     |<-----------------+
         +-------+-------+                  |
				 |                          |
                 v							|
         +-------+-------+          +-------+-------+
         |   分区和分桶   |          |  persist存储   |
         +-------+-------+          +-------+-------+
				 |                          Λ
                 v                          |
         +-------+-------+                  |
         |    保存数据    |+-----------------+
         +-------+-------+
				 |
                 v
         +-------+-------+
         |  父表计算完成   |
         +-------+-------+
```

注意：
- 考虑一些优化，例如persist持久化等等

参考：
- https://stackoverflow.com/questions/36648128/how-to-store-custom-objects-in-dataset

#### SparkLoadLodingTask

SparkLoadLodingTask主要完成向table相关的BE发送PushReq，进行让BE执行数据的下载和导入。需要修改EngineBatchLoadTask，如果etl任务完成了segment文件生成，那就只需要拉取segment文件，通过add_rowset实现数据的导入；如果没有，就拉取数据文件，然后读取文件内容，通过pusher逻辑实现导入。

## 总结

实现的时候，先走通最基本的流程，先实现FE中的语法支持；然后再实现spark etl job的提交逻辑，然后实现简单类型的etl，复杂bitmap类型ID计算、函数计算逻辑可以等后面再增加；然后实现基本的基于pusher逻辑的SparkLoadLodingTask。等整个流程打通之后，再实现一些复杂的功能和优化。