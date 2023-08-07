---
{
    "title": "文件系统性能测试工具",
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


# 简介

`fs_benchmark_tool` 可以用于测试包括 hdfs 和对象存储在内的远端存储系统的基本服务性能，如读取、写入性能。该工具主要用于分析或排查远端存储系统的性能问题。

# 编译与安装

`fs_benchmark_tool` 是 BE 代码的一部分，默认不编译。如需编译，请执行以下命令：

```
cd doris 
BUILD_FS_BENCHMARK=ON ./build.sh  --be
```
编译完之后会在`output/be/` 目录下生成如下相关内容：
```
bin/run-fs-benchmark.sh
lib/fs_benchmark_tool
```
> 注意，`fs_benchmark_tool` 需在BE运行环境目录下使用，因为其依赖 BE 相关的 jar 包、环境变量等内容。

# 使用

命令格式：

```shell
sh run-fs-benchmark.sh \
          --conf=配置文件 \
          --fs_type= 文件系统 \
          --operation= 对文件系统的操作  \
          --file_size= 文件的大小 \
          --threads= 线程数量 \
          --iterations= 迭代次数
```
## 参数解析

`--conf`必选参数


操作文件对应的配置文件。主要用于添加远端存储系统的相关连接信息。详见下文示例。

如连接`hdfs`，请将 `hdfs-site.xml`，`core-site.xml` 文件放置在 `be/conf` 目录下。

除连接信息外，还有以下额外参数：
- `file_size`：指定读取或写入文件的大小。

- `buffer_size`：一次读取操作读取的文件块大小。

- `base_dir`：指定读取或写入文件的 base 路径。

`--fs_type`必选参数

需要操作的文件系统类型。目前支持`hdfs`，`s3`。

`--operation` 必选参数

指定操作类型

- `create_write` ：每个线程在`base_dir(conf文件中设置)`目录下，创建文件名为`test_当前的线程号`，并写入文件，写入大小为`file_size`。

- `open_read`：在`create_write`创建好文件的基础下，每个线程读取文件名为`test_当前的线程号`的文件，读取大小为`file_size`。

- `single_read`：读取`file_path(conf文件中设置)`文件，读取大小为`file_size`。

- `prefetch_read`：使用 prefetch reader 读取`file_path(conf文件中设置)`文件，读取大小为`file_size`。仅适用于 s3。

- `exists` ：每个线程查询文件名为`test_当前的线程号`的文件是否存在。

- `rename` ：在`create_write`创建好文件的基础下，每个线程将文件名为为`test_当前的线程号`的文件更改为为`test_当前的线程号_new`。

- `list`：获取 `base_dir(conf文件中设置)` 目录下的文件列表。

`--file_size`
操作的文件大小，以字节为单位。

- `create_write`：默认为 10MB。

- `open_read`：默认为 10MB。

- `single_read`：默认为0，即读取完整文件。

`--threads`

操作的线程数量，默认数量为1。

`--iterations`

每个线程进行迭代的次数（函数执行次数），默认数量为1。

## 结果解析

除了`rename`操作外，其余操作都会重复三次，并求出平均值，中间值，标准偏差等。
```
--------------------------------------------------------------------------------------------------------------------------------
Benchmark                                                                      Time             CPU   Iterations UserCounters...
--------------------------------------------------------------------------------------------------------------------------------
HdfsReadBenchmark/iterations:1/repeats:3/manual_time/threads:1             13642 ms         2433 ms            1 OpenReaderTime(S)=4.80734 ReadRate(B/S)=101.104M/s ReadTime(S)=13.642 ReadTotal(B)=1.37926G
HdfsReadBenchmark/iterations:1/repeats:3/manual_time/threads:1              3918 ms         1711 ms            1 OpenReaderTime(S)=22.041u ReadRate(B/S)=352.011M/s ReadTime(S)=3.91824 ReadTotal(B)=1.37926G
HdfsReadBenchmark/iterations:1/repeats:3/manual_time/threads:1              3685 ms         1697 ms            1 OpenReaderTime(S)=35.837u ReadRate(B/S)=374.313M/s ReadTime(S)=3.68479 ReadTotal(B)=1.37926G
HdfsReadBenchmark/iterations:1/repeats:3/manual_time/threads:1_mean         7082 ms         1947 ms            3 OpenReaderTime(S)=1.60247 ReadRate(B/S)=275.809M/s ReadTime(S)=7.08166 ReadTotal(B)=1.37926G
HdfsReadBenchmark/iterations:1/repeats:3/manual_time/threads:1_median       3918 ms         1711 ms            3 OpenReaderTime(S)=35.837u ReadRate(B/S)=352.011M/s ReadTime(S)=3.91824 ReadTotal(B)=1.37926G
HdfsReadBenchmark/iterations:1/repeats:3/manual_time/threads:1_stddev       5683 ms          421 ms            3 OpenReaderTime(S)=2.7755 ReadRate(B/S)=151.709M/s ReadTime(S)=5.68258 ReadTotal(B)=0
HdfsReadBenchmark/iterations:1/repeats:3/manual_time/threads:1_cv          80.24 %         21.64 %             3 OpenReaderTime(S)=173.20% ReadRate(B/S)=55.01% ReadTime(S)=80.24% ReadTotal(B)=0.00%
HdfsReadBenchmark/iterations:1/repeats:3/manual_time/threads:1_max         13642 ms         2433 ms            3 OpenReaderTime(S)=4.80734 ReadRate(B/S)=374.313M/s ReadTime(S)=13.642 ReadTotal(B)=1.37926G
HdfsReadBenchmark/iterations:1/repeats:3/manual_time/threads:1_min          3685 ms         1697 ms            3 OpenReaderTime(S)=22.041u ReadRate(B/S)=101.104M/s ReadTime(S)=3.68479 ReadTotal(B)=1.37926G
```

重点关注前3行，分别代码3次重复执行的结果。其中第一次涉及到一些连接初始化等操作，所以耗时会较长。后两次通常代表正常的性能表现。

重点关注`UserCounters` 中的信息：
- `OpenReaderTime`：打开文件的耗时。
- `ReadRate`：读取速率。这里记录的是总体的吞吐。如果是多线程，可以除以线程数，即代表每线程平均速率。
- `ReadTime`：读取耗时。这里记录的是多线程累计时间。除以线程数，即代表每线程平均耗时。
- `ReadTotal`：读取总量。这里记录的是多线程累计值。除以线程数，即代表每线程平均读取量。
- `WriteRate`：同 `ReadRate`。代表写入速率。
- `WriteTime`：同 `ReadTime`。代表写入耗时。
- `WriteTotal`：同 `ReadTotal`。代表写入总量。
- `ListCost/RenameCost/ExistsCost`：对应操作的单个操作耗时。

# 示例

## HDFS

命令：
```
sh run-fs-benchmark.sh \
    --conf=hdfs.conf \
    --fs_type=hdfs \
    --operation=create_write  \
    --file_size=1024000 \
    --threads=3 \
    --iterations=5
```
使用`hdfs.conf`配置文件，对`hdfs`文件系统进行`create_write`操作，使用三个线程，每次操作写入 1MB，迭代次数为5次。

`hdfs.conf`配置文件：
```
fs.defaultFS=hdfs://HDFS8000871
hadoop.username=hadoop
dfs.nameservices=HDFS8000871
dfs.ha.namenodes.HDFS8000871=nn1,nn2
dfs.namenode.rpc-address.HDFS8000871.nn1=102.22.10.56:4007
dfs.namenode.rpc-address.HDFS8000871.nn2=102.22.10.57:4007
dfs.client.failover.proxy.provider.HDFS8000871=org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider
base_dir=hdfs://HDFS8000871/benchmarks/TestDFSIO/io_data/
```
运行结果：
```
---------------------------------------------------------------------------------------------------------------------------------------
Benchmark                                                                             Time             CPU   Iterations UserCounters...
---------------------------------------------------------------------------------------------------------------------------------------
HdfsCreateWriteBenchmark/iterations:5/repeats:3/manual_time/threads:3              61.7 ms         38.7 ms           15 WriteRate(B/S)=3.31902M/s WriteTime(S)=0.387954 WriteTotal(B)=3.072M
HdfsCreateWriteBenchmark/iterations:5/repeats:3/manual_time/threads:3              49.6 ms         3.09 ms           15 WriteRate(B/S)=4.12967M/s WriteTime(S)=0.427992 WriteTotal(B)=3.072M
HdfsCreateWriteBenchmark/iterations:5/repeats:3/manual_time/threads:3              45.2 ms         2.72 ms           15 WriteRate(B/S)=4.53148M/s WriteTime(S)=0.362854 WriteTotal(B)=3.072M
HdfsCreateWriteBenchmark/iterations:5/repeats:3/manual_time/threads:3_mean         52.2 ms         14.8 ms            3 WriteRate(B/S)=3.99339M/s WriteTime(S)=0.392933 WriteTotal(B)=3.072M
HdfsCreateWriteBenchmark/iterations:5/repeats:3/manual_time/threads:3_median       49.6 ms         3.09 ms            3 WriteRate(B/S)=4.12967M/s WriteTime(S)=0.387954 WriteTotal(B)=3.072M
HdfsCreateWriteBenchmark/iterations:5/repeats:3/manual_time/threads:3_stddev       8.55 ms         20.7 ms            3 WriteRate(B/S)=617.61k/s WriteTime(S)=0.0328536 WriteTotal(B)=0
HdfsCreateWriteBenchmark/iterations:5/repeats:3/manual_time/threads:3_cv          16.39 %        139.34 %             3 WriteRate(B/S)=15.47% WriteTime(S)=8.36% WriteTotal(B)=0.00%
HdfsCreateWriteBenchmark/iterations:5/repeats:3/manual_time/threads:3_max          61.7 ms         38.7 ms            3 WriteRate(B/S)=4.53148M/s WriteTime(S)=0.427992 WriteTotal(B)=3.072M
HdfsCreateWriteBenchmark/iterations:5/repeats:3/manual_time/threads:3_min          45.2 ms         2.72 ms            3 WriteRate(B/S)=3.31902M/s WriteTime(S)=0.362854 WriteTotal(B)=3.072M
HDFS 上生成的文件：
[hadoop@172 ~]$ hadoop fs -ls -h /benchmarks/TestDFSIO/io_data/
Found 3 items
-rw-r--r--   3 hadoop supergroup        100 2023-06-27 11:55 /benchmarks/TestDFSIO/io_data/test_0
-rw-r--r--   3 hadoop supergroup        100 2023-06-27 11:55 /benchmarks/TestDFSIO/io_data/test_1
-rw-r--r--   3 hadoop supergroup        100 2023-06-27 11:55 /benchmarks/TestDFSIO/io_data/test_2
```

## 对象存储

命令：
```
sh bin/run-fs-benchmark.sh \
     --conf=s3.conf \
     --fs_type=s3 \
     --operation=single_read \
     --threads=1 \
     --iterations=1
```

使用`s3.conf`配置文件，对 `s3`文件系统进行 `single_read`操作，使用1个线程，迭代次数为1次。

`s3.conf` 配置文件：
```
AWS_ACCESS_KEY=ak
AWS_SECRET_KEY=sk
AWS_ENDPOINT=cos.ap-beijing.myqcloud.com
AWS_REGION=ap-beijing
file_path=s3://bucket-123/test_data/parquet/000016_0
```
运行结果：
```
------------------------------------------------------------------------------------------------------------------------------
Benchmark                                                                    Time             CPU   Iterations UserCounters...
------------------------------------------------------------------------------------------------------------------------------
S3ReadBenchmark/iterations:1/repeats:3/manual_time/threads:1              7534 ms          140 ms            1 ReadRate(B/S)=11.9109M/s ReadTime(S)=7.53353 ReadTotal(B)=89.7314M
S3ReadBenchmark/iterations:1/repeats:3/manual_time/threads:1              5988 ms          118 ms            1 ReadRate(B/S)=14.985M/s ReadTime(S)=5.98808 ReadTotal(B)=89.7314M
S3ReadBenchmark/iterations:1/repeats:3/manual_time/threads:1              6060 ms          124 ms            1 ReadRate(B/S)=14.8081M/s ReadTime(S)=6.05961 ReadTotal(B)=89.7314M
S3ReadBenchmark/iterations:1/repeats:3/manual_time/threads:1_mean         6527 ms          127 ms            3 ReadRate(B/S)=13.9014M/s ReadTime(S)=6.52707 ReadTotal(B)=89.7314M
S3ReadBenchmark/iterations:1/repeats:3/manual_time/threads:1_median       6060 ms          124 ms            3 ReadRate(B/S)=14.8081M/s ReadTime(S)=6.05961 ReadTotal(B)=89.7314M
S3ReadBenchmark/iterations:1/repeats:3/manual_time/threads:1_stddev        872 ms         11.4 ms            3 ReadRate(B/S)=1.72602M/s ReadTime(S)=0.87235 ReadTotal(B)=0
S3ReadBenchmark/iterations:1/repeats:3/manual_time/threads:1_cv          13.37 %          8.94 %             3 ReadRate(B/S)=12.42% ReadTime(S)=13.37% ReadTotal(B)=0.00%
S3ReadBenchmark/iterations:1/repeats:3/manual_time/threads:1_max          7534 ms          140 ms            3 ReadRate(B/S)=14.985M/s ReadTime(S)=7.53353 ReadTotal(B)=89.7314M
S3ReadBenchmark/iterations:1/repeats:3/manual_time/threads:1_min          5988 ms          118 ms            3 ReadRate(B/S)=11.9109M/s ReadTime(S)=5.98808 ReadTotal(B)=89.7314M
``` 

