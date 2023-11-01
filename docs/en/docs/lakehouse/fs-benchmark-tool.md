---
{
    "title": "File system benchmark tools",
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



# Introduction

`fs_benchmark_tool` can be used to test the basic service performance of remote storage systems including hdfs and object storage, such as read and write performance. This tool is mainly used to analyze or troubleshoot the performance problems of remote storage systems.

# Compile and install

`fs_benchmark_tool` is part of the `BE` code and does not compile by default. To compile, execute the following command:

```
cd doris 
BUILD_FS_BENCHMARK=ON ./build.sh  --be
```
After compilation, the following contents will be generated in the `output/be/` directory:
```
bin/run-fs-benchmark.sh
lib/fs_benchmark_tool
```
> Note that `fs_benchmark_tool` it needs to be used in the BE running environment directory, because it depends on the BE-related jar package, environment variables, etc.

# Use

Command format:

```shell
sh run-fs-benchmark.sh \
          --conf= configuration file \
          --fs_type= file system \
          --operation= operations on the file system  \
          --file_size= file size \
          --threads= the number of threads \
          --iterations= the number of iterations
```

## Parameter parsing

 `--conf` Required parameter


Configuration file corresponding to the operation file. It is mainly used to add the relevant connection information of the remote storage system. See examples below.

If you want to connect `hdfs`, please put the `hdfs-site.xml` `core-site.xml` file in the `be/conf` directory.

In addition to the connection information, there are the following additional parameters:

- `file_size`: Specifies the size of the file to read or write.

- `buffer_size`: The block size of the file read by one read operation.

- `base_dir`: Specifies the base path to read or write to the file.

`--fs_type` Required parameter

The type of file system on which the operation is required. Currently supported `hdfs`,`s3`.

`--operation` Required parameter

Specifies the type of operation

- `create_write`: Each thread creates a file named `test_${current thread number}`  in the `base_dir(set in conf file)` directory and writes to the file with a write size `file_size` of.

- `open_read`: On `create_write` the basis of the created file, each thread reads the file with the name of `test_${current thread number}`  and the read size of `file_size`.

- `single_read`: Read `file_path(set in conf file)` file, read size is `file_size`.

- `prefetch_read`：Use prefetch reader to read `file_path(set in conf file)` file, read size is `file_size`. Only for s3 file system.

- `exists`: Each thread queries whether a file with  `test_${current thread number}` filename exists.

- `rename`: On `create_write` the basis of the created file, each thread changes the `test_${current thread number}` filename to `test_${current thread number}_new`.

- `list`: Get `base_dir(set in conf file)` the list of files in the directory.

`--file_size` 

The file size of the operation, in bytes.

- `create_write`: Default is 10 MB.

- `open_read`: Default is 10 MB.

- `single_read`: The default is 0, that is, the full file is read.

`--threads`

The number of threads for the operation. The default number is 1.

`--iterations`

The number of iterations ( The number of times the function was executed ) per thread. The default number is 1.

## Result analysis

Except for `rename` the operation, the other operations are repeated three times, and the average value, the median value, the standard deviation, and the like are calculated.
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

Focus on the first three lines, the result of three repeated executions of the code. The first time involves some operations such as connection initialization, so it will take a long time. The latter two times usually represent normal performance.

Focus `UserCounters` on information in:
-  `OpenReaderTime`: Time to open the file.
-  `ReadRate`: read rate. The overall throughput is recorded here. If it is multithreaded, it can be divided by the number of threads, which represents the average rate per thread.
-  `ReadTime`: Read time consuming. What is recorded here is the accumulated time of multiple threads. Divided by the number of threads, it represents the average time spent per thread.
-  `ReadTotal`: Total amount read. What is recorded here is the accumulated value of multiple threads. Divided by the number of threads, this represents the average reads per thread.
-  `WriteRate`: Same as `ReadRate`. Represents the write rate.
-  `WriteTime`: Same as `ReadTime`. Represents time to write.
-  `WriteTotal`: Same as `ReadTotal`. Represents the total amount written.
-  `ListCost/RenameCost/ExistsCost`: A single operation of the corresponding operation takes time.

# Examples

## HDFS

Command:
```
sh run-fs-benchmark.sh \
    --conf=hdfs.conf \
    --fs_type=hdfs \
    --operation=create_write  \
    --file_size=1024000 \
    --threads=3 \
    --iterations=5
```
Using `hdfs.conf` the configuration file,`create_write` operate on the `hdfs` file system , using three threads, write 1MB per operation, and iterate 5 times.

 `hdfs.conf` Profile:
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
Operation result:
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

## Object storage

Command:
```
sh bin/run-fs-benchmark.sh \
     --conf=s3.conf \
     --fs_type=s3 \
     --operation=single_read \
     --threads=1 \
     --iterations=1
```

Using `s3.conf` the configuration file, operate on the `s3` file system `single_read`, using 1 thread, with 1 iteration.

 `s3.conf` Profile:
```
AWS_ACCESS_KEY=ak
AWS_SECRET_KEY=sk
AWS_ENDPOINT=cos.ap-beijing.myqcloud.com
AWS_REGION=ap-beijing
file_path=s3://bucket-123/test_data/parquet/000016_0
```
Operation result:
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

