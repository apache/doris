---
{
    "title": "Doris Streamloader",
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


## 概述
[Doris Streamloader](https://github.com/apache/doris-streamloader) 是一款用于将数据导入 Doris 数据库的专用客户端工具。相比于直接使用 `curl` 的单并发导入，该工具可以提供多并发导入的功能，降低大数据量导入的耗时。拥有以下功能：

- 并发导入，实现 Stream Load 的多并发导入。可以通过 workers 值设置并发数。
- 多文件导入，一次导入可以同时导入多个文件及目录，支持设置通配符以及会自动递归获取文件夹下的所有文件。
- 断点续传，在导入过程中可能出现部分失败的情况，支持在失败点处进行继续传输。 
- 自动重传，在导入出现失败的情况后，无需手动重传，工具会自动重传默认的次数，如果仍然不成功，打印出手动重传的命令。

## 获取与安装

**1.0 版本** 

源代码:  https://github.com/apache/doris-streamloader



| 版本    | 日期      |  平台  |  链接  |
|---|---|---|---|
| v1.0   |  20240131 |  x64  | https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-streamloader-1.0.1-bin-x64.tar.xz|
| v1.0   | 20240131  | arm64 | https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-streamloader-1.0.1-bin-arm64.tar.xz|

:::note
获取结果即为可执行二进制。
:::

## 使用方法

```bash

doris-streamloader --source_file={FILE_LIST} --url={FE_OR_BE_SERVER_URL}:{PORT} --header={STREAMLOAD_HEADER} --db={TARGET_DATABASE} --table={TARGET_TABLE}

```


**1. `FILE_LIST` 支持：**

- 单个文件

    例如：导入单个文件 file.csv

    ```json
    doris-streamloader --source_file="dir" --url="http://localhost:8330" --header="column_separator:|?columns:col1,col2" --db="testdb" --table="testtbl"
    ```

- 单个目录

    例如：导入单个目录 dir

    ```json
    doris-streamloader --source_file="dir" --url="http://localhost:8330" --header="column_separator:|?columns:col1,col2" --db="testdb" --table="testtbl"        
    ```

- 带通配符的文件名（需要用引号包围）

    例如：导入 file0.csv, file1.csv, file2.csv

    ```json
    doris-streamloader --source_file="file*" --url="http://localhost:8330" --header="column_separator:|?columns:col1,col2" --db="testdb" --table="testtbl"
    ```

- 逗号分隔的文件名列表

    例如：导入 file0.csv, file1.csv file2.csv

    ```json
    doris-streamloader --source_file="file0.csv,file1.csv,file2.csv" --url="http://localhost:8330" --header="column_separator:|?columns:col1,col2" --db="testdb" --table="testtbl"
    ```

- 逗号分隔的目录列表

  例如：导入 dir1, dir2,dir3

   ```json
  doris-streamloader --source_file="dir1,dir2,dir3" --url="http://localhost:8330" --header="column_separator:|?columns:col1,col2" --db="testdb" --table="testtbl" 
   ```

:::tip 
当需要多个文件导入时，使用 Doris Streamloader 也只会产生一个版本号 
:::



**2.** `STREAMLOAD_HEADER` **支持 Stream Load 的所有参数，多个参数之间用  '?' 分隔。**

用法举例：

```bash
doris-streamloader --source_file="data.csv" --url="http://localhost:8330" --header="column_separator:|?columns:col1,col2" --db="testdb" --table="testtbl"
```

上述参数均为必要参数，下面介绍可选参数：

| 参数名    | 含义             |  默认值  |  建议  |
|---|---|---|---|
| --u      | 数据库用户名      |  root    |      |
| --p      | 数据库用户对应的密码   |  空字符串    |      |
| --compress      | 导入数据是否在 HTTP 传输时压缩   |  false    |   保持默认，打开后压缩解压会分别增加工具和 Doris BE 的 CPU 压力，所以仅在数据源所在机器网络带宽瓶颈时打开   |
|--timeout    | 向 Doris 发送 HTTP 请求的超时时间, 单位:秒   |  60\*60\*10    | 保持默认   |
| --batch      | 文件批量读取和发送的粒度, 单位: 行      |  4096    |  保持默认    |
| --batch_byte      | 文件批量读取和发送的粒度, 单位: byte      |  943718400 (900MB)    |  保持默认    |
| --workers   | 导入的并发数   |  0    |   设置成 0 为自动模式，会根据导入数据的大小，磁盘的吞吐量，Stream Load 导入速度计算一个值。 也可以手动设置，性能好的集群可以设置大点，最好不要超过 10。如果观察到导入内存过高（通过观察 Memtracker 或者 Exceed 日志）, 则可适当降低 worker 数量   |
| --disk_throughput      | 磁盘的吞吐量，单位 MB/s   |  800    |  通常保持默认即可。该值参与 --workers 的自动推算过程。 如果希望通过工具能计算出一个适当的 workers 数，可以根据实际磁盘吞吐设置。  |
|--streamload_throughput | Stream Load 导入实际的吞吐大小，单位 MB/s | 100 | 通常保持默认即可。该值参与 --workers 的自动推算过程。 默认值是通过每日性能测试环境给出的 Stream Load 吞吐量以及性能可预测性得出的。 如果希望通过工具能计算出一个适当的 workers 数，可以设置实测的 Stream Load 的吞吐，即：(LoadBytes\*1000)/(LoadTimeMs\*1024\*1024) 计算出实际的吞吐量 |
| --max_byte_per_task      | 每个导入任务数据量的最大大小，超过这个值剩下的数据会被拆分到一个新的导入任务中。  |  107374182400 (100G)    |  建议设置一个很大的值来减少导入的版本数。但如果遇到 body exceed max size 错误且不想调整 streaming_load_max_mb 参数（需重启 be），又或是遇到 -238 TOO MANY SEGMENT 错误，可以临时调小这个配置    |
| --check_utf8 | <p>是否对导入数据的编码进行检查：</p>   <p> 1） false，那么不做检查直接将原始数据导入; 2） true，那么对数据中非 utf-8 编码的字符用 � 进行替代</p> | true |保持默认|
|--debug |打印 Debug 日志 | false | 保持默认|
|--auto_retry| 自动重传失败的 worker 序号和 task 序号的列表 | 空字符串 | 仅导入失败时重传使用，正常导入无需关心。失败时会提示具体参数内容，复制执行即可。例：如果 --auto_retry="1,1,2,1" 则表示： 需要重传的task为：第一个 worker 的第一个 task，第二个 worker 的第一个 task。 |
|--auto_retry_times | 自动重传的次数 | 3 | 保持默认，如果不想重传需要把这个值设置为 0 |
|--auto_retry_interval | 自动重传的间隔 | 60 | 保持默认，如果 Doris 因宕机导致失败，建议根据实际 Doris 重启的时间间隔来设置该值 |
|--log_filename | 日志存储的位置 | "" | 默认将日志打印到控制台上，如果要打印到日志文件中，可以设置存储日志文件的路径，如--log_filename = "/var/log" |



## 结果说明

无论成功与失败，都会显示最终的结果，结果参数说明：


|参数名 | 描述 |
|---|---|
| Status |  导入成功（Success）与否（Failed）|
| TotalRows | 想要导入文件中所有的行数 |
| FailLoadRows | 想要导入文件中没有导入的行数 |
| LoadedRows | 实际导入 Doris 的行数 |
| FilteredRows | 实际导入过程中被 Doris 过滤的行数 |
| UnselectedRows | 实际导入过程中被 Doris 忽略的行数 |
| LoadBytes | 实际导入的 byte 大小 |
| LoadTimeMs | 实际导入的时间 |
| LoadFiles | 实际导入的文件列表|



具体例子如下:

- 导入成功，成功信息如下： 

  ```Go
  Load Result: {
          "Status": "Success",
          "TotalRows": 120,
          "FailLoadRows": 0,
          "LoadedRows": 120,
          "FilteredRows": 0,
          "UnselectedRows": 0,
          "LoadBytes": 40632,
          "LoadTimeMs": 971,
          "LoadFiles": [
                  "basic.csv",
                  "basic_data1.csv",
                  "basic_data2.csv",
                  "dir1/basic_data.csv",
                  "dir1/basic_data.csv.1",
                  "dir1/basic_data1.csv"
          ]
  }
  ```
  
- 导入失败：如果导入过程中部分数据没有导入失败了，会给出重传信息，如：
  
  ```Go
  load has some error, and auto retry failed, you can retry by : 
  ./doris-streamloader --source_file /mnt/disk1/laihui/doris/tools/tpch-tools/bin/tpch-data/lineitem.tbl.1  --url="http://127.0.0.1:8239" --header="column_separator:|?columns: l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag,l_linestatus, l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment,temp" --db="db" --table="lineitem1" -u root -p "" --compress=false --timeout=36000 --workers=3 --batch=4096 --batch_byte=943718400 --max_byte_per_task=1073741824 --check_utf8=true --report_duration=1 --auto_retry="2,1;1,1;0,1" --auto_retry_times=0 --auto_retry_interval=60
  ```
  

只需复制运行该命令即可，`auto_retry` 说明可参考, 并给出失败的结果信息：

```Go
Load Result: {
      "Status": "Failed",
      "TotalRows": 1,
      "FailLoadRows": 1,
      "LoadedRows": 0,
      "FilteredRows": 0,
      "UnselectedRows": 0,
      "LoadBytes": 0,
      "LoadTimeMs": 104,
      "LoadFiles": [
              "/mnt/disk1/laihui/doris/tools/tpch-tools/bin/tpch-data/lineitem.tbl.1"
      ]
}

```


## 最佳实践

### 1. 参数推荐

1. 必要参数，一定要配置： ```--source_file=FILE_LIST --url=FE_OR_BE_SERVER_URL_WITH_PORT --header=STREAMLOAD_HEADER --db=TARGET_DATABASE --table=TARGET_TABLE``` ，**如果需要导入多个文件时，推荐使用** `source_file` **方式。**

2. `workers`，默认值为 CPU 核数，在 CPU 核数过多的场景（比如 96 核）会产生太多的并发，需要减少这个值，**推荐一般设置为 8 即可。**

3. `max_byte_per_task`，可以设置一个很大的值来减少导入的 version 数。但如果遇到 `body exceed max size` 错误且不想调整 `streaming_load_max_mb` 参数（需重启 BE），又或是遇到 `-238 TOO MANY SEGMENT` 错误，可以临时调小这个配置，**一般使用默认即可。**

4. 影响 version 数的两个参数：
- `workers`：worker 数越多，版本号越多，并发越高，一般使用 8 即可。
- `max_byte_per_task`：`max_byte_per_task` 越大，单个 version 数据量越大，version 数越少，但是这个值过大可能会遇到 `-238 TOO MANY SEGMENT `的问题。一般使用默认值即可。



### 2. 推荐命令

设置必要参数以及设置 `workers=8` 即可。

```text
./doris-streamloader --source_file="demo.csv,demoFile*.csv,demoDir" --url="http://127.0.0.1:8030" --header="column_separator:," --db="demo" --table="test_load" --u="root" --workers=8
```


### 3. FAQ

- 在导入过程中，遇到了部分子任务失败的问题，当时没有断点续传续传的功能，导入失败后重新删表导入，如果遇到这个问题，工具会进行自动重传，如果重传失败会打印出重传命令，复制后可以手动重传。
- 该工具的默认单个导入是 100G，超过了 BE 默认的 `streaming_load_max_mb` 阈值如果不希望重启 BE，可以减少 `max_byte_per_task` 这个参数的大小。

  查看 `streaming_load_max_mb` 大小的方法：

  ```Go
  -curl "http://127.0.0.1:8040/api/show_config"
  ```
  
- 导入过程如果遇到 `-238 TOO MANY SEGMENT` 的问题，可以减少 `max_byte_per_task` 的大小。