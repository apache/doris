---
{
    "title": "Doris Streamloader",
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


## Overview
Doris Streamloader is a client tool designed for loading data into Apache Doris. In comparison to single-threaded load using `curl`, it reduces the load latency of large datasets by its concurrent loading capabilities. It comes with the following features:

- **Parallel loading**: multi-threaded load for the Stream Load method. You can set the parallelism level using the `workers` parameter.
- **Multi-file load:** simultaneously load of multiple files and directories with one shot. It supports recursive file fetching and allows you to specify file names with wildcard characters.
- **Path traversal support:** support path traversal when the source files are in directories
- **Resilience and continuity:** in case of partial load failures, it can resume data loading from the point of failure.
- **Automatic retry mechanism:** in case of loading failures, it can automatically retry a default number of times. If the loading remains unsuccessful, it will print the command for manual retry.


## Installation

**Version 1.0**

Source Code: https://github.com/apache/doris-streamloader/



| Version | Date |  Architecture  |  Link  |
|---|---|---|---|
| v1.0   |  20240131 |  x64  | https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-streamloader-1.0.1-bin-x64.tar.xz |
| v1.0   | 20240131  | arm64 | https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-streamloader-1.0.1-bin-arm64.tar.xz |



:::note
The obtained result is the executable binary.
:::


## How to use

```bash

doris-streamloader --source_file={FILE_LIST} --url={FE_OR_BE_SERVER_URL}:{PORT} --header={STREAMLOAD_HEADER} --db={TARGET_DATABASE} --table={TARGET_TABLE}


```

**1. `FILE_LIST` support:**

- Single file

    E.g. Load a single file


    ```json
    
    doris-streamloader --source_file="dir" --url="http://localhost:8330" --header="column_separator:|?columns:col1,col2" --db="testdb" --table="testtbl"
    
    ```

- Single directory

    E.g. Load a single directory

    ```json
    doris-streamloader --source_file="dir" --url="http://localhost:8330" --header="column_separator:|?columns:col1,col2" --db="testdb" --table="testtbl"        
    ```

- File names with wildcard characters (enclosed in quotes)

    E.g. Load file0.csv, file1.csv, file2.csv

    ```json
    doris-streamloader --source_file="file*" --url="http://localhost:8330" --header="column_separator:|?columns:col1,col2" --db="testdb" --table="testtbl"
    ```

- A list of files separated by commas

    E.g. Load file0.csv, file1.csv, file2.csv
    
  ```json
   doris-streamloader --source_file="file0.csv,file1.csv,file2.csv" --url="http://localhost:8330" --header="column_separator:|?columns:col1,col2" --db="testdb" --table="testtbl"
  ```

- A list of directories separated by commas

  E.g. Load dir1, dir2, dir3

   ```json
    doris-streamloader --source_file="dir1,dir2,dir3" --url="http://localhost:8330" --header="column_separator:|?columns:col1,col2" --db="testdb" --table="testtbl" 
   ```
  

**2. `STREAMLOAD_HEADER` supports all streamload headers separated with '?' if there is more than one**

Example:

```bash
doris-streamloader --source_file="data.csv" --url="http://localhost:8330" --header="column_separator:|?columns:col1,col2" --db="testdb" --table="testtbl"
```

The parameters above are required, and the following parameters are optional: 

| Parameter | Description  |  Default Value  |  Suggestions  |
|---|---|---|---|
| --u      | Username of the database |  root    |      |
| --p      | Password |  empty string  |      |
| --compress      | Whether to compress data upon HTTP transmission |  false    |   Remain as default. Compression and decompression can increase pressure on Doris Streamloader side and the CPU resources on Doris BE side, so it is advised to only enable this when network bandwidth is constrained.   |
|--timeout    | Timeout of the HTTP request sent to Doris (seconds) |  60\*60\*10  | Remain as default |
| --batch      | Granularity of batch reading and sending of files (rows) |  4096    | Remain as default |
| --batch_byte      | Granularity of batch reading and sending of files (byte) |  943718400 (900MB)    | Remain as default |
| --workers   | Concurrency  level of data loading |  0    |   "0" means the auto mode, in which the streamload speed is based on the data size and disk throughput. You can dial this up for a high-performance cluster, but it is advised to keep it below 10. If you observe excessive memory usage (via the memtracker in log), you can dial this down.   |
| --disk_throughput      | Disk throughput (MB/s) |  800    |  Usually remain as default. This parameter is a basis of the automatic inference of workers. You can adjust this based on your needs to get a more appropriate value of workers.  |
|--streamload_throughput | Streamload throughput (MB/s) | 100 | Usually remain as default. The default value is derived from the streamload throughput and predicted performance provided by the daily performance testing environment. To get a more appropriate value of workers, you can configure this based on your measured streamload throughput: (LoadBytes*1000)/(LoadTimeMs*1024*1024) |
| --max_byte_per_task      | Maximum data size for each load task. For a dataset exceeding this size, the remaining part will be split into a new load task. |  107374182400 (100G)    | This is recommended to be large in order to reduce the number of load versions. However, if you encounter a "body exceed max size" and try to avoid adjusting the streaming_load_max_mb parameter (which requires restarting the backend), or if you encounter a "-238 TOO MANY SEGMENT" error, you can temporarily dial this down. |
| --check_utf8 | <p>Whether to check the encoding of the data that has been loaded: </p>   <p> 1) false, direct load of raw data without checking;  2)  true, replacing non UTF-8 characters with � </p> | true |Remain as default|
|--debug |Print debug log | false | Remain as default |
|--auto_retry| The list of failed workers and tasks for auto retry | empty string | This is only used when there is an load failure. The serial numbers of the failed workers and tasks will be shown and all you need is to copy and execute the the entire command. For example, if --auto_retry="1,1;2,1", that means the failed tasks include the first task in the first worker and the first task in the second worker. |
|--auto_retry_times | Times of auto retries | 3 | Remain as default. If you don't need retries, you can set this to 0. |
|--auto_retry_interval | Interval of auto retries | 60 | Remain as default. If the load failure is caused by a Doris downtime, it is recommended to set this parameter based on the restart interval of Doris. |
|--log_filename | Path for log storage | "" | Logs are printed to the console by default. To print them to a log file, you can set the path, such as --log_filename="/var/log". |



## Result description

A result will be returned no matter the data loading succeeds or fails. 


|Parameter | Description |
|---|---|
| Status | Loading succeeded or failed |
| TotalRows | Total number of rows |
| FailLoadRows | Number of rows failed to be loaded |
| LoadedRows | Number of rows loaded |
| FilteredRows | Number of rows filtered |
| UnselectedRows | Number of rows unselected |
| LoadBytes | Number of bytes loaded |
| LoadTimeMs | Actual loading time |
| LoadFiles | List of loaded files |



Examples: 

- If the loading succeeds, you will see a result like: 
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
  
- If the loading fails (or partially fails), you will see a retry message: 
  
  ```Go
  load has some error and auto retry failed, you can retry by : 
  ./doris-streamloader --source_file /mnt/disk1/laihui/doris/tools/tpch-tools/bin/tpch-data/lineitem.tbl.1  --url="http://127.0.0.1:8239" --header="column_separator:|?columns: l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag,l_linestatus, l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment,temp" --db="db" --table="lineitem1" -u root -p "" --compress=false --timeout=36000 --workers=3 --batch=4096 --batch_byte=943718400 --max_byte_per_task=1073741824 --check_utf8=true --report_duration=1 --auto_retry="2,1;1,1;0,1" --auto_retry_times=0 --auto_retry_interval=60
  ```
  

You can copy and execute the command. The failure message will also be provided:

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


## Best practice

### Parameter suggestions

1. Required parameters:  
```--source_file=FILE_LIST --url=FE_OR_BE_SERVER_URL_WITH_PORT --header=STREAMLOAD_HEADER --db=TARGET_DATABASE --table=TARGET_TABLE``` 
   If you need to load multiple files, you should configure all of them at a time in `source_file`.

2. The default value of `workers` is the number of CPU cores. When that is large, for example, 96 cores, the value of `workers` should be dialed down. **The recommended value for most cases is 8.**

3. `max_byte_per_task` is recommended to be large in order to reduce the number of load versions. However, if you encounter a "body exceed max size" and try to avoid adjusting the streaming_load_max_mb parameter (which requires restarting the backend), or if you encounter a `-238 TOO MANY SEGMENT` error, you can temporarily dial this down. **For most cases, this can remain as default.**

**Two parameters that impacts the number of versions:**

- `workers`: The more `workers`, the higher concurrency level, and thus the more versions. The recommended value for most cases is 8.
- `max_byte_per_task`:  The larger `max_byte_per_task` , the larger data size in one single version, and thus the less versions. However, if this is excessively high, it could easily cause an `-238 TOO MANY SEGMENT ` error. For most cases, this can remain as default. 



### Recommended commands

In most cases, you only need to set the required parameters and `workers`. 

```text
./doris-streamloader --source_file="demo.csv,demoFile*.csv,demoDir" --url="http://127.0.0.1:8030" --header="column_separator:," --db="demo" --table="test_load" --u="root" --workers=8
```


### FAQ

- Before resumable loading was available, to fix any partial failures in loading would require deleting the current table and starting over. In this case, Doris Streamloader would retry automatically. If the retry fails, a retry command will be printed so you can copy and execute it.
- The default maximum data loading size for Doris Streamloader is limited by BE config `streaming_load_max_mb` (default: 100GB). If you don't want to restart BE, you can also dial down `max_byte_per_task`.

  To show current `streaming_load_max_mb`: 

  ```Go
  curl "http://127.0.0.1:8040/api/show_config"
  ```
  
- If you encounter an `-238 TOO MANY SEGMENT ` error, you can dial down `max_byte_per_task`.