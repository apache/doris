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

#  Data Lake Regression Testing Tool For External Table

Used to test the doris external table on object storage for cloud vendors

> Supported storage formats: HDFS, Alibaba Cloud OSS, Tencent Cloud COS, Huawei Cloud OBS

> Supported data lake table formats: Iceberg

The following provides the example of the command line options:

```
sh tools/emr_storage_regression/emr_tools.sh --profile default_emr_env.sh
```

Or

```
sh tools/emr_storage_regression/emr_tools.sh --case CASE --endpoint ENDPOINT --region REGION  --service SERVICE --ak AK --sk SK  --host HOST --user USER --port PORT
```

The usage of each option is described below.

## Connectivity Test

When the `--case` option is set to `ping`, will check Doris's connectivity on EMR:

- `--endpoint`, Object Storage Endpoint.

- `--region`, Object Storage Region.

- `--ak`, Object Storage Access Key.

- `--sk`, Object Storage Secret Key.

- `--host`, Doris Mysql Client IP.

- `--user`, Doris Mysql Client Username.

- `--port`, Doris Mysql Client Port.

- `--service`, EMR cloud vendors: ali(Alibaba), hw(Huawei), tx(tencent).

### Environment Variables

Need modify the environment variable in `default_emr_env.sh`, the script will execute `source default_emr_env.sh` to make the environment variable take effect.

If environment variables are configured, you can run the test script directly with the following command:

```
sh emr_tools.sh --profile default_emr_env.sh
```

### The Script Execution Steps For Connectivity Test

1. Create Spark and Hive tables on EMR
2. Use Spark and Hive command lines to insert sample data
3. Doris creates the Catalog for connectivity test 
4. Execute SQL for connectivity test: `ping.sql`

### Alibaba Cloud

```
sh emr_tools.sh --profile default_emr_env.sh
```

Or

Set `--service` to `ali`, and then test connectivity on Huawei Cloud.

```
sh emr_tools.sh --case ping --endpoint oss-cn-beijing-internal.aliyuncs.com --region cn-beijing  --service ali --ak ak --sk sk --host 127.0.0.1 --user root --port 9030 > log
```

Alibaba Cloud EMR also supports testing connectivity for both Doris with DLF metadata and Doris on OSS-HDFS storage.

- The DLF metadata connectivity test needs to be performed on the EMR cluster where the DLF serves as the metadata store, Default value of `DLF_ENDPOINT` is `datalake-vpc.cn-beijing.aliyuncs.com`, configured at ping_test/ping_poc.sh.

- To test the OSS-HDFS storage connectivity, need to [enable the HDFS service on the OSS storage and configure](https://www.alibabacloud.com/help/en/e-mapreduce/latest/oss-hdfsnew), Default value of `JINDO_ENDPOINT` is `cn-beijing.oss-dls.aliyuncs.com`, configured at ping_test/ping_poc.sh.

### Tencent Cloud

```
sh emr_tools.sh --profile default_emr_env.sh
```

Or

Set `--service` to `tx`, and then test connectivity on Huawei Cloud.

```
sh emr_tools.sh --case ping --endpoint cos.ap-beijing.myqcloud.com --region ap-beijing --service tx --ak ak --sk sk --host 127.0.0.1 --user root --port 9030 > log
```

### Huawei Cloud

```
sh emr_tools.sh --profile default_emr_env.sh
```

Or

Set `--service`to `hw`, and then test connectivity on Huawei Cloud.

```
sh emr_tools.sh --case ping --endpoint obs.cn-north-4.myhuaweicloud.com --region cn-north-4  --service hw --ak ak --sk sk --host 127.0.0.1 --user root --port 9030 > log 
```

## Performance Testing on Standard Test Set

When the `--case` option is set to `data_set`, will test the query performance of Doris external table:

- `--test` test data set: ssb, ssb_flat, tpch, clickbench and all. Default `all`.

- `--service`, EMR cloud vendors: ali(Alibaba), hw(Huawei), tx(tencent).

- `--host`, Doris Mysql Client IP.

- `--user`, Doris Mysql Client Username.

- `--port`, Doris Mysql Client Port.

### Environment Variables

Just modify the above environment variable in `default_emr_env.sh`, the script will execute `source default_emr_env.sh` to make the environment variable take effect.

If environment variables are configured, you can run the test script directly with the following command:

```
sh emr_tools.sh --profile default_emr_env.sh
```

### Prepare Data

1. To run the standard test set using the `emr_tools.sh` script, you need to rewrite the object storage bucket specified by the `BUCKET` variable, and then prepare data in advance and put them under the bucket. The script will generate table creation statements based on the bucket.

2. Now the `emr_tools.sh` script supports iceberg, parquet and orc data for ssb, ssb_flat, tpch, clickbench.

### Execution Steps

1. After the connectivity test, the Doris Catalog corresponding to the standard test set is created
2. Prepare the test set data based on the object storage bucket specified by the `BUCKET` variable
3. Generate Spark table creation statements and create Spark object storage tables on EMR
4. Create the spark table in the local HDFS directory: `hdfs:///benchmark-hdfs`
5. You can choose to analyze Doris tables ahead of time and manually execute the statements in `analyze.sql` in the Doris Catalog
6. Execute standard test set scripts: `run_standard_set.sh`

### Standard data set: ssb, ssb_flat, tpch, clickbench

- Full test. After executing the test command, Doris will run ssb, ssb_flat, tpch, clickbench tests in sequence, and the test results will include the cases on HDFS and on the object storage specified by `--service`.

```
sh emr_tools.sh --case data_set --service ali  --host 127.0.0.1 --user root --port 9030 > log
```

- Specify a single test. `--test` option can be set to one of ssb, ssb_flat, tpch and clickbench.

```
sh emr_tools.sh --case data_set --test ssb --service ali  --host 127.0.0.1 --user root --port 9030 > log
```
