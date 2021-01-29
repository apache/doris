---
{
    "title": "MINI LOAD",
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

# MINI LOAD
## Description

MINI LOAD and STEAM LOAD are implemented in exactly the same way. MINI LOAD is a subset of STREAM LOAD in import support.
Subsequent imports of new features will only be supported in STEAM LOAD, MINI LOAD will no longer add features. It is suggested that STREAM LOAD be used instead. Please use HELP STREAM LOAD.

MINI LOAD is imported through HTTP protocol. Users can import without relying on Hadoop or Mysql client.
The user describes the import through HTTP protocol, and the data is streamed into Doris in the process of receiving http requests. After the ** import job is completed, the ** returns to the user the imported results.

* Note: In order to be compatible with the old version of mini load usage habits, users can still view the import results through the 'SHOW LOAD' command.

Grammar:
Import:

curl --location-trusted -u user:passwd -T data.file http://host:port/api/{db}/{table}/_load?label=xxx

View import information

curl -u user:passwd http://host:port/api/{db}/_load_info?label=xxx

HTTP Protocol Specification

Privilege Authentication Currently Doris uses the Basic mode of HTTP for privilege authentication. So you need to specify a username and password when importing
This way is to pass the password in plaintext, and does not support encrypted transmission for the time being.

Expect Doris needs to send an HTTP request with the 'Expect' header information,'100-continue'.
Why? Because we need to redirect the request, we have to transfer the data content before.
This can avoid causing multiple data transmission, thereby improving efficiency.

Content-Length Doris needs to send a request with the header 'Content-Length'. If the content ratio is sent
'Content-Length' is less, so Doris believes that if there is a transmission problem, the submission task fails.
NOTE: If you send more data than 'Content-Length', Doris reads only 'Content-Length'.
Length content and import


Description of parameters:

User: User is user_name if the user is in default_cluster. Otherwise, it is user_name@cluster_name.

Label: The label used to specify this batch of imports for later job queries, etc.
This parameter must be passed in.

Columns: Used to describe the corresponding column name in the import file.
If it is not passed in, the column order in the file is considered to be the same as the order in which the table is built.
The specified method is comma-separated, such as columns = k1, k2, k3, K4

Column_separator: Used to specify the separator between columns, default is' t'
NOTE: Url encoding is required, for example
If you need to specify '\t' as a separator, you should pass in 'column_separator=% 09'
If you need to specify 'x01'as a delimiter, you should pass in 'column_separator=% 01'
If you need to specify','as a separator, you should pass in 'column_separator=% 2c'


Max_filter_ratio: Used to specify the maximum percentage allowed to filter irregular data, default is 0, not allowed to filter
Custom specification should be as follows:'max_filter_ratio = 0.2', meaning that 20% error rate is allowed.

Timeout: Specifies the timeout time of the load job in seconds. When the load execution time exceeds this threshold, it is automatically cancelled. The default timeout time is 86400 seconds.
It is recommended to specify a timeout time of less than 86400 seconds.

Hll: Used to specify the corresponding relationship between the HLL columns in the data and the tables, the columns in the tables and the columns specified in the data.
(If columns are not specified, the columns of the data column surface can also be other non-HLL columns in the table.) By "partition"
Specify multiple HLL columns using ":" splitting, for example:'hll1, cuid: hll2, device'

NOTE:
1. This method of importing is currently completed on a single machine, so it is not suitable to import a large amount of data.
It is recommended that the amount of data imported should not exceed 1 GB.

2. Currently, it is not possible to submit multiple files in the form of `curl-T', `{file1, file2}', because curl splits them into multiple files.
Request sent, multiple requests cannot share a label number, so it cannot be used

3. Miniload is imported in exactly the same way as streaming. It returns the results synchronously to users after the import of streaming is completed.
Although the information of mini load can be found in subsequent queries, it cannot be operated on. The queries are only compatible with the old ways of use.

4. When importing from the curl command line, you need to add escape before & or the parameter information will be lost.

'35;'35; example

1. Import the data from the local file 'testData' into the table of 'testTbl' in the database 'testDb'(the user is in default_cluster)
curl --location-trusted -u root -T testData http://host:port/api/testDb/testTbl/_load?label=123

2. Import the data from the local file 'testData' into the table of 'testTbl' in the database'testDb'(the user is in test_cluster). The timeout time is 3600 seconds.
curl --location-trusted -u root@test_cluster:root -T testData http://fe.host:port/api/testDb/testTbl/_load?label=123&timeout=3600

3. Import data from the local file 'testData' into the 'testTbl' table in the database 'testDb', allowing a 20% error rate (the user is in default_cluster)
curl --location-trusted -u root -T testData http://host:port/api/testDb/testTbl/_load?label=123\&max_filter_ratio=0.2

4. Import the data from the local file 'testData' into the table 'testTbl' in the database 'testDb', allowing a 20% error rate, and specify the column name of the file (the user is in default_cluster)
curl --location-trusted -u root -T testData http://host:port/api/testDb/testTbl/_load?label=123\&max_filter_ratio=0.2\&columns=k1,k2,k3

5. Import in streaming mode (user is in default_cluster)
seq 1 10 | awk '{OFS="\t"}{print $1, $1 * 10}' | curl --location-trusted -u root -T - http://host:port/api/testDb/testTbl/_load?label=123

6. Import tables containing HLL columns, which can be columns in tables or columns in data to generate HLL columns (users are in default_cluster)

    curl --location-trusted -u root -T testData http://host:port/api/testDb/testTbl/_load?label=123\&max_filter_ratio=0.2\&hll=hll_column1,k1:hll_column2,k2
        \&columns=k1,k2,k3

    curl --location-trusted -u root -T testData http://host:port/api/testDb/testTbl/_load?label=123\&max_filter_ratio=0.2
        \&hll=hll_column1,tmp_k4:hll_column2,tmp_k5\&columns=k1,k2,k3,tmp_k4,tmp_k5

7. View imports after submission

curl -u root http://host:port/api/testDb/_load_info?label=123

## keyword
MINI, LOAD
