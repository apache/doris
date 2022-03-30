---
{
    "title": "MULTI LOAD",
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

# MULTI LOAD
## Description

Syntax:
curl --location-trusted -u user:passwd -XPOST http://host:port/api/{db}/_multi_start?label=xxx
curl --location-trusted -u user:passwd -T data.file http://host:port/api/{db}/{table1}/_load?label=xxx\&sub_label=yyy
curl --location-trusted -u user:passwd -T data.file http://host:port/api/{db}/{table2}/_load?label=xxx\&sub_label=zzz
curl --location-trusted -u user:passwd -XPOST http://host:port/api/{db}/_multi_commit?label=xxx
curl --location-trusted -u user:passwd -XPOST http://host:port/api/{db}/_multi_desc?label=xxx

'MULTI LOAD'can support users to import multiple tables at the same time on the basis of'MINI LOAD'. The specific commands are shown above.
'/api/{db}/_multi_start' starts a multi-table import task
'/api/{db}/{table}/_load' adds a table to be imported to an import task. The main difference from 'MINI LOAD' is that the 'sub_label' parameter needs to be passed in.
'/api/{db}/_multi_commit' submits the entire multi-table import task and the background begins processing
'/api/{db}/_multi_abort' Abandons a multi-table import task
'/api/{db}/_multi_desc' shows the number of jobs submitted by a multi-table import task

HTTP Protocol Specification
Privilege Authentication Currently Doris uses the Basic mode of HTTP for privilege authentication. So you need to specify a username and password when importing
This way is to pass passwords in plaintext, since we are all in the Intranet environment at present...

Expect Doris needs to send an HTTP request, and needs the 'Expect' header information with the content of'100-continue'.
Why? Because we need to redirect the request, we have to transfer the data content before.
This can avoid causing multiple data transmission, thereby improving efficiency.

Content-Length Doris needs to send a request with the header 'Content-Length'. If the content ratio is sent
If'Content-Length'is less, Palo believes that if there is a transmission problem, the submission of the task fails.
NOTE: If you send more data than 'Content-Length', Doris reads only 'Content-Length'.
Length content and import

Description of parameters:
User: User is user_name if the user is in default_cluster. Otherwise, it is user_name@cluster_name.

Label: Used to specify the label number imported in this batch for later job status queries, etc.
This parameter must be passed in.

Sub_label: Used to specify a subversion number within a multi-table import task. For multi-table imported loads, this parameter must be passed in.

Columns: Used to describe the corresponding column name in the import file.
If it is not passed in, the column order in the file is considered to be the same as the order in which the table is built.
The specified method is comma-separated, such as columns = k1, k2, k3, K4

Column_separator: Used to specify the separator between columns, default is' t'
NOTE: Url encoding is required, such as specifying '\t'as a delimiter.
Then you should pass in 'column_separator=% 09'

Max_filter_ratio: Used to specify the maximum percentage allowed to filter irregular data, default is 0, not allowed to filter
Custom specification should be as follows:'max_filter_ratio = 0.2', meaning that 20% error rate is allowed.
Pass in effect at'_multi_start'

NOTE:
1. This method of importing is currently completed on a single machine, so it is not suitable to import a large amount of data.
It is recommended that the amount of data imported should not exceed 1GB

2. Currently, it is not possible to submit multiple files in the form of `curl-T', `{file1, file2}', because curl splits them into multiple files.
Request sent, multiple requests cannot share a label number, so it cannot be used

3. Supports streaming-like ways to use curl to import data into Doris, but Doris will have to wait until the streaming is over
Real import behavior will occur, and the amount of data in this way cannot be too large.

'35;'35; example

1. Import the data from the local file 'testData1'into the table of 'testTbl1' in the database 'testDb', and
Import the data from 'testData2'into the table 'testTbl2' in 'testDb'(the user is in default_cluster)
curl --location-trusted -u root -XPOST http://host:port/api/testDb/_multi_start?label=123
curl --location-trusted -u root -T testData1 http://host:port/api/testDb/testTbl1/_load?label=123\&sub_label=1
curl --location-trusted -u root -T testData2 http://host:port/api/testDb/testTbl2/_load?label=123\&sub_label=2
curl --location-trusted -u root -XPOST http://host:port/api/testDb/_multi_commit?label=123

2. Multi-table Import Midway Abandon (User in default_cluster)
curl --location-trusted -u root -XPOST http://host:port/api/testDb/_multi_start?label=123
curl --location-trusted -u root -T testData1 http://host:port/api/testDb/testTbl1/_load?label=123\&sub_label=1
curl --location-trusted -u root -XPOST http://host:port/api/testDb/_multi_abort?label=123

3. Multi-table import to see how much content has been submitted (user is in default_cluster)
curl --location-trusted -u root -XPOST http://host:port/api/testDb/_multi_start?label=123
curl --location-trusted -u root -T testData1 http://host:port/api/testDb/testTbl1/_load?label=123\&sub_label=1
curl --location-trusted -u root -XPOST http://host:port/api/testDb/_multi_desc?label=123

## keyword
MULTI, MINI, LOAD
