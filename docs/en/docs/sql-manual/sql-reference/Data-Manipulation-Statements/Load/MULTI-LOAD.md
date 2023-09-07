---
{
    "title": "MULTI-LOAD",
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

## MULTI-LOAD

### Name

MULTI LOAD

### Description

Users submit multiple import jobs through the HTTP protocol. Multi Load can ensure the atomic effect of multiple import jobs

````
Syntax:
    curl --location-trusted -u user:passwd -XPOST http://host:port/api/{db}/_multi_start?label=xxx
    curl --location-trusted -u user:passwd -T data.file http://host:port/api/{db}/{table1}/_load?label=xxx\&sub_label=yyy
    curl --location-trusted -u user:passwd -T data.file http://host:port/api/{db}/{table2}/_load?label=xxx\&sub_label=zzz
    curl --location-trusted -u user:passwd -XPOST http://host:port/api/{db}/_multi_commit?label=xxx
    curl --location-trusted -u user:passwd -XPOST http://host:port/api/{db}/_multi_desc?label=xxx

On the basis of 'MINI LOAD', 'MULTI LOAD' can support users to import to multiple tables at the same time. The specific commands are shown above.
'/api/{db}/_multi_start' starts a multi-table import task
'/api/{db}/{table}/_load' adds a table to be imported to an import task. The main difference from 'MINI LOAD' is that the 'sub_label' parameter needs to be passed in
'/api/{db}/_multi_commit' submits the entire multi-table import task, and starts processing in the background
'/api/{db}/_multi_abort' Abort a multi-table import task
'/api/{db}/_multi_desc' can display the number of jobs submitted by a multi-table import task

Description of the HTTP protocol
    Authorization Authentication Currently, Doris uses HTTP Basic authorization authentication. So you need to specify the username and password when importing
                        This method is to pass the password in clear text, since we are currently in an intranet environment. . .

    Expect Doris needs to send the http request, it needs to have 'Expect' header information, the content is '100-continue'
                        why? Because we need to redirect the request, before transmitting the data content,
                        This can avoid causing multiple transmissions of data, thereby improving efficiency.

    Content-Length Doris needs to send the request with the 'Content-Length' header. If the content sent is greater than
                        If the 'Content-Length' is less, then Palo thinks that there is a problem with the transmission, and fails to submit the task.
                        NOTE: If more data is sent than 'Content-Length', then Doris only reads 'Content-Length'
                        length content and import

Parameter Description:
    user: If the user is in the default_cluster, the user is the user_name. Otherwise user_name@cluster_name.

    label: Used to specify the label number imported in this batch, which is used for later job status query, etc.
                        This parameter is required.

    sub_label: Used to specify the subversion number inside a multi-table import task. For loads imported from multiple tables, this parameter must be passed in.

    columns: used to describe the corresponding column names in the import file.
                        If it is not passed in, then the order of the columns in the file is considered to be the same as the order in which the table was created.
                        The specified method is comma-separated, for example: columns=k1,k2,k3,k4

    column_separator: used to specify the separator between columns, the default is '\t'
                        NOTE: url encoding is required, for example, '\t' needs to be specified as the delimiter,
                        Then you should pass in 'column_separator=%09'

    max_filter_ratio: used to specify the maximum ratio of non-standard data allowed to filter, the default is 0, no filtering is allowed
                        The custom specification should be as follows: 'max_filter_ratio=0.2', which means 20% error rate is allowed
                        Passing in has effect when '_multi_start'

NOTE:
    1. This import method currently completes the import work on one machine, so it is not suitable for import work with a large amount of data.
    It is recommended that the amount of imported data should not exceed 1GB

    2. Currently it is not possible to submit multiple files using `curl -T "{file1, file2}"`, because curl splits them into multiple files
    The request is sent. Multiple requests cannot share a label number, so it cannot be used.

    3. Supports the use of curl to import data into Doris in a way similar to streaming, but only after the streaming ends Doris
    The real import behavior will occur, and the amount of data in this way cannot be too large.
````

### Example

````
1. Import the data in the local file 'testData1' into the table 'testTbl1' in the database 'testDb', and
Import the data of 'testData2' into table 'testTbl2' in 'testDb' (user is in defalut_cluster)
    curl --location-trusted -u root -XPOST http://host:port/api/testDb/_multi_start?label=123
    curl --location-trusted -u root -T testData1 http://host:port/api/testDb/testTbl1/_load?label=123\&sub_label=1
    curl --location-trusted -u root -T testData2 http://host:port/api/testDb/testTbl2/_load?label=123\&sub_label=2
    curl --location-trusted -u root -XPOST http://host:port/api/testDb/_multi_commit?label=123

2. Abandoned in the middle of multi-table import (user is in defalut_cluster)
    curl --location-trusted -u root -XPOST http://host:port/api/testDb/_multi_start?label=123
    curl --location-trusted -u root -T testData1 http://host:port/api/testDb/testTbl1/_load?label=123\&sub_label=1
    curl --location-trusted -u root -XPOST http://host:port/api/testDb/_multi_abort?label=123

3. Multi-table import to see how much content has been submitted (the user is in the defalut_cluster)
    curl --location-trusted -u root -XPOST http://host:port/api/testDb/_multi_start?label=123
    curl --location-trusted -u root -T testData1 http://host:port/api/testDb/testTbl1/_load?label=123\&sub_label=1
    curl --location-trusted -u root -XPOST http://host:port/api/testDb/_multi_desc?label=123
````

### Keywords

```
MULTI, MINI, LOAD
```

### Best Practice
