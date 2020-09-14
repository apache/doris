---
{
    "title": "GET TABLETS DISTRIBUTION BETWEEN DIFFERENT DISKS",
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

# GET TABLETS DISTRIBUTION BETWEEN DIFFERENT DISKS
   
Get the distribution of tablets under each partition between different disks on a particular BE node via FE

```
curl -X GET http://fe_host:webserver_port/api/tablet_disk_distribution?be_host=XXXXX&http_port=XXXXX
```

The return is the distribution of tablets under each partition between different disks on a particular BE node.

```
{
    msg: "OK",
    code: 0,
    data: {
        backend: "192.168.1.103",
        http_port: 8040,
        tablets_distribution: [
            {
                partition_id: ,
                table_name: "",
                disks:[
                    {
                        disk_path_hash: ,
                        total_capacity: ,
                        aviliable_capacity: ,
                        used_percentage: ,
                        tablets_num: ,
                        tablets: [
                            {
                                tablet_id: ,
                                schema_hash: ,
                                tablet_size: 
                            },
                
                                ...
                
                            {
                                tablet_id: ,
                                schema_hash: ,
                                tablet_size: 
                            }
                        ]
                    },
                    {
                        disk_path_hash: ,
                        ...
                    },
                    ...
                ]
            },
            {
                partition_id: ,
                table_name: "",
                disks:[
                    ...
                ]
            },
            ...
        ]
    },
    count: 
}
```
