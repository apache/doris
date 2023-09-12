---
{
    "title": "CloudCanal Data Import",
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

# CloudCanal Data Import

## Introduction

CloudCanal Community Edition is a free data migration and synchronization platform published by [ClouGence Co., Ltd](https://www.cloudcanalx.com) that integrates Schema Migration, Full Data Migration, verification, Correction, and real-time Incremental Synchronization.
CloudCanal help users build a modern data stack in a simple way.
![image.png](/images/cloudcanal/cloudcanal-1.jpg)

## Download

[CloudCanal Download Link](https://www.cloudcanalx.com)

## Function Description

- It is highly recommended to utilize CloudCanal version 2.2.5.0 or higher for efficient data import into Doris.
- It is advisable to exercise control over the ingestion frequency when using CloudCanal to import **incremental data** into Doris. The default import frequency for writing data from CloudCanal to Doris can be adjusted using the `realFlushPauseSec` parameter, which is set to 10 seconds by default.
- In the current community edition with a maximum memory configuration of 2GB, if DataJobs encounter OOM exceptions or significant GC pauses, it is recommended to reduce the batch size to minimize memory usage. For full DataTask, you can adjust the `fullBatchSize` and `fullRingBufferSize` parameters. For incremental DataTask, the `increBatchSize` and `increRingBufferSize` parameters can be adjusted accordingly.
- Supported Source endpoints and features：

  | Source Endpoints \ Feature | Schema Migration | Full Data | Incremental | Verification | 
  | --- | --- | --- | --- | --- |
  | Oracle      | Yes | Yes | Yes | Yes |
  | PostgreSQL  | Yes | Yes | Yes | Yes |
  | Greenplum   | Yes | Yes | No | Yes |
  | MySQL       | Yes | Yes | Yes | Yes |

## Instructions for Use

CloudCanal offers a comprehensive productized capability where users can seamlessly add DataSources and create DataJobs through a visual interface. This enables automated schema migration, full data migration, and real-time incremental synchronization. The following example demonstrates how to migrate and synchronize data from a MySQL to the target Doris. Similar procedures can be applied when synchronizing other source endpoints with Doris.

### Prerequisites

First, refer to the [CloudCanal Quick Start](https://www.cloudcanalx.com/us/cc-doc/quick/quick_start) to complete the installation and deployment of the CloudCanal Community Edition.

### Add DataSource

- Log in to the CloudCanal platform
- Go to **DataSource Management** -> **Add DataSource**
- Select **Doris** from the options for self-built databases

![image.png](/images/cloudcanal/cloudcanal-11.png)

> Tips:
>
> - Client Address: The address of the Doris server's MySQL client service port. CloudCanal primarily uses this address to query metadata information of the database tables.
>
> - HTTP Address: The HTTP address is mainly used to receive data import requests from CloudCanal.

### Create DataJob

Once the DataSource has been added successfully, you can follow these steps to create data migration and synchronization DataJob.

- Go to **DataJob Management** -> **Create DataJob** in the CloudCanal
- Select the source and target databases for the DataJob
- Click Next Step

![image.png](/images/cloudcanal/cloudcanal-12.png)

- Choose **Incremental** and enable **Full Data**
- Select DDL Sync
- Click Next Step

![image.png](/images/cloudcanal/cloudcanal-13.png)

- Select the tables you want to subscribe to. Please note that **the tables automatically created during structural migration follow the primary key model, so tables without a primary key are not currently supported**
- Click Next Step

![image.png](/images/cloudcanal/cloudcanal-14.png)

- Configure the column mapping
- Click Next Step

![image.png](/images/cloudcanal/cloudcanal-15.png)

- Create DataJob

![image.png](/images/cloudcanal/cloudcanal-16.png)

- Check the status of DataJob. The DataJob will automatically go through the stages of Schema Migration, Full Data, and Incremental after it has been created

![image.png](/images/cloudcanal/cloudcanal-17.png)

