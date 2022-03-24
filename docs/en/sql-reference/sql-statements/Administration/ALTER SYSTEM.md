---
{
    "title": "ALTER SYSTEM",
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

# ALTER SYSTEM
## Description

This statement is used to operate on nodes in a system. (Administrator only!)
Grammar:
1) Adding nodes (without multi-tenant functionality, add in this way)
ALTER SYSTEM ADD BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...];
2) Adding idle nodes (that is, adding BACKEND that does not belong to any cluster)
ALTER SYSTEM ADD FREE BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...];
3) Adding nodes to a cluster
ALTER SYSTEM ADD BACKEND TO cluster_name "host:heartbeat_port"[,"host:heartbeat_port"...];
4) Delete nodes
ALTER SYSTEM DROP BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...];
5) Node offline
ALTER SYSTEM DECOMMISSION BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...];
6)226;- 21152;-Broker
ALTER SYSTEM ADD BROKER broker_name "host:port"[,"host:port"...];
(7) 20943;"23569;" Broker
ALTER SYSTEM DROP BROKER broker_name "host:port"[,"host:port"...];
8) Delete all Brokers
ALTER SYSTEM DROP ALL BROKER broker_name
9) Set up a Load error hub for centralized display of import error information
ALTER SYSTEM SET LOAD ERRORS HUB PROPERTIES ("key" = "value"[, ...]);
10) Modify property of BE
ALTER SYSTEM MODIFY BACKEND "host:heartbeat_port" SET ("key" = "value"[, ...]);

Explain:
1) Host can be hostname or IP address
2) heartbeat_port is the heartbeat port of the node
3) Adding and deleting nodes are synchronous operations. These two operations do not take into account the existing data on the node, the node is directly deleted from the metadata, please use cautiously.
4) Node offline operations are used to secure offline nodes. This operation is asynchronous. If successful, the node will eventually be removed from the metadata. If it fails, the offline will not be completed.
5) The offline operation of the node can be cancelled manually. See CANCEL DECOMMISSION for details
6) Load error hub:
   Currently, two types of Hub are supported: Mysql and Broker. You need to specify "type" = "mysql" or "type" = "broker" in PROPERTIES.
   If you need to delete the current load error hub, you can set type to null.
   1) When using the Mysql type, the error information generated when importing will be inserted into the specified MySQL library table, and then the error information can be viewed directly through the show load warnings statement.

     Hub of Mysql type needs to specify the following parameters:
     host: mysql host
     port: mysql port
     user: mysql user
     password: mysql password
     database mysql database
     table: mysql table

   2) When the Broker type is used, the error information generated when importing will form a file and be written to the designated remote storage system through the broker. Make sure that the corresponding broker is deployed
     Hub of Broker type needs to specify the following parameters:
     Broker: Name of broker
     Path: Remote Storage Path
     Other properties: Other information necessary to access remote storage, such as authentication information.

7) Modify BE node attributes currently supports the following attributes:
   1. tag.location：Resource tag
   2. disable_query: Query disabled attribute
   3. disable_load: Load disabled attribute

## example

1. Add a node
ALTER SYSTEM ADD BACKEND "host:port";

2. Adding an idle node
ALTER SYSTEM ADD FREE BACKEND "host:port";

3. Delete two nodes
ALTER SYSTEM DROP BACKEND "host1:port", "host2:port";

4. offline two nodes
ALTER SYSTEM DECOMMISSION BACKEND "host1:port", "host2:port";

5. Add two Hdfs Broker
ALTER SYSTEM ADD BROKER hdfs "host1:port", "host2:port";

6. Add a load error hub of Mysql type
ALTER SYSTEM SET LOAD ERRORS HUB PROPERTIES
("type"= "mysql",
"host" = "192.168.1.17"
"port" = "3306",
"User" = "my" name,
"password" = "my_passwd",
"database" = "doris_load",
"table" = "load_errors"
);

7. 添加一个 Broker 类型的 load error hub
ALTER SYSTEM SET LOAD ERRORS HUB PROPERTIES
("type"= "broker",
"Name" = BOS,
"path" = "bos://backup-cmy/logs",
"bos_endpoint" ="http://gz.bcebos.com",
"bos_accesskey" = "069fc278xxxxxx24ddb522",
"bos_secret_accesskey"="700adb0c6xxxxxx74d59eaa980a"
);

8. Delete the current load error hub
ALTER SYSTEM SET LOAD ERRORS HUB PROPERTIES
("type"= "null");

9. Modify BE resource tag

ALTER SYSTEM MODIFY BACKEND "host1:9050" SET ("tag.location" = "group_a");

10. Modify the query disabled attribute of BE

ALTER SYSTEM MODIFY BACKEND "host1:9050" SET ("disable_query" = "true");

11. Modify the load disabled attribute of BE
       
ALTER SYSTEM MODIFY BACKEND "host1:9050" SET ("disable_load" = "true"); 

## keyword
AGE,SYSTEM,BACKGROUND,BROKER,FREE
