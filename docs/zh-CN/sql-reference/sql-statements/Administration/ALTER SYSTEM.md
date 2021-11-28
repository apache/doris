---
{
    "title": "ALTER SYSTEM",
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

# ALTER SYSTEM
## description

    该语句用于操作一个系统内的节点。（仅管理员使用！）
    语法：
        1) 增加节点(不使用多租户功能则按照此方法添加)
            ALTER SYSTEM ADD BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...];
        2) 增加空闲节点(即添加不属于任何cluster的BACKEND)
            ALTER SYSTEM ADD FREE BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...];
        3) 增加节点到某个cluster
            ALTER SYSTEM ADD BACKEND TO cluster_name "host:heartbeat_port"[,"host:heartbeat_port"...];
        4) 删除节点
            ALTER SYSTEM DROP BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...];
        5) 节点下线
            ALTER SYSTEM DECOMMISSION BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...];
        6) 增加Broker
            ALTER SYSTEM ADD BROKER broker_name "host:port"[,"host:port"...];
        7) 减少Broker
            ALTER SYSTEM DROP BROKER broker_name "host:port"[,"host:port"...];
        8) 删除所有Broker
            ALTER SYSTEM DROP ALL BROKER broker_name
        9) 设置一个 Load error hub，用于集中展示导入时的错误信息
            ALTER SYSTEM SET LOAD ERRORS HUB PROPERTIES ("key" = "value"[, ...]);
        10) 修改一个 BE 节点的属性
            ALTER SYSTEM MODIFY BACKEND "host:heartbeat_port" SET ("key" = "value"[, ...]);

    说明：
        1) host 可以是主机名或者ip地址
        2) heartbeat_port 为该节点的心跳端口
        3) 增加和删除节点为同步操作。这两种操作不考虑节点上已有的数据，节点直接从元数据中删除，请谨慎使用。
        4) 节点下线操作用于安全下线节点。该操作为异步操作。如果成功，节点最终会从元数据中删除。如果失败，则不会完成下线。
        5) 可以手动取消节点下线操作。详见 CANCEL DECOMMISSION
        6) Load error hub:
            当前支持两种类型的 Hub：Mysql 和 Broker。需在 PROPERTIES 中指定 "type" = "mysql" 或 "type" = "broker"。
            如果需要删除当前的 load error hub，可以将 type 设为 null。
            1) 当使用 Mysql 类型时，导入时产生的错误信息将会插入到指定的 mysql 库表中，之后可以通过 show load warnings 语句直接查看错误信息。
               
                Mysql 类型的 Hub 需指定以下参数：
                    host：mysql host
                    port：mysql port
                    user：mysql user
                    password：mysql password
                    database：mysql database
                    table：mysql table

            2) 当使用 Broker 类型时，导入时产生的错误信息会形成一个文件，通过 broker，写入到指定的远端存储系统中。须确保已经部署对应的 broker
                Broker 类型的 Hub 需指定以下参数：
                    broker: broker 的名称
                    path: 远端存储路径
                    other properties: 其他访问远端存储所必须的信息，比如认证信息等。

        7) 修改 BE 节点属性目前支持以下属性：

            1. tag.location：资源标签
            2. disable_query: 查询禁用属性
            3. disable_load: 导入禁用属性
        
## example

    1. 增加一个节点
        ALTER SYSTEM ADD BACKEND "host:port";

    2. 增加一个空闲节点
        ALTER SYSTEM ADD FREE BACKEND "host:port";
        
    3. 删除两个节点
        ALTER SYSTEM DROP BACKEND "host1:port", "host2:port";
        
    4. 下线两个节点
        ALTER SYSTEM DECOMMISSION BACKEND "host1:port", "host2:port";

    5. 增加两个Hdfs Broker
        ALTER SYSTEM ADD BROKER hdfs "host1:port", "host2:port";

    6. 添加一个 Mysql 类型的 load error hub
        ALTER SYSTEM SET LOAD ERRORS HUB PROPERTIES
        ("type"= "mysql",
         "host" = "192.168.1.17"
         "port" = "3306",
         "user" = "my_name",
         "password" = "my_passwd",
         "database" = "doris_load",
         "table" = "load_errors"
        );

    7. 添加一个 Broker 类型的 load error hub
        ALTER SYSTEM SET LOAD ERRORS HUB PROPERTIES
        ("type"= "broker",
         "name" = "bos",
         "path" = "bos://backup-cmy/logs",
         "bos_endpoint" = "http://gz.bcebos.com",
         "bos_accesskey" = "069fc278xxxxxx24ddb522",
         "bos_secret_accesskey"="700adb0c6xxxxxx74d59eaa980a"
        );

    8. 删除当前的 load error hub
        ALTER SYSTEM SET LOAD ERRORS HUB PROPERTIES
        ("type"= "null");

    9. 修改 BE 的资源标签

        ALTER SYSTEM MODIFY BACKEND "host1:9050" SET ("tag.location" = "group_a");
    
    10. 修改 BE 的查询禁用属性
        
        ALTER SYSTEM MODIFY BACKEND "host1:9050" SET ("disable_query" = "true");
        
    11. 修改 BE 的导入禁用属性
       
        ALTER SYSTEM MODIFY BACKEND "host1:9050" SET ("disable_load" = "true"); 
       
## keyword
    ALTER,SYSTEM,BACKEND,BROKER,FREE

