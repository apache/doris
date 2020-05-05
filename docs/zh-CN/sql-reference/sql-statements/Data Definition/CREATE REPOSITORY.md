---
{
    "title": "CREATE REPOSITORY",
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

# CREATE REPOSITORY
## description
    该语句用于创建仓库。仓库用于属于备份或恢复。仅 root 或 superuser 用户可以创建仓库。
    语法：
        CREATE [READ ONLY] REPOSITORY `repo_name`
        WITH BROKER `broker_name`
        ON LOCATION `repo_location`
        PROPERTIES ("key"="value", ...);
            
    说明：
        1. 仓库的创建，依赖于已存在的 broker
        2. 如果是只读仓库，则只能在仓库上进行恢复。如果不是，则可以进行备份和恢复操作。
        3. 根据 broker 的不同类型，PROPERTIES 有所不同，具体见示例。
        
## example
    1. 创建名为 bos_repo 的仓库，依赖 BOS broker "bos_broker"，数据根目录为：bos://palo_backup
        CREATE REPOSITORY `bos_repo`
        WITH BROKER `bos_broker`
        ON LOCATION "bos://palo_backup"
        PROPERTIES
        (
            "bos_endpoint" = "http://gz.bcebos.com",
            "bos_accesskey" = "069fc2786e664e63a5f111111114ddbs22",
            "bos_secret_accesskey"="70999999999999de274d59eaa980a"
        );
     
    2. 创建和示例 1 相同的仓库，但属性为只读：
        CREATE READ ONLY REPOSITORY `bos_repo`
        WITH BROKER `bos_broker`
        ON LOCATION "bos://palo_backup"
        PROPERTIES
        (
            "bos_endpoint" = "http://gz.bcebos.com",
            "bos_accesskey" = "069fc2786e664e63a5f111111114ddbs22",
            "bos_secret_accesskey"="70999999999999de274d59eaa980a"
        );

    3. 创建名为 hdfs_repo 的仓库，依赖 Baidu hdfs broker "hdfs_broker"，数据根目录为：hdfs://hadoop-name-node:54310/path/to/repo/
        CREATE REPOSITORY `hdfs_repo`
        WITH BROKER `hdfs_broker`
        ON LOCATION "hdfs://hadoop-name-node:54310/path/to/repo/"
        PROPERTIES
        (
            "username" = "user",
            "password" = "password"
        );
        
## keyword
    CREATE REPOSITORY

