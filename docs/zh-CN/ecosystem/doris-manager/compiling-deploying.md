---
{
    "title": "编译与部署",
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

# 编译与部署

## 编译
直接在manager路径下的build.sh脚本
```shell
cd incubator-doris-manager
sh build.sh
```
编译完成后会在manager路径下生成安装包output目录，目录结构如下
```text
├── agent  //agent 目录
│   ├── bin
│   │	├── agent_start.sh
│   │	├── agent_stop.sh
│   │	└── download_doris.sh
│   ├── config
│   │	└── application.properties
│   └── lib
│   	└── dm-agent.jar
└── server  //server 目录
    ├── bin
    │	├── start_manager.sh   //Doris Manager启动脚本
    │	└── stop_manager.sh    //Doris Manager停止脚本
    ├── conf
    │	└── manager.conf       //Doris Manager配置文件
    ├── lib
    │	└── doris-manager.jar  //Doris Manager的运行包doris-manager.jar
    └── web-resource         
```

## 运行
### 1 配置
修改配置文件`server/conf/manager.conf`，重点关注的配置项内容如下：
```$xslt
服务的启动http端口
STUDIO_PORT=8080

后端数据存放的数据库的类型，包括mysql/h2/postgresql.默认是支持mysql
MB_DB_TYPE=mysql

数据库连接信息
如果是配置的h2类型数据库，就不需要配置这些信息，会把数据以本地文件存放在本地
h2数据文件存放路径，默认直接存放在当前路径
H2_FILE_PATH=

如果是mysql/postgresql就需要配置如下连接信息
数据库地址
MB_DB_HOST=

数据库端口
MB_DB_PORT=3306

数据库访问端口
MB_DB_USER=

数据库访问密码
MB_DB_PASS=

数据库的database名称
MB_DB_DBNAME=

服务运行的路径，默认直接存放在当前运行路径的log文件夹中
LOG_PATH=

web容器的等待队列长度，默认100。队列也做缓冲池用，但也不能无限长，不但消耗内存，而且出队入队也消耗CPU
WEB_ACCEPT_COUNT=100

Web容器的最大工作线程数，默认200。（一般是CPU核数*200）
WEB_MAX_THREADS=200

Web容器的最小工作空闲线程数，默认10。（适当增大一些，以便应对突然增长的访问量）
WEB_MIN_SPARE_THREADS=10

Web容器的最大连接数，默认10000。（适当增大一些，以便应对突然增长的访问量）
WEB_MAX_CONNECTIONS=10000

访问数据库连接池最大连接数量，默认为10
DB_MAX_POOL_SIZE=20

访问数据库连接池最小空闲连接数，默认为10
DB_MIN_IDLE=10
```

### 2 启动
配置修改完成后，启动doris manger
```$xslt
cd server
sh bin/start_manager.sh
```
查看logs中的日志即可判断程序是否启动成功

### 3 使用
Doris Manager预设了一个超级管理员用户，信息如下：
```$xslt
用户名: Admin
密码: Admin@123
```
为确保使用安全，登陆后请修改密码！
