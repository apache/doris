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

# Instructions for use

This example is based on SpringBoot and Mybatis to integrate, access doris data through JDBC, and then provide data API access services through rest.

For specific table structure and sample data, please refer to the documentation in the stream-load-demo project.

## Directory structure description

![](/images/20210730144136.png)

包说明：

1. config : 数据源配置类，及Application配置
2. controller：rest接口类
3. datasource ： 数据源配置，主要是多数据源动态切换
4. domain：实体java bean
5. mapper：mybatis数据访问接口定义
6. service：业务服务接口
7. util：工具类
8. DorisApplication ： SpringBoot启动类

配置文件说明：

1. application.yml  ：Spring启动配置参数
2. application-druid.yml ：数据库练级配置参数
3. mybatis ： mybatis配置的相关参数，及mybatis数据访问的mapper.xml配置文件

## Interface call

Start Springboot, enter restful url in the browser address bar

### Query list

visit：[localhost:8080/rest/demo/skulist](http://localhost:8080/rest/demo/skulist)

![](/images/20210730145555.png)

### Query by ID



![](/images/20210730145906.png)
