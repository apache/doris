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

Package instruction:

1. config : Data source configuration class, and Application configuration
2. controller：rest interface class
3. datasource ： Data source configuration, mainly dynamic switching of multiple data sources
4. domain：Entity java bean
5. mapper：mybatis data access interface definition
6. service：Business service interface
7. util：Tools
8. DorisApplication ： SpringBoot startup class

Configuration instruction:

1. application.yml  ：SpringBoot startup class
2. application-druid.yml ：Database connection configuration parameters
3. mybatis ： Related parameters of mybatis configuration, and mapper.xml configuration file for mybatis data access

## Interface call

Start Springboot, enter restful url in the browser address bar

### Query list

visit：[localhost:8080/rest/demo/skulist](http://localhost:8080/rest/demo/skulist)

![](/images/20210730145555.png)

### Query by ID



![](/images/20210730145906.png)
