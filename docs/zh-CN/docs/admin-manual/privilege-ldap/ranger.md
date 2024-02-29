---
{
    "title": "集成 Apache Ranger",
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

# 集成 Apache Ranger

Apache Ranger是一个用来在Hadoop平台上进行监控，启用服务，以及全方位数据安全访问管理的安全框架。

在 2.1.0 版本中，Doris 支持通过集成 Apache Ranger，进行统一的权限管理。

> 注意：
> 
> - 目前该功能是实验性功能，在 Ranger 中可配置的资源对象和权限可能会在之后的版本中有所变化。
> 
> - Apache Ranger 版本需在 2.1.0 以上。

## 安装步骤

### 安装 Doris Ranger 插件

1. 下载以下文件

	- [ranger-doris-plugin-3.0.0-SNAPSHOT.jar](https://selectdb-doris-1308700295.cos.ap-beijing.myqcloud.com/ranger/ranger-doris-plugin-3.0.0-SNAPSHOT.jar)
	- [mysql-connector-java-8.0.25.jar](https://selectdb-doris-1308700295.cos.ap-beijing.myqcloud.com/release/jdbc_driver/mysql-connector-java-8.0.25.jar)

2. 将下载好的文件放到 Ranger 服务的 plugins 目录下，如：

	```
	/usr/local/service/ranger/ews/webapp/WEB-INF/classes/ranger-plugins/doris/ranger-doris-plugin-3.0.0-SNAPSHOT.jar
	/usr/local/service/ranger/ews/webapp/WEB-INF/classes/ranger-plugins/doris/mysql-connector-java-8.0.25.jar
	```
	
3. 重启 Ranger 服务。

4. 下载 [ranger-servicedef-doris.json](https://selectdb-doris-1308700295.cos.ap-beijing.myqcloud.com/ranger/ranger-servicedef-doris.json)

5. 执行以下命令上传定义文件到 Ranger 服务:

	```
	curl -u user:password -X POST \
		-H "Accept: application/json" \
		-H "Content-Type: application/json" \
		http://172.21.0.32:6080/service/plugins/definitions \
		-d@ranger-servicedef-doris.json
	```
	
	其中用户名密码是登录 Ranger WebUI 所使用的用户名密码。

	服务地址端口可以再 `ranger-admin-site.xml` 配置文件的 `ranger.service.http.port` 配置项查看。

	如执行成功，会返回 Json 格式的服务定义，如：
	
	```
	{
	  "id": 207,
	  "guid": "d3ff9e41-f9dd-4217-bb5f-3fa9996454b6",
	  "isEnabled": true,
	  "createdBy": "Admin",
	  "updatedBy": "Admin",
	  "createTime": 1705817398112,
	  "updateTime": 1705817398112,
	  "version": 1,
	  "name": "doris",
	  "displayName": "Apache Doris",
	  "implClass": "org.apache.ranger.services.doris.RangerServiceDoris",
	  "label": "Doris",
	  "description": "Apache Doris",
	  "options": {
	    "enableDenyAndExceptionsInPolicies": "true"
	  },
	  ...
	}
	```

	如想重新创建，则可以使用以下命令删除服务定义后，再重新上传：
	
	```
	curl -v -u user:password -X DELETE \
	http://172.21.0.32:6080/service/plugins/definitions/207
	```
	
	其中 `207` 是创建时返回的 id。删除前，需在 Ranger WebUI 界面删除已创建的 Doris 服务。
	
	也可以通过以下命令列举当前已添加的服务定义，以便获取 id：
	
	```
	curl -v -u user:password -X GET \
	http://172.21.0.32:6080/service/plugins/definitions/
	```

### 配置 Doris Ranger 插件

安装完毕后，打开 Ranger WebUI，可以再 Service Manger 界面中看到 Apache Doris 插件：

![](/images/ranger/ranger1.png)

点击插件旁边的 `+` 号添加一个  Doris 服务：

![](/images/ranger/ranger2.png)

Config Properties 部分参数含义如下:

- `Username`/`Pasword`：Doris 集群的用户名密码，这里建议使用 Admin 用户。
- `jdbc.driver_class`：连接 Doris 使用的 JDBC 驱动。`com.mysql.cj.jdbc.Driver`
- `jdbc.url`：Doris 集群的 JDBC url 连接串。`jdbc:mysql://172.21.0.101:9030?useSSL=false`
- 额外参数：
	- `resource.lookup.timeout.value.in.ms`：获取元信息的超时时间，建议填写 `10000`，即 10 秒。

可以点击 `Test Connection` 检查是否可以联通。

之后点击 `Add` 添加服务。

之后，可以在 Service Manger 界面的 Apache Doris 插件中看到创建的服务，点击服务，即可开始配置 Ranger。

### 配置 Doris 集群

1. 在所有 FE 的 conf 目录创建 `ranger-doris-security.xml` 文件，内容如下:

	```
	<?xml version="1.0" encoding="UTF-8"?>
	<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
	<configuration>
	    <property>
	        <name>ranger.plugin.doris.policy.cache.dir</name>
	        <value>/path/to/ranger/cache/</value>
	    </property>
	    <property>
	        <name>ranger.plugin.doris.policy.pollIntervalMs</name>
	        <value>30000</value>
	    </property>
	    <property>
	        <name>ranger.plugin.doris.policy.rest.client.connection.timeoutMs</name>
	        <value>60000</value>
	    </property>
	    <property>
	        <name>ranger.plugin.doris.policy.rest.client.read.timeoutMs</name>
	        <value>60000</value>
	    </property>
	    <property>
	        <name>ranger.plugin.doris.policy.rest.url</name>
	        <value>http://172.21.0.32:6080</value>
	    </property>
	    <property>
	        <name>ranger.plugin.doris.policy.source.impl</name>
	        <value>org.apache.ranger.admin.client.RangerAdminRESTClient</value>
	    </property>
	    <property>
	        <name>ranger.plugin.doris.service.name</name>
	        <value>doris</value>
	    </property>
	</configuration>
	```

	其中需要将 `ranger.plugin.doris.policy.cache.dir` 和 `ranger.plugin.doris.policy.rest.url` 改为实际值。
	
2. 在所有 FE 的 conf 目录创建 `log4j.properties` 文件，内容如下:

	```
	log4j.rootLogger = debug,stdout,D

	log4j.appender.stdout = org.apache.log4j.ConsoleAppender
	log4j.appender.stdout.Target = System.out
	log4j.appender.stdout.layout = org.apache.log4j.PatternLayout
	log4j.appender.stdout.layout.ConversionPattern = [%-5p] %d{yyyy-MM-dd HH:mm:ss,SSS} method:%l%n%m%n
	
	log4j.appender.D = org.apache.log4j.DailyRollingFileAppender
	log4j.appender.D.File = /path/to/fe/log/ranger.log
	log4j.appender.D.Append = true
	log4j.appender.D.Threshold = INFO
	log4j.appender.D.layout = org.apache.log4j.PatternLayout
	log4j.appender.D.layout.ConversionPattern = %-d{yyyy-MM-dd HH:mm:ss}  [ %t:%r ] - [ %p ]  %m%n
	```
	
	其中 `log4j.appender.D.File` 改为实际值，用于存放 Ranger 插件的日志。

3. 在所有 FE 的 fe.conf 中添加配置：

	`access_controller_type=ranger-doris`

4. 重启所有 FE 节点即可。

## 资源和权限

1. 目前 Ranger 中支持的 Doris 资源包括：

	- `Catalog`
	- `Database`
	- `Table`
	- `Column`
	- `Resource`
	- `Workload Group`

2. 目前 Ranger 中支持的 Doris 权限包括：

	- `SHOW`
	- `SHOW_VIEW`
	- `SHOW_RESOURCES`
	- `SHOW_WORKLOAD_GROUP`
	- `LOAD`
	- `ALTER`
	- `CREATE`
	- `ALTER_CREATE`
	- `ALTER_CREATE_DROP`
	- `DROP`
	- `SELECT`

## 最佳实践

### 示例 1

1. 在 Doris 中创建 `user1`。
2. 在 Doris 中，先使用 `admin` 用户创建一个 Catalog：`hive`。
3. 在 Ranger 中创建 `user1`。
4. 在 Ranger 中添加一个 Policy：`show_hive_catalog`

	![](/images/ranger/ranger3.png)

5. 使用 `user1` 登录 Doris，执行 `show catalogs`，只能看到 `hive` catalog。
6. 在 Ranger 中添加一个 Policy：`select_hive_catalog`

	![](/images/ranger/ranger4.png)

7. 使用 `user1` 登录 Doris。该用户可以查看或查询 `hive` catalog 下，所有以 `tpch` 开头的 database 下的所有表。

## 常见问题

1. 暂不支持 Ranger 中的列权限、行权限以及 Data Mask 功能。
