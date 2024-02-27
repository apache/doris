---
{
    "title": "Integration with Apache Ranger",
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

# Integration with Apache Ranger

Apache Ranger is a security framework used to monitor, enable services, and manage all-round data security access on the Hadoop platform.

In version 2.1.0, Doris supports unified permission management by integrating Apache Ranger.

> Note: 
> 
> - This feature is currently experimental, and the resource objects and permissions configurable in Ranger may change in subsequent versions.
> 
> - Apache Ranger version needs to be above 2.1.0.

## Installation

### Install Doris Ranger plug-in

1. Download the following files

	- [ranger-doris-plugin-3.0.0-SNAPSHOT.jar](https://selectdb-doris-1308700295.cos.ap-beijing.myqcloud.com/ranger/ranger-doris-plugin-3.0.0-SNAPSHOT.jar)
	- [mysql-connector-java-8.0.25.jar](https://selectdb-doris-1308700295.cos.ap-beijing.myqcloud.com/release/jdbc_driver/mysql-connector-java-8.0.25.jar)

2. Place the downloaded file in the plugins directory of the Ranger service, such as:

	```
	/usr/local/service/ranger/ews/webapp/WEB-INF/classes/ranger-plugins/doris/ranger-doris-plugin-3.0.0-SNAPSHOT.jar
	/usr/local/service/ranger/ews/webapp/WEB-INF/classes/ranger-plugins/doris/mysql-connector-java-8.0.25.jar
	```
	
3. Restart the Ranger service.

4. Download [ranger-servicedef-doris.json](https://selectdb-doris-1308700295.cos.ap-beijing.myqcloud.com/ranger/ranger-servicedef-doris.json)

5. Execute the following command to upload the definition file to the Ranger service:

	```
	curl -u user:password -X POST \
		-H "Accept: application/json" \
		-H "Content-Type: application/json" \
		http://172.21.0.32:6080/service/plugins/definitions \
		-d@ranger-servicedef-doris.json
	```
	
	The username and password are the username and password used to log in to Ranger WebUI.

	The service address port can be viewed in the `ranger.service.http.port` configuration item of the `ranger-admin-site.xml` configuration file.

	If the execution is successful, the service definition in Json format will be returned, such as:
	
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

	If you want to recreate it, you can use the following command to delete the service definition and then upload it again:
	
	```
	curl -v -u user:password -X DELETE \
	http://172.21.0.32:6080/service/plugins/definitions/207
	```
	
	Where `207` is the id returned when created. Before deletion, you need to delete the created Doris service in the Ranger WebUI.
	
	You can also use the following command to list the currently added service definitions in order to obtain the id:
	
	```
	curl -v -u user:password -X GET \
	http://172.21.0.32:6080/service/plugins/definitions/
	```

### Configure the Doris Ranger plug-in

After the installation is complete, open the Ranger WebUI and you can see the Apache Doris plug-in in the Service Manger interface:

![](/images/ranger/ranger1.png)

Click the `+` button next to the plugin to add a Doris service:

![](/images/ranger/ranger2.png)

The meaning of some parameters of Config Properties is as follows:

- `Username`/`Password`: the username and password of the Doris cluster. It is recommended to use the Admin user here.
- `jdbc.driver_class`: Connect to the JDBC driver used by Doris. `com.mysql.cj.jdbc.Driver`
- `jdbc.url`: JDBC url connection string of Doris cluster. `jdbc:mysql://172.21.0.101:9030?useSSL=false`
- Additional parameters:
	- `resource.lookup.timeout.value.in.ms`: timeout for obtaining meta-information. It is recommended to fill in `10000`, which is 10 seconds.

You can click `Test Connection` to check whether the connection can be made.

Then click `Add` to add the service.

Afterwards, you can see the created service in the Apache Doris plug-in on the Service Manger page. Click on the service to start configuring Ranger.

### Configure Doris cluster

1. Create a `ranger-doris-security.xml` file in the conf directory of all FEs with the following content:

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

	You need to change `ranger.plugin.doris.policy.cache.dir` and `ranger.plugin.doris.policy.rest.url` to actual values.
	
2. Create a `log4j.properties` file in the conf directory of all FEs with the following content:

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
	
	You need to change `log4j.appender.D.File` to the actual value, which is used to store the log of the Ranger plug-in.

3. Add configuration in fe.conf of all FEs:

	`access_controller_type=ranger-doris`

4. Restart all FE nodes.

## Resources and permissions

1. Doris resources currently supported in Ranger include:

	- `Catalog`
	- `Database`
	- `Table`
	- `Column`
	- `Resource`
	- `Workload Group`

2. Doris permissions currently supported in Ranger include:

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

## Best Practices

### Example 1

1. Create `user1` in Doris.
2. In Doris, first create a Catalog: `hive` using the `admin` user.
3. Create `user1` in Ranger.
4. Add a Policy in Ranger: `show_hive_catalog`

	![](/images/ranger/ranger3.png)

5. Use `user1` to log in to Doris and execute `show catalogs`. Only the `hive` catalog can be seen.
6. Add a Policy in Ranger: `select_hive_catalog`

	![](/images/ranger/ranger4.png)

7. Log in to Doris using `user1`. This user can view or query all tables under the `hive` catalog and all databases starting with `tpch`.

## FAQ

1. The column permissions, row permissions and Data Mask in Ranger are not supported yet.
