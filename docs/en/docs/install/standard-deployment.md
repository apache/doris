---
{
    "title": "Standard Deployment",
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


# Installation and Deployment

This topic is about the hardware and software environment needed to deploy Doris, the recommended deployment mode, cluster scaling, and common problems occur in creating and running clusters.

Before continue reading, you might want to compile Doris following the instructions in the [Compile](https://doris.apache.org/docs/dev/install/source-install/compilation-general/) topic.

## Software and Hardware Requirements

### Overview

Doris, as an open source OLAP database with an MPP architecture, can run on most mainstream commercial servers. For you to take full advantage of the high concurrency and high availability of Doris, we recommend that your computer meet the following requirements:

#### Linux Operating System Version Requirements

| Linux System | Version|
|---|---|
| Centos | 7.1 and above |
| Ubuntu | 16.04 and above |

#### Software Requirements

| Soft | Version | 
|---|---|
| Java | 1.8  |
| GCC  | 4.8.2 and above |

#### OS Installation Requirements

**Set the maximum number of open file descriptors in the system**

````
vi /etc/security/limits.conf
* soft nofile 65536
* hard nofile 65536
````

##### Clock synchronization

The metadata in Doris requires a time precision of less than 5000ms, so all machines in all clusters need to synchronize their clocks to avoid service exceptions caused by inconsistencies in metadata caused by clock problems.

##### Close the swap partition

The Linux swap partition can cause serious performance problems for Doris, so you need to disable the swap partition before installation.

##### Linux file system

Both ext4 and xfs file systems are supported.

#### Development Test Environment

| Module | CPU | Memory | Disk | Network | Number of Instances |
|---|---|---|---|---|---|
| Frontend | 8 core + | 8GB + | SSD or SATA, 10GB + * | Gigabit Network Card | 1|
| Backend | 8 core + | 16GB + | SSD or SATA, 50GB + * | Gigabit Network Card | 1-3*|

#### Production Environment

| Module | CPU | Memory | Disk | Network | Number of Instances (Minimum Requirements) |
|---|---|---|---|---|--------------------------------------------|
| Frontend | 16 core + | 64GB + | SSD or RAID card, 100GB + * | 10,000 Mbp network card | 1-3*                                       |
| Backend | 16 core + | 64GB + | SSD or SATA, 100G + * | 10-100 Mbp network card | 3 *                                        |

> Note 1:
> 
> 1. The disk space of FE is mainly used to store metadata, including logs and images. It usually ranges from several hundred MB to several GB.
> 2. The disk space of BE is mainly used to store user data. The total disk space taken up is 3 times the total user data (3 copies). Then an additional 40% of the space is reserved for background compaction and intermediate data storage.
> 3. On one single machine, you can deploy multiple BE instances but **only one FE instance**. If you need 3 copies of the data, you need to deploy 3 BE instances on 3 machines (1 instance per machine) instead of 3 BE instances on one machine). **Clocks of the FE servers must be consistent (allowing a maximum clock skew of 5 seconds).**
> 4. The test environment can also be tested with only 1 BE instance. In the actual production environment, the number of BE instances directly determines the overall query latency.
> 5. Disable swap for all deployment nodes.

> Note 2: Number of FE nodes
> 
> 1. FE nodes are divided into Followers and Observers based on their roles. (Leader is an elected role in the Follower group, hereinafter referred to as Follower, too.)
> 2. The number of FE nodes should be at least 1 (1 Follower). If you deploy 1 Follower and 1 Observer, you can achieve high read availability; if you deploy 3 Followers, you can achieve high read-write  availability (HA).
> 3. Although multiple BEs can be deployed on one machine, **only one instance** is recommended to be deployed, and **only one FE** can be deployed at the same time. If 3 copies of data are required, at least 3 machines are required to deploy a BE instance (instead of 1 machine deploying 3 BE instances). **The clocks of the servers where multiple FEs are located must be consistent (up to 5 seconds of clock deviation is allowed)**.
> 4. According to past experience, for business that requires high cluster availability (e.g. online service providers), we recommend that you deploy 3 Followers and 1-3 Observers; for offline business, we recommend that you deploy 1 Follower and 1-3 Observers.

* **Usually we recommend 10 to 100 machines to give full play to Doris' performance (deploy FE on 3 of them (HA) and BE on the rest).**
* **The performance of Doris is positively correlated with the number of nodes and their configuration. With a minimum of four machines (one FE, three BEs; hybrid deployment of one BE and one Observer FE to provide metadata backup) and relatively low configuration, Doris can still run smoothly.**
* **In hybrid deployment of FE and BE, you might need to be watchful for resource competition and ensure that the metadata catalogue and data catalogue belong to different disks.**

#### Broker Deployment

Broker is a process for accessing external data sources, such as hdfs. Usually, deploying one broker instance on each machine should be enough.

#### Network Requirements

Doris instances communicate directly over the network. The following table shows all required ports.

| Instance Name | Port Name | Default Port | Communication Direction | Description|
| ---|---|---|---|---|
| BE | be_port | 9060 | FE --> BE | Thrift server port on BE for receiving requests from FE |
| BE | webserver\_port | 8040 | BE <--> BE | HTTP server port on BE |
| BE | heartbeat\_service_port | 9050 | FE --> BE | Heart beat service port (thrift) on BE, used to receive heartbeat from FE |
| BE | brpc\_port | 8060 | FE <--> BE, BE <--> BE | BRPC port on BE for communication between BEs |
| FE | http_port | 8030 | FE <--> FE, user <--> FE | HTTP server port on FE |
| FE | rpc_port | 9020 | BE --> FE, FE <--> FE | Thrift server port on FE; The configurations of each FE should be consistent. |
| FE | query_port | 9030 | user <--> FE | MySQL server port on FE |
| FE | arrow_flight_sql_port | 9040 | user <--> FE | Arrow Flight SQL server port on FE |
| FE | edit\_log_port | 9010 | FE <--> FE | Port on FE for BDBJE communication |
| Broker | broker ipc_port | 8000 | FE --> Broker, BE --> Broker | Thrift server port on Broker for receiving requests |

> Note:
> 
> 1. When deploying multiple FE instances, make sure that the http_port configuration of each FE is consistent.
> 2. Make sure that each port has access in its proper direction before deployment.

#### IP Binding

Because of the existence of multiple network cards, or the existence of virtual network cards caused by the installation of docker and other environments, the same host may have multiple different IPs. Currently Doris does not automatically identify available IPs. So when you encounter multiple IPs on the deployment host, you must specify the correct IP via the `priority_networks` configuration item.

`priority_networks` is a configuration item that both FE and BE have. It needs to be written in fe.conf and be.conf. It is used to tell the process which IP should be bound when FE or BE starts. Examples are as follows:

`priority_networks=10.1.3.0/24`

This is a representation of [CIDR](https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing). FE or BE will find the matching IP based on this configuration item as their own local IP.

**Note**: Configuring `priority_networks` and starting FE or BE only ensure the correct IP binding of FE or BE.  You also need to specify the same IP in ADD BACKEND or ADD FRONTEND statements, otherwise the cluster cannot be created. For example:

BE is configured as `priority_networks = 10.1.3.0/24'.`.

If you use the following IP in the ADD BACKEND statement: `ALTER SYSTEM ADD BACKEND "192.168.0.1:9050";`

Then FE and BE will not be able to communicate properly.

At this point, you must DROP the wrong BE configuration and use the correct IP to perform ADD BACKEND.

The same works for FE.

Broker currently does not have the `priority_networks` configuration item, nor does it need. Broker's services are bound to 0.0.0.0 by default. You can simply execute the correct accessible BROKER IP when using ADD BROKER.

#### Table Name Case Sensitivity

By default, table names in Doris are case-sensitive. If you need to change that, you may do it before cluster initialization. The table name case sensitivity cannot be changed after cluster initialization is completed.

See the `lower_case_table_names` section in [Variables](../advanced/variables.md) for details.

## Cluster Deployment

### Manual Deployment

#### Deploy FE

* Copy the FE deployment file into the specified node

	Find the Fe folder under the output generated by source code compilation, copy it into the specified deployment path of FE nodes and put it the corresponding directory.

* Configure FE

  1. The configuration file is conf/fe.conf. Note: `meta_dir` indicates the metadata storage location. The default value is `${DORIS_HOME}/doris-meta`. The directory needs to be **created manually**.

     **Note: For production environments, it is better not to put the directory under the Doris installation directory but in a separate disk (SSD would be the best); for test and development environments, you may use the default configuration.**

  2. The default maximum Java heap memory of JAVA_OPTS in fe.conf is 8GB.

* Start FE

	`bin/start_fe.sh --daemon`

	The FE process starts and enters the background for execution. Logs are stored in the log/ directory by default. If startup fails, you can view error messages by checking out log/fe.log or log/fe.out.

* For details about deployment of multiple FEs, see the [FE scaling](https://doris.apache.org/docs/dev/admin-manual/cluster-management/elastic-expansion/) section.

#### BE Deployment

* Copy the BE deployment file to all nodes to deploy BE

  Copy the be folder under the output generated by source code compilation to the specified deployment path of the BE node.

  > Note: The `output/be/lib/debug_info/` directory contains debugging information files, which are relatively large, but these files are not needed for actual operation and can not be deployed.

* Modify all BE configurations

  Modify be/conf/be.conf. Mainly configure `storage_root_path`: data storage directory. By default, it is under be/storage. If you need to specify a directory, you need to **pre-create the directory**. Multiple paths are separated by a semicolon `;` in English.
  The hot and cold data storage directories in the node can be distinguished by path, HDD (cold data directory) or SSD (hot data directory). If you don't need the hot and cold mechanism in the BE node, you only need to configure the path without specifying the medium type; and you don't need to modify the default storage medium configuration of FE

  **Notice:**
    1. If the storage type of the storage path is not specified, all are HDD (cold data directory) by default.
    2. The HDD and SSD here have nothing to do with the physical storage medium, but only to distinguish the storage type of the storage path, that is, you can mark a certain directory on the disk of the HDD medium as SSD (hot data directory).

  Example 1 is as follows:

  `storage_root_path=/home/disk1/doris;/home/disk2/doris;/home/disk2/doris`

  Example 2 is as follows:

  **Use the storage_root_path parameter to specify medium**

  `storage_root_path=/home/disk1/doris,medium:HDD;/home/disk2/doris,medium:SSD`

  **illustrate**

    - /home/disk1/doris,medium:HDD: Indicates that the directory stores cold data;
    - /home/disk2/doris,medium:SSD: Indicates that the directory stores hot data;

* BE webserver_port port configuration

  If be is deployed in a hadoop cluster, pay attention to adjusting `webserver_port = 8040` in be.conf to avoid port conflicts

* Configure the JAVA_HOME environment variable

  <version since="1.2.0"></version>
  Since Java UDF functions are supported from version 1.2, BE depends on the Java environment. So to pre-configure the `JAVA_HOME` environment variable, you can also add `export JAVA_HOME=your_java_home_path` to the first line of the `start_be.sh` startup script to add the environment variable.

* Install Java UDF functions

  <version since="1.2.0">Install Java UDF functions</version>
  Because Java UDF functions are supported from version 1.2, you need to download the JAR package of Java UDF functions from the official website and put them in the lib directory of BE, otherwise it may fail to start.

* Add all BE nodes in FE

  BE nodes need to be added in FE before they can join the cluster. You can use mysql-client ([Download MySQL 5.7](https://dev.mysql.com/downloads/mysql/5.7.html)) to connect to FE:

  `./mysql-client -h fe_host -P query_port -uroot`

  Among them, fe_host is the ip of the node where FE is located; query_port is in fe/conf/fe.conf; the root account is used by default, and there is no password to log in.

  Once logged in, execute the following command to add each BE:

  `ALTER SYSTEM ADD BACKEND "be_host:heartbeat-service_port";`

  Where be_host is the node ip where BE is located; heartbeat_service_port is in be/conf/be.conf.

* Start BE

  `bin/start_be.sh --daemon`

  The BE process will start and enter the background execution. Logs are stored in the be/log/ directory by default. If the startup fails, you can view the error message by viewing be/log/be.log or be/log/be.out.

* View BE status

  Use mysql-client to connect to FE, and execute `SHOW PROC '/backends';` to check the running status of BE. If all is well, the `isAlive` column should be `true`.

#### (Optional) FS_Broker Deployment

Broker is deployed as a plug-in, which is independent of Doris. If you need to import data from a third-party storage system, you need to deploy the corresponding Broker. By default, Doris provides fs_broker for HDFS reading and object storage (supporting S3 protocol). fs_broker is stateless and we recommend that you deploy a Broker for each FE and BE node.

* Copy the corresponding Broker directory in the output directory of the source fs_broker to all the nodes that need to be deployed. It is recommended to keep the Broker directory on the same level as the BE or FE directories.

* Modify the corresponding Broker configuration

	You can modify the configuration in the corresponding broker/conf/directory configuration file.

* Start Broker

	`bin/start_broker.sh --daemon`

* Add Broker

	To let Doris FE and BE know which nodes Broker is on, add a list of Broker nodes by SQL command.

	Use mysql-client to connect the FE started, and execute the following commands:

	`ALTER SYSTEM ADD BROKER broker_name "broker_host1:broker_ipc_port1","broker_host2:broker_ipc_port2",...;`

	`broker\_host` is the Broker node ip;  `broker_ipc_port` is in conf/apache_hdfs_broker.conf in the Broker configuration file.

* View Broker status

	Connect any started FE using mysql-client and execute the following command to view Broker status: `SHOW PROC '/brokers';`

#### FE and BE Startup Methods

##### Version >= 2.0.2
1. Start with start_xx.sh: This method logs the output to a file and does not exit the startup script process. It is recommended to use this method when using tools like Supervisor for automatic restarting.
2. Start with start_xx.sh --daemon: FE/BE will run as a background process, and the log output will be written to the specified log file by default. This startup method is suitable for production environments.
3. Start with start_xx.sh --console: This parameter is used to start FE/BE in console mode. When started with the --console parameter, the server will start in the current terminal session, and the log output and console interaction will be printed to that terminal. This startup method is suitable for development and testing scenarios.
##### Version < 2.0.2
1. Start with start_xx.sh --daemon: FE/BE will run as a background process, and the log output will be written to the specified log file by default. This startup method is suitable for production environments.
2. Start with start_xx.sh: This parameter is used to start FE/BE in console mode. When started with the --console parameter, the server will start in the current terminal session, and the log output and console interaction will be printed to that terminal. This startup method is suitable for development and testing scenarios.

**Note: In production environments, daemons should be used to start all instances to ensure that processes are automatically pulled up after they exit, such as [Supervisor](http://supervisord.org/). For daemon startup, in Doris 0.9.0 and previous versions, you need to remove the last `&` symbol in the start_xx.sh scripts**. In Doris 0.10.0 and the subsequent versions, you may just call `sh start_xx.sh` directly to start.

## FAQ

### Process-Related Questions

1. How can we know whether the FE process startup succeeds?

	After the FE process starts, metadata is loaded first. Based on the role of FE, you can see ```transfer from UNKNOWN to MASTER/FOLLOWER/OBSERVER``` in the log. Eventually, you will see the ``thrift server started`` log and can connect to FE through MySQL client, which indicates that FE started successfully.

	You can also check whether the startup was successful by connecting as follows:
	
	`http://fe_host:fe_http_port/api/bootstrap`

	If it returns:
	
	`{"status":"OK","msg":"Success"}`

	The startup is successful; otherwise, there may be problems.

	> Note: If you can't see the information of boot failure in fe. log, you may check in fe. out.

2. How can we know whether the BE process startup succeeds?

	After the BE process starts, if there have been data there before, it might need several minutes for data index loading.

	If BE is started for the first time or the BE has not joined any cluster, the BE log will periodically scroll the words `waiting to receive first heartbeat from frontend`, meaning that BE has not received the Master's address through FE's heartbeat and is waiting passively. Such error log will disappear after sending the heartbeat by ADD BACKEND in FE. If the word `````master client', get client from cache failed. host:, port: 0, code: 7````` appears after receiving the heartbeat, it indicates that FE has successfully connected BE, but BE cannot actively connect FE. You may  need to check the connectivity of rpc_port from BE to FE.

	If BE has been added to the cluster, the heartbeat log from FE will be scrolled every five seconds: ```get heartbeat, host:xx. xx.xx.xx, port:9020, cluster id:xxxxxxx```, indicating that the heartbeat is normal.

	Secondly, if the word `finish report task success. return code: 0`  is scrolled every 10 seconds in the log, that indicates that the BE-to-FE communication is normal.

	Meanwhile, if there is a data query, you will see the rolling logs and the  `execute time is xxx` logs, indicating that BE is started successfully, and the query is normal.

	You can also check whether the startup was successful by connecting as follows:
	
	`http://be_host:webserver_port/api/health`

	If it returns:
	
	`{"status": "OK","msg": "To Be Added"}`

	That means the startup is successful; otherwise, there may be problems.

	> Note: If you can't see the information of boot failure in be.INFO, you may see it in be.out.

3. How can we confirm that the connectivity of FE and BE is normal after building the system?

	Firstly, you need to confirm that FE and BE processes have been started separately and worked normally. Then, you need to confirm that all nodes have been added through `ADD BACKEND` or `ADD FOLLOWER/OBSERVER` statements.

	If the heartbeat is normal, BE logs will show ``get heartbeat, host:xx.xx.xx.xx, port:9020, cluster id:xxxxx``; if the heartbeat fails, you will see ```backend [10001] got Exception: org.apache.thrift.transport.TTransportException``` in FE's log, or other thrift communication abnormal log, indicating that the heartbeat from FE to 10001 BE fails. Here you need to check the connectivity of the FE to BE host heartbeat port.

	If the BE-to-FE communication is normal, the BE log will display the words `finish report task success. return code: 0`. Otherwise, the words `master client, get client from cache failed` will appear. In this case, you need to check the connectivity of BE to the rpc_port of FE.

4. What is the Doris node authentication mechanism?

	In addition to Master FE, the other role nodes (Follower FE, Observer FE, Backend) need to register to the cluster through the `ALTER SYSTEM ADD` statement before joining the cluster.

	When Master FE is started for the first time, a cluster_id is generated in the doris-meta/image/VERSION file.

	When FE joins the cluster for the first time, it first retrieves the file from Master FE. Each subsequent reconnection between FEs (FE reboot) checks whether its cluster ID is the same as that of other existing FEs. If it is not the same, the FE will exit automatically.

	When BE first receives the heartbeat of Master FE, it gets the cluster ID from the heartbeat and records it in the `cluster_id` file of the data directory. Each heartbeat after that compares that to the cluster ID sent by FE. If the cluster IDs are not matched, BE will refuse to respond to FE's heartbeat.

	The heartbeat also contains Master FE's IP. If the Master FE changes, the new Master FE will send the heartbeat to BE together with its own IP, and BE will update the Master FE IP it saved.

	> **priority\_network**
	>
	> priority_network is a configuration item that both FE and BE have. It is used to help FE or BE identify their own IP addresses in the cases of multi-network cards. priority_network uses CIDR notation: [RFC 4632](https://tools.ietf.org/html/rfc4632)
	>
	> If the connectivity of FE and BE is confirmed to be normal, but timeout still occurs in creating tables, and the FE log shows the error message  `backend does not find. host:xxxx.xxx.XXXX`, this means that there is a problem with the IP address automatically identified by Doris and that the priority\_network parameter needs to be set manually.
	>
	> The explanation to this error is as follows. When the user adds BE through the `ADD BACKEND` statement, FE recognizes whether the statement specifies hostname or IP. If it is a hostname, FE automatically converts it to an IP address and stores it in the metadata. When BE reports on the completion of the task, it carries its own IP address. If FE finds that the IP address reported by BE is different from that in the metadata, the above error message will show up.
	>
	> Solutions to this error: 1) Set **priority\_network** in FE and BE separately. Usually FE and BE are in one network segment, so their priority_network can be set to be the same. 2) Put the correct IP address instead of the hostname in the `ADD BACKEND` statement to prevent FE from getting the wrong IP address.

5. What is the number of file descriptors of a BE process?

   The number of file descriptor of a BE process is determined by two parameters: `min_file_descriptor_number`/`max_file_descriptor_number`.

   If it is not in the range [`min_file_descriptor_number`, `max_file_descriptor_number`], error will occurs when starting a BE process. You may use the ulimit command to reset the parameters.

   The default value of `min_file_descriptor_number` is 65536.

   The default value of `max_file_descriptor_number` is 131072.

   For example, the command `ulimit -n 65536;` means to set the number of file descriptors to 65536.

   After starting a BE process, you can use **cat /proc/$pid/limits** to check the actual number of file descriptors of the process.

   If you have used  `supervisord` and encountered a file descriptor error, you can fix it by modifying  `minfds` in supervisord.conf.

   ```shell
   vim /etc/supervisord.conf
   
   minfds=65535                 ; (min. avail startup file descriptors;default 1024)
   ```
