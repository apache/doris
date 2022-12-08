---
{
    "title": "Installation and deployment",
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


# Installation and deployment

This document mainly introduces the hardware and software environment needed to deploy Doris, the proposed deployment mode, cluster expansion and scaling, and common problems in the process of cluster building and running.
Before reading this document, compile Doris according to the compiled document.

## Software and hardware requirements

### Overview

Doris, as an open source MPP architecture OLAP database, can run on most mainstream commercial servers. In order to make full use of the concurrency advantages of MPP architecture and the high availability features of Doris, we recommend that the deployment of Doris follow the following requirements:

#### Linux Operating System Version Requirements

| Linux System | Version|
|---|---|
| Centos | 7.1 and above |
| Ubuntu | 16.04 and above |

#### Software requirements

| Soft | Version | 
|---|---|
| Java | 1.8 and above |
| GCC  | 4.8.2 and above |

#### OS Installation Requirements

##### Set the maximum number of open file handles in the system

````
vi /etc/security/limits.conf
* soft nofile 65536
* hard nofile 65536
````

##### Clock synchronization

The metadata of Doris requires the time precision to be less than 5000ms, so all machines in the cluster need to synchronize the clocks to avoid service exceptions caused by inconsistencies in metadata caused by clock problems.

##### Close the swap partition (swap)

The Linux swap partition will cause serious performance problems for Doris, you need to disable the swap partition before installation

##### Linux file system

Here we recommend using the ext4 file system. When installing the operating system, please select the ext4 file system.

#### Development Test Environment

| Module | CPU | Memory | Disk | Network | Instance Number|
|---|---|---|---|---|---|
| Frontend | 8 core + | 8GB + | SSD or SATA, 10GB + * | Gigabit Network Card | 1|
| Backend | 8 core + | 16GB + | SSD or SATA, 50GB + * | Gigabit Network Card | 1-3*|

#### Production environment

| Module | CPU | Memory | Disk | Network | Number of Instances (Minimum Requirements)|
|---|---|---|---|---|---|
| Frontend | 16 core + | 64GB + | SSD or RAID card, 100GB + * | 10,000 Mbp network card | 1-5*|
| Backend | 16 core + | 64GB + | SSD or SATA, 100G + * | 10-100 Mbp network card | 10-100 * |

> Note 1:
> 
> 1. The disk space of FE is mainly used to store metadata, including logs and images. Usually it ranges from several hundred MB to several GB.
> 2. BE's disk space is mainly used to store user data. The total disk space is calculated according to the user's total data * 3 (3 copies). Then an additional 40% of the space is reserved for background compaction and some intermediate data storage.
> 3. Multiple BE instances can be deployed on a single machine, but **can only deploy one FE**. If you need three copies of data, you need at least one BE instance per machine (instead of three BE instances per machine). **Clocks of multiple FE servers must be consistent (allowing a maximum of 5 seconds clock deviation)**
> 4. The test environment can also be tested with only one BE. In the actual production environment, the number of BE instances directly determines the overall query latency.
> 5. All deployment nodes close Swap.

> Note 2: Number of FE nodes
> 
> 1. FE roles are divided into Follower and Observer. (Leader is an elected role in the Follower group, hereinafter referred to as Follower, for the specific meaning)
> 2. FE node data is at least 1 (1 Follower). When one Follower and one Observer are deployed, high read availability can be achieved. When three Followers are deployed, read-write high availability (HA) can be achieved.
> 3. The number of Followers **must be** odd, and the number of Observers is arbitrary.
> 4. According to past experience, when cluster availability requirements are high (e.g. providing online services), three Followers and one to three Observers can be deployed. For offline business, it is recommended to deploy 1 Follower and 1-3 Observers.

* **Usually we recommend about 10 to 100 machines to give full play to Doris's performance (3 of them deploy FE (HA) and the rest deploy BE)**
* **Of course, Doris performance is positively correlated with the number and configuration of nodes. With a minimum of four machines (one FE, three BEs, one BE mixed with one Observer FE to provide metadata backup) and a lower configuration, Doris can still run smoothly.**
* **If FE and BE are mixed, we should pay attention to resource competition and ensure that metadata catalogue and data catalogue belong to different disks.**

#### Broker deployment

Broker is a process for accessing external data sources, such as hdfs. Usually, a broker instance is deployed on each machine.

#### Network Requirements

Doris instances communicate directly over the network. The following table shows all required ports

| Instance Name | Port Name | Default Port | Communication Direction | Description|
| ---|---|---|---|---|
| BE | be_port | 9060 | FE --> BE | BE for receiving requests from FE|
| BE | webserver\_port | 8040 | BE <--> BE | BE|
| BE | heartbeat\_service_port | 9050 | FE --> BE | the heart beat service port (thrift) on BE, used to receive heartbeat from FE|
| BE | brpc\_port | 8060 | FE <--> BE, BE <--> BE | BE for communication between BEs|
| FE | http_port | 8030 | FE <--> FE, user <--> FE | HTTP server port on FE |
| FE | rpc_port | 9020 | BE --> FE, FE <--> FE | thrift server port on FE, the configuration of each fe needs to be consistent|
| FE | query_port | 9030 | user <--> FE | FE|
| FE | edit\_log_port | 9010 | FE <--> FE | FE|
| Broker | broker ipc_port | 8000 | FE --> Broker, BE --> Broker | Broker for receiving requests|

> Note:
> 
> 1. When deploying multiple FE instances, make sure that the http port configuration of FE is the same.
> 2. Make sure that each port has access in its proper direction before deployment.

#### IP binding

Because of the existence of multiple network cards, or the existence of virtual network cards caused by the installation of docker and other environments, the same host may have multiple different ips. Currently Doris does not automatically identify available IP. So when you encounter multiple IP on the deployment host, you must force the correct IP to be specified through the priority\_networks configuration item.

Priority\_networks is a configuration that both FE and BE have, and the configuration items need to be written in fe.conf and be.conf. This configuration item is used to tell the process which IP should be bound when FE or BE starts. Examples are as follows:

`priority_networks=10.1.3.0/24`

This is a representation of [CIDR](https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing). FE or BE will find the matching IP based on this configuration item as their own local IP.

**Note**: When priority networks is configured and FE or BE is started, only the correct IP binding of FE or BE is ensured. In ADD BACKEND or ADD FRONTEND statements, you also need to specify IP matching priority networks configuration, otherwise the cluster cannot be established. Give an example:

BE is configured as `priority_networks = 10.1.3.0/24'.`.

When you want to ADD BACKEND use: `ALTER SYSTEM ADD BACKEND "192.168.0.1:9050";`

Then FE and BE will not be able to communicate properly.

At this point, DROP must remove the BE that added errors and re-use the correct IP to perform ADD BACKEND.

FE is the same.

BROKER does not currently have, nor does it need, priority\_networks. Broker's services are bound to 0.0.0 by default. Simply execute the correct accessible BROKER IP when ADD BROKER is used.

#### Table Name Case Sensitivity Setting

By default, doris is case-sensitive. If there is a need for case-insensitive table names, you need to set it before cluster initialization. The table name case sensitivity cannot be changed after cluster initialization is completed.

See the section on `lower_case_table_names` variables in [Variables](../../advanced/variables) for details.

## Cluster deployment

### Manual deployment

#### Deploy FE

* Copy the FE deployment file to the specified node

	Copy the Fe folder under output generated by source code compilation to the node specified deployment path of FE and enter this directory.

* Configure FE

	1. The configuration file is conf/fe.conf. Note: `meta_dir` indicates the Metadata storage location. The default value is `${DORIS_HOME}/doris-meta`. The directory needs to be **created manually**.
	2. JAVA_OPTS in fe.conf defaults to a maximum heap memory of 4GB for java, and it is recommended that the production environment be adjusted to more than 8G.

* Start FE

	`bin/start_fe.sh --daemon`

	The FE process starts and enters the background execution. Logs are stored in the log/ directory by default. If startup fails, you can view error messages by looking at log/fe.log or log/fe.out.

* For deployment of multiple FEs, see the section "FE scaling and downsizing"

#### Deploy BE

* Copy BE deployment files to all nodes to deploy BE

	Copy the be folder under output generated by source code compilation to the specified deployment path of the BE node.

* Modify all BE configurations

  Modify be/conf/be.conf. Mainly configure `storage_root_path`: data storage directory. The default is be/storage, this directory needs to be **created manually** by. In multi directories case, using `;` separation (do not add `;` after the last directory).   
  If the user does not use a mix of SSD and HDD disks, they do not need to configure the configuration methods in Example 1 and Example 2 below, but only need to specify the storage directory; they also do not need to modify the default storage media configuration of FE.  

    eg.1: 

    Note: For SSD disks, '.SSD 'is followed by the directory, and for HDD disks,'.HDD 'is followed by the directory

    `storage_root_path=/home/disk1/doris.HDD;/home/disk2/doris.SSD;/home/disk2/doris`

    **instructions**

    * 1./home/disk1/doris.HDD : The storage medium is HDD;
    * 2./home/disk2/doris.SSD : The storage medium is HDDSSD;
    * 3./home/disk2/doris  :  The storage medium is HDD(default)

    eg.2: 

    Note: you do not need to add the suffix to either HDD or SSD disk directories. You only need to set the medium parameter

    `storage_root_path=/home/disk1/doris,medium:hdd;/home/disk2/doris,medium:ssd`
      
    **instructions**
      

    * 1./home/disk1/doris,medium:hdd  :  The storage medium is HDD;
    * 2./home/disk2/doris,medium:ssd  :  The storage medium is SSD;

* BE webserver_port configuration

	If the Be componet is installed in hadoop cluster , need to change configuration `webserver_port=8040`  to avoid port used.
* 	Install Java UDF functions 
   <version since="1.2.0">Install Java UDF functions</version>
   Because Java UDF functions are supported from version 1.2, you need to download the JAR package of Java UDF functions from the official website and put them in the lib directory of BE, otherwise it may fail to start.
	
* Add all BE nodes to FE

	BE nodes need to be added in FE before they can join the cluster. You can use mysql-client([Download MySQL 5.7](https://dev.mysql.com/downloads/mysql/5.7.html)) to connect to FE:

	`./mysql-client -h fe_host -P query_port -uroot`

	The fe_host is the node IP where FE is located; the query_port in fe/conf/fe.conf; the root account is used by default and no password is used to login.

	After login, execute the following commands to add each BE:

	`ALTER SYSTEM ADD BACKEND "be_host:heartbeat_service_port";`

	The be_host is the node IP where BE is located; the heartbeat_service_port in be/conf/be.conf.

* Start BE

	`bin/start_be.sh --daemon`

	The BE process will start and go into the background for execution. Logs are stored in be/log/directory by default. If startup fails, you can view error messages by looking at be/log/be.log or be/log/be.out.

* View BE status

	Connect to FE using mysql-client and execute `SHOW PROC '/backends'; `View BE operation. If everything is normal, the `isAlive`column should be `true`.

#### (Optional) FS_Broker deployment

Broker is deployed as a plug-in, independent of Doris. If you need to import data from a third-party storage system, you need to deploy the corresponding Broker. By default, it provides fs_broker to read HDFS ,Object storage (supporting S3 protocol). Fs_broker is stateless and it is recommended that each FE and BE node deploy a Broker.

* Copy the corresponding Broker directory in the output directory of the source fs_broker to all the nodes that need to be deployed. It is recommended to maintain the same level as the BE or FE directories.

* Modify the corresponding Broker configuration

	In the corresponding broker/conf/directory configuration file, you can modify the corresponding configuration.

* Start Broker

	`bin/start_broker.sh --daemon`

* Add Broker

	To let Doris FE and BE know which nodes Broker is on, add a list of Broker nodes by SQL command.

	Use mysql-client to connect the FE started, and execute the following commands:

	`ALTER SYSTEM ADD BROKER broker_name "broker_host1:broker_ipc_port1","broker_host2:broker_ipc_port2",...;`

	The broker\_host is Broker's node ip; the broker_ipc_port is in the Broker configuration file.

* View Broker status

	Connect any booted FE using mysql-client and execute the following command to view Broker status: `SHOW PROC '/brokers';`

**Note: In production environments, daemons should be used to start all instances to ensure that processes are automatically pulled up after they exit, such as [Supervisor](http://supervisord.org/). For daemon startup, in 0.9.0 and previous versions, you need to modify the start_xx.sh scripts to remove the last & symbol**. Starting with version 0.10.0, call `sh start_xx.sh` directly to start. Also refer to [here](https://www.cnblogs.com/lenmom/p/9973401.html)

## Common Questions

### Process correlation

1. How to determine the success of FE process startup

	After the FE process starts, metadata is loaded first. According to the different roles of FE, you can see ```transfer from UNKNOWN to MASTER/FOLLOWER/OBSERVER```in the log. Eventually, you will see the ``thrift server started`` log and connect to FE through MySQL client, which indicates that FE started successfully.

	You can also check whether the startup was successful by connecting as follows:
	
	`http://fe_host:fe_http_port/api/bootstrap`

	If returned:
	
	`{"status":"OK","msg":"Success"}`

	The startup is successful, there may be problems in other cases.

	> Note: If you can't see the information of boot failure in fe. log, you may see it in fe. out.

2. How to determine the success of BE process startup

	After the BE process starts, if there is data before, there may be several minutes of data index loading time.

	If BE is started for the first time or the BE has not joined any cluster, the BE log will periodically scroll the words `waiting to receive first heartbeat from frontend`. BE has not received Master's address through FE's heartbeat and is waiting passively. This error log will disappear after ADD BACKEND in FE sends the heartbeat. If the word `````master client', get client from cache failed. host:, port: 0, code: 7````` master client appears again after receiving heartbeat, it indicates that FE has successfully connected BE, but BE cannot actively connect FE. It may be necessary to check the connectivity of rpc_port from BE to FE.

	If BE has been added to the cluster, the heartbeat log from FE should be scrolled every five seconds: ```get heartbeat, host:xx. xx.xx.xx, port:9020, cluster id:xxxxxxx```, indicating that the heartbeat is normal.

	Secondly, the word `finish report task success. return code: 0` should be scrolled every 10 seconds in the log to indicate that BE's communication to FE is normal.

	At the same time, if there is a data query, you should see the rolling logs, and have `execute time is xxx` logs, indicating that BE started successfully, and the query is normal.

	You can also check whether the startup was successful by connecting as follows:
	
	`http://be_host:be_http_port/api/health`

	If returned:
	
	`{"status": "OK","msg": "To Be Added"}`

	If the startup is successful, there may be problems in other cases.

	> Note: If you can't see the information of boot failure in be.INFO, you may see it in be.out.

3. How to determine the normal connectivity of FE and BE after building the system

	Firstly, confirm that FE and BE processes have been started separately and normally, and confirm that all nodes have been added through `ADD BACKEND` or `ADD FOLLOWER/OBSERVER` statements.

	If the heartbeat is normal, BE logs will show ``get heartbeat, host:xx.xx.xx.xx, port:9020, cluster id:xxxxx`` If the heartbeat fails, the words ```backend [10001] get Exception: org.apache.thrift.transport.TTransportException``` will appear in FE's log, or other thrift communication abnormal log, indicating that the heartbeat fails from FE to 10001 BE. Here you need to check the connectivity of FE to BE host's heart-beating port.

	If BE's communication to FE is normal, the BE log will display the words `finish report task success. return code: 0`. Otherwise, the words `master client`, get client from cache failed` will appear. In this case, the connectivity of BE to the rpc_port of FE needs to be checked.

4. Doris Node Authentication Mechanism

	In addition to Master FE, the other role nodes (Follower FE, Observer FE, Backend) need to register to the cluster through the `ALTER SYSTEM ADD` statement before joining the cluster.

	When Master FE is first started, a cluster_id is generated in the doris-meta/image/VERSION file.

	When FE first joins the cluster, it first retrieves the file from Master FE. Each subsequent reconnection between FEs (FE reboot) checks whether its cluster ID is the same as that of other existing FEs. If different, the FE will exit automatically.

	When BE first receives the heartbeat of Master FE, it gets the cluster ID from the heartbeat and records it in the `cluster_id` file of the data directory. Each heartbeat after that compares to the cluster ID sent by FE. If cluster IDs are not equal, BE will refuse to respond to FE's heartbeat.

	The heartbeat also contains Master FE's ip. When FE cuts the master, the new Master FE will carry its own IP to send the heartbeat to BE, BE will update its own saved Master FE ip.

	> **priority\_network**
	>
	> priority network is that both FE and BE have a configuration. Its main purpose is to assist FE or BE to identify their own IP addresses in the case of multi-network cards. Priority network is represented by CIDR: [RFC 4632](https://tools.ietf.org/html/rfc4632)
	>
	> When the connectivity of FE and BE is confirmed to be normal, if the table Timeout still occurs, and the FE log has an error message with the words `backend does not find. host:xxxx.xxx.XXXX`. This means that there is a problem with the IP address that Doris automatically identifies and that priority\_network parameters need to be set manually.
	>
	> The main reason for this problem is that when the user adds BE through the `ADD BACKEND` statement, FE recognizes whether the statement specifies hostname or IP. If it is hostname, FE automatically converts it to an IP address and stores it in metadata. When BE reports on the completion of the task, it carries its own IP address. If FE finds that BE reports inconsistent IP addresses and metadata, it will make the above error.
	>
	> Solutions to this error: 1) Set **priority\_network** parameters in FE and BE respectively. Usually FE and BE are in a network segment, so this parameter can be set to the same. 2) Fill in the `ADD BACKEND` statement directly with the correct IP address of BE instead of hostname to avoid FE getting the wrong IP address.

5. File descriptor number of BE process

   The number of file descriptor of BE process is controlled by the two parameters min_file_descriptor_number/max_file_descriptor_number.

   If it is not in the [min_file_descriptor_number, max_file_descriptor_number] interval, error will occurs when starting BE process.

   Please using ulimit command to set file descriptor under this circumstance.

   The default value of min_file_descriptor_number is 65536.

   The default value of max_file_descriptor_number is 131072.

   For Example: ulimit -n 65536; this command set file descriptor to 65536.

   After starting BE process, you can use **cat /proc/$pid/limits** to see the actual limit of process.

   if use `supervisord`, try to modify `minfds` in supervisord.conf

   ```shell
   vim /etc/supervisord.conf
   
   minfds=65535                 ; (min. avail startup file descriptors;default 1024)
   ```
