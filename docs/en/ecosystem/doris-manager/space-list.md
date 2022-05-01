---
{
    "title": "Space list",
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

# Space list

The super administrator can perform the following operations in the space list:

- Perform new cluster and cluster hosting operations

- Recovery and deletion of unfinished spaces

- Completed space deletion operation

The space administrator can mainly perform the following operations in the space list:

- View authorized space information

## Completed space

The super administrator can operate the completed space through the button to the right of the space name. Space administrators can click to enter the space to manage clusters or data in the space.

![](/images/doris-manager/spacelist-1.png)

## Unfinished space

Doris Manger provides a draft save function of the space creation process to record the incomplete space creation process. Super administrators can view the list of unfinished spaces by switching tabs, and perform recovery or deletion operations.

![](/images/doris-manager/spacelist-2.png)

# New space

There are two ways to create a new space: new cluster and cluster hosting.

## New cluster

### 1 Registration space

Space information includes space name, space introduction, and selection of space administrators.

Space name and administrator are required/optional fields.

![](/images/doris-manager/spacelist-3.png)

### 2 Add host

![](/images/doris-manager/spacelist-4.png)

#### Configure SSH login-free

Doris Manager needs to distribute the Agent installation package during installation, so it is necessary to configure SSH login-free on the server (agent01) where Doris is to be installed.

```shell
#1. To log in to the server, you need to use the manager and agent accounts to be consistent
su - xxx
pwd
#2. Generate a key pair on the machine where doris manager is deployed
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa

#3. Copy the public key to the machine agent01
scp ~/.ssh/id_rsa.pub root@agent01:~

#4. Log in to agent01 and append the public key to authorized_keys
cat ~/id_rsa.pub >> .ssh/authorized_keys

#5. After doing this, we can log in to agent01 without password on the doris manger machine
ssh agent01@xx.xxx.xx.xx
````
For details, please refer to: https://blog.csdn.net/universe_hao/article/details/52296811

In addition, it should be noted that the permissions of the .ssh directory are 700, and the permissions of the authorized_keys and private keys under it are 600. Otherwise, you will not be able to log in without a password due to permission issues. We can see that the known_hosts file will be generated after logging in. At the same time, when starting doris, you need to use a password-free login account.


When adding nodes in the second step of the Doris Manager installation cluster, use the private key of the doris manager machine, that is, ~/.ssh/id_rsa (note: including the head and tail of the key file)

#### Host list
Enter the host IP to add a new host, or add it in batches.

### 3 Installation options

#### Get the installation package

1. Code package path

   When deploying a cluster through Doris Manager, you need to provide the compiled Doris installation package. You can compile it yourself from the Doris source code, or use the officially provided [binary version](https://doris.apache.org/zh-CN/ downloads/downloads.html).

`Doris Manager will pull the Doris installation package through http. If you need to build your own http service, please refer to the bottom of the document - Self-built http service`.

#### Specify the installation path

1. Doris and Doris Manger Agent will be installed in this directory. Make sure this directory is dedicated to Doirs and related components.
2. Specify the Agent startup port, the default is 8001, if there is a conflict, you can customize it.

### 4 Verify the host

The system will automatically perform verification according to the host status. When the verification is completed, the Agent will start sending back the heartbeat, and you can click to proceed to the next step.

![](/images/doris-manager/spacelist-5.png)

### 5 Planning Nodes

Click the Assign Node button to plan FE/BE/Broker nodes for the host.

![](/images/doris-manager/spacelist-6.png)

### 6 Configuration Parameters

Configure parameters for the nodes planned in the previous step. You can use the default values ​​or turn on the custom configuration switch to customize the configuration.

### 7 Deploy the cluster

The system will automatically perform verification according to the status of the host installation progress. When the verification is completed, it will start the node and return the heartbeat. You can click to proceed to the next step.

![](/images/doris-manager/spacelist-7.png)

### 8 Complete the creation

Complete the above steps to complete the new cluster.

![](/images/doris-manager/spacelist-8.png)

## Cluster hosting

### 1 Registration space

Space information includes space name, space introduction, and selection of space administrators.

Space name and administrator are required/optional fields.

### 2 Connect to the cluster

Cluster information includes cluster address, HTTP port, JDBC port, cluster username, and cluster password. Users can fill in according to their own cluster information.

Click the Link Test button to test it.

### 3 Hosting Options

![](/images/doris-manager/spacelist-9.png)

#### Configure SSH login-free

Doris Manager needs to distribute the Agent installation package during installation, so it is necessary to configure SSH login-free on the server (agent01) where Doris is to be installed.

```shell
#1. To log in to the server, you need to use the manger and agent accounts to be consistent
su - xxx
pwd
#2. Generate a key pair on the machine where doris manager is deployed
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa

#3. Copy the public key to the machine agent01
scp ~/.ssh/id_rsa.pub root@agent01:~

#4. Log in to agent01 and append the public key to authorized_keys
cat ~/id_rsa.pub >> .ssh/authorized_keys

#5. After doing this, we can log in to agent01 without password on the doris manger machine
ssh agent01@xx.xxx.xx.xx
````

In addition, it should be noted that the permissions of the .ssh directory are 700, and the permissions of the authorized_keys and private keys under it are 600. Otherwise, you will not be able to log in without a password due to permission issues. We can see that the known_hosts file will be generated after logging in. At the same time, when starting doris, you need to use a password-free login account.

When installing a cluster in Doris Manager, just use the private key of the doris manager machine, ie ~/.ssh/id_rsa

For details, please refer to: https://blog.csdn.net/universe_hao/article/details/52296811

#### Specify the installation path

1. Doris and Doris Manger Agent will be installed in this directory. Make sure this directory is dedicated to Doirs and related components.
2. Specify the Agent startup port, the default is 8001, if there is a conflict, you can customize it.

### 4 Verify the host

The system will automatically perform verification according to the host status. When the verification is completed, the Agent will start sending back the heartbeat, and you can click to proceed to the next step.

![](/images/doris-manager/spacelist-10.png)

### 5 Verify the cluster

Verify the cluster quantile instance installation verification, instance dependency verification, and instance startup verification. After the verification is successful, click Next to complete the creation.

![](/images/doris-manager/spacelist-11.png)

### 6 Complete access

Complete the above steps to complete cluster hosting.

## Self-built http service

### 1 yum source installation

1. Installation
yum install -y nginx
2. Start
systemctl start nginx

### 2 Source installation

Reference: https://www.runoob.com/linux/nginx-install-setup.html

### 3 Configuration

1. Put the doris installation package in the nginx root directory
mv PALO-0.15.1-rc03-binary.tar.gz /usr/share/nginx/html

2. Modify ngixn.conf

````
location /download {
   alias /home/work/nginx/nginx/html/;
}
````

Restart ngxin access after modification:
https://host:port/download/PALO-0.15.1-rc03-binary.tar.gz
