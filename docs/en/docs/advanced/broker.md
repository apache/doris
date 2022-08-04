---
{
    "title": "Broker",
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

# Broker

Broker is an optional process in the Doris cluster. It is mainly used to support Doris to read and write files or directories on remote storage, such as HDFS, BOS, and AFS.

Broker provides services through an RPC service port. It is a stateless JVM process that is responsible for encapsulating some POSIX-like file operations for read and write operations on remote storage, such as open, pred, pwrite, and so on.
In addition, the Broker does not record any other information, so the connection information, file information, permission information, and so on stored remotely need to be passed to the Broker process in the RPC call through parameters in order for the Broker to read and write files correctly .

Broker only acts as a data channel and does not participate in any calculations, so it takes up less memory. Usually one or more Broker processes are deployed in a Doris system. And the same type of Broker will form a group and set a ** Broker name **.

Broker's position in the Doris system architecture is as follows:

```
+----+   +----+
| FE |   | BE |
+-^--+   +--^-+
  |         |
  |         |
+-v---------v-+
|   Broker    |
+------^------+
       |
       |
+------v------+
|HDFS/BOS/AFS |
+-------------+
```

This document mainly introduces the parameters that Broker needs when accessing different remote storages, such as connection information,
authorization information, and so on.

## Supported Storage System

Different types of brokers support different storage systems.

1. Community HDFS

    * Support simple authentication access
    * Support kerberos authentication access
    * Support HDFS HA mode access
2. Object storage
- All object stores that support the S3 protocol

## Function provided by Broker

1. [Broker Load](../data-operate/import/import-way/broker-load-manual.md)
2. [Export](../data-operate/export/export-manual.md)
3. [Backup](../admin-manual/data-admin/backup.md)

## Broker Information

Broker information includes two parts: ** Broker name ** and ** Certification information **. The general syntax is as follows:

```
WITH BROKER "broker_name" 
(
    "username" = "xxx",
    "password" = "yyy",
    "other_prop" = "prop_value",
    ...
);
```

### Broker Name

Usually the user needs to specify an existing Broker Name through the `WITH BROKER" broker_name "` clause in the operation command.
Broker Name is a name that the user specifies when adding a Broker process through the ALTER SYSTEM ADD BROKER command.
A name usually corresponds to one or more broker processes. Doris selects available broker processes based on the name.
You can use the `SHOW BROKER` command to view the Brokers that currently exist in the cluster.

**Note: Broker Name is just a user-defined name and does not represent the type of Broker.**

### Certification Information

Different broker types and different access methods need to provide different authentication information.
Authentication information is usually provided as a Key-Value in the Property Map after `WITH BROKER" broker_name "`.

#### Community HDFS

1. Simple Authentication

    Simple authentication means that Hadoop configures `hadoop.security.authentication` to` simple`.

    Use system users to access HDFS. Or add in the environment variable started by Broker: ```HADOOP_USER_NAME```.
    
    ```
    (
        "username" = "user",
        "password" = ""
    );
    ```
    
    Just leave the password blank.

2. Kerberos Authentication

    The authentication method needs to provide the following information::
    
    * `hadoop.security.authentication`: Specify the authentication method as kerberos.
    * `kerberos_principal`: Specify the principal of kerberos.
    * `kerberos_keytab`: Specify the path to the keytab file for kerberos. The file must be an absolute path to a file on the server where the broker process is located. And can be accessed by the Broker process.
    * `kerberos_keytab_content`: Specify the content of the keytab file in kerberos after base64 encoding. You can choose one of these with `kerberos_keytab` configuration.

    Examples are as follows:
    
    ```
    (
        "hadoop.security.authentication" = "kerberos",
        "kerberos_principal" = "doris@YOUR.COM",
        "kerberos_keytab" = "/home/doris/my.keytab"
    )
    ```
    ```
    (
        "hadoop.security.authentication" = "kerberos",
        "kerberos_principal" = "doris@YOUR.COM",
        "kerberos_keytab_content" = "ASDOWHDLAWIDJHWLDKSALDJSDIWALD"
    )
    ```
    If Kerberos authentication is used, the [krb5.conf](https://web.mit.edu/kerberos/krb5-1.12/doc/admin/conf_files/krb5_conf.html) file is required when deploying the Broker process.
    The krb5.conf file contains Kerberos configuration information, Normally, you should install your krb5.conf file in the directory /etc. You can override the default location by setting the environment variable KRB5_CONFIG.
    An example of the contents of the krb5.conf file is as follows:
    ```
    [libdefaults]
        default_realm = DORIS.HADOOP
        default_tkt_enctypes = des3-hmac-sha1 des-cbc-crc
        default_tgs_enctypes = des3-hmac-sha1 des-cbc-crc
        dns_lookup_kdc = true
        dns_lookup_realm = false
    
    [realms]
        DORIS.HADOOP = {
            kdc = kerberos-doris.hadoop.service:7005
        }
    ```
    
3. HDFS HA Mode

    This configuration is used to access HDFS clusters deployed in HA mode.
    
    * `dfs.nameservices`: Specify the name of the hdfs service, custom, such as "dfs.nameservices" = "my_ha".
    * `dfs.ha.namenodes.xxx`:  Custom namenode names. Multiple names are separated by commas, where xxx is the custom name in `dfs.nameservices`, such as" dfs.ha.namenodes.my_ha "=" my_nn ".
    * `dfs.namenode.rpc-address.xxx.nn`: Specify the rpc address information of namenode, Where nn represents the name of the namenode configured in `dfs.ha.namenodes.xxx`, such as: "dfs.namenode.rpc-address.my_ha.my_nn" = "host:port".
    * `dfs.client.failover.proxy.provider`: Specify the provider for the client to connect to the namenode. The default is: org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider.

    Examples are as follows:
    
    ```
    (
        "dfs.nameservices" = "my_ha",
        "dfs.ha.namenodes.my_ha" = "my_namenode1, my_namenode2",
        "dfs.namenode.rpc-address.my_ha.my_namenode1" = "nn1_host:rpc_port",
        "dfs.namenode.rpc-address.my_ha.my_namenode2" = "nn2_host:rpc_port",
        "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
    )
    ```
    
    The HA mode can be combined with the previous two authentication methods for cluster access. If you access HA HDFS with simple authentication:
    
    ```
    (
        "username"="user",
        "password"="passwd",
        "dfs.nameservices" = "my_ha",
        "dfs.ha.namenodes.my_ha" = "my_namenode1, my_namenode2",
        "dfs.namenode.rpc-address.my_ha.my_namenode1" = "nn1_host:rpc_port",
        "dfs.namenode.rpc-address.my_ha.my_namenode2" = "nn2_host:rpc_port",
        "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
    )
    ```
   The configuration for accessing the HDFS cluster can be written to the hdfs-site.xml file. When users use the Broker process to read data from the HDFS cluster, they only need to fill in the cluster file path and authentication information.
