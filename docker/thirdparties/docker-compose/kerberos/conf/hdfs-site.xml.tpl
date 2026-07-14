<?xml version="1.0"?>
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
<configuration>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:///data/hdfs/name</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///data/hdfs/data</value>
    </property>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.permissions.enabled</name>
        <value>false</value>
    </property>
    <property>
        <name>dfs.namenode.datanode.registration.ip-hostname-check</name>
        <value>false</value>
    </property>
    <property>
        <name>dfs.block.access.token.enable</name>
        <value>true</value>
    </property>
    <property>
        <name>dfs.namenode.kerberos.principal</name>
        <value>hdfs/${HOST}@${REALM}</value>
    </property>
    <property>
        <name>dfs.namenode.keytab.file</name>
        <value>/data/keytabs/hdfs.keytab</value>
    </property>
    <property>
        <name>dfs.datanode.kerberos.principal</name>
        <value>hdfs/${HOST}@${REALM}</value>
    </property>
    <property>
        <name>dfs.datanode.keytab.file</name>
        <value>/data/keytabs/hdfs.keytab</value>
    </property>
    <property>
        <name>dfs.namenode.kerberos.internal.spnego.principal</name>
        <value>HTTP/${HOST}@${REALM}</value>
    </property>
    <property>
        <name>dfs.web.authentication.kerberos.principal</name>
        <value>HTTP/${HOST}@${REALM}</value>
    </property>
    <property>
        <name>dfs.web.authentication.kerberos.keytab</name>
        <value>/data/keytabs/spnego.keytab</value>
    </property>
    <property>
        <name>dfs.data.transfer.protection</name>
        <value>authentication</value>
    </property>
    <property>
        <name>dfs.http.policy</name>
        <value>HTTP_ONLY</value>
    </property>
    <property>
        <name>ignore.secure.ports.for.testing</name>
        <value>true</value>
    </property>
    <property>
        <name>dfs.datanode.address</name>
        <value>${HOST}:${DFS_DN_PORT}</value>
    </property>
    <property>
        <name>dfs.datanode.http.address</name>
        <value>${HOST}:${DFS_DN_HTTP_PORT}</value>
    </property>
    <property>
        <name>dfs.datanode.ipc.address</name>
        <value>${HOST}:${DFS_DN_IPC_PORT}</value>
    </property>
    <property>
        <name>dfs.namenode.http-address</name>
        <value>${HOST}:${DFS_NN_HTTP_PORT}</value>
    </property>
</configuration>
