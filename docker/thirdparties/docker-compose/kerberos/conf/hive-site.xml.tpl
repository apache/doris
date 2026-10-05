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
        <name>javax.jdo.option.ConnectionURL</name>
        <value>jdbc:derby:;databaseName=/data/metastore/metastore_db;create=true</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionDriverName</name>
        <value>org.apache.derby.jdbc.EmbeddedDriver</value>
    </property>
    <property>
        <name>datanucleus.autoCreateSchema</name>
        <value>false</value>
    </property>
    <property>
        <name>hive.metastore.schema.verification</name>
        <value>true</value>
    </property>
    <property>
        <name>hive.metastore.uris</name>
        <value>thrift://${HOST}:${HMS_PORT}</value>
    </property>
    <property>
        <name>hive.metastore.warehouse.dir</name>
        <value>hdfs://${HOST}:${FS_PORT}/user/hive/warehouse</value>
    </property>
    <property>
        <name>hive.metastore.sasl.enabled</name>
        <value>true</value>
    </property>
    <property>
        <name>hive.metastore.kerberos.principal</name>
        <value>hive/${HOST}@${REALM}</value>
    </property>
    <property>
        <name>hive.metastore.kerberos.keytab.file</name>
        <value>/data/keytabs/hive.keytab</value>
    </property>
    <property>
        <name>hive.metastore.execute.setugi</name>
        <value>true</value>
    </property>
    <property>
        <name>metastore.storage.schema.reader.impl</name>
        <value>org.apache.hadoop.hive.metastore.SerDeStorageSchemaReader</value>
    </property>
</configuration>
