// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_hive_kerberos", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String hms_port = context.config.otherConfigs.get("hms_port")
        String hdfs_port = context.config.otherConfigs.get("hdfs_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String externalEnvHost = context.config.otherConfigs.get("externalEnvHost")

        String catalog_name = "hive_test_kerberos"

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
            'hadoop.security.authentication' = 'kerberos',
            'hadoop.kerberos.keytab' = '/etc/krb5.keytab',   
            'hadoop.kerberos.principal' = 'hdfs/${externalEnvHost}@NODE.DC1.CONSUL',
            'java.security.krb5.conf' = '/etc/krb5.conf'
        );"""

        sql """switch ${catalog_name}"""
        sql """use `default`"""
        qt_q1 """ select * from types """
    }
}
