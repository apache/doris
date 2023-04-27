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

suite("test_catalog_with_resource", "p0") {
    String enabledJdbc = context.config.otherConfigs.get("enableJdbcTest")
    String enabledHive = context.config.otherConfigs.get("enableHiveTest")
    String enabledEs = context.config.otherConfigs.get("enableEsTest")
    if (enabledJdbc != null && enabledJdbc.equalsIgnoreCase("true")
            && enabledHive != null && enabledHive.equalsIgnoreCase("true")
            && enabledEs != null && enabledEs.equalsIgnoreCase("true")) {

        String jdbc_resource_name = "jdbc1_resource"
        String hive_resource_name = "hive1_resource"
        String es_resource_name = "es1_resource"

        String jdbc_catalog_name = "jdbc1_catalog";
        String hive_catalog_name = "hive1_catalog";
        String es_catalog_name = "es1_catalog";

        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
        String hms_port = context.config.otherConfigs.get("hms_port")
        String es_7_port = context.config.otherConfigs.get("es_7_port")

        sql """drop catalog if exists ${jdbc_catalog_name} """
        sql """drop resource if exists ${jdbc_resource_name} """
        sql """drop catalog if exists ${hive_catalog_name} """
        sql """drop resource if exists ${hive_resource_name} """
        sql """drop catalog if exists ${es_catalog_name} """
        sql """drop resource if exists ${es_resource_name} """

        // jdbc
        sql """create resource if not exists ${jdbc_resource_name} properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://127.0.0.1:${mysql_port}/doris_test?useSSL=false",
            "driver_url" = "https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/mysql-connector-java-8.0.25.jar",
            "driver_class" = "com.mysql.cj.jdbc.Driver"
        );"""
        sql """CREATE CATALOG ${jdbc_catalog_name} WITH RESOURCE ${jdbc_resource_name}"""

        qt_sql11 """show create catalog ${jdbc_catalog_name}"""
        sql """alter catalog ${jdbc_catalog_name} set properties("user"="root2")"""
        qt_sql12 """show create catalog ${jdbc_catalog_name}"""
        sql """alter resource ${jdbc_resource_name} properties("user"="root3")"""
        qt_sql13 """show create catalog ${jdbc_catalog_name}"""
        sql """alter resource ${jdbc_resource_name} properties("driver_class"="com.mysql.jdbc.Driver")"""
        qt_sql14 """show create catalog ${jdbc_catalog_name}"""

        // hive
        sql """drop catalog if exists ${hive_catalog_name} """
        sql """drop resource if exists ${hive_resource_name} """
        sql """create resource if not exists ${hive_resource_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://127.0.0.1:${hms_port}'
        );"""
        sql """create catalog if not exists ${hive_catalog_name} with resource ${hive_resource_name};"""
        qt_sql21 """show create catalog ${hive_catalog_name}"""
        sql """alter catalog ${hive_catalog_name} set properties("new_config"="value1")"""
        qt_sql22 """show create catalog ${hive_catalog_name}"""
        sql """alter resource ${hive_resource_name} properties('hive.metastore.uris' = 'thrift://127.0.0.2:${hms_port}')"""
        qt_sql23 """show create catalog ${hive_catalog_name}"""

        // es
        sql """drop catalog if exists ${es_catalog_name} """
        sql """drop resource if exists ${es_resource_name} """

        sql """create resource if not exists ${es_resource_name} properties(
            "type"="es",
            "hosts"="http://127.0.0.1:$es_7_port",
            "nodes_discovery"="false",
            "enable_keyword_sniff"="true"
        );
        """
        sql """create catalog ${es_catalog_name} with resource ${es_resource_name} properties
            ("nodes_discovery"="true");
        """ 
        qt_sql31 """show create catalog ${es_catalog_name}"""
        sql """alter catalog ${es_catalog_name} set properties("enable_keyword_sniff"="false")"""
        qt_sql22 """show create catalog ${es_catalog_name}"""
        sql """alter resource ${es_resource_name} properties('hosts' = 'http://127.0.0.2:${es_7_port}')"""
        qt_sql23 """show create catalog ${es_catalog_name}"""
    }
}
