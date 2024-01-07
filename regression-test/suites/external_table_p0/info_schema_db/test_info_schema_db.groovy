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

suite("test_info_schema_db", "p0,external,hive,external_docker,external_docker_hive") {

    String catalog_name = "hive_test_other";
    String innerdb = "innerdb";
    String innertbl = "innertbl";
    sql """drop database if exists ${innerdb}""";
    sql """create database if not exists ${innerdb}"""
    sql """create table ${innerdb}.${innertbl} (
                id int not null,
                name varchar(20) not null
        )
        distributed by hash(id) buckets 4
        properties (
                "replication_num"="1"
        );
        """

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return;
    }

    String hms_port = context.config.otherConfigs.get("hms_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    sql """drop catalog if exists ${catalog_name}"""
    sql """create catalog if not exists ${catalog_name} properties (
        "type"="hms",
        'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
    );"""

    // 1. test show columns
    // test internal catalog
    sql "switch internal"
    qt_sql11 """show columns from ${innerdb}.${innertbl}"""
    qt_sql12 """show full columns from ${innerdb}.${innertbl}"""
    qt_sql13 """show full columns from ${innerdb}.${innertbl} where field = 'id'"""
    qt_sql14 """show columns from ${innerdb}.${innertbl} where field = 'id'"""
    qt_sql15 """show columns from ${innerdb}.${innertbl} like '%i%'"""

    // test external catalog
    sql "switch ${catalog_name}"
    qt_sql21 """show columns from `default`.student;"""
    qt_sql22 """show full columns from `default`.student;"""
    qt_sql23 """show full columns from `default`.student where field like '%i%'"""
    qt_sql24 """show columns from `default`.student where field = 'id'"""
    qt_sql25 """show columns from `default`.student like '%i%'"""

    // 2. test show tables
    // test internal catalog
    sql "use internal.${innerdb}"
    qt_sql31 """show tables"""
    qt_sql32 """show tables like 'inner%'"""
    qt_sql33 """show tables from ${innerdb} where table_name = '${innertbl}'"""

    // test external catalog
    sql "use ${catalog_name}.`default`"
    qt_sql41 """show tables like '%account%'"""
    qt_sql42 """show tables where table_name = 'account_fund';"""

    // test cross catalog
    qt_sql51 """show tables from internal.${innerdb}"""
    qt_sql52 """show tables from internal.${innerdb} like 'inner%'"""
    qt_sql52 """show tables from internal.${innerdb} where table_name = '${innertbl}'"""

    // 3. test show databases
    // test internal catalog
    sql "switch internal"
    qt_sql61 """show databases like '%${innerdb}%'"""
    qt_sql62 """show databases where schema_name='${innerdb}'"""

    // test external catalog
    sql "switch ${catalog_name}"
    qt_sql71 """show databases like 'tpch%'"""
    qt_sql72 """show databases where schema_name='hive.tpch1_orc'"""

    // test cross catalog
    qt_sql81 """show databases from internal where schema_name='${innerdb}'"""
    qt_sql82 """show databases from internal like '${innerdb}'"""

    // 4. test show index for external catalog
    qt_sql91 """show index from ${catalog_name}.tpch1_parquet.lineitem"""

    // 5. test show table status
    sql "use ${catalog_name}.tpch1_parquet"
    qt_sql101 """show table status"""
    qt_sql102 """show table status like '%line%'"""
    qt_sql102 """show table status where name='lineitem'"""
    qt_sql102 """show table status from ${catalog_name}.tpch1_parquet where name='lineitem'"""
    qt_sql102 """show table status from internal.${innerdb} where name='${innertbl}'"""

    // 6. test info db
    sql "switch internal"
    qt_sql111 """select * from information_schema.tables where table_catalog!='internal'"""
    qt_sql112 """select * from ${catalog_name}.information_schema.tables where table_catalog!='${catalog_name}'"""
    qt_sql113 """select * from information_schema.columns where table_catalog!='internal'"""
    qt_sql114 """select * from ${catalog_name}.information_schema.columns where table_catalog!='${catalog_name}'"""
    qt_sql115 """select table_catalog, table_schema, table_name from information_schema.tables where table_schema='${innerdb}'"""
    qt_sql116 """select table_catalog, table_schema, table_name from ${catalog_name}.information_schema.columns where table_schema='${catalog_name}.tpch1_parquet'"""
}
