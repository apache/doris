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

suite("test_hive_partition_values_tvf", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return;
    }
    for (String hivePrefix : ["hive3"]) {
        String extHiveHmsHost = context.config.otherConfigs.get("externalEnvIp")
        String extHiveHmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "${hivePrefix}_test_external_catalog_hive_partition"

        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """

        // 1. test qualifier
        qt_sql01 """ select * from ${catalog_name}.multi_catalog.orc_partitioned_columns\$partitions order by t_int, t_float, t_string"""
        sql """ switch ${catalog_name} """
        qt_sql02 """ select * from multi_catalog.orc_partitioned_columns\$partitions order by t_int, t_float, t_string"""
        sql """ use multi_catalog"""
        qt_sql03 """ select * from orc_partitioned_columns\$partitions order by t_int, t_float, t_string"""

        // 2. test select order
        qt_sql11 """ select * except(t_string) from orc_partitioned_columns\$partitions order by t_int, t_float, t_string"""
        qt_sql12 """ select t_float, t_int from orc_partitioned_columns\$partitions order by t_int, t_float, t_string"""
        qt_sql13 """ select t_string, t_int from orc_partitioned_columns\$partitions order by t_int, t_float, t_string"""

        // 3. test agg
        qt_sql21 """ select max(t_string), max(t_int), max(t_float) from orc_partitioned_columns\$partitions"""
        qt_sql22 """ select max(t_string) from orc_partitioned_columns\$partitions group by t_int, t_float order by t_int, t_float"""
        qt_sql22 """ select count(*) from orc_partitioned_columns\$partitions;"""
        qt_sql22 """ select count(1) from orc_partitioned_columns\$partitions;"""

        // 4. test alias
        qt_sql31 """ select pv.t_float, pv.t_int from orc_partitioned_columns\$partitions as pv group by t_int, t_float order by t_int, t_float"""
        
        // 5. test CTE
        qt_sql41 """ with v1 as (select t_string, t_int from orc_partitioned_columns\$partitions order by t_int, t_float, t_string) select max(t_int) from v1; """
        qt_sql42 """ with v1 as (select t_string, t_int from orc_partitioned_columns\$partitions order by t_int, t_float, t_string) select c1 from (select max(t_string) as c1 from v1) x; """
 
        // 6. test subquery
        qt_sql51 """select c1 from (select max(t_string) as c1 from (select * from multi_catalog.orc_partitioned_columns\$partitions)x)y;"""

        // 7. test where
        qt_sql61 """select * from orc_partitioned_columns\$partitions where t_int != "__HIVE_DEFAULT_PARTITION__" order by t_int, t_float, t_string; """
        
        // 8. test view
        sql """drop database if exists internal.partition_values_db"""
        sql """create database if not exists internal.partition_values_db"""
        sql """create view internal.partition_values_db.v1 as select * from ${catalog_name}.multi_catalog.orc_partitioned_columns\$partitions"""
        qt_sql71 """select * from internal.partition_values_db.v1"""
        qt_sql72 """select t_string, t_int from internal.partition_values_db.v1 where t_int != "__HIVE_DEFAULT_PARTITION__""""
        qt_sql73 """with v1 as (select t_string, t_int from internal.partition_values_db.v1 order by t_int, t_float, t_string) select c1 from (select max(t_string) as c1 from v1) x;"""
        
        // 9. test join
        qt_sql81 """select * from orc_partitioned_columns\$partitions p1 join orc_partitioned_columns\$partitions p2 on p1.t_int = p2.t_int order by p1.t_int, p1.t_float"""

        // 10. test desc
        qt_sql91 """desc orc_partitioned_columns\$partitions"""
        qt_sql92 """desc function partition_values("catalog" = "${catalog_name}", "database" = "multi_catalog", "table" = "orc_partitioned_columns");"""
        qt_sql91 """desc orc_partitioned_one_column\$partitions"""
        qt_sql92 """desc function partition_values("catalog" = "${catalog_name}", "database" = "multi_catalog", "table" = "orc_partitioned_one_column");"""

        // 11. test non partition table
        test {
            sql """select * from hive_text_complex_type\$partitions"""
            exception "is not a partitioned table"
        }
        test {
            sql """desc hive_text_complex_type\$partitions"""
            exception "is not a partitioned table"
        }

        // 12. test inner table
        sql """create table internal.partition_values_db.pv_inner1 (k1 int) distributed by hash (k1) buckets 1 properties("replication_num" = "1")"""
        qt_sql101 """desc internal.partition_values_db.pv_inner1"""
        qt_sql102 """select * from internal.partition_values_db.pv_inner1"""
        test {
            sql """desc internal.partition_values_db.pv_inner1\$partitions"""
            exception """Unknown table 'pv_inner1\$partitions'"""
        }

        test {
            sql """select * from internal.partition_values_db.pv_inner1\$partitions"""
            exception """Table [pv_inner1\$partitions] does not exist in database [partition_values_db]"""
        }
    }
}

