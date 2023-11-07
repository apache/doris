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

suite("test_druid_jdbc_catalog", "p0,external,druid,external_docker,external_docker_druid") {

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/avatica-1.23.0.jar"


    if (enabled != null && enabled.equalsIgnoreCase("true")) {
            String catalog_name_coordinator = "druid_jdbc_catalog";
            String internal_db_name = "regression_test_jdbc_catalog_p0";
            String catalog_name = "doris_jdbc_catalog_druid";
            String ex_db_name = "druid";
            String druid_coordinator_port = context.config.otherConfigs.get("coordinator_druid_port");
            String druid_broker_port = context.config.otherConfigs.get("broker_druid_port");
            String ex_tb0 = "nyc_druid";
            String ex_tb1 = "xtrip_druid";
            String ex_tb2 = "wikipiead_druid";
            String ex_tb3 = "nested_data_example_copy";
            String internalDorisTable1 = "test_insert_json";
            String internalDorisTable2 = "test_insert_xtrip";

            sql """create database if not exists ${internal_db_name}; """

            sql """drop catalog if exists ${catalog_name} """

            sql """create catalog if not exists ${catalog_name} properties(
                "type"="jdbc",
                "jdbc_url" = "jdbc:avatica:remote:url=http://${externalEnvIp}:${druid_coordinator_port}/druid/v2/sql/avatica/",
                "driver_url" = "${driver_url}",
                "driver_class" = "org.apache.calcite.avatica.remote.Driver"
            );"""

            sql """use ${internal_db_name}"""
            sql  """ drop table if exists ${internal_db_name}.${internalDorisTable1} """
            sql  """
                  create table ${internalDorisTable1}
                  (__time datetime(6),
                  product string,
                  department string,
                  shipTo json,
                  details json)
                  UNIQUE KEY(`__time`)
                  DISTRIBUTED BY HASH(`__time`) BUCKETS 1
                  PROPERTIES (
                  "replication_allocation" = "tag.location.default: 1"
                  );
            """

            sql """use ${internal_db_name}"""
            sql  """ drop table if exists ${internal_db_name}.${internalDorisTable2} """
            sql """
                create table ${internalDorisTable2}(
                  __time datetime(6),
                  trip_id bigint,
                 vendor_id bigint,
                 store_and_fwd_flag varchar(20),
                 rate_code_id varchar(20),
                 pickup_longitude double,
                 pickup_latitude double,
                 dropoff_longitude double,
                 dropoff_latitude double,
                 passenger_count bigint,
                 trip_distance float,
                 fare_amount float,
                 extra double,
                 mta_tax double,
                 tip_amount float,
                 tolls_amount bigint,
                 ehail_fee varchar(20),
                 improvement_surcharge varchar(20),
                 total_amount float,
                 payment_type bigint,
                 trip_type varchar(200),
                 pickup varchar(200),
                 dropoff varchar(200),
                 cab_type varchar(20),
                 precipitation bigint,
                 snow_depth bigint,
                 snowfall bigint,
                 max_temperature bigint,
                 min_temperature bigint,
                 average_wind_speed float,
                 pickup_nyct2010_gid bigint,
                 pickup_ctlabel bigint,
                 pickup_borocode bigint,
                 pickup_boroname varchar(20),
                 pickup_ct2010 bigint,
                 pickup_boroct2010 bigint,
                 pickup_cdeligibil  varchar(20),
                 pickup_ntacode varchar(50),
                 pickup_ntaname varchar(50),
                 pickup_puma bigint,
                 dropoff_nyct2010_gid bigint,
                 dropoff_ctlabel bigint,
                 dropoff_borocode bigint,
                 dropoff_boroname varchar(50),
                 dropoff_ct2010 bigint,
                 dropoff_boroct2010 bigint,
                 dropoff_cdeligibil varchar(50),
                 dropoff_ntacode varchar(50),
                 dropoff_ntaname varchar(50),
                 dropoff_puma bigint
                )UNIQUE KEY(`__time`)
                DISTRIBUTED BY HASH(`__time`) BUCKETS 1
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
                );
            """

            qt_sql """select current_catalog()"""
            sql """switch ${catalog_name}"""
            qt_sql """select current_catalog()"""
            sql """ use ${ex_db_name}"""
            sql  """ select count(*) from ${ex_tb0}; """

            // order by all types  bigint,float,double,varchar,
            order_qt_ex_bigint1_tb0  """ select * from ${ex_tb0} order by OriginAirportSeqID"""
            order_qt_ex_bigint2_tb0  """ select * from ${ex_tb0} order by DestAirportID"""
            order_qt_ex_float1_tb1  """ select * from ${ex_tb1} order by trip_distance"""
            order_qt_ex_float2_tb1  """ select * from ${ex_tb1} order by total_amount"""
            order_qt_ex_double_tb1  """ select * from ${ex_tb1} order by pickup_longitude"""
            order_qt_ex_double_tb1  """ select * from ${ex_tb1} order by pickup_longitude"""
            order_qt_ex_varchar1_tb2  """ select * from ${ex_tb2} order by cityName"""
            order_qt_ex_varchar2_tb2  """ select * from ${ex_tb2} order by countryName"""
            order_qt_ex_varchar1_tb3  """ select * from ${ex_tb3} order by product,department"""

            // sum float double bigint
            qt_ex_tb0 """ select sum(Distance) as dis from ${ex_tb0}"""
            qt_ex_tb1 """ select sum(total_amount) as amount from ${ex_tb1} """
            qt_ex_tb2 """ select sum(added) as amount from ${ex_tb2} """

            //test insert all types
            sql  """ insert into internal.${internal_db_name}.${internalDorisTable1} select * from ${ex_tb3}; """
            order_qt_test_insert1 """SELECT product,department,shipTo,details FROM internal.${internal_db_name}.${internalDorisTable1} order by product,department """
            sql  """ insert into internal.${internal_db_name}.${internalDorisTable2} select * from ${ex_tb1}; """
            order_qt_test_insert2 """ select * from  internal.${internal_db_name}.${internalDorisTable2} order by trip_id,vendor_id"""


            //// test only_specified_database and exclude_database_list argument
            sql """drop catalog if exists ${catalog_name} """

            sql """create catalog if not exists ${catalog_name} properties(
                "type"="jdbc",
                "jdbc_url" = "jdbc:avatica:remote:url=http://${externalEnvIp}:${druid_coordinator_port}/druid/v2/sql/avatica/",
                "driver_url" = "${driver_url}",
                "only_specified_database"= "true",
                "include_database_list" = "druid",
                "driver_class" = "org.apache.calcite.avatica.remote.Driver"
            );"""
            sql """switch ${catalog_name}"""
            qt_specified_database_1 """ show databases;"""

            sql """drop catalog if exists ${catalog_name} """
            sql """create catalog if not exists ${catalog_name} properties(
                "type"="jdbc",
                "jdbc_url" = "jdbc:avatica:remote:url=http://${externalEnvIp}:${druid_coordinator_port}/druid/v2/sql/avatica/",
                "driver_url" = "${driver_url}",
                "only_specified_database"= "true",
                "exclude_database_list" = "druid",
                "driver_class" = "org.apache.calcite.avatica.remote.Driver"
            );"""
            sql """switch ${catalog_name}"""
            qt_specified_database_2 """ show databases;"""


            sql """drop catalog if exists ${catalog_name} """
            sql """create catalog if not exists ${catalog_name} properties(
                "type"="jdbc",
                "jdbc_url" = "jdbc:avatica:remote:url=http://${externalEnvIp}:${druid_coordinator_port}/druid/v2/sql/avatica/",
                "driver_url" = "${driver_url}",
                "only_specified_database"= "true",
                "include_database_list" = "druid",
                "exclude_database_list" = "druid",
                "driver_class" = "org.apache.calcite.avatica.remote.Driver"
            );"""
            sql """switch ${catalog_name}"""
            qt_specified_database_3 """ show databases;"""

            //test druid broker broker port
            sql """drop catalog if exists ${catalog_name} """
            sql """create catalog if not exists ${catalog_name} properties(
                "type"="jdbc",
                "jdbc_url" = "jdbc:avatica:remote:url=http://${externalEnvIp}:${druid_broker_port}/druid/v2/sql/avatica/",
                "driver_url" = "${driver_url}",
                "driver_class" = "org.apache.calcite.avatica.remote.Driver"
            );"""
            sql """switch ${catalog_name}"""
            qt_specified_database_4 """ show databases;"""

            //clean
            qt_sql """select current_catalog()"""
            sql "switch internal"
            qt_sql """select current_catalog()"""
            sql """ drop catalog if exists ${catalog_name} """
            sql "use ${internal_db_name}"
            sql """ drop table if exists ${internalDorisTable1} """
            sql """ drop table if exists ${internalDorisTable2} """
    }
}

