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

/*
// type_change_{origin, orc, parquet} are defined as:
// type_change_origin dosen't have column altered
create table type_change_origin(
  numeric_boolean boolean,
  numeric_tinyint tinyint,
  numeric_smallint smallint,
  numeric_int int,
  numeric_bigint bigint,
  numeric_float float,
  numeric_double double,

  ts_boolean boolean,
  ts_int int,
  ts_double double,
  ts_decimal decimal(12, 4),
  ts_date date,
  ts_timestamp timestamp,

  fs_boolean string,
  fs_int string,
  fs_float string,
  fs_decimal string,
  fs_date string,
  fs_timestamp string,

  td_boolean boolean,
  td_bigint bigint,
  td_float float,
  td_double double,
  td_decimal decimal(8, 4),
  
  fd_boolean decimal(12, 3),
  fd_int decimal(10, 2),
  fd_float decimal(11, 4),
  fd_double decimal(9, 4),

  date_timestamp date,
  timestamp_date timestamp
)
// change column type
alter table type_change_parquet change column numeric_boolean numeric_boolean int;
alter table type_change_parquet change column numeric_tinyint numeric_tinyint double;
alter table type_change_parquet change column numeric_smallint numeric_smallint bigint;
alter table type_change_parquet change column numeric_bigint numeric_bigint float;
alter table type_change_parquet change column numeric_double numeric_double bigint;

alter table type_change_parquet change column ts_boolean ts_boolean string;
alter table type_change_parquet change column ts_int ts_int string;
alter table type_change_parquet change column ts_double ts_double string;
alter table type_change_parquet change column ts_decimal ts_decimal string;
alter table type_change_parquet change column ts_date ts_date string;
alter table type_change_parquet change column ts_timestamp ts_timestamp string;

alter table type_change_parquet change column fs_boolean fs_boolean boolean;
alter table type_change_parquet change column fs_int fs_int int;
alter table type_change_parquet change column fs_float fs_float float;
alter table type_change_parquet change column fs_decimal fs_decimal decimal(12, 3);
alter table type_change_parquet change column fs_date fs_date date;
alter table type_change_parquet change column fs_timestamp fs_timestamp timestamp;

alter table type_change_parquet change column td_boolean td_boolean decimal(5, 2);
alter table type_change_parquet change column td_bigint td_bigint decimal(11, 4);
alter table type_change_parquet change column td_float td_float decimal(10, 3);
alter table type_change_parquet change column td_double td_double decimal(9, 2);

alter table type_change_parquet change column fd_boolean fd_boolean boolean;
alter table type_change_parquet change column fd_int fd_int int;
alter table type_change_parquet change column fd_float fd_float float;
alter table type_change_parquet change column fd_double fd_double double;

alter table type_change_parquet change column date_timestamp date_timestamp timestamp;
alter table type_change_parquet change column timestamp_date timestamp_date date;
*/
suite("test_hive_schema_change", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        for (String hivePrefix : ["hive2", "hive3"]) {
            setHivePrefix(hivePrefix)
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
            String catalog_name = "test_hive_schema_change"
            sql """drop catalog if exists ${catalog_name};"""
            sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}'
            );
            """
            sql """ switch ${catalog_name} """
            sql """ use `multi_catalog` """
            order_qt_type_change_origin """ select * from type_change_origin """
            order_qt_type_change_orc """ select * from type_change_orc """
            order_qt_type_change_parquet """ select * from type_change_parquet """
            sql """ drop catalog ${catalog_name} """
        }
    }
}
