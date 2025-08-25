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

suite("test_stream_load_big_file_with_special_delimiter", "p1") {
    sql "show tables"

    def tableName = "test_csv_big_file_with_special_delimiter"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` char(24) NOT NULL,
            `k2` char(12) NOT NULL,
            `k3` bigint(20) NOT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    streamLoad {
        table "${tableName}"

        set 'column_separator', '\\x01\\x01\\x02\\x02'
        set 'line_delimiter', '\\x03\\x03\\x01\\x0a'
        set 'columns', 'k1, k2, k3'
        set 'strict_mode', 'true'

        file 'test_csv_big_file_with_special_delimiter.csv'
    }

    sql "sync"
    qt_sql "select count(*) from ${tableName}"

    tableName = "test_csv_big_file_truncate_delimiter";
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName} (
            `measureid` VARCHAR(500) NOT NULL,
            `measuretag` VARCHAR(500) NOT NULL,
            `timestamp` VARCHAR(500) NOT NULL,
            `ds` VARCHAR(255) NULL,
            `hh` VARCHAR(255) NULL,
            `meter_id` VARCHAR(500) NULL,
            `maintenance_team` VARCHAR(1000) NULL,
            `psr_class_name` VARCHAR(500) NULL,
            `inst_id` VARCHAR(500) NULL,
            `location_type` VARCHAR(500) NULL,
            `name` VARCHAR(500) NULL,
            `depart` VARCHAR(500) NULL,
            `measurepoint_id` VARCHAR(500) NULL,
            `district` VARCHAR(500) NULL,
            `enddevice_psr_class_name` VARCHAR(500) NULL,
            `enddevice_psr_id` VARCHAR(500) NULL,
            `root_id` VARCHAR(500) NULL,
            `rt` VARCHAR(500) NULL,
            `measurevalue` VARCHAR(500) NULL,
            `dataquality` VARCHAR(500) NULL,
            `datatablename` VARCHAR(500) NULL,
            `tag` VARCHAR(500) NULL,
            `equip_src_id` VARCHAR(500) NULL,
            `root_class_name` VARCHAR(500) NULL,
            `ssid` VARCHAR(500) NULL,
            `sysdate_uep` VARCHAR(500) NULL
        ) ENGINE=OLAP
          DUPLICATE KEY(`measureid`, `measuretag`, `timestamp`, `ds`)
          AUTO PARTITION BY LIST (`ds`)(
          )
          DISTRIBUTED BY HASH(`measureid`) BUCKETS 10
          PROPERTIES (
              "replication_allocation" = "tag.location.default: 1"
          );
    """
    streamLoad {
        table "${tableName}"

        set 'column_separator', '@@@'
        set 'columns', 'hh,ds,meter_id,maintenance_team,measureid,psr_class_name,inst_id,location_type,name,depart,measurepoint_id,district,enddevice_psr_class_name,enddevice_psr_id,root_id,measuretag,rt,measurevalue,timestamp,dataquality,datatablename,tag,equip_src_id,root_class_name,ssid,sysdate_uep'
        set 'enclose', '`'
        set 'format', "CSV"
        set 'compress_type', 'GZ'

        file 'test_csv_big_file_truncate_delimiter.csv.gz'
    }

    sql "sync"
    qt_sql "select count(*) from ${tableName}"
}
