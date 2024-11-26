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

suite("test_iot_auto_detect_fail") {
    multi_sql """
    drop table if exists fail_src;
    CREATE TABLE `fail_src` (
  `qsrq` int NULL,
  `lsh` varchar(32) NULL,
  `wth` bigint NULL,
  `khh` varchar(16) NULL,
  `dt` varchar(8) NULL
) ENGINE=OLAP
DUPLICATE KEY(`qsrq`, `lsh`)
AUTO PARTITION BY LIST (`dt`)
(PARTITION p202307078 VALUES IN ("20230707"),
PARTITION p202307108 VALUES IN ("20230710"),
PARTITION p202307118 VALUES IN ("20230711"),
PARTITION p202307128 VALUES IN ("20230712"),
PARTITION p202307138 VALUES IN ("20230713"),
PARTITION p202307148 VALUES IN ("20230714"),
PARTITION p202410088 VALUES IN ("20241008"),
PARTITION p202410098 VALUES IN ("20241009"),
PARTITION p202410108 VALUES IN ("20241010"),
PARTITION p202410118 VALUES IN ("20241011"),
PARTITION p202410148 VALUES IN ("20241014"),
PARTITION p202410158 VALUES IN ("20241015"),
PARTITION p202410168 VALUES IN ("20241016"),
PARTITION p202410178 VALUES IN ("20241017"),
PARTITION p202410188 VALUES IN ("20241018"),
PARTITION p202410218 VALUES IN ("20241021"),
PARTITION p202410228 VALUES IN ("20241022"),
PARTITION p202410238 VALUES IN ("20241023"),
PARTITION p202410248 VALUES IN ("20241024"),
PARTITION p202410258 VALUES IN ("20241025"),
PARTITION p202410288 VALUES IN ("20241028"),
PARTITION p202410298 VALUES IN ("20241029"),
PARTITION p202410308 VALUES IN ("20241030"),
PARTITION p202410318 VALUES IN ("20241031"),
PARTITION p202411018 VALUES IN ("20241101"),
PARTITION p202411028 VALUES IN ("20241102"),
PARTITION p202411038 VALUES IN ("20241103"),
PARTITION p202411048 VALUES IN ("20241104"),
PARTITION p202411058 VALUES IN ("20241105"),
PARTITION p202411068 VALUES IN ("20241106"),
PARTITION p202411078 VALUES IN ("20241107"),
PARTITION p202411088 VALUES IN ("20241108"),
PARTITION p202411118 VALUES IN ("20241111"),
PARTITION p202411128 VALUES IN ("20241112"),
PARTITION p202411138 VALUES IN ("20241113"),
PARTITION p202411148 VALUES IN ("20241114"),
PARTITION p202411158 VALUES IN ("20241115"),
PARTITION p202411248 VALUES IN ("20241124"),
PARTITION p202411258 VALUES IN ("20241125"),
PARTITION p202411268 VALUES IN ("20241126"),
PARTITION p202411278 VALUES IN ("20241127"))
DISTRIBUTED BY HASH(`khh`, `dt`) BUCKETS AUTO
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V1",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);

insert into fail_src values (3,'a',10,'b','20241128'),(4,'a',10,'b','20241128'),(5,'a',10,'b','20241128'),(6,'a',10,'b','20241128'),(7,'a',10,'b','20241128'),(8,'a',10,'b','20241128'),(9,'a',10,'b','20241128');

drop table if exists fail_tag;
CREATE TABLE `fail_tag` (
  `qsrq` int NULL,
  `lsh` varchar(32) NULL,
  `wth` bigint NULL,
  `khh` varchar(16) NULL,
  `dt` varchar(8) NULL
) ENGINE=OLAP
DUPLICATE KEY(`qsrq`, `lsh`)
AUTO PARTITION BY LIST (`dt`)
(PARTITION p202307078 VALUES IN ("20230707"),
PARTITION p202307108 VALUES IN ("20230710"),
PARTITION p202307118 VALUES IN ("20230711"),
PARTITION p202307128 VALUES IN ("20230712"),
PARTITION p202307138 VALUES IN ("20230713"),
PARTITION p202307148 VALUES IN ("20230714"),
PARTITION p202410088 VALUES IN ("20241008"),
PARTITION p202410098 VALUES IN ("20241009"),
PARTITION p202410108 VALUES IN ("20241010"),
PARTITION p202410118 VALUES IN ("20241011"),
PARTITION p202410148 VALUES IN ("20241014"),
PARTITION p202410158 VALUES IN ("20241015"),
PARTITION p202410168 VALUES IN ("20241016"),
PARTITION p202410178 VALUES IN ("20241017"),
PARTITION p202410188 VALUES IN ("20241018"),
PARTITION p202410218 VALUES IN ("20241021"),
PARTITION p202410228 VALUES IN ("20241022"),
PARTITION p202410238 VALUES IN ("20241023"),
PARTITION p202410248 VALUES IN ("20241024"),
PARTITION p202410258 VALUES IN ("20241025"),
PARTITION p202410288 VALUES IN ("20241028"),
PARTITION p202410298 VALUES IN ("20241029"),
PARTITION p202410308 VALUES IN ("20241030"),
PARTITION p202410318 VALUES IN ("20241031"),
PARTITION p202411018 VALUES IN ("20241101"),
PARTITION p202411028 VALUES IN ("20241102"),
PARTITION p202411038 VALUES IN ("20241103"),
PARTITION p202411048 VALUES IN ("20241104"),
PARTITION p202411058 VALUES IN ("20241105"),
PARTITION p202411068 VALUES IN ("20241106"),
PARTITION p202411078 VALUES IN ("20241107"),
PARTITION p202411088 VALUES IN ("20241108"),
PARTITION p202411118 VALUES IN ("20241111"),
PARTITION p202411128 VALUES IN ("20241112"),
PARTITION p202411138 VALUES IN ("20241113"),
PARTITION p202411148 VALUES IN ("20241114"),
PARTITION p202411158 VALUES IN ("20241115"),
PARTITION p202411248 VALUES IN ("20241124"),
PARTITION p202411258 VALUES IN ("20241125"),
PARTITION p202411268 VALUES IN ("20241126"),
PARTITION p202411278 VALUES IN ("20241127"))
DISTRIBUTED BY HASH(`khh`, `dt`) BUCKETS AUTO
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V1",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);
    """

    test {
        sql "insert overwrite table fail_tag PARTITION(*) select qsrq,lsh,wth,khh,dt from fail_src where dt='20241128';"
        exception "Cannot found origin partitions"
    }
    test {
        sql "insert overwrite table fail_tag PARTITION(*) select qsrq,lsh,wth,khh,dt from fail_src where dt='20241128';"
        exception "Cannot found origin partitions"
    }
    test {
        sql "insert overwrite table fail_tag PARTITION(*) select qsrq,lsh,wth,khh,dt from fail_src where dt='20241128';"
        exception "Cannot found origin partitions"
    }
}
