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

suite("column_stats") {
    multi_sql """
        set global enable_auto_analyze=false;
        SET enable_nereids_planner=true;
        
        SET enable_fallback_to_original_planner=false;
        set disable_nereids_rules=PRUNE_EMPTY_PARTITION;

        
        drop table if exists region;
        CREATE TABLE region  (
        r_regionkey      int NOT NULL,
        r_name       VARCHAR(25) NOT NULL,
        r_comment    VARCHAR(152)
        )ENGINE=OLAP
        DUPLICATE KEY(`r_regionkey`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`r_regionkey`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );

        drop table if exists nation;
         CREATE TABLE `nation` (
        `n_nationkey` int(11) NOT NULL,
        `n_name`      varchar(25) NOT NULL,
        `n_regionkey` int(11) NOT NULL,
        `n_comment`   varchar(152) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`N_NATIONKEY`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`N_NATIONKEY`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
        alter table nation modify column n_nationkey set stats ('ndv'='25', 'num_nulls'='0', 'min_value'='0', 'max_value'='24', 'row_count'='25');

        alter table nation modify column n_name set stats ('ndv'='25', 'num_nulls'='0', 'min_value'='ALGERIA', 'max_value'='VIETNAM', 'row_count'='25');

        alter table nation modify column n_regionkey set stats ('ndv'='5', 'num_nulls'='0', 'min_value'='0', 'max_value'='4', 'row_count'='25');

        alter table nation modify column n_comment set stats ('ndv'='25', 'num_nulls'='0', 'min_value'=' haggle. carefully final deposits detect slyly agai', 'max_value'='y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be', 'row_count'='25');

    """

    explain {
        sql "select * from region"
        notContains("planed with unknown column statistics")
    }

    explain {
        sql "select * from region where r_regionkey=1"
        contains("planed with unknown column statistics")
    }

    explain {
        sql "select r_regionkey from region group by r_regionkey"
        contains("planed with unknown column statistics")
    }

    explain {
        sql "select r_regionkey from region join nation on r_regionkey=n_regionkey"
        contains("planed with unknown column statistics")
    }

    sql "alter table region modify column r_regionkey set stats ('ndv'='5', 'num_nulls'='0', 'min_value'='0', 'max_value'='4', 'row_count'='5');"
    
    explain {
        sql "select * from region where r_regionkey=1"
        notContains("planed with unknown column statistics")
    }

    explain {
        sql "select r_regionkey from region group by r_regionkey"
        notContains("planed with unknown column statistics")
    }

    explain {
        sql "select r_regionkey from region join nation on r_regionkey=n_regionkey"
        notContains("planed with unknown column statistics")
    }

    explain {
        sql "select r_name from region join nation on r_regionkey=n_regionkey"
        notContains("planed with unknown column statistics")
    }

    explain {
        sql """
            select r_name 
            from (select r_name, r_regionkey + 1 x from region) T join nation on T.x=n_regionkey
            """
        notContains("planed with unknown column statistics")
    }
}
