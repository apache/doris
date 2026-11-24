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

suite("cold_value_selectivity") {
    sql "DROP TABLE IF EXISTS cold_value_selectivity"
    sql """
        CREATE TABLE cold_value_selectivity (id INT NOT NULL, v INT NULL)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num" = "1")
    """
    sql """
        INSERT INTO cold_value_selectivity
        SELECT number,
            CASE WHEN number < 320 THEN NULL
                 WHEN number < 1440 THEN 0
                 ELSE 1 + (number - 1440) DIV 40 END
        FROM numbers("number" = "1600")
    """
    sql """
        ALTER TABLE cold_value_selectivity MODIFY COLUMN v SET STATS
        ('row_count'='1600', 'ndv'='5', 'num_nulls'='320', 'min_value'='0', 'max_value'='4', 'hot_values'='0 :0.875')
    """

    explain {
        sql "OPTIMIZED PLAN SELECT id FROM cold_value_selectivity WHERE v = 2"
        contains "stats=40, projects="
    }
    explain {
        sql "OPTIMIZED PLAN SELECT id FROM cold_value_selectivity WHERE v IN (2, 3)"
        contains "stats=80, projects="
    }
    explain {
        sql "OPTIMIZED PLAN SELECT id FROM cold_value_selectivity WHERE v IN (0, 2)"
        contains "stats=1,160, projects="
    }
    explain {
        sql "OPTIMIZED PLAN SELECT id FROM cold_value_selectivity WHERE v = 0"
        contains "stats=1,120, projects="
    }

    sql "DROP STATS cold_value_selectivity"
    sql "ANALYZE TABLE cold_value_selectivity(v) WITH SYNC WITH HOT VALUE"
    explain {
        sql "OPTIMIZED PLAN SELECT id FROM cold_value_selectivity WHERE v = 2"
        contains "stats=40, projects="
    }

    order_qt_cold_value_result """
        SELECT v, COUNT(*) FROM cold_value_selectivity WHERE v IN (0, 2, 3) GROUP BY v
    """
}
