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

suite("test_timestamp_ns_rf") {
    sql "drop table if exists timestamp_ns_rf_probe"
    sql "drop table if exists timestamp_ns_rf_build"
    for (def tableName : ["timestamp_ns_rf_probe", "timestamp_ns_rf_build"]) {
        sql """
            create table ${tableName} (
                id int,
                dt timestamp_ns
            )
            duplicate key(id)
            distributed by hash(id) buckets 4
            properties("replication_num" = "1")
        """
    }
    sql """
        insert into timestamp_ns_rf_probe values
        (1, '1677-09-21 00:12:43.145224192'),
        (2, '1969-12-31 23:59:59.999999999'),
        (3, '1970-01-01 00:00:00.000000000'),
        (4, '1970-01-01 00:00:00.000000001'),
        (5, '2262-04-11 23:47:16.854775807'),
        (6, null)
    """
    sql """
        insert into timestamp_ns_rf_build values
        (11, '1677-09-21 00:12:43.145224192'),
        (12, '1970-01-01 00:00:00.000000001'),
        (13, '2262-04-11 23:47:16.854775807')
    """

    def originalEnableRuntimeFilterPrune = sql("select @@enable_runtime_filter_prune")[0][0]
    def originalRuntimeFilterType = sql("select @@runtime_filter_type")[0][0]
    try {
        sql "set enable_runtime_filter_prune = false"
        sql "set runtime_filter_type = 1"
        explain {
            sql """
                verbose select p.id, b.id
                from timestamp_ns_rf_probe p
                join timestamp_ns_rf_build b on p.dt = b.dt
            """
            contains "runtime filters: RF000[in]"
        }
        order_qt_runtime_filter_in """
            select p.id, p.dt, b.id
            from timestamp_ns_rf_probe p
            join timestamp_ns_rf_build b on p.dt = b.dt
            order by p.id, b.id
        """

        sql "set runtime_filter_type = 2"
        explain {
            sql """
                verbose select p.id, b.id
                from timestamp_ns_rf_probe p
                join timestamp_ns_rf_build b on p.dt = b.dt
            """
            contains "runtime filters: RF000[bloom]"
        }
        order_qt_runtime_filter_bloom """
            select p.id, p.dt, b.id
            from timestamp_ns_rf_probe p
            join timestamp_ns_rf_build b on p.dt = b.dt
            order by p.id, b.id
        """

        sql "set runtime_filter_type = 4"
        explain {
            sql """
                verbose select p.id, b.id
                from timestamp_ns_rf_probe p
                join timestamp_ns_rf_build b on p.dt = b.dt
            """
            contains "runtime filters: RF000[min_max]"
        }
        order_qt_runtime_filter_min_max """
            select p.id, p.dt, b.id
            from timestamp_ns_rf_probe p
            join timestamp_ns_rf_build b on p.dt = b.dt
            order by p.id, b.id
        """
    } finally {
        sql "set runtime_filter_type = '${originalRuntimeFilterType}'"
        sql "set enable_runtime_filter_prune = ${originalEnableRuntimeFilterPrune}"
    }
}
