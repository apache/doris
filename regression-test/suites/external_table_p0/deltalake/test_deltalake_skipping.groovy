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

suite("test_deltalake_skipping", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableDeltaLakeTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("deltalakeMinioPort")
    String warehouse = context.config.otherConfigs.get("deltalakeWarehouse")
    sql "drop catalog if exists test_deltalake_skipping"
    sql """create catalog test_deltalake_skipping properties (
        'type'='deltalake', 'warehouse'='${warehouse}',
        's3.endpoint'='http://${host}:${minioPort}', 's3.region'='us-east-1',
        's3.access_key'='admin', 's3.secret_key'='password', 'use_path_style'='true'
    )"""
    sql "switch test_deltalake_skipping"
    sql "use delta_m1"
    sql "set enable_count_push_down_for_external_table=true"
    order_qt_all "select * from skipping order by id"
    order_qt_range "select * from skipping where id < 5 order by id"
    order_qt_reversed "select * from skipping where 5 > id order by id"
    order_qt_between "select * from skipping where id >= 5 and id <= 7 order by id"
    order_qt_or "select * from skipping where id < 2 or id > 107 order by id"
    order_qt_null "select * from skipping where id is null order by id"
    order_qt_not_null "select * from skipping where id is not null order by id"
    order_qt_not_null_safe "select * from skipping where not (id <=> 1) order by id"
    order_qt_in "select * from skipping where id in (1, 101, null) order by id"
    order_qt_not_in "select * from skipping where id not in (1, 101, null) order by id"
    order_qt_partial_and "select * from skipping where id < 5 and substring(s, 1, 3)='low' order by id"
    order_qt_partial_or "select * from skipping where id > 107 or substring(s, 1, 3)='low' order by id"
    order_qt_partial_not "select * from skipping where not (id > 107 and substring(s, 1, 4)='high') order by id"
    order_qt_decimal "select * from skipping where dec >= 100.0000 order by id"
    order_qt_float "select * from skipping where f > 107.5 order by id"
    order_qt_double "select * from skipping where d > 107.5 order by id"
    qt_count "select count(*) from skipping"
    qt_count_column "select count(id) from skipping"
    qt_filtered_count "select count(*) from skipping where id < 5"
    qt_function_count "select count(*) from skipping where substring(s, 1, 3)='low'"
    explain {
        sql "select * from skipping"
        contains "inputSplitNum=3"
        contains "Delta snapshot:"
    }
    explain {
        sql "select * from skipping where id < 5"
        // Kernel conservatively retains the all-null file, whose min/max are absent.
        contains "inputSplitNum=2"
    }
    explain {
        sql "select * from skipping where id is null"
        contains "inputSplitNum=1"
    }
    explain {
        sql "select * from skipping where id is not null"
        contains "inputSplitNum=2"
    }
    explain {
        sql "select count(*) from skipping"
        contains "inputSplitNum=1"
        contains "pushdown agg=COUNT (21)"
    }
    sql "set enable_count_push_down_for_external_table=false"
    qt_count_without_pushdown "select count(*) from skipping"
}
