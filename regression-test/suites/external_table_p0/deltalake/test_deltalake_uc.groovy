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

suite("test_deltalake_uc", "p0,external,deltalake,external_docker") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableDeltaLakeUcTest"))) {
        return
    }
    String uri = context.config.otherConfigs.get("deltalakeUcUri")
    sql "drop catalog if exists test_deltalake_uc"
    sql """create catalog test_deltalake_uc properties (
        'type'='deltalake', 'deltalake.catalog.type'='uc',
        'deltalake.uc.uri'='${uri}', 'deltalake.uc.catalog'='doris_delta'
    )"""
    qt_schema "show databases from test_deltalake_uc like 'm1'"
    qt_table "show tables from test_deltalake_uc.m1 like 'uc_managed'"
    qt_columns "desc test_deltalake_uc.m1.uc_managed"
    test {
        sql "insert into test_deltalake_uc.m1.uc_managed values (7, 'forbidden')"
        exception "Connector does not provide a write plan for target uc_managed"
    }
    for (boolean streaming : [false, true]) {
        sql "set enable_external_table_batch_mode=${streaming}"
        "qt_rows_${streaming}"("select id, label from test_deltalake_uc.m1.uc_managed order by id")
        "qt_count_${streaming}"("select count(*) from test_deltalake_uc.m1.uc_managed")
    }
}
