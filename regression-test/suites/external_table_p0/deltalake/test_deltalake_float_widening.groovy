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

import org.apache.doris.regression.action.ProfileAction

suite("test_deltalake_float_widening", "p0,external,deltalake") {
    def config = context.config.otherConfigs
    if (!"true".equalsIgnoreCase(config.get("enableDeltaLakeTest"))) {
        return
    }
    String warehouse = config.get("deltalakeWarehouse")
    String storage = ""
    if (warehouse.startsWith("s3")) {
        storage = """, 's3.endpoint'='http://${config.get('externalEnvIp')}:${config.get('deltalakeMinioPort')}',
            's3.region'='us-east-1', 's3.access_key'='admin', 's3.secret_key'='password', 'use_path_style'='true'"""
    }
    sql "drop catalog if exists test_deltalake_float_widening"
    sql """create catalog test_deltalake_float_widening properties (
        'type'='deltalake', 'warehouse'='${warehouse}'${storage})"""

    sql "drop table if exists delta_float_widening_reference"
    sql """create table delta_float_widening_reference (id int, p double, v double)
           duplicate key(id) distributed by hash(id) buckets 1 properties ('replication_num'='1')"""
    // Delta partition values are strings. The old Parquet data value is the exact promotion of FLOAT 0.1.
    sql """insert into delta_float_widening_reference values
           (1, 0.1, cast('0.10000000149011612' as double)), (2, 1.25, 1.25), (3, 0.1, 0.1)"""

    String exact = "cast('0.10000000149011612' as double)"
    String tenth = "cast('0.1' as double)"
    def predicates = [
            exact: "v = ${exact}",
            narrow: "v > ${tenth} and v < cast('0.100000002' as double)",
            tenth: "v = ${tenth}"
    ]
    predicates.each { tag, predicate ->
        "order_qt_reference_${tag}"("select id from delta_float_widening_reference where ${predicate} order by id")
    }

    sql "set enable_sql_cache=false"
    sql "set num_files_in_batch_mode=1"
    sql "set enable_profile=true"
    def profiles = new ProfileAction(context)
    for (boolean batch : [false, true]) {
        sql "set enable_external_table_batch_mode=${batch}"
        def compare = { String tag, String predicate, Integer expectedFiles = null ->
            String token = UUID.randomUUID().toString()
            quickTest("${batch}_${tag}",
                    "select id from delta_float_widening_reference where ${predicate} order by id",
                    """select /* ${token} */ id from test_deltalake_float_widening.delta_m1.float_widening
                       where ${predicate} order by id""")
            if (expectedFiles != null) {
                String profile = profiles.getProfileBySql(token, ["Delta Scan Metrics", "EnumerationComplete: true"])
                assertTrue(profile.readLines()*.trim().contains("- SelectedFiles: " + expectedFiles), profile)
            }
        }
        predicates.each { tag, predicate -> compare(tag, predicate) }
        compare("safe_conjunct", "id = 1 and v = ${exact}", 1)
        compare("partition", "p = ${tenth}", 2)
        compare("null_count", "v is null", 0)
    }
}
