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

suite("test_deltalake_signed_zero", "p0,external,deltalake") {
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
    sql "drop catalog if exists test_deltalake_signed_zero"
    sql """create catalog test_deltalake_signed_zero properties (
        'type'='deltalake', 'warehouse'='${warehouse}'${storage})"""

    sql "drop table if exists delta_signed_zero_reference"
    sql """create table delta_signed_zero_reference (id bigint, f float, d double)
           duplicate key(id) distributed by hash(id) buckets 1 properties ('replication_num'='1')"""
    sql """insert into delta_signed_zero_reference values
           (1, '-0.0', '-0.0'), (2, '0.0', '0.0'), (3, -1, -1), (4, 1, 1), (5, null, null)"""

    sql "set enable_sql_cache=false"
    sql "set num_files_in_batch_mode=1"
    for (boolean batch : [false, true]) {
        sql "set enable_external_table_batch_mode=${batch}"
        [f: "float", d: "double"].each { column, type ->
            String minus = "cast('-0.0' as ${type})"
            String plus = "cast('0.0' as ${type})"
            String one = "cast('1.0' as ${type})"
            [
                eq_minus: "${column} = ${minus}",
                eq_plus: "${column} = ${plus}",
                ne: "${column} <> ${minus}",
                not_null_safe: "not (${column} <=> ${minus})",
                lt: "${column} < ${plus}",
                le: "${column} <= ${minus}",
                gt: "${column} > ${minus}",
                ge: "${column} >= ${plus}",
                reversed: "${minus} < ${column}",
                in_list: "${column} in (${minus}, ${one})",
                not_in: "${column} not in (${plus}, ${one})",
                in_null: "${column} in (${minus}, null)",
                not_in_null: "${column} not in (${plus}, null)"
            ].each { label, predicate ->
                quickTest("${batch}_${column}_${label}",
                        "select id from delta_signed_zero_reference where ${predicate} order by id",
                        """select id from test_deltalake_signed_zero.delta_m1.signed_zero
                           where ${predicate} order by id""")
            }
        }
    }

    sql "set enable_profile=true"
    def profiles = new ProfileAction(context)
    def checkPartition = { String tag, String value, int expected ->
        String token = UUID.randomUUID().toString()
        "order_qt_${tag}"("""select /* ${token} */ id from test_deltalake_signed_zero.delta_m1.signed_zero_partition
                where p = cast('${value}' as float) order by id""")
        String profile = profiles.getProfileBySql(token, ["Delta Scan Metrics", "EnumerationComplete: true"])
        def metrics = profile.readLines()*.trim()
        assertTrue(metrics.contains("- SelectedPartitions: " + expected), profile)
        assertTrue(metrics.contains("- SelectedFiles: " + expected), profile)
    }
    for (boolean batch : [false, true]) {
        sql "set enable_external_table_batch_mode=${batch}"
        checkPartition("partition_${batch}_positive_zero", "0.0", 2)
        checkPartition("partition_${batch}_negative_zero", "-0.0", 2)
        checkPartition("partition_${batch}_one", "1.0", 1)
    }
    order_qt_partition_names "show partitions from test_deltalake_signed_zero.delta_m1.signed_zero_partition"
}
