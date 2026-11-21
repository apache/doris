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

suite("test_deltalake_double_partition", "p0,external,deltalake,external_docker,nonConcurrent") {
    def config = context.config.otherConfigs
    def catalogs = [:]
    if ("true".equalsIgnoreCase(config.get("enableDeltaLakeTest"))) {
        String warehouse = config.get("deltalakeWarehouse")
        String storage = ""
        if (warehouse.startsWith("s3")) {
            storage = """, 's3.endpoint'='http://${config.get('externalEnvIp')}:${config.get('deltalakeMinioPort')}',
                's3.region'='us-east-1', 's3.access_key'='admin', 's3.secret_key'='password', 'use_path_style'='true'"""
        }
        catalogs.fs = "'deltalake.catalog.type'='filesystem', 'warehouse'='${warehouse}'${storage}"
    }
    if ("true".equalsIgnoreCase(config.get("enableDeltaLakeUcTest"))) {
        catalogs.uc = """'deltalake.catalog.type'='uc', 'deltalake.uc.uri'='${config.get('deltalakeUcUri')}',
            'deltalake.uc.catalog'='doris_delta'"""
    }
    catalogs.each { kind, properties ->
        String catalog = "test_deltalake_double_partition_${kind}"
        sql "drop catalog if exists ${catalog}"
        sql """create catalog ${catalog} properties ('type'='deltalake', ${properties})"""
        String db = "${catalog}.delta_reader_edges"
        sql "set enable_sql_cache=false"
        sql "set num_files_in_batch_mode=1"
        "order_qt_${kind}_names"("show partitions from ${db}.double_partition")
        for (boolean batch : [false, true]) {
            sql "set enable_external_table_batch_mode=${batch}"
            def compare = { String label, String actual, String expected ->
                quickTest("${kind}_${batch}_${label}", expected, actual)
            }
            String low = "cast('-2147483648' as double)"
            String other = "cast('100.125' as double)"
            [double_partition: "p", double_widen_partition: "i_d"].each { table, key ->
                String from = "from ${db}.${table} where"
                compare("${table}_equal", "select id ${from} ${key} = ${low}", "select 1")
                compare("${table}_range", """select id ${from} ${key} >= cast('-2147483649' as double)
                        and ${key} < cast('-2147483647' as double)""", "select 1")
                compare("${table}_or", "select id ${from} ${key} = ${low} or ${key} = ${other} order by id",
                        "select 1 as id union all select 3 order by id")
                compare("${table}_absent", "select count(*) ${from} ${key} = cast('-2147483650' as double)", "select 0")
            }

            // The log contains separate -0.0 and +0.0 AddFiles (ids 4 and 5) and a genuine NULL (id 7).
            String zero = "cast('-0.0' as double)"
            [equal: ["p = ${zero}", "4,5"], nullsafe: ["p <=> ${zero}", "4,5"],
             ne: ["p != ${zero}", "1,2,3,6,8"], lt: ["p < ${zero}", "1,6"],
             le: ["p <= ${zero}", "1,4,5,6"], gt: ["${zero} < p", "2,3,8"],
             ge: ["${zero} <= p", "2,3,4,5,8"], in_list: ["p in (${zero}, ${other})", "3,4,5"],
             not_in: ["p not in (${zero}, ${other})", "1,2,6,8"], is_null: ["p is null", "7"],
             residual: ["p = ${low} or abs(id) = 3", "1,3"],
             adjacent: ["p = cast('0.10000000000000002' as double)", "8"]].each { label, example ->
                compare("double_${label}", "select id from ${db}.double_partition where ${example[0]} order by id",
                        "select number from numbers('number'='9') where number in (${example[1]}) order by number")
            }
        }

        // An enabled partition-count rule requires FE to enumerate the pinned partition view.
        String rule = "delta_double_partition_limit"
        String user = "delta_double_partition_user"
        sql "drop sql_block_rule if exists ${rule}"
        sql "drop user if exists ${user}"
        try {
            sql "create user ${user}"
            sql "grant select_priv on internal.`${context.config.defaultDb}`.* to ${user}"
            sql "grant select_priv on ${catalog}.*.* to ${user}"
            if (isCloudMode()) {
                def clusters = sql "show clusters"
                sql "grant usage_priv on cluster `${clusters[0][0]}` to ${user}"
            }
            sql """create sql_block_rule ${rule} properties ("partition_num"="1", "global"="false")"""
            sql """set property for '${user}' 'sql_block_rules' = '${rule}'"""
            connect(user, "", context.config.jdbcUrl) {
                String table = "${db}.double_partition"
                String one = "p = cast('-2147483648' as double)"
                "qt_${kind}_eager_single"("select id from ${table} where ${one}")
                test {
                    sql "select count(*) from ${table} where ${one} or p = cast('100.125' as double)"
                    exception "reach partition_num : 1"
                }
            }
        } finally {
            sql "drop sql_block_rule if exists ${rule}"
            sql "drop user if exists ${user}"
        }
    }
}
