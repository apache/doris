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

suite("test_load_with_transfer_encoding", "p0") {
    def table_name = "test_load_with_transfer_encoding"

    sql "DROP TABLE IF EXISTS ${table_name}"
    sql """
        CREATE TABLE ${table_name} (
        `place_id` varchar(14) NOT NULL,
        `card_id` varchar(12) NOT NULL,
        `shift_id` varchar(4) NULL,
        `id` bigint NOT NULL AUTO_INCREMENT (1),
        `created` datetime(6) NOT NULL,
        `creater` bigint NULL,
        `deleted` int NOT NULL,
        `updated` datetime(6) NULL,
        `card_type_id` varchar(8) NOT NULL,
        `card_type_name` varchar(20) NULL,
        `cash_balance` int NOT NULL,
        `cashier_id` varchar(8) NULL,
        `client_id` varchar(4) NULL,
        `cost` int NOT NULL,
        `creater_name` varchar(50) NULL,
        `details` varchar(200) NULL,
        `id_name` varchar(50) NOT NULL,
        `id_number` varchar(18) NOT NULL,
        `last_client_id` varchar(4) NULL,
        `login_id` varchar(16) NULL,
        `operation_type` varchar(50) NOT NULL,
        `present` int NOT NULL,
        `present_balance` int NOT NULL,
        `remark` varchar(200) NULL,
        `source_type` varchar(50) NOT NULL,
        `online_account` int NOT NULL
        ) ENGINE = OLAP DUPLICATE KEY (`place_id`, `card_id`, `shift_id`) DISTRIBUTED BY HASH (`operation_type`) BUCKETS 10 PROPERTIES (
        "file_cache_ttl_seconds" = "0",
        "is_being_synced" = "false",
        "storage_medium" = "hdd",
        "storage_format" = "V2",
        "inverted_index_storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false",
        "group_commit_interval_ms" = "10000",
        "replication_num" = "1",
        "group_commit_data_bytes" = "134217728"
        );
    """


    String db = context.config.getDbNameByFile(context.file)

    def command = """curl --location-trusted -u ${context.config.feHttpUser}:${context.config.feHttpPassword} -H read_json_by_line:false -H Expect:100-continue -H max_filter_ratio:1 -H strict_mode:false -H strip_outer_array:true -H columns:id,created,creater,deleted,updated,card_id,card_type_id,card_type_name,cash_balance,cashier_id,client_id,cost,creater_name,details,id_name,id_number,last_client_id,login_id,operation_type,place_id,present,present_balance,remark,shift_id,source_type,online_account -H format:json -H Transfer-Encoding:chunked -T ${context.config.dataPath}/load_p0/stream_load/test_load_with_transfer_encoding.json -XPUT http://${context.config.feHttpAddress}/api/${db}/${table_name}/_stream_load"""
    log.info("stream load: ${command}")
    def process = command.execute()
    def code = process.waitFor()
    def out = process.text
    def json = parseJson(out)
    log.info("stream load result is:: ${out}".toString())
    assertEquals("success", json.Status.toLowerCase())
    assertEquals(15272, json.NumberLoadedRows)

    qt_sql """ select count() from ${table_name} """

}

