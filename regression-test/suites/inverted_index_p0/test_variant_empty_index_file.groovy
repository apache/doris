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

suite("test_variant_empty_index_file", "p0") {
    def tableName = "test_variant_empty_index_file"
    sql """ drop table if exists ${tableName} """
    // create table
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}
        (   
            `id`   bigint NOT NULL,
            `v`    variant NULL,
            INDEX  v_idx (`v`) USING INVERTED
        ) DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH (`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "inverted_index_storage_format" = "v2",
        "disable_auto_compaction" = "true"
        );
    """

    sql """ set enable_memtable_on_sink_node = true """
    sql """ insert into ${tableName} values (1, NULL) """
    qt_sql9 "select * from ${tableName}"
    sql "sync"
    def tablets = sql_return_maparray """ show tablets from ${tableName}; """

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    String tablet_id = tablets[0].TabletId
    String backend_id = tablets[0].BackendId
    String ip = backendId_to_backendIP.get(backend_id)
    String port = backendId_to_backendHttpPort.get(backend_id)
    def (code, out, err) = http_client("GET", String.format("http://%s:%s/api/show_nested_index_file?tablet_id=%s", ip, port, tablet_id))
    logger.info("Run show_nested_index_file_on_tablet: code=" + code + ", out=" + out + ", err=" + err)
    assertEquals("E-6004", parseJson(out.trim()).status)
    assertTrue(out.contains(" is empty"))

    try {
        sql """ select /*+ SET_VAR(enable_match_without_inverted_index = 0) */  * from ${tableName} where v match 'abcd';  """
    } catch (Exception e) {
        log.info(e.getMessage());
        assertTrue(e.getMessage().contains("match_any not support execute_match"))
    }
}