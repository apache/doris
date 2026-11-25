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

import groovy.json.JsonSlurper

suite("test_deltalake_uc_history", "p0,external,deltalake,external_docker") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableDeltaLakeUcTest"))) {
        return
    }
    String reference = context.config.otherConfigs.get("deltalakeUcHistoryReference")
    if (!reference) {
        logger.info("Set deltalakeUcHistoryReference to the Spark fixture's history reference to run this suite")
        return
    }
    String uri = context.config.otherConfigs.get("deltalakeUcUri")
    def json = new JsonSlurper()
    def expected = json.parse(new File(reference))
    def uc = json.parse(new URL("${uri}/api/2.1/unity-catalog/delta/v1/catalogs/doris_delta/schemas/m1/tables/uc_managed"))
    long latest = uc["latest-table-version"]
    def staged = uc.commits.find { it.version == latest }
    assertNotNull(staged)
    def log = new File(new File(new URI(uc.metadata.location)), "_delta_log")
    assertTrue(new File(log, "_staged_commits/${staged['file-name']}").isFile())
    assertFalse(new File(log, String.format('%020d.json', latest)).exists())

    sql "drop catalog if exists test_deltalake_uc_history"
    sql """create catalog test_deltalake_uc_history properties (
        'type'='deltalake', 'deltalake.catalog.type'='uc',
        'deltalake.uc.uri'='${uri}', 'deltalake.uc.catalog'='doris_delta'
    )"""
    sql "set time_zone='UTC'"
    String history = 'test_deltalake_uc_history.m1.`uc_managed$history`'
    String fields = """version, millisecond_timestamp(cast(timestamp as datetimev2(6))),
        userId, userName, operation, to_json(operationParameters), to_json(job), to_json(notebook),
        clusterId, readVersion, isolationLevel, isBlindAppend, to_json(operationMetrics),
        userMetadata, engineInfo"""
    def normalize = { row ->
        def values = row.toList()
        values[0] = values[0].longValue()
        values[9] = values[9]?.longValue()
        if (values[11] instanceof Number) {
            values[11] = values[11].intValue() != 0
        }
        [5, 6, 7, 12].each { index ->
            if (values[index] instanceof String) {
                values[index] = json.parseText(values[index])
            }
        }
        values
    }
    assertEquals(expected.collect(normalize),
        sql("select ${fields} from ${history} order by version").collect(normalize))
    qt_uc_commits "select version, operation from ${history} order by version"
    qt_uc_latest "select version, operation from ${history} where version = ${latest} order by version"
    qt_uc_count "select count(*) from ${history}"
}
