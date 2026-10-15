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

// The two lake rows differ in the `region,code` part of their composite key,
// while its ordinary region and code parts are equal. The frozen log tail
// updates only key-a. If BE splits the quoted name on its comma, it suppresses
// both lake rows and loses key-b.
suite("test_fluss_lake_pk_comma_key", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableFlussTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String coordinatorPort = context.config.otherConfigs.get("fluss_coordinator_port")
    String minioPort = context.config.otherConfigs.get("fluss_minio_port")
    String catalog = "test_fluss_lake_pk_comma_key"

    sql """drop catalog if exists ${catalog}"""
    sql """
        create catalog ${catalog} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${externalEnvIp}:${coordinatorPort}",
            "fluss.lake.paimon.s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "fluss.lake.paimon.s3.access-key" = "minioadmin",
            "fluss.lake.paimon.s3.secret-key" = "minioadmin",
            "fluss.union_read.mode" = "required"
        );
    """
    sql """switch ${catalog}"""
    sql """use fluss_test"""
    sql """set enable_file_scanner_v2 = true"""

    String plan = sql("""explain select * from lake_pk_comma""")
            .collect { row -> row[0].toString() }.join("\n")
    assertTrue(plan.contains("unionRead=yes"), "the scan did not combine the lake and tail: ${plan}")
    def suppressed = (plan =~ /suppressedLakeSplits=(\d+)/)
    assertTrue(suppressed.find() && suppressed.group(1).toInteger() > 0,
            "no lake split was suppressed: ${plan}")
    assertTrue(plan.contains("pkTailRanges=1"), "no primary-key tail was replayed: ${plan}")

    order_qt_lake_rows """
        select `region,code`, region, code, name from lake_pk_comma\$lake
        order by `region,code`
    """
    order_qt_merged_rows """
        select `region,code`, region, code, name from lake_pk_comma
        order by `region,code`
    """
    sql """set fluss_union_read_mode = 'disabled'"""
    order_qt_fluss_only """
        select `region,code`, region, code, name from lake_pk_comma
        order by `region,code`
    """
    sql """set fluss_union_read_mode = ''"""
}
