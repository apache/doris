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

// A deletion vector addresses rows by their position in the whole file. Row group and page pruning skip rows before
// the delete predicate runs, so the surviving rows must keep those file positions: if id 101 is deleted and the first
// page was skipped, counting from the first row read would delete id 201 instead. The table has one file whose ids
// equal their positions, which makes such a shift visible in the results.
//
// The profile counters prove that pruning and the deletion vector were both active in the same scan. The abs()
// queries return the same ranges through a predicate that Parquet statistics cannot evaluate, so a pruning-only
// difference shows up between neighbouring result blocks.
suite("test_deltalake_parquet_deletion_vector", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableDeltaLakeTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("deltalakeMinioPort")
    String hmsPort = context.config.otherConfigs.get("deltalakeHmsPort")
    String warehouse = context.config.otherConfigs.get("deltalakeWarehouse")
    def profileAction = new ProfileAction(context)
    def counterValues = { String profile, String counter ->
        def values = []
        def matcher = profile =~ ("(?m)^\\s*(?:-\\s*)?"
                + java.util.regex.Pattern.quote(counter) + ":\\s+([^\\n]+)")
        while (matcher.find()) {
            String text = matcher.group(1)
            def exact = text =~ /\(([0-9,]+)\)/
            def number = text =~ /([0-9,]+)/
            values.add(Long.parseLong((exact.find() ? exact.group(1) : number.find() ? number.group(1) : "0")
                    .replace(",", "")))
        }
        return values
    }
    List<String> requiredCounters = ["RowGroupsFilteredByMinMax", "FilteredRowsByPage", "PageIndexReadCalls",
            "NumDeleteRows"]
    sql "set enable_profile=true"
    sql "set enable_sql_cache=false"
    ["filesystem", "hms"].each { String metastore ->
        String catalog = "test_deltalake_parquet_dv_${metastore}"
        String table = "${catalog}.delta_m1.parquet_pruning_dv"
        sql "drop catalog if exists ${catalog}"
        sql """create catalog ${catalog} properties (
            'type'='deltalake', 'deltalake.catalog.type'='${metastore}', 'warehouse'='${warehouse}',
            'hive.metastore.uris'='thrift://${host}:${hmsPort}',
            's3.endpoint'='http://${host}:${minioPort}', 's3.region'='us-east-1',
            's3.access_key'='admin', 's3.secret_key'='password', 'use_path_style'='true'
        )"""

        // First row group, around the start of the second page of the id column, near the page edge at about id 200,
        // and a row group far from the start of the file.
        Map<String, List<Integer>> ranges = ["early": [100, 120], "page_edge": [195, 205], "late": [9000, 9020]]
        ranges.each { String range, List<Integer> bounds ->
            int lower = bounds[0]
            int upper = bounds[1]
            // The one file overlaps this predicate, so Kernel cannot prune the file itself.
            explain {
                sql "select id, length(payload) from ${table} where id >= ${lower} and id < ${upper}"
                contains "inputSplitNum=1"
            }
            String token = UUID.randomUUID().toString()
            "order_qt_${metastore}_${range}"("""
                select /* ${token} */ id, length(payload)
                from ${table} where id >= ${lower} and id < ${upper} order by id
            """)
            String profile = profileAction.getProfileBySql(token, requiredCounters)
            requiredCounters.each { String counter ->
                assertTrue(counterValues(profile, counter).sum(0L) > 0L,
                        "${metastore}/${range}: ${counter} did not show pruning with a deletion vector; ${profile}")
            }
            "order_qt_${metastore}_${range}_no_pruning"("""
                select id, length(payload)
                from ${table} where abs(id) >= ${lower} and abs(id) < ${upper} order by id
            """)
            "qt_${metastore}_${range}_count"(
                    "select count(*) from ${table} where id >= ${lower} and id < ${upper}")
        }

        // Footer statistics still count deleted rows. Ids 0 and 9999 are deleted, so MIN and MAX must not use them.
        "qt_${metastore}_count"("select count(*) from ${table}")
        "qt_${metastore}_min_max"("select min(id), max(id) from ${table}")
    }
}
