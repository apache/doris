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

package org.apache.doris.statistics.analysis;

import org.apache.commons.text.StringSubstitutor;

import java.util.Map;

/** Both consumers read the same materialized input, including the non-null hot-value denominator. */
final class TableStatisticsSqlBuilder {
    private TableStatisticsSqlBuilder() {
    }

    static final String HOT_ENTRY = "CONCAT(REPLACE(REPLACE(hot_top.column_key, \":\", \"\\\\:\"), \";\", \"\\\\;\"), "
            + "\" :\", ROUND(hot_top.freq / stats.sampled_non_null_rows, 2))";

    private static final String HOT_PROJECTION = hotProjection(false);

    private static String hotProjection(boolean singletons) {
        return "SELECT stats.id, stats.catalog_id, stats.db_id, stats.tbl_id, stats.idx_id, "
            + "stats.col_id, stats.part_id, stats.row_count, stats.ndv, stats.null_count, stats.min, stats.max, "
            + "stats.data_size, stats.update_time, "
            + "IFNULL(GROUP_CONCAT(" + HOT_ENTRY + ", \" ;\"), '') as `hot_value` "
            + (singletons ? ", MAX(stats.singletons) AS singletons " : "")
            + "FROM stats LEFT JOIN hot_top ON TRUE GROUP BY "
            + "stats.id, stats.catalog_id, stats.db_id, stats.tbl_id, stats.idx_id, stats.col_id, stats.part_id, "
            + "stats.row_count, stats.ndv, stats.null_count, stats.min, stats.max, stats.data_size, stats.update_time";
    }

    private static final String HOT_TOP = "hot_top AS (SELECT col_value AS column_key, `count` AS freq FROM cte1 "
            + "WHERE col_value IS NOT NULL ORDER BY freq DESC "
            + "LIMIT ${hotValueCollectCount}) ";

    private static final String GROUPED_SAMPLE = "WITH cte1 AS ("
            + "SELECT MAX(t0.`col_value`) as `col_value`, ${sampleFrequency:-COUNT(1)} as `count`, "
            + "SUM(`len`) as `column_length` "
            + "FROM "
            +     "(SELECT "
            +     "${subStringColName} AS `hash_value`, "
            +     "${colName} AS `col_value`, "
            +     "LENGTH(${colName}) as `len`${lengthAssert} "
            +     "FROM ${catalogName}.${dbName}.${tblName} ${index} ${sampleHints} ${limit}) as `t0` "
            +     "${preAggHint} GROUP BY `t0`.`hash_value`), ";

    static final String FULL_ANALYZE_TEMPLATE = "WITH cte1 AS ("
            +     "SELECT ${colName}${lengthAssert} "
            +     "FROM ${catalogName}.${dbName}.${tblName} ${index}), "
            + "stats AS ("
            +     "SELECT CONCAT(${tblId}, '-', ${idxId}, '-', '${colId}') AS `id`, "
            +     "${catalogId} AS `catalog_id`, "
            +     "${dbId} AS `db_id`, "
            +     "${tblId} AS `tbl_id`, "
            +     "${idxId} AS `idx_id`, "
            +     "'${colId}' AS `col_id`, "
            +     "NULL AS `part_id`, "
            +     "COUNT(1) AS `row_count`, "
            +     "NDV(${colName}) AS `ndv`, "
            +     "COUNT(1) - COUNT(${colName}) AS `null_count`, "
            +     "SUBSTRING(CAST(MIN(${colName}) AS STRING), 1, 1024) AS `min`, "
            +     "SUBSTRING(CAST(MAX(${colName}) AS STRING), 1, 1024) AS `max`, "
            +     "${dataSizeFunction} AS `data_size`, "
            +     "NOW() AS update_time, COUNT(${colName}) AS sampled_non_null_rows "
            + "FROM cte1${lengthFilter:-}), "
            + "hot_top AS (SELECT MAX(${colName}) AS column_key, COUNT(1) AS freq FROM cte1 "
            + "WHERE ${colName} IS NOT NULL GROUP BY ${subStringColName} ORDER BY freq DESC "
            + "LIMIT ${hotValueCollectCount}) "
            + HOT_PROJECTION;

    static final String DUJ1_ANALYZE_TEMPLATE = GROUPED_SAMPLE
            + "stats AS ( "
            +     "SELECT CONCAT('${tblId}', '-', '${idxId}', '-', '${colId}') AS `id`, "
            +     "${catalogId} AS `catalog_id`, "
            +     "${dbId} AS `db_id`, "
            +     "${tblId} AS `tbl_id`, "
            +     "${idxId} AS `idx_id`, "
            +     "'${colId}' AS `col_id`, "
            +     "NULL AS `part_id`, "
            +     "${rowCount} AS `row_count`, "
            +     "${ndvFunction} as `ndv`, "
            +     "IFNULL(SUM(IF(`t1`.`col_value` IS NULL, `t1`.`count`, 0)), 0) * ${scaleFactor} as `null_count`, "
            +     "SUBSTRING(CAST(${min} AS STRING), 1, 1024) AS `min`, "
            +     "SUBSTRING(CAST(${max} AS STRING), 1, 1024) AS `max`, "
            +     "${dataSizeFunction} * ${scaleFactor} AS `data_size`, "
            +     "NOW() AS update_time, SUM(IF(t1.col_value IS NULL, 0, t1.count)) AS sampled_non_null_rows "
            +     "FROM cte1 t1), "
            + HOT_TOP
            + HOT_PROJECTION;

    private static final String SCALED_NULLS = "LEAST(${rowCount}, GREATEST(0, CAST(ROUND("
            + "${rowCount} * CAST(z AS DOUBLE) / NULLIF(n, 0)) AS LARGEINT)))";

    // A NULL is not a distinct value or a singleton. All three sample moments use non-NULL rows.
    private static final String DUJ1_NDV = "LEAST(${rowCount} - scaled_nulls, GREATEST(0, IF(d = 0, 0, "
            + "CAST(ROUND(CAST(n - z AS DOUBLE) * d "
            + "/ (n - z - f1 + CAST(f1 AS DOUBLE) * (n - z) / ${rowCount})) AS LARGEINT))))";

    static String fixed(Map<String, String> params, boolean singleNonNullUnique) {
        return sampleSql(params, singleNonNullUnique, false);
    }

    static String sampleSql(Map<String, String> params, boolean singleNonNullUnique, boolean raw) {
        String ndv = singleNonNullUnique ? "${rowCount}" : DUJ1_NDV;
        String moments = singleNonNullUnique
                ? "WITH sample_rows AS (SELECT ${colName}${lengthAssert} FROM ${catalogName}.${dbName}.${tblName} "
                        + "${index} ${sampleHints}), moments AS (SELECT COUNT(1) AS n, 0 AS z, "
                        + "IFNULL(${rawDataSizeFunction}, 0) "
                        + "AS bytes FROM sample_rows${lengthFilter:-}), "
                : GROUPED_SAMPLE + "moments AS (SELECT IFNULL(SUM(t1.count), 0) AS n, "
                + "IFNULL(SUM(IF(t1.col_value IS NULL, t1.count, 0)), 0) AS z, COUNT(t1.col_value) AS d, "
                + "SUM(IF(t1.col_value IS NOT NULL AND t1.count = 1, 1, 0)) AS f1, "
                + "IFNULL(${dataSizeFunction}, 0) AS bytes FROM cte1 t1), ";
        String fields = raw
                ? "n AS row_count, " + (singleNonNullUnique ? "n" : "d") + " AS ndv, z AS null_count, "
                : "IF(n = 0, -1, ${rowCount}) AS row_count, " + ndv + " AS ndv, scaled_nulls AS null_count, ";
        String size = raw ? "bytes" : "ROUND(CAST(bytes AS DOUBLE) * ${rowCount} / NULLIF(n, 0))";
        String source = raw ? "moments" : "(SELECT moments.*, " + SCALED_NULLS + " AS scaled_nulls FROM moments) m";
        String projection = singleNonNullUnique
                ? " SELECT stats.* EXCEPT (sampled_non_null_rows" + (raw ? ", singletons" : "")
                        + "), NULL AS hot_value" + (raw ? ", singletons" : "") + " FROM stats"
                : ", " + HOT_TOP + hotProjection(raw);
        String sql = moments + "stats AS (SELECT CONCAT('${tblId}', '-', '${idxId}', '-', '${colId}') AS id, "
                + "${catalogId} AS catalog_id, ${dbId} AS db_id, ${tblId} AS tbl_id, ${idxId} AS idx_id, "
                + "'${colId}' AS col_id, NULL AS part_id, " + fields
                + "SUBSTRING(CAST(${min} AS STRING), 1, 1024) AS min, "
                + "SUBSTRING(CAST(${max} AS STRING), 1, 1024) AS max, "
                + size + " AS data_size, NOW() AS update_time, n - z AS sampled_non_null_rows"
                + (raw ? ", " + (singleNonNullUnique ? "n" : "IFNULL(f1, 0)") + " AS singletons" : "")
                + " FROM " + source + ")" + projection;
        return new StringSubstitutor(params).replace(sql);
    }
}
