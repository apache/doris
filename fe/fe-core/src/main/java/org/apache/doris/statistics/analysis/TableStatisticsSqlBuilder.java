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

import org.apache.doris.statistics.StatisticConstants;

import org.apache.commons.text.StringSubstitutor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    private static final String GROUPED_SAMPLE = groupedSample(false);

    private static String groupedSample(boolean inSampleBounds) {
        return "WITH cte1 AS ("
                + "SELECT MAX(t0.`col_value`) as `col_value`, "
                + (inSampleBounds ? "MIN(t0.`col_value`) as `min_value`, " : "")
                + "${sampleFrequency:-COUNT(1)} as `count`, "
                + "SUM(`len`) as `column_length` "
                + "FROM "
                +     "(SELECT "
                +     "${subStringColName} AS `hash_value`, "
                +     "${colName} AS `col_value`, "
                +     "LENGTH(${colName}) as `len`${lengthAssert} "
                +     "FROM ${catalogName}.${dbName}.${tblName} ${index} ${sampleHints}${sampleWhere:-} ${limit}) "
                +     "as `t0` ${preAggHint} GROUP BY `t0`.`hash_value`), ";
    }

    static final String FULL_ANALYZE_TEMPLATE = "WITH cte1 AS ("
            +     "SELECT ${colPrefix:-}${colName}${lengthAssert} "
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
    // When hash buckets are subsampled for a distribution column, domainRate < 1 scales intra-bucket DUJ1
    // across unvisited buckets while clamping to [d, ${rowCount} - scaled_nulls].
    private static final String DUJ1_NDV = "LEAST(${rowCount} - scaled_nulls, GREATEST(0, IF(d = 0, 0, "
            + "GREATEST(d, CAST(ROUND((1.0 / (${domainRate:-1.0})) * CAST(n - z AS DOUBLE) * d "
            + "/ NULLIF(n - z - f1 + CAST(f1 AS DOUBLE) * (n - z) "
            + "/ NULLIF(GREATEST(CAST(n - z AS DOUBLE), ${rowCount} * (${domainRate:-1.0})), 0), 0)) "
            + "AS LARGEINT)))))";

    static String fixed(Map<String, String> params, boolean singleNonNullUnique) {
        return sampleSql(params, singleNonNullUnique, false, false);
    }

    static String sampleSql(Map<String, String> params, boolean singleNonNullUnique, boolean raw) {
        return sampleSql(params, singleNonNullUnique, raw, false);
    }

    static String sampleSql(Map<String, String> params, boolean singleNonNullUnique, boolean raw,
            boolean inSampleBounds) {
        String ndv = singleNonNullUnique ? "${rowCount}" : DUJ1_NDV;
        String moments = singleNonNullUnique
                ? "WITH sample_rows AS (SELECT ${colPrefix:-}${colName}${lengthAssert} "
                        + "FROM ${catalogName}.${dbName}.${tblName} "
                        + "${index} ${sampleHints}${sampleWhere:-}), moments AS (SELECT COUNT(1) AS n, 0 AS z, "
                        + "IFNULL(${rawDataSizeFunction}, 0) AS bytes"
                        + (inSampleBounds
                                ? ", SUBSTRING(CAST(MIN(${colName}) AS STRING), 1, 1024) AS sample_min"
                                + ", SUBSTRING(CAST(MAX(${colName}) AS STRING), 1, 1024) AS sample_max"
                                : "")
                        + " FROM sample_rows${lengthFilter:-}), "
                : groupedSample(inSampleBounds) + "moments AS (SELECT IFNULL(SUM(t1.count), 0) AS n, "
                + "IFNULL(SUM(IF(t1.col_value IS NULL, t1.count, 0)), 0) AS z, COUNT(t1.col_value) AS d, "
                + "SUM(IF(t1.col_value IS NOT NULL AND t1.count = 1, 1, 0)) AS f1, "
                + "IFNULL(${dataSizeFunction}, 0) AS bytes"
                + (inSampleBounds
                        ? ", SUBSTRING(CAST(MIN(t1.min_value) AS STRING), 1, 1024) AS sample_min"
                        + ", SUBSTRING(CAST(MAX(t1.col_value) AS STRING), 1, 1024) AS sample_max"
                        : "")
                + " FROM cte1 t1), ";
        String fields = raw
                ? "n AS row_count, " + (singleNonNullUnique ? "n" : "d") + " AS ndv, z AS null_count, "
                : "IF(n = 0, -1, ${rowCount}) AS row_count, " + ndv + " AS ndv, scaled_nulls AS null_count, ";
        String size = raw ? "bytes" : "ROUND(CAST(bytes AS DOUBLE) * ${rowCount} / NULLIF(n, 0))";
        String source = raw ? "moments" : "(SELECT moments.*, " + SCALED_NULLS + " AS scaled_nulls FROM moments) m";
        String projection = singleNonNullUnique
                ? " SELECT stats.* EXCEPT (sampled_non_null_rows" + (raw ? ", singletons" : "")
                        + "), NULL AS hot_value" + (raw ? ", singletons" : "") + " FROM stats"
                : ", " + HOT_TOP + hotProjection(raw);
        String minExpr = inSampleBounds ? "sample_min" : "SUBSTRING(CAST(${min} AS STRING), 1, 1024)";
        String maxExpr = inSampleBounds ? "sample_max" : "SUBSTRING(CAST(${max} AS STRING), 1, 1024)";
        String sql = moments + "stats AS (SELECT CONCAT('${tblId}', '-', '${idxId}', '-', '${colId}') AS id, "
                + "${catalogId} AS catalog_id, ${dbId} AS db_id, ${tblId} AS tbl_id, ${idxId} AS idx_id, "
                + "'${colId}' AS col_id, NULL AS part_id, " + fields
                + minExpr + " AS min, "
                + maxExpr + " AS max, "
                + size + " AS data_size, NOW() AS update_time, n - z AS sampled_non_null_rows"
                + (raw ? ", " + (singleNonNullUnique ? "n" : "IFNULL(f1, 0)") + " AS singletons" : "")
                + " FROM " + source + ")" + projection;
        return new StringSubstitutor(params).replace(sql);
    }

    record BatchedColumnSpec(
            int ord,
            long catalogId,
            long dbId,
            long tblId,
            long idxId,
            String colId,
            String ident,
            boolean stringType,
            int slotSize,
            boolean singleUniqueKey,
            boolean singleNonNullUnique,
            boolean raw,
            boolean census,
            boolean inSampleBounds,
            double domainRate,
            long rowCount,
            long maxStringLen) {
    }

    static String batchedSampleSql(String tableFromClause, List<BatchedColumnSpec> columns,
            int hotValueCollectCount) {
        String baseProjection = columns.stream()
                .map(c -> c.ident() + " AS `__s" + c.ord() + "`")
                .collect(Collectors.joining(", "));
        StringBuilder sql = new StringBuilder();
        sql.append("WITH `sample_base` AS (SELECT ").append(baseProjection)
                .append(" FROM ").append(tableFromClause).append(")");
        List<String> branches = new ArrayList<>(columns.size());
        for (BatchedColumnSpec c : columns) {
            int i = c.ord();
            String rawCol = "`sample_base`.`__s" + i + "`";
            String overExpr = c.maxStringLen() > 0
                    ? "IF(" + rawCol + " IS NOT NULL AND LENGTH(" + rawCol + ") > " + c.maxStringLen() + ", 1, 0)"
                    : "0";
            String safeCol = c.maxStringLen() > 0
                    ? "IF(" + rawCol + " IS NOT NULL AND LENGTH(" + rawCol + ") > " + c.maxStringLen() + ", NULL, "
                            + rawCol + ")"
                    : rawCol;
            if (c.singleNonNullUnique()) {
                String bytesExpr = c.stringType()
                        ? "IFNULL(SUM(LENGTH(" + safeCol + ")), 0)"
                        : "COUNT(1) * " + c.slotSize();
                String minExpr = c.inSampleBounds()
                        ? "SUBSTRING(CAST(MIN(" + safeCol + ") AS STRING), 1, 1024)"
                        : "CAST(NULL AS STRING)";
                String maxExpr = c.inSampleBounds()
                        ? "SUBSTRING(CAST(MAX(" + safeCol + ") AS STRING), 1, 1024)"
                        : "CAST(NULL AS STRING)";
                sql.append(", `__m").append(i).append("` AS (SELECT COUNT(1) AS n, ")
                        .append(bytesExpr).append(" AS bytes, ")
                        .append(minExpr).append(" AS sample_min, ")
                        .append(maxExpr).append(" AS sample_max, ")
                        .append("IFNULL(MAX(").append(overExpr).append("), 0) AS over_limit FROM `sample_base`)");
                String rowCountExpr = (c.raw() || c.census())
                        ? "CAST(n AS LARGEINT)"
                        : "CAST(IF(n = 0, -1, " + c.rowCount() + ") AS LARGEINT)";
                String ndvExpr = (c.raw() || c.census())
                        ? "CAST(n AS LARGEINT)"
                        : "CAST(" + c.rowCount() + " AS LARGEINT)";
                String sizeExpr = (c.raw() || c.census())
                        ? "CAST(bytes AS LARGEINT)"
                        : "CAST(IFNULL(ROUND(CAST(bytes AS DOUBLE) * " + c.rowCount() + " / NULLIF(n, 0)), 0) "
                                + "AS LARGEINT)";
                String singletonsExpr = c.raw() ? "CAST(n AS LARGEINT)" : "CAST(0 AS LARGEINT)";
                branches.add("SELECT " + i + " AS `__ord`, CAST(over_limit AS INT) AS `__over`, "
                        + "CONCAT('" + c.tblId() + "', '-', '" + c.idxId() + "', '-', '" + c.colId() + "') AS id, "
                        + "CAST(" + c.catalogId() + " AS BIGINT) AS catalog_id, "
                        + "CAST(" + c.dbId() + " AS BIGINT) AS db_id, "
                        + "CAST(" + c.tblId() + " AS BIGINT) AS tbl_id, "
                        + "CAST(" + c.idxId() + " AS BIGINT) AS idx_id, "
                        + "'" + c.colId() + "' AS col_id, CAST(NULL AS STRING) AS part_id, "
                        + rowCountExpr + " AS row_count, " + ndvExpr + " AS ndv, "
                        + "CAST(0 AS LARGEINT) AS null_count, sample_min AS min, sample_max AS max, "
                        + sizeExpr + " AS data_size, NOW() AS update_time, "
                        + "CAST(NULL AS STRING) AS hot_value, " + singletonsExpr + " AS singletons "
                        + "FROM `__m" + i + "`");
            } else {
                String hashExpr = c.stringType()
                        ? "xxhash_64(SUBSTRING(CAST(" + safeCol + " AS STRING), 1, 1024))"
                        : safeCol;
                sql.append(", `__g").append(i).append("` AS (SELECT MAX(t0.`col_value`) AS `col_value`, ")
                        .append(c.inSampleBounds() ? "MIN(t0.`col_value`) AS `min_value`, " : "")
                        .append("COUNT(1) AS `count`, ")
                        .append(c.stringType() ? "SUM(t0.`len`) AS `column_length`, " : "")
                        .append("MAX(t0.`over`) AS `over_limit` FROM (SELECT ")
                        .append(hashExpr).append(" AS `hash_value`, ")
                        .append(safeCol).append(" AS `col_value`, ")
                        .append(c.stringType() ? "LENGTH(" + safeCol + ") AS `len`, " : "")
                        .append(overExpr).append(" AS `over` FROM `sample_base`) t0 GROUP BY t0.`hash_value`)");
                String dataSizeExpr = c.stringType()
                        ? "IFNULL(SUM(t1.`column_length`), 0)"
                        : "IFNULL(SUM(t1.`count`), 0) * " + c.slotSize();
                String minExpr = c.inSampleBounds()
                        ? "SUBSTRING(CAST(MIN(t1.`min_value`) AS STRING), 1, 1024)"
                        : "CAST(NULL AS STRING)";
                String maxExpr = c.inSampleBounds()
                        ? "SUBSTRING(CAST(MAX(t1.`col_value`) AS STRING), 1, 1024)"
                        : "CAST(NULL AS STRING)";
                sql.append(", `__m").append(i).append("` AS (SELECT IFNULL(SUM(t1.`count`), 0) AS n, ")
                        .append("IFNULL(SUM(IF(t1.`col_value` IS NULL, t1.`count`, 0)), 0) AS z, ")
                        .append("COUNT(t1.`col_value`) AS d, ")
                        .append("IFNULL(SUM(IF(t1.`col_value` IS NOT NULL AND t1.`count` = 1, 1, 0)), 0) AS f1, ")
                        .append(dataSizeExpr).append(" AS bytes, ")
                        .append(minExpr).append(" AS sample_min, ")
                        .append(maxExpr).append(" AS sample_max, ")
                        .append("IFNULL(MAX(t1.`over_limit`), 0) AS over_limit FROM `__g").append(i).append("` t1)");
                String rowCountExpr;
                String ndvExpr;
                String nullsExpr;
                String sizeExpr;
                String singletonsExpr;
                String fromSource;
                if (c.raw() || c.census()) {
                    rowCountExpr = "CAST(n AS LARGEINT)";
                    ndvExpr = "CAST(d AS LARGEINT)";
                    nullsExpr = "CAST(z AS LARGEINT)";
                    sizeExpr = "CAST(bytes AS LARGEINT)";
                    singletonsExpr = c.raw() ? "CAST(f1 AS LARGEINT)" : "CAST(0 AS LARGEINT)";
                    fromSource = "`__m" + i + "`";
                } else {
                    String scaledNulls = "LEAST(" + c.rowCount() + ", GREATEST(0, CAST(ROUND("
                            + c.rowCount() + " * CAST(z AS DOUBLE) / NULLIF(n, 0)) AS LARGEINT)))";
                    String duj1Ndv = "LEAST(" + c.rowCount() + " - scaled_nulls, GREATEST(0, IF(d = 0, 0, "
                            + "GREATEST(d, CAST(ROUND((1.0 / (" + c.domainRate() + ")) * CAST(n - z AS DOUBLE) * d "
                            + "/ NULLIF(n - z - f1 + CAST(f1 AS DOUBLE) * (n - z) "
                            + "/ NULLIF(GREATEST(CAST(n - z AS DOUBLE), " + c.rowCount()
                            + " * (" + c.domainRate() + ")), 0), 0)) AS LARGEINT)))))";
                    rowCountExpr = "CAST(IF(n = 0, -1, " + c.rowCount() + ") AS LARGEINT)";
                    ndvExpr = "CAST(" + duj1Ndv + " AS LARGEINT)";
                    nullsExpr = "CAST(scaled_nulls AS LARGEINT)";
                    sizeExpr = "CAST(IFNULL(ROUND(CAST(bytes AS DOUBLE) * " + c.rowCount()
                            + " / NULLIF(n, 0)), 0) AS LARGEINT)";
                    singletonsExpr = "CAST(0 AS LARGEINT)";
                    fromSource = "(SELECT m.*, " + scaledNulls + " AS scaled_nulls FROM `__m" + i + "` m) ms";
                }
                sql.append(", `__st").append(i).append("` AS (SELECT over_limit, ")
                        .append("CONCAT('").append(c.tblId()).append("', '-', '").append(c.idxId())
                        .append("', '-', '").append(c.colId()).append("') AS id, ")
                        .append("CAST(").append(c.catalogId()).append(" AS BIGINT) AS catalog_id, ")
                        .append("CAST(").append(c.dbId()).append(" AS BIGINT) AS db_id, ")
                        .append("CAST(").append(c.tblId()).append(" AS BIGINT) AS tbl_id, ")
                        .append("CAST(").append(c.idxId()).append(" AS BIGINT) AS idx_id, ")
                        .append("'").append(c.colId()).append("' AS col_id, CAST(NULL AS STRING) AS part_id, ")
                        .append(rowCountExpr).append(" AS row_count, ")
                        .append(ndvExpr).append(" AS ndv, ")
                        .append(nullsExpr).append(" AS null_count, sample_min AS min, sample_max AS max, ")
                        .append(sizeExpr).append(" AS data_size, NOW() AS update_time, ")
                        .append("n - z AS sampled_non_null_rows, ")
                        .append(singletonsExpr).append(" AS singletons FROM ").append(fromSource).append(")")
                        .append(", `__ht").append(i).append("` AS (SELECT CAST(`col_value` AS STRING) AS column_key, ")
                        .append("`count` AS freq FROM `__g").append(i)
                        .append("` WHERE `col_value` IS NOT NULL ORDER BY freq DESC LIMIT ")
                        .append(hotValueCollectCount).append(")");
                branches.add("SELECT " + i + " AS `__ord`, CAST(MAX(stats.over_limit) AS INT) AS `__over`, "
                        + "stats.id, stats.catalog_id, stats.db_id, stats.tbl_id, stats.idx_id, "
                        + "stats.col_id, stats.part_id, stats.row_count, stats.ndv, stats.null_count, "
                        + "stats.min, stats.max, stats.data_size, stats.update_time, "
                        + "IFNULL(GROUP_CONCAT(" + HOT_ENTRY + ", \" ;\"), '') AS `hot_value`, "
                        + "MAX(stats.singletons) AS singletons "
                        + "FROM `__st" + i + "` stats LEFT JOIN `__ht" + i + "` hot_top ON TRUE GROUP BY "
                        + "stats.id, stats.catalog_id, stats.db_id, stats.tbl_id, stats.idx_id, "
                        + "stats.col_id, stats.part_id, stats.row_count, stats.ndv, stats.null_count, "
                        + "stats.min, stats.max, stats.data_size, stats.update_time");
            }
        }
        sql.append(" ").append(String.join(" UNION ALL ", branches));
        return sql.toString();
    }

    record BatchedPartitionColumnSpec(
            int ord,
            String colId,
            String ident,
            boolean stringType,
            int slotSize,
            boolean singleNonNullUnique,
            double domainRate) {
    }

    static String batchedPartitionSampleInsertSql(
            String tableFromClause,
            long catalogId,
            long dbId,
            long tblId,
            long idxId,
            String escapedPartName,
            long partId,
            boolean census,
            boolean logicalSample,
            double sampleProbability,
            long partitionPopulationRows,
            List<BatchedPartitionColumnSpec> columns) {
        String baseProjection = columns.stream()
                .map(c -> c.ident() + " AS `__s" + c.ord() + "`")
                .collect(Collectors.joining(", "));
        StringBuilder sql = new StringBuilder();
        sql.append(StatisticConstants.INSERT_INTO_PARTITION_STATS_PREFIX)
                .append("WITH `sample_base` AS (SELECT ")
                .append(baseProjection)
                .append(" FROM ")
                .append(tableFromClause)
                .append("), `__agg` AS (SELECT COUNT(1) AS `n`");
        for (BatchedPartitionColumnSpec c : columns) {
            int i = c.ord();
            String colRef = "`__s" + i + "`";
            String hllArg = c.stringType()
                    ? "SUBSTRING(CAST(" + colRef + " AS STRING), 1, 1024)"
                    : colRef;
            String bytesExpr = c.stringType()
                    ? "IFNULL(SUM(LENGTH(" + colRef + ")), 0)"
                    : "COUNT(1) * " + c.slotSize();
            sql.append(", COUNT(1) - COUNT(").append(colRef).append(") AS `z").append(i).append("`")
                    .append(", HLL_UNION(HLL_HASH(").append(hllArg).append(")) AS `hll").append(i).append("`")
                    .append(", SUBSTRING(CAST(MIN(").append(colRef).append(") AS STRING), 1, 1024) AS `min")
                    .append(i).append("`")
                    .append(", SUBSTRING(CAST(MAX(").append(colRef).append(") AS STRING), 1, 1024) AS `max")
                    .append(i).append("`")
                    .append(", ").append(bytesExpr).append(" AS `bytes").append(i).append("`");
        }
        sql.append(" FROM `sample_base`)");

        if (!census) {
            for (BatchedPartitionColumnSpec c : columns) {
                if (c.singleNonNullUnique()) {
                    continue;
                }
                int i = c.ord();
                String colRef = "`__s" + i + "`";
                String hashExpr = c.stringType()
                        ? "xxhash_64(SUBSTRING(CAST(" + colRef + " AS STRING), 1, 1024))"
                        : colRef;
                sql.append(", `__d").append(i).append("` AS (SELECT COUNT(1) AS `d`, ")
                        .append("IFNULL(SUM(IF(g.`cnt` = 1, 1, 0)), 0) AS `f1` ")
                        .append("FROM (SELECT COUNT(1) AS `cnt` FROM `sample_base` WHERE ")
                        .append(colRef).append(" IS NOT NULL GROUP BY ").append(hashExpr).append(") g)");
            }
        }

        String popExpr;
        if (census) {
            popExpr = "CAST(`__agg`.`n` AS BIGINT)";
        } else if (logicalSample) {
            popExpr = "GREATEST(`__agg`.`n`, CAST(ROUND(CAST(`__agg`.`n` AS DOUBLE) / ("
                    + sampleProbability + ")) AS BIGINT))";
        } else {
            popExpr = "GREATEST(`__agg`.`n`, CAST(" + partitionPopulationRows + " AS BIGINT))";
        }

        List<String> branches = new ArrayList<>(columns.size());
        for (BatchedPartitionColumnSpec c : columns) {
            int i = c.ord();
            String zRef = "`__agg`.`z" + i + "`";
            String hllRef = "`__agg`.`hll" + i + "`";
            String minRef = "`__agg`.`min" + i + "`";
            String maxRef = "`__agg`.`max" + i + "`";
            String bytesRef = "`__agg`.`bytes" + i + "`";
            String countExpr;
            String nullsExpr;
            String sizeExpr;
            String unseenExpr;
            String fromClause;
            if (census) {
                countExpr = popExpr;
                nullsExpr = "CAST(" + zRef + " AS BIGINT)";
                sizeExpr = "CAST(" + bytesRef + " AS BIGINT)";
                unseenExpr = "CAST(0 AS BIGINT)";
                fromClause = " FROM `__agg`";
            } else {
                countExpr = popExpr;
                nullsExpr = "LEAST(" + popExpr + ", GREATEST(0, CAST(ROUND(CAST(" + popExpr
                        + " AS DOUBLE) * CAST(" + zRef + " AS DOUBLE) / NULLIF(`__agg`.`n`, 0)) AS BIGINT)))";
                sizeExpr = "GREATEST(0, CAST(ROUND(CAST(" + bytesRef + " AS DOUBLE) * CAST("
                        + popExpr + " AS DOUBLE) / NULLIF(`__agg`.`n`, 0)) AS BIGINT))";
                if (c.singleNonNullUnique()) {
                    unseenExpr = "GREATEST(0, CAST(" + popExpr + " - HLL_CARDINALITY(" + hllRef + ") AS BIGINT))";
                    fromClause = " FROM `__agg` WHERE `__agg`.`n` > 0";
                } else {
                    String dRef = "`__d" + i + "`.`d`";
                    String f1Ref = "`__d" + i + "`.`f1`";
                    String nonNullSample = "(`__agg`.`n` - " + zRef + ")";
                    String estNdv = "LEAST(" + popExpr + " - (" + nullsExpr + "), GREATEST(0, IF(" + dRef + " = 0, 0, "
                            + "GREATEST(" + dRef + ", CAST(ROUND((1.0 / (" + c.domainRate() + ")) * CAST("
                            + nonNullSample + " AS DOUBLE) * " + dRef
                            + " / NULLIF(" + nonNullSample + " - " + f1Ref + " + CAST(" + f1Ref + " AS DOUBLE) * "
                            + nonNullSample + " / NULLIF(GREATEST(CAST(" + nonNullSample + " AS DOUBLE), CAST("
                            + popExpr + " AS DOUBLE) * (" + c.domainRate() + ")), 0), 0)) AS BIGINT)))))";
                    unseenExpr = "GREATEST(0, CAST((" + estNdv + ") - HLL_CARDINALITY(" + hllRef + ") AS BIGINT))";
                    fromClause = " FROM `__agg` CROSS JOIN `__d" + i + "` WHERE `__agg`.`n` > 0";
                }
            }
            branches.add("SELECT '" + catalogId + "' AS `catalog_id`, "
                    + "'" + dbId + "' AS `db_id`, "
                    + "'" + tblId + "' AS `tbl_id`, "
                    + "'" + idxId + "' AS `idx_id`, "
                    + "'" + escapedPartName + "' AS `part_name`, "
                    + "CAST(" + partId + " AS BIGINT) AS `part_id`, "
                    + "'" + c.colId() + "' AS `col_id`, "
                    + countExpr + " AS `count`, "
                    + hllRef + " AS `ndv`, "
                    + nullsExpr + " AS `null_count`, "
                    + minRef + " AS `min`, "
                    + maxRef + " AS `max`, "
                    + sizeExpr + " AS `data_size_in_bytes`, "
                    + "NOW() AS `update_time`, "
                    + unseenExpr + " AS `unseen_ndv`"
                    + fromClause);
        }
        sql.append(" ").append(String.join(" UNION ALL ", branches));
        return sql.toString();
    }

    record BatchedExternalColumnSpec(
            int ord,
            long catalogId,
            long dbId,
            long tblId,
            String colId,
            String ident,
            boolean stringType,
            int slotSize,
            boolean linearEstimator,
            long maxStringLen) {
    }

    static String batchedExternalSampleSql(String tableFromClause, List<BatchedExternalColumnSpec> columns,
            double scaleFactor, int hotValueCollectCount) {
        String baseProjection = columns.stream()
                .map(c -> c.ident() + " AS `__s" + c.ord() + "`")
                .collect(Collectors.joining(", "));
        StringBuilder sql = new StringBuilder();
        sql.append("WITH `sample_base` AS (SELECT ").append(baseProjection)
                .append(" FROM ").append(tableFromClause).append(")");
        List<String> branches = new ArrayList<>(columns.size());
        for (BatchedExternalColumnSpec c : columns) {
            int i = c.ord();
            String rawCol = "`sample_base`.`__s" + i + "`";
            String overExpr = c.maxStringLen() > 0
                    ? "IF(" + rawCol + " IS NOT NULL AND LENGTH(" + rawCol + ") > " + c.maxStringLen() + ", 1, 0)"
                    : "0";
            String safeCol = c.maxStringLen() > 0
                    ? "IF(" + rawCol + " IS NOT NULL AND LENGTH(" + rawCol + ") > " + c.maxStringLen() + ", NULL, "
                            + rawCol + ")"
                    : rawCol;
            if (c.linearEstimator()) {
                String bytesExpr = c.stringType()
                        ? "IFNULL(SUM(LENGTH(" + safeCol + ")), 0)"
                        : "COUNT(1) * " + c.slotSize();
                sql.append(", `__m").append(i).append("` AS (SELECT COUNT(1) AS n, ")
                        .append("IFNULL(SUM(CASE WHEN ").append(safeCol)
                        .append(" IS NULL THEN 1 ELSE 0 END), 0) AS z, ")
                        .append("NDV(").append(safeCol).append(") AS d, ")
                        .append(bytesExpr).append(" AS bytes, ")
                        .append("IFNULL(MAX(").append(overExpr).append("), 0) AS over_limit FROM `sample_base`)");
                String scaledRows = "CAST(ROUND(n * " + scaleFactor + ") AS LARGEINT)";
                String scaledNulls = "LEAST(" + scaledRows + ", GREATEST(0, CAST(ROUND(z * "
                        + scaleFactor + ") AS LARGEINT)))";
                String scaledNdv = "LEAST(GREATEST(0, " + scaledRows + " - (" + scaledNulls + ")), "
                        + "GREATEST(0, CAST(ROUND(d * " + scaleFactor + ") AS LARGEINT)))";
                String scaledSize = "GREATEST(0, CAST(ROUND(bytes * " + scaleFactor + ") AS LARGEINT))";
                branches.add("SELECT " + i + " AS `__ord`, CAST(over_limit AS INT) AS `__over`, "
                        + "CONCAT('" + c.tblId() + "', '--1-', '" + c.colId() + "') AS id, "
                        + "CAST(" + c.catalogId() + " AS BIGINT) AS catalog_id, "
                        + "CAST(" + c.dbId() + " AS BIGINT) AS db_id, "
                        + "CAST(" + c.tblId() + " AS BIGINT) AS tbl_id, "
                        + "CAST(-1 AS BIGINT) AS idx_id, "
                        + "'" + c.colId() + "' AS col_id, CAST(NULL AS STRING) AS part_id, "
                        + scaledRows + " AS row_count, " + scaledNdv + " AS ndv, "
                        + scaledNulls + " AS null_count, CAST(NULL AS STRING) AS min, CAST(NULL AS STRING) AS max, "
                        + scaledSize + " AS data_size, NOW() AS update_time, "
                        + "CAST(NULL AS STRING) AS hot_value FROM `__m" + i + "`");
            } else {
                String hashExpr = c.stringType()
                        ? "xxhash_64(SUBSTRING(CAST(" + safeCol + " AS STRING), 1, 1024))"
                        : safeCol;
                sql.append(", `__g").append(i).append("` AS (SELECT MAX(t0.`col_value`) AS `col_value`, ")
                        .append("COUNT(1) AS `count`, ")
                        .append(c.stringType() ? "SUM(t0.`len`) AS `column_length`, " : "")
                        .append("MAX(t0.`over`) AS `over_limit` FROM (SELECT ")
                        .append(hashExpr).append(" AS `hash_value`, ")
                        .append(safeCol).append(" AS `col_value`, ")
                        .append(c.stringType() ? "LENGTH(" + safeCol + ") AS `len`, " : "")
                        .append(overExpr).append(" AS `over` FROM `sample_base`) t0 GROUP BY t0.`hash_value`)");
                String dataSizeExpr = c.stringType()
                        ? "IFNULL(SUM(t1.`column_length`), 0)"
                        : "IFNULL(SUM(t1.`count`), 0) * " + c.slotSize();
                sql.append(", `__m").append(i).append("` AS (SELECT IFNULL(SUM(t1.`count`), 0) AS n, ")
                        .append("IFNULL(SUM(IF(t1.`col_value` IS NULL, t1.`count`, 0)), 0) AS z, ")
                        .append("COUNT(t1.`col_value`) AS d, ")
                        .append("IFNULL(SUM(IF(t1.`col_value` IS NOT NULL AND t1.`count` = 1, 1, 0)), 0) AS f1, ")
                        .append(dataSizeExpr).append(" AS bytes, ")
                        .append("IFNULL(MAX(t1.`over_limit`), 0) AS over_limit FROM `__g").append(i).append("` t1)");
                String scaledRows = "CAST(ROUND(n * " + scaleFactor + ") AS LARGEINT)";
                String scaledNulls = "LEAST(scaled_rows, GREATEST(0, CAST(ROUND(z * "
                        + scaleFactor + ") AS LARGEINT)))";
                String duj1Ndv = "LEAST(GREATEST(0, scaled_rows - scaled_nulls), "
                        + "IF(d = 0, 0, GREATEST(d, CAST(ROUND(CAST(n - z AS DOUBLE) * d "
                        + "/ NULLIF(n - z - f1 + CAST(f1 AS DOUBLE) * (n - z) "
                        + "/ NULLIF(GREATEST(CAST(n - z AS DOUBLE), "
                        + "CAST(GREATEST(0, scaled_rows - scaled_nulls) AS DOUBLE)), 0), 0)) AS LARGEINT))))";
                String scaledSize = "GREATEST(0, CAST(ROUND(bytes * " + scaleFactor + ") AS LARGEINT))";
                String fromSource = "(SELECT ms0.*, " + scaledNulls
                        + " AS scaled_nulls FROM (SELECT m.*, " + scaledRows
                        + " AS scaled_rows FROM `__m" + i + "` m) ms0) ms";
                sql.append(", `__st").append(i).append("` AS (SELECT over_limit, ")
                        .append("CONCAT('").append(c.tblId()).append("', '--1-', '").append(c.colId())
                        .append("') AS id, ")
                        .append("CAST(").append(c.catalogId()).append(" AS BIGINT) AS catalog_id, ")
                        .append("CAST(").append(c.dbId()).append(" AS BIGINT) AS db_id, ")
                        .append("CAST(").append(c.tblId()).append(" AS BIGINT) AS tbl_id, ")
                        .append("CAST(-1 AS BIGINT) AS idx_id, ")
                        .append("'").append(c.colId()).append("' AS col_id, CAST(NULL AS STRING) AS part_id, ")
                        .append("scaled_rows AS row_count, ")
                        .append("CAST(").append(duj1Ndv).append(" AS LARGEINT) AS ndv, ")
                        .append("scaled_nulls AS null_count, ")
                        .append("CAST(NULL AS STRING) AS min, CAST(NULL AS STRING) AS max, ")
                        .append(scaledSize).append(" AS data_size, NOW() AS update_time, ")
                        .append("n - z AS sampled_non_null_rows FROM ").append(fromSource).append(")")
                        .append(", `__ht").append(i).append("` AS (SELECT CAST(`col_value` AS STRING) AS column_key, ")
                        .append("`count` AS freq FROM `__g").append(i)
                        .append("` WHERE `col_value` IS NOT NULL ORDER BY freq DESC LIMIT ")
                        .append(hotValueCollectCount).append(")");
                branches.add("SELECT " + i + " AS `__ord`, CAST(MAX(stats.over_limit) AS INT) AS `__over`, "
                        + "stats.id, stats.catalog_id, stats.db_id, stats.tbl_id, stats.idx_id, "
                        + "stats.col_id, stats.part_id, stats.row_count, stats.ndv, stats.null_count, "
                        + "stats.min, stats.max, stats.data_size, stats.update_time, "
                        + "IFNULL(GROUP_CONCAT(" + HOT_ENTRY + ", \" ;\"), '') AS `hot_value` "
                        + "FROM `__st" + i + "` stats LEFT JOIN `__ht" + i + "` hot_top ON TRUE GROUP BY "
                        + "stats.id, stats.catalog_id, stats.db_id, stats.tbl_id, stats.idx_id, "
                        + "stats.col_id, stats.part_id, stats.row_count, stats.ndv, stats.null_count, "
                        + "stats.min, stats.max, stats.data_size, stats.update_time");
            }
        }
        sql.append(" ").append(String.join(" UNION ALL ", branches));
        return sql.toString();
    }
}
