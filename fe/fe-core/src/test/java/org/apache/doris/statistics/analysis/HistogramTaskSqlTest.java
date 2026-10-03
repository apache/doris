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

import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests for the histogram analyze sql built by HistogramTask.
 */
public class HistogramTaskSqlTest {

    // the identifiers are quoted by doExecute, as SqlUtils.getIdentSql leaves them
    private static Map<String, String> params() {
        Map<String, String> p = new HashMap<>();
        p.put("internalDB", "`__internal_schema`");
        p.put("histogramStatTbl", "`histogram_statistics`");
        p.put("catalogId", "0");
        p.put("dbId", "10");
        p.put("tblId", "20");
        p.put("idxId", "-1");
        p.put("colId", "k");
        p.put("dbName", "`je`");
        p.put("tblName", "`l50`");
        p.put("colName", "`k`");
        p.put("maxBucketNum", "128");
        p.put("mcvCount", "10");
        p.put("sampleRate", "0");
        return p;
    }

    @Test
    public void testHotValueCountComesFromTheJob() {
        AnalysisInfo submitted = new AnalysisInfoBuilder().setHotValueCollectCount(3).build();
        Assertions.assertEquals(3, BaseAnalysisTask.getHotValueCollectCount(submitted));
        AnalysisInfo old = new AnalysisInfoBuilder().build();
        Assertions.assertEquals(SessionVariable.getHotValueCollectCount(),
                BaseAnalysisTask.getHotValueCollectCount(old));
    }

    private static int count(String s, String needle) {
        int n = 0;
        for (int i = s.indexOf(needle); i >= 0; i = s.indexOf(needle, i + needle.length())) {
            n++;
        }
        return n;
    }

    @Test
    public void testPlainModeScansTableOnce() {
        String sql = HistogramTask.buildAnalyzeSql(params(), false);
        Assertions.assertEquals(1, count(sql, "`je`.`l50`"), sql);
        Assertions.assertEquals(1, count(sql, "HISTOGRAM(`k`, 128)"), sql);
        Assertions.assertTrue(sql.contains("0 AS sample_rate"), sql);
        Assertions.assertTrue(sql.contains("HISTOGRAM(`k`, 128) AS buckets"), sql);
        Assertions.assertTrue(sql.endsWith("`je`.`l50`"), sql);
        Assertions.assertFalse(sql.contains("mcv_histogram"), sql);
        Assertions.assertFalse(sql.contains("${"), sql);
    }

    @Test
    public void testMcvModeScansTableOnceAndAddsSection() {
        String sql = HistogramTask.buildAnalyzeSql(params(), true);
        Assertions.assertEquals(1, count(sql, "`je`.`l50`"), sql);
        Assertions.assertEquals(2, count(sql, "HISTOGRAM(`k`, 128)"), sql);
        Assertions.assertEquals(4, count(sql, "FROM src"), sql);
        Assertions.assertTrue(sql.contains("LIMIT 10"), sql);
        Assertions.assertTrue(sql.contains("WHERE t.c / nn.c >= 0.0001 ORDER BY t.c DESC"), sql);
        Assertions.assertTrue(sql.contains("NOT IN (SELECT v FROM hot)"), sql);
        Assertions.assertTrue(sql.contains("JSON_INSERT(fh.h, '$.mcv_histogram', JSON_OBJECT("), sql);
        Assertions.assertFalse(sql.contains("JSON_PARSE(CONCAT"), sql);
        Assertions.assertTrue(sql.endsWith("FROM fh, mcv, excl"), sql);
        Assertions.assertFalse(sql.contains("${"), sql);
    }

    @Test
    public void testHotValueEscapingMatchesColumnStatistics() {
        // Escape ':' and ';' in hot_value encoding.
        String sql = HistogramTask.buildAnalyzeSql(params(), true);
        Assertions.assertTrue(sql.contains("REPLACE(REPLACE(CAST(hot.v AS STRING), ':', '\\\\:'), ';', '\\\\;')"), sql);
    }
}
