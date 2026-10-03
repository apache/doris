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

package org.apache.doris.statistics.model;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.statistics.repository.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;

/** Tests for parsing the optional mcv_histogram section of a histogram. */
public class HistogramMcvParseTest {

    private static final String PLAIN = "{\"num_buckets\":2,\"buckets\":["
            + "{\"lower\":\"0\",\"upper\":\"7\",\"ndv\":8,\"count\":8000,\"pre_sum\":0},"
            + "{\"lower\":\"8\",\"upper\":\"15\",\"ndv\":8,\"count\":8000,\"pre_sum\":8000}]}";

    private static final String WITH_MCV = "{\"num_buckets\":2,\"buckets\":["
            + "{\"lower\":\"0\",\"upper\":\"0\",\"ndv\":1,\"count\":500000,\"pre_sum\":0},"
            + "{\"lower\":\"1\",\"upper\":\"999\",\"ndv\":999,\"count\":500000,\"pre_sum\":500000}],"
            + "\"mcv_histogram\":{"
            + "\"mcv\":\"0 :0.5 ;895 :0.0005\","
            + "\"buckets\":[{\"lower\":\"1\",\"upper\":\"8\",\"ndv\":8,\"count\":4001,\"pre_sum\":0}]}}";

    // a histogram_statistics row: id, catalog_id, db_id, tbl_id, idx_id, col_id, sample_rate, buckets, update_time
    private static Histogram parse(String buckets) {
        try (MockedStatic<StatisticsUtil> mockedStatisticsUtil = Mockito.mockStatic(
                StatisticsUtil.class, Mockito.CALLS_REAL_METHODS)) {
            mockedStatisticsUtil.when(() -> StatisticsUtil.findColumn(
                    Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong(),
                    Mockito.anyLong(), Mockito.anyString()))
                    .thenReturn(new Column("k", PrimitiveType.INT));
            return Histogram.fromResultRow(new ResultRow(Lists.newArrayList(
                    "1-1-k", "0", "1", "1", "-1", "k", "0", buckets, "2026-09-07 00:00:00")));
        }
    }

    @Test
    public void testPlainFormatUnchanged() throws Exception {
        Histogram h = parse(PLAIN);
        Assertions.assertEquals(2, h.buckets.size());
        Assertions.assertTrue(h.mcv.isEmpty());
        Assertions.assertTrue(h.mcvBuckets.isEmpty());
    }

    @Test
    public void testMcvHistogramSection() throws Exception {
        Histogram h = parse(WITH_MCV);
        // the section is additional: the ordinary buckets still cover all rows
        Assertions.assertEquals(2, h.buckets.size());
        Assertions.assertEquals(500000, h.buckets.get(0).count, 0);

        Assertions.assertNotNull(h.mcv);
        Assertions.assertEquals(2, h.mcv.size(), "two point masses");
        float dominant = -1;
        for (Map.Entry<Literal, Float> e : h.mcv.entrySet()) {
            if (e.getKey().getDouble() == 0) {
                dominant = e.getValue();
            }
        }
        Assertions.assertEquals(0.5f, dominant, 1e-9, "value 0 carries half the non-null rows");

        Assertions.assertEquals(1, h.mcvBuckets.size());
        Assertions.assertEquals(1, h.mcvBuckets.get(0).lower, 0, "excluded histogram starts after the hot value");
        Assertions.assertEquals(4001, h.mcvBuckets.get(0).count, 0);
    }
}
