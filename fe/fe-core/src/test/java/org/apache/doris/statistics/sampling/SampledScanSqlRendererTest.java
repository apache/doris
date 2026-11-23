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

package org.apache.doris.statistics.sampling;

import org.apache.doris.analysis.FixedBlockSampleSpec;
import org.apache.doris.statistics.analysis.AnalyzeSkipException;
import org.apache.doris.statistics.sampling.TabletSampleSpec.FixedBlockScanClause;
import org.apache.doris.statistics.sampling.TabletSampleSpec.FullScanClause;
import org.apache.doris.statistics.sampling.TabletSampleSpec.LegacyTabletScanClause;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

class SampledScanSqlRendererTest {
    @Test
    void rendersOnlySampleClausesAndChecksCompleteSqlInUtf8() {
        String index = "INDEX `索引``一`";
        var group = new FixedBlockSampleSpec.Group(0.2, 0.00001, 87, List.of(123L, 456L));
        var fixed = new FixedBlockScanClause(new FixedBlockSampleSpec(1024, Long.MAX_VALUE, List.of(group)));
        String sql = "TABLESAMPLE (FIXED_BLOCK unit_rows = 1024, GROUP (tablet_rate = 0.2, "
                + "unit_rate = 1.0E-5, max_units = 87, TABLETS (123, 456))) REPEATABLE " + Long.MAX_VALUE;
        int bytes = sql.getBytes(StandardCharsets.UTF_8).length;
        String complete = "SELECT 1 FROM tbl " + index + " " + sql;
        int totalBytes = complete.getBytes(StandardCharsets.UTF_8).length;
        Assertions.assertEquals(complete, SampledScanSqlRenderer.checked(complete, totalBytes));
        Assertions.assertThrows(AnalyzeSkipException.class, () -> SampledScanSqlRenderer.checked(complete, totalBytes - 1));
        Assertions.assertEquals(sql, SampledScanSqlRenderer.render(fixed, bytes));
        Assertions.assertThrows(AnalyzeSkipException.class, () -> SampledScanSqlRenderer.render(fixed, bytes - 1));
        Assertions.assertEquals("", SampledScanSqlRenderer.render(new FullScanClause(), bytes));
        Assertions.assertEquals("TABLET(3, 1)", SampledScanSqlRenderer.render(
                new LegacyTabletScanClause(List.of(3L, 1L)), bytes));
    }
}
