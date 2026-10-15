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

package org.apache.doris.nereids.stats;

import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;

/**
 * Unit test of the relations a summary reports: an external table is described by the token of its table,
 * so the filter above it has to report that table - an empty relation list makes the prefilter of the
 * pinned entries reject such an entry before its fingerprint is ever looked up.
 */
public class GroupStructInfoFileScanSummaryTest {

    @Test
    public void testAnExternalTableIsARelationOfTheSummary() {
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getNameWithFullQualifiers()).thenReturn("hive.db.t");
        LogicalFileScan scan = new LogicalFileScan(new RelationId(1), table,
                ImmutableList.of("hive", "db", "t"), ImmutableList.of(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());
        LogicalFilter<LogicalFileScan> filter = new LogicalFilter<>(ImmutableSet.of(), scan);

        GroupStructInfo.HboStructSummary summary = GroupStructInfo.summaryOf(new Memo(null, filter).getRoot());
        Assertions.assertEquals(ImmutableList.of("hive.db.t"), summary.getRelations());
        Assertions.assertEquals(1, summary.getScanCount());
    }
}
