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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.catalog.stream.StreamReadMode;
import org.apache.doris.mtmv.BaseTableInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class IvmRewriteContextTest {
    @Test
    void testCreateModeAllowsNullMtmv() {
        IvmRewriteContext context = IvmRewriteContext.create();

        Assertions.assertEquals(IvmRewriteContext.Mode.CREATE, context.getMode());
        Assertions.assertTrue(context.isCreate());
        Assertions.assertNull(context.getMtmv());
        Assertions.assertFalse(context.isIncludeExhaustedStreams());
    }

    @Test
    void testNormalizeModeRequiresMtmv() {
        Assertions.assertThrows(NullPointerException.class, () -> IvmRewriteContext.normalize(null));
    }

    @Test
    void testNormalizeModeCanCarryMtmv() {
        MTMV mtmv = Mockito.mock(MTMV.class);

        IvmRewriteContext context = IvmRewriteContext.normalize(mtmv);

        Assertions.assertEquals(IvmRewriteContext.Mode.NORMALIZE, context.getMode());
        Assertions.assertSame(mtmv, context.getMtmv());
        Assertions.assertFalse(context.isIncludeExhaustedStreams());
    }

    @Test
    void testFullModeAlwaysDisablesIncludeUpToDateStreams() {
        MTMV mtmv = Mockito.mock(MTMV.class);

        IvmRewriteContext context = IvmRewriteContext.full(mtmv);

        Assertions.assertEquals(IvmRewriteContext.Mode.FULL, context.getMode());
        Assertions.assertSame(mtmv, context.getMtmv());
        Assertions.assertFalse(context.isIncludeExhaustedStreams());
        Assertions.assertFalse(context.hasFullRefreshStreamScans());
        Assertions.assertFalse(context.getFullRefreshNonPctReadMode().isPresent());
    }

    @Test
    void testFullRefreshStreamScansKeepResetPartitionScopeAndNonPctMode() {
        MTMV mtmv = Mockito.mock(MTMV.class);
        BaseTableInfo resetTable = new BaseTableInfo(new TableNameInfo("ctl", "db", "reset_tbl"));
        Set<Long> resetPartitionIds = new HashSet<>(Collections.singleton(1L));
        Map<BaseTableInfo, Set<Long>> resetScopes = new HashMap<>();
        resetScopes.put(resetTable, resetPartitionIds);

        IvmRewriteContext context = IvmRewriteContext.full(mtmv,
                resetScopes, StreamReadMode.SNAPSHOT);
        resetPartitionIds.add(2L);

        Assertions.assertTrue(context.hasFullRefreshStreamScans());
        Set<Long> firstRead = context.getFullRefreshResetPartitionIds(resetTable).orElseThrow();
        Assertions.assertEquals(Collections.singleton(1L), firstRead);
        Assertions.assertEquals(StreamReadMode.SNAPSHOT,
                context.getFullRefreshNonPctReadMode().orElseThrow());

        firstRead.add(3L);
        Assertions.assertEquals(Collections.singleton(1L),
                context.getFullRefreshResetPartitionIds(resetTable).orElseThrow());
    }
}
