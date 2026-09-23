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

package org.apache.doris.qe;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.thrift.TStorageType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

public class ShortCircuitQueryContextTest {
    private OlapTable table(String name, int schemaVersion) {
        OlapTable table = Mockito.spy(new OlapTable());
        Mockito.doReturn(name).when(table).getName();
        Mockito.doReturn(schemaVersion).when(table).getBaseSchemaVersion();
        return table;
    }

    private ConnectContext connectContext(long fileCacheQueryLimitBytes) {
        ConnectContext ctx = new ConnectContext();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.fileCacheQueryLimitBytes = fileCacheQueryLimitBytes;
        ctx.setSessionVariable(sessionVariable);
        return ctx;
    }

    @Test
    public void testReusableRequiresSameFileCacheQueryLimitBytes() {
        ShortCircuitQueryContext context =
                new ShortCircuitQueryContext(table("tbl", 10), 10, -1);

        Assertions.assertTrue(context.isReusable(connectContext(-1)));
        Assertions.assertFalse(context.isReusable(connectContext(0)));
    }

    @Test
    public void testReusableStillChecksTableMetadata() {
        ShortCircuitQueryContext context =
                new ShortCircuitQueryContext(table("tbl", 11), 10, 0);

        Assertions.assertFalse(context.isReusable(connectContext(0)));
    }

    @Test
    public void testReusableRequiresSamePartitionTopologyVersion() {
        long baseIndexId = 2L;
        Column key = new Column("k", PrimitiveType.INT);
        key.setIsKey(true);
        List<Column> baseSchema = Collections.singletonList(key);
        OlapTable table = new OlapTable(1L, "tbl", baseSchema, KeysType.DUP_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(1));
        table.setIndexMeta(baseIndexId, "tbl", baseSchema, 10, 0, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        table.setBaseIndexId(baseIndexId);
        ShortCircuitQueryContext context = new ShortCircuitQueryContext(table, 10, -1);

        Assertions.assertTrue(context.isReusable(connectContext(-1)));
        table.addPartition(new Partition(3L, "p1",
                new MaterializedIndex(baseIndexId, MaterializedIndex.IndexState.NORMAL),
                new RandomDistributionInfo(1)));
        Assertions.assertFalse(context.isReusable(connectContext(-1)));
    }
}
