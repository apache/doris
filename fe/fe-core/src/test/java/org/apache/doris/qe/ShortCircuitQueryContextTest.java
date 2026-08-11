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

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Queriable;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.Planner;
import org.apache.doris.thrift.TQueryOptions;

import org.apache.thrift.TDeserializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;

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
                new ShortCircuitQueryContext(table("tbl", 10), "tbl", 10, -1);

        Assertions.assertTrue(context.isReusable(connectContext(-1)));
        Assertions.assertFalse(context.isReusable(connectContext(0)));
    }

    @Test
    public void testReusableStillChecksTableMetadata() {
        ShortCircuitQueryContext context =
                new ShortCircuitQueryContext(table("tbl", 11), "tbl", 10, 0);

        Assertions.assertFalse(context.isReusable(connectContext(0)));
    }

    @Test
    public void testSerializedQueryOptionsKeepBitmapOpCountVersion() throws Exception {
        TQueryOptions queryOptions = new SessionVariable().toThrift();
        Planner planner = Mockito.mock(Planner.class);
        Mockito.when(planner.getQueryOptions()).thenReturn(queryOptions);
        DescriptorTable descriptorTable = new DescriptorTable();
        descriptorTable.createTupleDescriptor();
        Mockito.when(planner.getDescTable()).thenReturn(descriptorTable);

        OlapScanNode scanNode = Mockito.mock(OlapScanNode.class);
        OlapTable table = table("tbl", 10);
        Mockito.when(scanNode.getPointQueryProjectList()).thenReturn(Collections.emptyList());
        Mockito.when(scanNode.getOlapTable()).thenReturn(table);
        Mockito.when(scanNode.getTableNameInPlan()).thenReturn("tbl");
        Mockito.when(planner.getScanNodes()).thenReturn(Collections.singletonList(scanNode));

        ShortCircuitQueryContext context =
                new ShortCircuitQueryContext(planner, Mockito.mock(Queriable.class));
        TQueryOptions serializedQueryOptions = new TQueryOptions();
        new TDeserializer().deserialize(serializedQueryOptions, context.serializedQueryOptions.toByteArray());

        Assertions.assertTrue(serializedQueryOptions.isSetNewVersionBitmapOpCount());
        Assertions.assertTrue(serializedQueryOptions.isNewVersionBitmapOpCount());
    }
}
