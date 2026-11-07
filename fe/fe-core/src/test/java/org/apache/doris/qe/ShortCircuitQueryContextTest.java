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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.Planner;
import org.apache.doris.thrift.TAccessPathType;
import org.apache.doris.thrift.TColumnAccessPath;
import org.apache.doris.thrift.TDataAccessPath;
import org.apache.doris.thrift.TDescriptorTable;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TScalarType;
import org.apache.doris.thrift.TSlotDescriptor;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTypeDesc;
import org.apache.doris.thrift.TTypeNode;
import org.apache.doris.thrift.TTypeNodeType;

import org.apache.thrift.TException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
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

    @Test
    public void testDroppedTableCannotReuseCachedPlan() {
        OlapTable table = table("tbl", 10);
        ShortCircuitQueryContext context = new ShortCircuitQueryContext(table, 10, -1);
        Assertions.assertTrue(context.isReusable(connectContext(-1)));
        table.isDropped = true;
        Assertions.assertFalse(context.isReusable(connectContext(-1)));
    }

    @Test
    public void testRenamedTableCannotReuseCachedPlan() {
        OlapTable table = table("tbl", 10);
        ShortCircuitQueryContext context = new ShortCircuitQueryContext(table, 10, -1);
        Assertions.assertTrue(context.isReusable(connectContext(-1)));
        Mockito.doReturn("renamed").when(table).getName();
        Assertions.assertFalse(context.isReusable(connectContext(-1)));
    }

    @Test
    public void testEffectiveRoutingOptionsAreCapturedBeforePublication() throws Exception {
        // Planner query options may have been created before preprocessing applied SET_VAR.
        TQueryOptions options = new TQueryOptions().setEnablePruneNestedColumn(false);
        SessionVariable effective = new SessionVariable();
        effective.enablePruneNestedColumns = true;
        ShortCircuitQueryContext context = plannedContext(options, effective);
        ConnectContext connection = connectContext(-1);
        Assertions.assertTrue(context.isReusable(connection));
        // Simulate SET_VAR rollback before ExecuteCommand publishes this already-built context.
        connection.getSessionVariable().enablePruneNestedColumns = false;
        effective.enablePruneNestedColumns = false;
        Assertions.assertFalse(context.isReusable(connection));
        connection.getSessionVariable().enablePruneNestedColumns = true;
        Assertions.assertTrue(context.isReusable(connection));
        Assertions.assertTrue(connection.getSessionVariable().setVarOnce(
                SessionVariable.ENABLE_SHORT_CIRCUIT_QUERY, "false"));
        Assertions.assertFalse(context.isReusable(connection));
    }

    @Test
    public void testValidateActualSerializedDescriptor() throws Exception {
        TTypeNode type = new TTypeNode().setType(TTypeNodeType.SCALAR)
                .setScalarType(new TScalarType().setType(TPrimitiveType.INT));
        TSlotDescriptor slot = new TSlotDescriptor().setSlotType(
                new TTypeDesc().setTypes(Collections.singletonList(type)));
        TDescriptorTable descriptors = new TDescriptorTable()
                .setSlotDescriptors(Collections.singletonList(slot));
        ShortCircuitQueryContext.validateDescriptors(descriptors);
        TColumnAccessPath path = new TColumnAccessPath(TAccessPathType.DATA)
                .setDataAccessPath(new TDataAccessPath(Collections.singletonList("k")));
        slot.setAllAccessPaths(Collections.singletonList(path));
        Assertions.assertThrows(TException.class, () -> ShortCircuitQueryContext.validateDescriptors(descriptors));
        slot.setAllAccessPaths(Collections.emptyList());
        slot.setPredicateAccessPaths(Collections.singletonList(path));
        Assertions.assertThrows(TException.class, () -> ShortCircuitQueryContext.validateDescriptors(descriptors));
        slot.setPredicateAccessPaths(Collections.emptyList());
        slot.setColumnPaths(Collections.singletonList("leaf"));
        Assertions.assertThrows(TException.class, () -> ShortCircuitQueryContext.validateDescriptors(descriptors));
        slot.setColumnPaths(Collections.emptyList());
        type.setType(TTypeNodeType.STRUCT);
        Assertions.assertThrows(TException.class, () -> ShortCircuitQueryContext.validateDescriptors(descriptors));
        type.setType(TTypeNodeType.SCALAR).getScalarType().setType(TPrimitiveType.VARIANT);
        Assertions.assertThrows(TException.class, () -> ShortCircuitQueryContext.validateDescriptors(descriptors));
        type.getScalarType().setType(TPrimitiveType.INT);
        TSlotDescriptor auxiliary = slot.deepCopy().setId(1).setParent(1);
        auxiliary.getSlotType().getTypes().get(0).setType(TTypeNodeType.STRUCT);
        descriptors.setSlotDescriptors(Arrays.asList(slot, auxiliary));
        ShortCircuitQueryContext.validateDescriptors(descriptors);
        auxiliary.setParent(0);
        Assertions.assertThrows(TException.class, () -> ShortCircuitQueryContext.validateDescriptors(descriptors));
    }

    private ShortCircuitQueryContext plannedContext(TQueryOptions options, SessionVariable effective) throws Exception {
        Planner planner = Mockito.mock(Planner.class);
        Mockito.when(planner.getQueryOptions()).thenReturn(options);
        DescriptorTable descriptors = new DescriptorTable();
        descriptors.createTupleDescriptor();
        Mockito.when(planner.getDescTable()).thenReturn(descriptors);
        OlapScanNode scan = Mockito.mock(OlapScanNode.class);
        OlapTable olapTable = table("tbl", 10);
        Mockito.when(scan.getOlapTable()).thenReturn(olapTable);
        Mockito.when(scan.getTableNameInPlan()).thenReturn("tbl");
        Mockito.when(planner.getScanNodes()).thenReturn(Collections.singletonList(scan));
        return new ShortCircuitQueryContext(planner, Mockito.mock(Queriable.class), effective);
    }
}
