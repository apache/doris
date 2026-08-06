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

package org.apache.doris.planner;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.thrift.TOlapScanNode;
import org.apache.doris.thrift.TPartitionBoundary;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OlapScanNodeTest {
    // columnA in (1) hashmode=3
    @Test
    public void testHashDistributionOneUser() throws AnalysisException {

        List<Long> partitions = new ArrayList<>();
        partitions.add(new Long(0));
        partitions.add(new Long(1));
        partitions.add(new Long(2));


        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("columnA", PrimitiveType.BIGINT));

        List<Expr> inList = Lists.newArrayList();
        inList.add(new IntLiteral(1));

        Expr compareExpr = new SlotRef(new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, "db", "tableName"),
                "columnA");
        InPredicate inPredicate = new InPredicate(compareExpr, inList, false);

        PartitionColumnFilter  columnFilter = new PartitionColumnFilter();
        columnFilter.setInPredicate(inPredicate);
        Map<String, PartitionColumnFilter> filterMap = new CaseInsensitiveMap();
        filterMap.put("COLUMNA", columnFilter);

        DistributionPruner partitionPruner  = new HashDistributionPruner(
                null,
                partitions,
                columns,
                filterMap,
                3,
                true);

        Collection<Long> ids = partitionPruner.prune();
        Assert.assertEquals(ids.size(), 1);

        for (Long id : ids) {
            Assert.assertEquals((1 & 0xffffffff) % 3, id.intValue());
        }
    }

    // columnA in (1, 2 ,3, 4, 5, 6) hashmode=3
    @Test
    public void testHashPartitionManyUser() throws AnalysisException {

        List<Long> partitions = new ArrayList<>();
        partitions.add(new Long(0));
        partitions.add(new Long(1));
        partitions.add(new Long(2));

        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("columnA", PrimitiveType.BIGINT));

        List<Expr> inList = Lists.newArrayList();
        inList.add(new IntLiteral(1));
        inList.add(new IntLiteral(2));
        inList.add(new IntLiteral(3));
        inList.add(new IntLiteral(4));
        inList.add(new IntLiteral(5));
        inList.add(new IntLiteral(6));

        Expr compareExpr = new SlotRef(new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, "db", "tableName"),
                "columnA");
        InPredicate inPredicate = new InPredicate(compareExpr, inList, false);

        PartitionColumnFilter  columnFilter = new PartitionColumnFilter();
        columnFilter.setInPredicate(inPredicate);
        Map<String, PartitionColumnFilter> filterMap = Maps.newHashMap();
        filterMap.put("columnA", columnFilter);

        DistributionPruner partitionPruner  = new HashDistributionPruner(
                null,
                partitions,
                columns,
                filterMap,
                3,
                true);

        Collection<Long> ids = partitionPruner.prune();
        Assert.assertEquals(ids.size(), 3);
    }

    @Test
    public void testHashForIntLiteral() {
        { // CHECKSTYLE IGNORE THIS LINE
            PartitionKey hashKey = new PartitionKey();
            hashKey.pushColumn(new IntLiteral(1), PrimitiveType.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 1);
        } // CHECKSTYLE IGNORE THIS LINE
        { // CHECKSTYLE IGNORE THIS LINE
            PartitionKey hashKey = new PartitionKey();
            hashKey.pushColumn(new IntLiteral(2), PrimitiveType.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 0);
        } // CHECKSTYLE IGNORE THIS LINE
        { // CHECKSTYLE IGNORE THIS LINE
            PartitionKey hashKey = new PartitionKey();
            hashKey.pushColumn(new IntLiteral(3), PrimitiveType.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 0);
        } // CHECKSTYLE IGNORE THIS LINE
        { // CHECKSTYLE IGNORE THIS LINE
            PartitionKey hashKey = new PartitionKey();
            hashKey.pushColumn(new IntLiteral(4), PrimitiveType.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 1);
        } // CHECKSTYLE IGNORE THIS LINE
        { // CHECKSTYLE IGNORE THIS LINE
            PartitionKey hashKey = new PartitionKey();
            hashKey.pushColumn(new IntLiteral(5), PrimitiveType.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 2);
        } // CHECKSTYLE IGNORE THIS LINE
        { // CHECKSTYLE IGNORE THIS LINE
            PartitionKey hashKey = new PartitionKey();
            hashKey.pushColumn(new IntLiteral(6), PrimitiveType.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 2);
        } // CHECKSTYLE IGNORE THIS LINE
    }

    @Test
    public void testHasPartitionPredicateWithEquality() {
        TupleDescriptor tupleDescriptor = new TupleDescriptor(new TupleId(1));
        SlotDescriptor partitionSlot = addSlot(tupleDescriptor, 1, "p1");
        addSlot(tupleDescriptor, 2, "c1");

        List<Expr> conjuncts = Lists.newArrayList(new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new SlotRef(partitionSlot), new IntLiteral(1)));

        Assert.assertTrue(ScanNode.containsPartitionPredicate(
                Lists.newArrayList(partitionSlot.getColumn()), tupleDescriptor, conjuncts, null));
    }

    @Test
    public void testHasPartitionPredicateWithInPredicate() {
        TupleDescriptor tupleDescriptor = new TupleDescriptor(new TupleId(1));
        SlotDescriptor partitionSlot = addSlot(tupleDescriptor, 1, "p1");
        addSlot(tupleDescriptor, 2, "c1");

        List<Expr> inList = Lists.newArrayList(new IntLiteral(1), new IntLiteral(2));
        List<Expr> conjuncts = Lists.newArrayList(new InPredicate(new SlotRef(partitionSlot), inList, false));

        Assert.assertTrue(ScanNode.containsPartitionPredicate(
                Lists.newArrayList(partitionSlot.getColumn()), tupleDescriptor, conjuncts, null));
    }

    @Test
    public void testHasPartitionPredicateIgnoresNonPartitionColumn() {
        TupleDescriptor tupleDescriptor = new TupleDescriptor(new TupleId(1));
        SlotDescriptor partitionSlot = addSlot(tupleDescriptor, 1, "p1");
        SlotDescriptor nonPartitionSlot = addSlot(tupleDescriptor, 2, "c1");

        List<Expr> conjuncts = Lists.newArrayList(new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new SlotRef(nonPartitionSlot), new IntLiteral(1)));

        Assert.assertFalse(ScanNode.containsPartitionPredicate(
                Lists.newArrayList(partitionSlot.getColumn()), tupleDescriptor, conjuncts, null));
    }

    @Test
    public void testRuntimeFilterPartitionBoundariesUsePlanningSnapshot() throws AnalysisException {
        long oldTargetPartitionId = 1L;
        long afterPartitionId = 2L;
        long replacementPartitionId = 3L;
        Column partitionColumn = new Column("event_date", PrimitiveType.INT);
        RangePartitionInfo partitionInfo = new RangePartitionInfo(Lists.newArrayList(partitionColumn));
        setRangePartitionItem(partitionInfo, oldTargetPartitionId, "20260721", "20260722");
        setRangePartitionItem(partitionInfo, afterPartitionId, "20260722", "20260723");

        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getName()).thenReturn("rfpp_queue_range_fact");
        Mockito.when(table.getDistributionColumnNames()).thenReturn(Collections.emptySet());
        Mockito.when(table.getPartitionInfo()).thenReturn(partitionInfo);

        Map<Long, Partition> livePartitions = new HashMap<>();
        livePartitions.put(oldTargetPartitionId, mockPartition("p_target"));
        livePartitions.put(afterPartitionId, mockPartition("p_after"));
        Mockito.when(table.getPartition(Mockito.anyLong()))
                .thenAnswer(invocation -> livePartitions.get(invocation.getArgument(0)));
        Mockito.when(table.getPartitions()).thenAnswer(invocation -> livePartitions.values());

        TupleDescriptor tupleDescriptor = new TupleDescriptor(new TupleId(1));
        tupleDescriptor.setTable(table);
        SlotDescriptor partitionSlot = new SlotDescriptor(new SlotId(1), tupleDescriptor.getId());
        partitionSlot.setColumn(partitionColumn);
        partitionSlot.setType(partitionColumn.getType());
        tupleDescriptor.addSlot(partitionSlot);

        OlapScanNode scanNode = new OlapScanNode(
                new PlanNodeId(1), tupleDescriptor, "rfppScanNode", ScanContext.EMPTY);
        scanNode.setSelectedPartitionIds(Lists.newArrayList(oldTargetPartitionId, afterPartitionId));
        scanNode.snapshotSelectedPartitionNames();
        scanNode.snapshotPartitionBoundariesForRuntimeFilter();

        // Simulate REPLACE PARTITION after planning but before Thrift serialization.
        partitionInfo.dropPartition(oldTargetPartitionId);
        setRangePartitionItem(partitionInfo, replacementPartitionId, "20260721", "20260722");
        livePartitions.remove(oldTargetPartitionId);
        livePartitions.put(replacementPartitionId, mockPartition("p_target"));

        scanNode.snapshotPartitionBoundariesForRuntimeFilter();
        TOlapScanNode thriftScanNode = new TOlapScanNode();
        scanNode.setPartitionBoundariesForRuntimeFilter(thriftScanNode);
        List<Long> serializedPartitionIds = thriftScanNode.getPartitionBoundaries().stream()
                .map(TPartitionBoundary::getPartitionId)
                .collect(Collectors.toList());

        Assert.assertEquals(Lists.newArrayList(oldTargetPartitionId, afterPartitionId), serializedPartitionIds);

        Assert.assertEquals("p_target,p_after", scanNode.getSelectedPartitionNamesForExplain());
    }

    private Partition mockPartition(String name) {
        Partition partition = Mockito.mock(Partition.class);
        Mockito.when(partition.getName()).thenReturn(name);
        return partition;
    }

    private void setRangePartitionItem(RangePartitionInfo partitionInfo, long partitionId,
            String lowerValue, String upperValue) throws AnalysisException {
        List<Column> partitionColumns = partitionInfo.getPartitionColumns();
        PartitionKey lower = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue(lowerValue)), partitionColumns);
        PartitionKey upper = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue(upperValue)), partitionColumns);
        partitionInfo.setItem(partitionId, false, new RangePartitionItem(Range.closedOpen(lower, upper)));
    }

    private SlotDescriptor addSlot(TupleDescriptor tupleDescriptor, int slotId, String columnName) {
        SlotDescriptor slotDescriptor = new SlotDescriptor(new SlotId(slotId), tupleDescriptor.getId());
        slotDescriptor.setColumn(new Column(columnName, PrimitiveType.BIGINT));
        tupleDescriptor.addSlot(slotDescriptor);
        return slotDescriptor;
    }
}
