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

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.ListPartitionInfo;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.common.UserException;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Injectable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

public class OlapTableSinkTest {
    private static final Logger LOG = LogManager.getLogger(OlapTableSinkTest.class);

    @Injectable
    public OlapTable dstTable;

    @Before
    public void setUp() {

    }

    private TupleDescriptor getTuple() {
        DescriptorTable descTable = new DescriptorTable();
        TupleDescriptor tuple = descTable.createTupleDescriptor("DstTable");
        // k1
        SlotDescriptor k1 = descTable.addSlotDescriptor(tuple);
        k1.setColumn(new Column("k1", PrimitiveType.BIGINT));
        k1.setIsMaterialized(true);

        // k2
        SlotDescriptor k2 = descTable.addSlotDescriptor(tuple);
        k2.setColumn(new Column("k2", ScalarType.createVarchar(25)));
        k2.setIsMaterialized(true);
        // v1
        SlotDescriptor v1 = descTable.addSlotDescriptor(tuple);
        v1.setColumn(new Column("v1", ScalarType.createVarchar(25)));
        v1.setIsMaterialized(true);
        // v2
        SlotDescriptor v2 = descTable.addSlotDescriptor(tuple);
        v2.setColumn(new Column("v2", PrimitiveType.BIGINT));
        v2.setIsMaterialized(true);

        return tuple;
    }

    @Test
    public void testSinglePartition() throws UserException {
        TupleDescriptor tuple = getTuple();
        SinglePartitionInfo partInfo = new SinglePartitionInfo();
        partInfo.setReplicaAllocation(2, new ReplicaAllocation((short) 3));
        MaterializedIndex index = new MaterializedIndex(2, MaterializedIndex.IndexState.NORMAL);
        HashDistributionInfo distInfo = new HashDistributionInfo(
                2, Lists.newArrayList(new Column("k1", PrimitiveType.BIGINT)));
        Partition partition = new Partition(2, "p1", index, distInfo);

        new Expectations() {
            {
                dstTable.getIndexNumber();
                result = 1;
                dstTable.getId();
                result = 1;
                dstTable.getPartitionInfo();
                result = partInfo;
                dstTable.getPartitions();
                result = Lists.newArrayList(partition);
                dstTable.getPartition(2L);
                result = partition;
            }
        };

        dstTable.getPartitionInfo().setDataProperty(partition.getId(),
                new DataProperty(DataProperty.DEFAULT_STORAGE_MEDIUM));
        dstTable.getPartitionInfo().setIsMutable(partition.getId(), true);
        OlapTableSink sink = new OlapTableSink(dstTable, tuple, Lists.newArrayList(2L), false);
        sink.init(new TUniqueId(1, 2), 3, 4, 1000, 1, false, false, false);
        sink.complete(null);
        LOG.info("sink is {}", sink.toThrift());
        LOG.info("{}", sink.getExplainString("", TExplainLevel.NORMAL));
    }

    @Test
    public void testRangePartition(
            @Injectable RangePartitionInfo partInfo,
            @Injectable MaterializedIndex index) throws UserException {
        TupleDescriptor tuple = getTuple();

        HashDistributionInfo distInfo = new HashDistributionInfo(
                2, Lists.newArrayList(new Column("k1", PrimitiveType.BIGINT)));

        Column partKey = new Column("k2", PrimitiveType.VARCHAR);
        Partition p1 = new Partition(1, "p1", index, distInfo);
        Partition p2 = new Partition(2, "p2", index, distInfo);

        new Expectations() {
            {
                dstTable.getIndexNumber();
                result = 1;
                dstTable.getId();
                result = 1;
                dstTable.getPartitionInfo();
                result = partInfo;
                partInfo.getType();
                result = PartitionType.RANGE;
                partInfo.getPartitionColumns();
                result = Lists.newArrayList(partKey);
                dstTable.getPartitions();
                result = Lists.newArrayList(p1, p2);
                dstTable.getPartition(p1.getId());
                result = p1;
            }
        };

        OlapTableSink sink = new OlapTableSink(dstTable, tuple, Lists.newArrayList(p1.getId()), false);
        sink.init(new TUniqueId(1, 2), 3, 4, 1000, 1, false, false, false);
        try {
            sink.complete(null);
        } catch (UserException e) {
            // CHECKSTYLE IGNORE THIS LINE
        }
        LOG.info("sink is {}", sink.toThrift());
        LOG.info("{}", sink.getExplainString("", TExplainLevel.NORMAL));
    }

    @Test(expected = UserException.class)
    public void testRangeUnknownPartition(
            @Injectable RangePartitionInfo partInfo,
            @Injectable MaterializedIndex index) throws UserException {
        TupleDescriptor tuple = getTuple();

        long unknownPartId = 12345L;
        new Expectations() {
            {
                dstTable.getPartition(unknownPartId);
                result = null;
            }
        };

        OlapTableSink sink = new OlapTableSink(dstTable, tuple, Lists.newArrayList(unknownPartId), false);
        sink.init(new TUniqueId(1, 2), 3, 4, 1000, 1, false, false, false);
        sink.complete(null);
        LOG.info("sink is {}", sink.toThrift());
        LOG.info("{}", sink.getExplainString("", TExplainLevel.NORMAL));
    }

    @Test
    public void testListPartition(
            @Injectable ListPartitionInfo partInfo,
            @Injectable MaterializedIndex index) throws UserException {
        TupleDescriptor tuple = getTuple();

        HashDistributionInfo distInfo = new HashDistributionInfo(
                2, Lists.newArrayList(new Column("k1", PrimitiveType.BIGINT)));

        Column partKey = new Column("k2", PrimitiveType.VARCHAR);
        Partition p1 = new Partition(1, "p1", index, distInfo);
        Partition p2 = new Partition(2, "p2", index, distInfo);

        new Expectations() {
            {
                dstTable.getIndexNumber();
                result = 1;
                dstTable.getId();
                result = 1;
                dstTable.getPartitionInfo();
                result = partInfo;
                partInfo.getType();
                result = PartitionType.LIST;
                partInfo.getPartitionColumns();
                result = Lists.newArrayList(partKey);
                dstTable.getPartitions();
                result = Lists.newArrayList(p1, p2);
                dstTable.getPartition(p1.getId());
                result = p1;
            }
        };

        OlapTableSink sink = new OlapTableSink(dstTable, tuple, Lists.newArrayList(p1.getId()), false);
        sink.init(new TUniqueId(1, 2), 3, 4, 1000, 1, false, false, false);
        try {
            sink.complete(null);
        } catch (UserException e) {
            // CHECKSTYLE IGNORE THIS LINE
        }
        LOG.info("sink is {}", sink.toThrift());
        LOG.info("{}", sink.getExplainString("", TExplainLevel.NORMAL));
    }
}
