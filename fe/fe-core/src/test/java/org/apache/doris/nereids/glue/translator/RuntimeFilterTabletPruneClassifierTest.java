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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.glue.translator.RuntimeFilterTabletPruneClassifier.Classification;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class RuntimeFilterTabletPruneClassifierTest {
    private static final Column DIST_COLUMN = new Column("dist_col", PrimitiveType.INT);

    @Test
    void testInFilterOnDistributionColumnSupported() {
        Classification classification = classify(TRuntimeFilterType.IN, distSlotRef(),
                hashScanNode(1, false));

        Assertions.assertTrue(classification.canPruneTablets());
        Assertions.assertTrue(classification.getUnsupportedReason().isEmpty());
    }

    @Test
    void testBloomFilterUnsupported() {
        Classification classification = classify(TRuntimeFilterType.BLOOM, distSlotRef(),
                hashScanNode(1, false));

        assertUnsupported(classification, "IN");
    }

    @Test
    void testInOrBloomFilterUnsupported() {
        // IN_OR_BLOOM may degrade to a bitmap bloom filter at runtime, whose values
        // cannot be enumerated for hashing.
        Classification classification = classify(TRuntimeFilterType.IN_OR_BLOOM, distSlotRef(),
                hashScanNode(1, false));

        assertUnsupported(classification, "IN");
    }

    @Test
    void testMinMaxFilterUnsupported() {
        Classification classification = classify(TRuntimeFilterType.MIN_MAX, distSlotRef(),
                hashScanNode(1, false));

        assertUnsupported(classification, "IN");
    }

    @Test
    void testNonOlapScanNodeUnsupported() {
        PlanNode scanNode = Mockito.mock(PlanNode.class);

        Classification classification = RuntimeFilterTabletPruneClassifier.classify(
                TRuntimeFilterType.IN, distSlotRef(), scanNode);

        assertUnsupported(classification, "OlapScanNode");
    }

    @Test
    void testRandomDistributionUnsupported() {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getDefaultDistributionInfo())
                .thenReturn(new RandomDistributionInfo(4));
        OlapScanNode scanNode = Mockito.mock(OlapScanNode.class);
        Mockito.when(scanNode.getOlapTable()).thenReturn(table);

        Classification classification = RuntimeFilterTabletPruneClassifier.classify(
                TRuntimeFilterType.IN, distSlotRef(), scanNode);

        assertUnsupported(classification, "hash");
    }

    @Test
    void testAutoBucketUnsupported() {
        Classification classification = classify(TRuntimeFilterType.IN, distSlotRef(),
                hashScanNode(1, true));

        assertUnsupported(classification, "auto bucket");
    }

    @Test
    void testMultiColumnDistributionKeyUnsupported() {
        Classification classification = classify(TRuntimeFilterType.IN, distSlotRef(),
                hashScanNode(2, false));

        assertUnsupported(classification, "single-column");
    }

    @Test
    void testNonDistributionColumnUnsupported() {
        Column otherColumn = new Column("other_col", PrimitiveType.INT);
        Classification classification = classify(TRuntimeFilterType.IN, slotRefOf(otherColumn),
                hashScanNode(1, false));

        assertUnsupported(classification, "distribution column");
    }

    @Test
    void testCastWrappedTargetUnsupported() {
        // A casted target would be hashed by BE on a value whose type/encoding
        // differs from the raw distribution column values used at load time.
        CastExpr castTarget = new CastExpr(Type.BIGINT, distSlotRef(), true);
        Classification classification = classify(TRuntimeFilterType.IN, castTarget,
                hashScanNode(1, false));

        assertUnsupported(classification, "SlotRef");
    }

    private static SlotRef distSlotRef() {
        return slotRefOf(DIST_COLUMN);
    }

    private static SlotRef slotRefOf(Column column) {
        SlotDescriptor slotDescriptor = new SlotDescriptor(new SlotId(1), new TupleId(1));
        slotDescriptor.setColumn(column);
        slotDescriptor.setType(column.getType());
        return new SlotRef(slotDescriptor);
    }

    private static OlapScanNode hashScanNode(int distributionColumnCount, boolean autoBucket) {
        ImmutableList.Builder<Column> columns = ImmutableList.builder();
        for (int i = 0; i < distributionColumnCount; i++) {
            columns.add(i == 0 ? DIST_COLUMN : new Column("dist_col_" + i, PrimitiveType.INT));
        }
        HashDistributionInfo distributionInfo =
                new HashDistributionInfo(4, autoBucket, columns.build());
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getDefaultDistributionInfo()).thenReturn(distributionInfo);
        OlapScanNode scanNode = Mockito.mock(OlapScanNode.class);
        Mockito.when(scanNode.getOlapTable()).thenReturn(table);
        Mockito.when(scanNode.getId()).thenReturn(new PlanNodeId(7));
        return scanNode;
    }

    private static Classification classify(
            TRuntimeFilterType filterType, Expr targetExpr, PlanNode scanNode) {
        return RuntimeFilterTabletPruneClassifier.classify(filterType, targetExpr, scanNode);
    }

    private static void assertUnsupported(Classification classification, String reasonKeyword) {
        Assertions.assertFalse(classification.canPruneTablets());
        Assertions.assertTrue(classification.getUnsupportedReason().contains(reasonKeyword),
                "reason should mention '" + reasonKeyword + "' but was: "
                        + classification.getUnsupportedReason());
    }
}
