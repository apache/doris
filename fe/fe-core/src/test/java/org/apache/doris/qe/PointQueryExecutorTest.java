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

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.analysis.UuidLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.LocalTablet;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.Type;
import org.apache.doris.planner.HashDistributionPruner;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PartitionColumnFilter;

import org.apache.thrift.TException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.CRC32;

public class PointQueryExecutorTest {
    @Test
    public void testCandidateBackendsShuffleDependsOnQuerySelectionOrder() {
        OlapScanNode scanNode = Mockito.mock(OlapScanNode.class);

        Mockito.when(scanNode.isScanBackendOrderBySelection()).thenReturn(false);
        Assertions.assertTrue(PointQueryExecutor.shouldShuffleCandidateBackends(scanNode));

        Mockito.when(scanNode.isScanBackendOrderBySelection()).thenReturn(true);
        Assertions.assertFalse(PointQueryExecutor.shouldShuffleCandidateBackends(scanNode));
    }

    @Test
    public void testRebindUuidBeforeDistributionPruning() throws Exception {
        Column column = new Column("uuid_key", Type.UUID);
        SlotRef slot = createSlot(column);
        MaterializedIndex index = new MaterializedIndex();
        for (long tabletId = 0; tabletId < 3; tabletId++) {
            index.addTablet(new LocalTablet(tabletId), null, true);
        }
        for (boolean literalOnLeft : List.of(false, true)) {
            UuidLiteral initialValue = new UuidLiteral(UuidLiteral.UUID_MIN);
            BinaryPredicate predicate = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                    literalOnLeft ? initialValue : slot, literalOnLeft ? slot : initialValue);
            OlapScanNode scanNode = mockScanNode(predicate);
            for (String value : List.of(UuidLiteral.UUID_MIN, "00000000-0000-0000-0000-000000000001",
                    "00112233445566778899AABBCCDDEEFF", "00112233-4455-6677-8899-AABBCCDDEEFF",
                    "80000000-0000-0000-0000-000000000000", UuidLiteral.UUID_MAX, UuidLiteral.UUID_MIN)) {
                PointQueryExecutor.updateScanNodeConjuncts(scanNode, Map.of("uuid_key", new StringLiteral(value)));
                Expr rebound = predicate.getChild(literalOnLeft ? 0 : 1);
                Assertions.assertInstanceOf(UuidLiteral.class, rebound);
                Assertions.assertEquals(new UuidLiteral(value), rebound);
                Assertions.assertSame(slot, predicate.getChild(literalOnLeft ? 1 : 0));

                PartitionColumnFilter filter = new PartitionColumnFilter();
                filter.setLowerBound((LiteralExpr) rebound, true);
                filter.setUpperBound((LiteralExpr) rebound, true);
                HashDistributionPruner pruner = new HashDistributionPruner(null, index, List.of(column),
                        Map.of("uuid_key", filter), 3, true);
                UUID uuid = UUID.fromString(((LiteralExpr) rebound).getStringValue());
                ByteBuffer bytes = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
                bytes.putLong(uuid.getLeastSignificantBits()).putLong(uuid.getMostSignificantBits());
                CRC32 checksum = new CRC32();
                checksum.update(bytes.array());
                Assertions.assertEquals(List.of(checksum.getValue() % 3), new ArrayList<>(pruner.prune()));
            }
        }
    }

    @Test
    public void testRebindPreservesTypedAndFixedLiterals() throws Exception {
        UuidLiteral fixedValue = new UuidLiteral(UuidLiteral.UUID_MAX);
        BinaryPredicate fixedPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                createSlot(new Column("fixed_key", Type.UUID)), fixedValue);
        BinaryPredicate predicate = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                createSlot(new Column("uuid_key", Type.UUID)), new UuidLiteral(UuidLiteral.UUID_MIN));
        OlapScanNode scanNode = mockScanNode(predicate, fixedPredicate);
        UuidLiteral boundValue = new UuidLiteral("00000000-0000-0000-0000-000000000001");
        PointQueryExecutor.updateScanNodeConjuncts(scanNode, Map.of("uuid_key", boundValue));
        Assertions.assertSame(boundValue, predicate.getChild(1));
        Assertions.assertSame(fixedValue, fixedPredicate.getChild(1));
    }

    @Test
    public void testRebindIntegerUsesColumnType() throws Exception {
        BinaryPredicate predicate = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                createSlot(new Column("int_key", Type.INT)), new IntLiteral(0, Type.INT));
        PointQueryExecutor.updateScanNodeConjuncts(mockScanNode(predicate),
                Map.of("int_key", new StringLiteral("202")));
        Assertions.assertInstanceOf(IntLiteral.class, predicate.getChild(1));
        Assertions.assertEquals(Type.INT, predicate.getChild(1).getType());
        Assertions.assertEquals(202, ((LiteralExpr) predicate.getChild(1)).getLongValue());
    }

    @Test
    public void testInvalidUuidDoesNotReplaceCachedLiteral() throws Exception {
        UuidLiteral initialValue = new UuidLiteral(UuidLiteral.UUID_MIN);
        BinaryPredicate predicate = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                createSlot(new Column("uuid_key", Type.UUID)), initialValue);
        OlapScanNode scanNode = mockScanNode(predicate);
        TException exception = Assertions.assertThrows(TException.class,
                () -> PointQueryExecutor.updateScanNodeConjuncts(scanNode,
                        Map.of("uuid_key", new StringLiteral("not-a-uuid"))));
        Assertions.assertTrue(exception.getMessage().contains("Invalid UUID format"));
        Assertions.assertSame(initialValue, predicate.getChild(1));
        PointQueryExecutor.updateScanNodeConjuncts(scanNode,
                Map.of("uuid_key", new StringLiteral(UuidLiteral.UUID_MAX)));
        Assertions.assertEquals(new UuidLiteral(UuidLiteral.UUID_MAX), predicate.getChild(1));
    }

    private SlotRef createSlot(Column column) {
        SlotDescriptor descriptor = new SlotDescriptor(new SlotId(0), new TupleId(0));
        descriptor.setColumn(column);
        descriptor.setType(column.getType());
        return new SlotRef(descriptor);
    }

    private OlapScanNode mockScanNode(Expr... predicates) {
        OlapScanNode scanNode = Mockito.mock(OlapScanNode.class);
        Mockito.when(scanNode.getConjuncts()).thenReturn(new ArrayList<>(List.of(predicates)));
        return scanNode;
    }
}
