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
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class OlapScanNodeConjunctsPrunnerTest {

    @Test
    public void testPruneConjuncts() {
        TupleDescriptor tupleDescriptor = new TupleDescriptor(new TupleId(1));
        SlotDescriptor slot1 = new SlotDescriptor(new SlotId(1), tupleDescriptor);
        slot1.setType(Type.BIGINT);
        slot1.setColumn(new Column("slot1", PrimitiveType.INT));
        tupleDescriptor.addSlot(slot1);
        SlotRef slotRef1 = new SlotRef(slot1);
        SlotDescriptor slot2 = new SlotDescriptor(new SlotId(2), tupleDescriptor);
        slot2.setType(Type.BIGINT);
        slot2.setColumn(new Column("slot2", PrimitiveType.INT));
        tupleDescriptor.addSlot(slot2);
        SlotRef slotRef2 = new SlotRef(slot2);
        SlotDescriptor slot3 = new SlotDescriptor(new SlotId(3), tupleDescriptor);
        slot3.setType(Type.BIGINT);
        slot3.setColumn(new Column("slot3", PrimitiveType.INT));
        tupleDescriptor.addSlot(slot3);
        SlotRef slotRef3 = new SlotRef(slot3);

        OlapScanNodeConjunctsPrunner prunner = new OlapScanNodeConjunctsPrunner(tupleDescriptor);

        List<Column> distributionColumns = Arrays.asList(new Column("slot1", PrimitiveType.INT));

        List<Long> inElements = new ArrayList<>();
        List<Expr> inExprs = new ArrayList<>();
        for (long i = 0; i < 1000; i++) {
            inElements.add(i);
            inExprs.add(new IntLiteral(i));
        }

        Set<Integer> targetBucket = new HashSet<Integer>(){{add(8); add(21);}};
        Set<Long> targetPrunnedElements = new HashSet<>();
        Set<Long> targetTablets = new HashSet<>();

        // Build selected tablet info
        buildSelecetedTabletsInfo(prunner, distributionColumns, inElements, 1, 20,
                targetBucket, targetPrunnedElements, targetTablets);
        buildSelecetedTabletsInfo(prunner, distributionColumns, inElements, 2, 50,
                targetBucket, targetPrunnedElements, targetTablets);

        // Case: "where col_x in (...)" should prune.
        List<Expr> conjuncts1 = Arrays.asList(new InPredicate(slotRef1, inExprs, false));
        List<Expr> rets1 = prunner.pruneConjuncts(conjuncts1, targetTablets);
        Assert.assertNotEquals(conjuncts1, rets1);
        InPredicate prunedIn = (InPredicate) rets1.get(0);
        Assert.assertEquals(targetPrunnedElements.size(), prunedIn.getInElementNum());
        for (int i = 0; i < prunedIn.getInElementNum(); i++) {
            long element = ((IntLiteral) prunedIn.getChild(1 + i)).getValue();
            Assert.assertTrue(targetPrunnedElements.contains(element));
        }

        // Case: The flowing conjuncts should not pruned.
        List<Expr> conjuncts2 = new ArrayList<>();
        // case 1. Not In Predicate
        conjuncts2.clear();
        conjuncts2.add(new InPredicate(slotRef1, inExprs, true));
        Assert.assertEquals(conjuncts2, prunner.pruneConjuncts(conjuncts2, targetTablets));

        // case 2. child(0) of the in predicate is a constant expression
        conjuncts2.clear();
        conjuncts2.add(new InPredicate(new IntLiteral(1), inExprs, false));
        Assert.assertEquals(conjuncts2, prunner.pruneConjuncts(conjuncts2, targetTablets));

        // case 3. InPredicate that not bound to distribution column
        conjuncts2.clear();
        conjuncts2.add(new InPredicate(slotRef2, inExprs, false));
        Assert.assertEquals(conjuncts2, prunner.pruneConjuncts(conjuncts2, targetTablets));

        // case 4. conjuncts other than InPredicate
        conjuncts2.clear();
        conjuncts2.add(new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef1, new IntLiteral(1)));
        Assert.assertEquals(conjuncts2, prunner.pruneConjuncts(conjuncts2, targetTablets));
    }

    private void buildSelecetedTabletsInfo(OlapScanNodeConjunctsPrunner prunner,
                                           List<Column> distributionColumns,
                                           List<Long> inElements,
                                           int partitionId,
                                           int bucketNum,
                                           Set<Integer> targetBucket,
                                           Set<Long> targetPrunnedElements,
                                           Set<Long> targetTablets) {
        HashDistributionInfo hashDis = new HashDistributionInfo(bucketNum, distributionColumns);
        List<Long> tabletsInOrder = new ArrayList<>(hashDis.getBucketNum());
        for (int i = 0; i < hashDis.getBucketNum(); i++) {
            tabletsInOrder.add(partitionId * 10000 + (long) i);
        }
        List<Long> selectedTablets = new ArrayList<>();
        for (long inElemenet : inElements) {
            PartitionKey partitionKey = new PartitionKey();
            partitionKey.pushColumn(new IntLiteral(inElemenet), PrimitiveType.INT);
            int bucket = (int) ((partitionKey.getHashValue() & 0xffffffff) % hashDis.getBucketNum());
            selectedTablets.add(tabletsInOrder.get(bucket));
            if (targetBucket.contains(bucket)) {
                targetPrunnedElements.add(inElemenet);
                targetTablets.add(tabletsInOrder.get(bucket));
            }
        }
        prunner.addSelectedTablets(partitionId, hashDis, tabletsInOrder, selectedTablets);
    }
}
