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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * PartitionPruner
 */
public class PartitionPruner {
    private List<OnePartitionEvaluator> partitions;
    private Expression partitionPredicate;

    /** Different type of table may have different partition prune behavior. */
    public enum PartitionTableType {
        OLAP,
        HIVE
    }

    private PartitionPruner(List<OnePartitionEvaluator> partitions, Expression partitionPredicate) {
        this.partitions = Objects.requireNonNull(partitions, "partitions cannot be null");
        this.partitionPredicate = Objects.requireNonNull(partitionPredicate, "partitionPredicate cannot be null");
    }

    public List<Long> prune() {
        return partitions.stream()
                .filter(partitionEvaluator -> !canPrune(partitionEvaluator))
                .map(OnePartitionEvaluator::getPartitionId)
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * prune partition with `partitionInfo` as parameter.
     */
    public static List<Long> prune(List<Slot> partitionSlots, Expression partitionPredicate,
            PartitionInfo partitionInfo, CascadesContext cascadesContext, PartitionTableType partitionTableType) {
        return prune(partitionSlots, partitionPredicate, partitionInfo.getIdToItem(false), cascadesContext,
                partitionTableType);
    }

    /**
     * prune partition with `idToPartitions` as parameter.
     */
    public static List<Long> prune(List<Slot> partitionSlots, Expression partitionPredicate,
            Map<Long, PartitionItem> idToPartitions, CascadesContext cascadesContext,
            PartitionTableType partitionTableType) {
        partitionPredicate = TryEliminateUninterestedPredicates.rewrite(
                partitionPredicate, ImmutableSet.copyOf(partitionSlots), cascadesContext);

        List<OnePartitionEvaluator> evaluators = idToPartitions.entrySet()
                .stream()
                .map(kv -> toPartitionEvaluator(kv.getKey(), kv.getValue(), partitionSlots, cascadesContext,
                        partitionTableType))
                .collect(ImmutableList.toImmutableList());

        PartitionPruner partitionPruner = new PartitionPruner(evaluators, partitionPredicate);
        return partitionPruner.prune();
    }

    /**
     * convert partition item to partition evaluator
     */
    public static final OnePartitionEvaluator toPartitionEvaluator(long id, PartitionItem partitionItem,
            List<Slot> partitionSlots, CascadesContext cascadesContext, PartitionTableType partitionTableType) {
        if (partitionItem instanceof ListPartitionItem) {
            if (partitionTableType == PartitionTableType.HIVE
                    && ((ListPartitionItem) partitionItem).isHiveDefaultPartition()) {
                return new HiveDefaultPartitionEvaluator(id, partitionSlots);
            } else {
                return new OneListPartitionEvaluator(
                        id, partitionSlots, (ListPartitionItem) partitionItem, cascadesContext);
            }
        } else if (partitionItem instanceof RangePartitionItem) {
            return new OneRangePartitionEvaluator(
                    id, partitionSlots, (RangePartitionItem) partitionItem, cascadesContext);
        } else {
            return new UnknownPartitionEvaluator(id, partitionItem);
        }
    }

    private boolean canPrune(OnePartitionEvaluator evaluator) {
        List<Map<Slot, PartitionSlotInput>> onePartitionInputs = evaluator.getOnePartitionInputs();
        for (Map<Slot, PartitionSlotInput> currentInputs : onePartitionInputs) {
            Expression result = evaluator.evaluate(partitionPredicate, currentInputs);
            if (!result.equals(BooleanLiteral.FALSE)) {
                return false;
            }
        }
        return true;
    }
}
