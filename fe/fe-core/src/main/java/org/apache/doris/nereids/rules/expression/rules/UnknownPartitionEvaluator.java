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

import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/** UnknownPartitionEvaluator */
public class UnknownPartitionEvaluator implements OnePartitionEvaluator {
    private final long partitionId;
    private final PartitionItem partitionItem;

    public UnknownPartitionEvaluator(long partitionId, PartitionItem partitionItem) {
        this.partitionId = partitionId;
        this.partitionItem = partitionItem;
    }

    @Override
    public long getPartitionId() {
        return partitionId;
    }

    @Override
    public List<Map<Slot, PartitionSlotInput>> getOnePartitionInputs() {
        return ImmutableList.of(ImmutableMap.of());
    }

    @Override
    public Expression evaluate(Expression expression, Map<Slot, PartitionSlotInput> currentInputs) {
        // do not prune
        return expression;
    }
}
