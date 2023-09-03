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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * Represents a hive default partition.
 * For any partition predicate, the evaluate() will always return true.
 */
public class HiveDefaultPartitionEvaluator implements OnePartitionEvaluator {
    private long id;
    private List<Slot> partitionSlots;

    public HiveDefaultPartitionEvaluator(long id, List<Slot> partitionSlots) {
        this.id = id;
        this.partitionSlots = partitionSlots;
    }

    @Override
    public long getPartitionId() {
        return id;
    }

    @Override
    public List<Map<Slot, PartitionSlotInput>> getOnePartitionInputs() {
        // this is mocked result.
        PartitionSlotInput partitionSlotInput = new PartitionSlotInput(BooleanLiteral.TRUE, Maps.newHashMap());
        Map<Slot, PartitionSlotInput> map = Maps.newHashMap();
        map.put(partitionSlots.get(0), partitionSlotInput);
        List<Map<Slot, PartitionSlotInput>> list = Lists.newArrayList();
        list.add(map);
        return list;
    }

    @Override
    public Expression evaluate(Expression expression, Map<Slot, PartitionSlotInput> currentInputs) {
        return BooleanLiteral.TRUE;
    }
}
