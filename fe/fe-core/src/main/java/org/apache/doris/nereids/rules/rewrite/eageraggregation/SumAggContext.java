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

package org.apache.doris.nereids.rules.rewrite.eageraggregation;

import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;

import com.google.common.collect.ImmutableList;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * SumAggContext
 */
public class SumAggContext {
    public final List<NamedExpression> aliasToBePushDown;
    public final List<EqualTo> ifConditions;
    public final List<SlotReference> ifThenSlots;
    public final List<SlotReference> groupKeys;

    public SumAggContext(List<NamedExpression> aliasToBePushDown,
            List<EqualTo> ifConditions, List<SlotReference> ifThenSlots,
            List<SlotReference> groupKeys) {
        this.aliasToBePushDown = ImmutableList.copyOf(aliasToBePushDown);
        this.ifConditions = ImmutableList.copyOf(ifConditions);
        Set<SlotReference> distinct = new HashSet<>(ifThenSlots);
        this.ifThenSlots = ImmutableList.copyOf(distinct);
        this.groupKeys = ImmutableList.copyOf(groupKeys);
    }

    public SumAggContext withIfThenSlots(List<SlotReference> ifThenSlots) {
        return new SumAggContext(this.aliasToBePushDown,
                this.ifConditions,
                ifThenSlots,
                this.groupKeys);
    }

}
