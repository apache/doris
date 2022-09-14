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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;

/**
 * GroupingId Shape
 * eg:
 * select grouping_id(k1, k2) from t1 group by grouping sets((k1, k2), (k1, v1), (v2, k2));
 *
 * groupingSlot: k1, k2
 * shouldBeErasedToNull: [[false, false], [false, true], [true, false]]
 */
public class GroupingIdSlotShape extends GroupingSetShape {
    // expressions in groupingId.
    protected final List<Expression> groupingSlots;

    public GroupingIdSlotShape(List<Expression> groupingSlots, List<List<Boolean>> shouldBeErasedToNull) {
        super(shouldBeErasedToNull);
        this.groupingSlots = ImmutableList.copyOf(
                Objects.requireNonNull(groupingSlots, "groupingSlots can not be null"));
    }

    public List<Expression> getGroupingSlots() {
        return groupingSlots;
    }

    @Override
    public String toString() {
        String slots = StringUtils.join(this.groupingSlots, ", ");
        String shouldBeErasedToNull = StringUtils.join(this.shouldBeErasedToNull, ", ");
        return "GroupingIdSlotShape(groupingSlots=["
                + slots + "], shouldBeErasedToNull=[" + shouldBeErasedToNull + "])";
    }
}
