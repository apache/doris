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

package org.apache.doris.nereids.trees.plans.algebra;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingScalarFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.util.BitUtils;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Common interface for logical/physical Repeat.
 */
public interface Repeat<CHILD_PLAN extends Plan> extends Aggregate<CHILD_PLAN> {
    String COL_GROUPING_ID = "GROUPING_ID";
    String GROUPING_PREFIX = "GROUPING_PREFIX_";

    List<List<Expression>> getGroupingSets();

    List<NamedExpression> getOutputExpressions();

    @Override
    default List<Expression> getGroupByExpressions() {
        return ExpressionUtils.flatExpressions(getGroupingSets());
    }

    @Override
    default Aggregate<CHILD_PLAN> pruneOutputs(List<NamedExpression> prunedOutputs) {
        // just output reserved outputs and COL_GROUPING_ID for repeat correctly.
        ImmutableList.Builder<NamedExpression> outputBuilder
                = ImmutableList.builderWithExpectedSize(prunedOutputs.size() + 1);
        outputBuilder.addAll(prunedOutputs);
        for (NamedExpression output : getOutputExpressions()) {
            Set<VirtualSlotReference> v = output.collect(VirtualSlotReference.class::isInstance);
            if (v.stream().anyMatch(slot -> slot.getName().equals(COL_GROUPING_ID))) {
                outputBuilder.add(output);
            }
        }
        // prune groupingSets, if parent operator do not need some exprs in grouping sets, we removed it.
        // this could not lead to wrong result because be repeat other columns by normal.
        ImmutableList.Builder<List<Expression>> groupingSetsBuilder
                = ImmutableList.builderWithExpectedSize(getGroupingSets().size());
        for (List<Expression> groupingSet : getGroupingSets()) {
            ImmutableList.Builder<Expression> groupingSetBuilder
                    = ImmutableList.builderWithExpectedSize(groupingSet.size());
            for (Expression expr : groupingSet) {
                if (prunedOutputs.contains(expr)) {
                    groupingSetBuilder.add(expr);
                }
            }
            groupingSetsBuilder.add(groupingSetBuilder.build());
        }
        return withGroupSetsAndOutput(groupingSetsBuilder.build(), outputBuilder.build());
    }

    Repeat<CHILD_PLAN> withGroupSetsAndOutput(List<List<Expression>> groupingSets,
            List<NamedExpression> outputExpressions);

    static VirtualSlotReference generateVirtualGroupingIdSlot() {
        return new VirtualSlotReference(COL_GROUPING_ID, BigIntType.INSTANCE, Optional.empty(),
                GroupingSetShapes::computeVirtualGroupingIdValue);
    }

    static VirtualSlotReference generateVirtualSlotByFunction(GroupingScalarFunction function) {
        return new VirtualSlotReference(
                generateVirtualSlotName(function), function.getDataType(), Optional.of(function),
                function::computeVirtualSlotValue);
    }

    /**
     * get common grouping set expressions.
     * e.g. grouping sets((a, b, c), (b, c), (c))
     * the common expressions is [c]
     */
    default Set<Expression> getCommonGroupingSetExpressions() {
        List<List<Expression>> groupingSets = getGroupingSets();
        Iterator<List<Expression>> iterator = groupingSets.iterator();
        Set<Expression> commonGroupingExpressions = Sets.newLinkedHashSet(iterator.next());
        while (iterator.hasNext()) {
            commonGroupingExpressions =
                    Sets.intersection(commonGroupingExpressions, Sets.newLinkedHashSet(iterator.next()));
            if (commonGroupingExpressions.isEmpty()) {
                break;
            }
        }
        return commonGroupingExpressions;
    }

    /**
     * getSortedVirtualSlots: order by virtual GROUPING_ID slot first.
     */
    default Set<VirtualSlotReference> getSortedVirtualSlots() {
        Set<VirtualSlotReference> virtualSlots =
                ExpressionUtils.collect(getOutputExpressions(), VirtualSlotReference.class::isInstance);

        VirtualSlotReference virtualGroupingSetIdSlot = virtualSlots.stream()
                .filter(slot -> slot.getName().equals(COL_GROUPING_ID))
                .findFirst()
                .get();

        return ImmutableSet.<VirtualSlotReference>builder()
                .add(virtualGroupingSetIdSlot)
                .addAll(Sets.difference(virtualSlots, ImmutableSet.of(virtualGroupingSetIdSlot)))
                .build();
    }

    /**
     * computeVirtualSlotValues. backend will fill this long value to the VirtualSlotRef
     */
    default List<List<Long>> computeVirtualSlotValues(Set<VirtualSlotReference> sortedVirtualSlots) {
        GroupingSetShapes shapes = toShapes();

        return sortedVirtualSlots.stream()
                .map(virtualSlot -> virtualSlot.getComputeLongValueMethod().apply(shapes))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * flatten the grouping sets and build to a GroupingSetShapes.
     */
    default GroupingSetShapes toShapes() {
        Set<Expression> flattenGroupingSet = ImmutableSet.copyOf(ExpressionUtils.flatExpressions(getGroupingSets()));
        List<GroupingSetShape> shapes = Lists.newArrayList();
        for (List<Expression> groupingSet : getGroupingSets()) {
            List<Boolean> shouldBeErasedToNull = Lists.newArrayListWithCapacity(flattenGroupingSet.size());
            for (Expression groupingSetExpression : flattenGroupingSet) {
                shouldBeErasedToNull.add(!groupingSet.contains(groupingSetExpression));
            }
            shapes.add(new GroupingSetShape(shouldBeErasedToNull));
        }
        return new GroupingSetShapes(flattenGroupingSet, shapes);
    }

    /**
     * Generate repeat slot id list corresponding to SlotId according to the original grouping sets
     * and the actual SlotId.
     *
     * eg: groupingSets=((b, a), (a)), output=[a, b]
     * slotId in the outputTuple: [3, 4]
     *
     * return: [(4, 3), (3)]
     */
    default List<Set<Integer>> computeRepeatSlotIdList(List<Integer> slotIdList) {
        List<Set<Integer>> groupingSetsIndexesInOutput = getGroupingSetsIndexesInOutput();
        List<Set<Integer>> repeatSlotIdList = Lists.newArrayList();
        for (Set<Integer> groupingSetIndex : groupingSetsIndexesInOutput) {
            // keep order
            Set<Integer> repeatSlotId = Sets.newLinkedHashSet();
            for (Integer exprInOutputIndex : groupingSetIndex) {
                repeatSlotId.add(slotIdList.get(exprInOutputIndex));
            }
            repeatSlotIdList.add(repeatSlotId);
        }
        return repeatSlotIdList;
    }

    /**
     * getGroupingSetsIndexesInOutput: find the location where the grouping output exists
     *
     * e.g. groupingSets=((b, a), (a)), output=[a, b]
     * return ((1, 0), (1))
     */
    default List<Set<Integer>> getGroupingSetsIndexesInOutput() {
        Map<Expression, Integer> indexMap = indexesOfOutput();

        List<Set<Integer>> groupingSetsIndex = Lists.newArrayList();
        List<List<Expression>> groupingSets = getGroupingSets();
        for (List<Expression> groupingSet : groupingSets) {
            // keep the index order
            Set<Integer> groupingSetIndex = Sets.newLinkedHashSet();
            for (Expression expression : groupingSet) {
                Integer index = indexMap.get(expression);
                if (index == null) {
                    throw new AnalysisException("Can not find grouping set expression in output: " + expression);
                }
                groupingSetIndex.add(index);
            }
            groupingSetsIndex.add(groupingSetIndex);
        }

        return groupingSetsIndex;
    }

    /**
     * indexesOfOutput: get the indexes which mapping from the expression to the index in the output.
     *
     * e.g. output=[a + 1, b + 2, c]
     *
     * return the map(
     *   `a + 1`: 0,
     *   `b + 2`: 1,
     *   `c`: 2
     * )
     */
    default Map<Expression, Integer> indexesOfOutput() {
        Map<Expression, Integer> indexes = Maps.newLinkedHashMap();
        List<NamedExpression> outputs = getOutputExpressions();
        for (int i = 0; i < outputs.size(); i++) {
            NamedExpression output = outputs.get(i);
            indexes.put(output, i);
            if (output instanceof Alias) {
                indexes.put(((Alias) output).child(), i);
            }
        }
        return indexes;
    }

    static String generateVirtualSlotName(GroupingScalarFunction function) {
        String colName = function.getArguments()
                .stream()
                .map(Expression::toSql)
                .collect(Collectors.joining("_"));
        return GROUPING_PREFIX + colName;
    }

    /** GroupingSetShapes */
    class GroupingSetShapes {
        public final List<Expression> flattenGroupingSetExpression;
        public final List<GroupingSetShape> shapes;

        public GroupingSetShapes(Set<Expression> flattenGroupingSetExpression, List<GroupingSetShape> shapes) {
            this.flattenGroupingSetExpression = ImmutableList.copyOf(flattenGroupingSetExpression);
            this.shapes = ImmutableList.copyOf(shapes);
        }

        /**compute a long value that backend need to fill to the GROUPING_ID slot*/
        public List<Long> computeVirtualGroupingIdValue() {
            Set<Long> res = Sets.newLinkedHashSet();
            long k = (long) Math.pow(2, flattenGroupingSetExpression.size());
            for (GroupingSetShape shape : shapes) {
                Long val = shape.computeLongValue();
                while (res.contains(val)) {
                    val += k;
                }
                res.add(val);
            }
            return ImmutableList.copyOf(res);
        }

        public int indexOf(Expression expression) {
            return flattenGroupingSetExpression.indexOf(expression);
        }

        @Override
        public String toString() {
            String exprs = StringUtils.join(flattenGroupingSetExpression, ", ");
            return "GroupingSetShapes(flattenGroupingSetExpression=" + exprs + ", shapes=" + shapes + ")";
        }
    }

    /**
     * GroupingSetShape is used to compute which group column should be erased to null,
     * and as the computation source of grouping() / grouping_id() function.
     *
     * for example: this grouping sets will create 3 group sets
     * <pre>
     * select b, a
     * from tbl
     * group by
     * grouping sets
     * (
     *      (a, b)              -- GroupingSetShape(shouldBeErasedToNull=[false, false])
     *      (   b)              -- GroupingSetShape(shouldBeErasedToNull=[true, false])
     *      (    )              -- GroupingSetShape(shouldBeErasedToNull=[true, true])
     * )
     * </pre>
     */
    class GroupingSetShape {
        List<Boolean> shouldBeErasedToNull;

        public GroupingSetShape(List<Boolean> shouldBeErasedToNull) {
            this.shouldBeErasedToNull = shouldBeErasedToNull;
        }

        public boolean shouldBeErasedToNull(int index) {
            return shouldBeErasedToNull.get(index);
        }

        /**
         * convert shouldBeErasedToNull to bits, combine the bits to long,
         * backend will set the column to null if the bit is 1.
         *
         * The compute method, e.g.
         * shouldBeErasedToNull = [false, true, true, true] means [0, 1, 1, 1],
         * we combine the bits of big endian to long value 7.
         *
         * The example in class comment:
         * grouping sets
         * (
         *      (a, b)       -- [0, 0], to long value is 0
         *      (   b)       -- [1, 0], to long value is 2
         *      (    )       -- [1, 1], to long value is 3
         * )
         */
        public Long computeLongValue() {
            return BitUtils.bigEndianBitsToLong(shouldBeErasedToNull);
        }

        @Override
        public String toString() {
            String shouldBeErasedToNull = StringUtils.join(this.shouldBeErasedToNull, ", ");
            return "GroupingSetShape(shouldBeErasedToNull=" + shouldBeErasedToNull + ")";
        }
    }
}
