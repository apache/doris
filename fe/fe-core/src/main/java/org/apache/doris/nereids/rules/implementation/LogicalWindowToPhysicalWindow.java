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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.nereids.annotation.DependsRules;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.properties.OrderSpec;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.properties.RequireProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.CheckAndStandardizeWindowFunctionAndFrame;
import org.apache.doris.nereids.rules.rewrite.ExtractAndNormalizeWindowExpression;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.physical.PhysicalWindow;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Implementation rule that convert logical window to physical window, and add RequiredProperties
 *
 * step 1: compute three kinds of group:
 *      WindowFrameGroup: maintain windows with same PartitionKey, OrderKey and WindowFrame
 *      OrderKeyGroup: maintain windows with same PartitionKey and OrderKey
 *      PartitionKeyGroup: maintain windows with same PartitionKey
 * step 2: sort PartitionKeyGroup with increasing order of tupleSize
 * step 3: for every WindowFrameGroup of each SortGroup, generate one PhysicalWindow node, with common PartitionKeys,
 *  OrderKeys, unique WindowFrame and a function list.
 * step 4: for each PhysicalWindow, generate RequiredProperties, including PartitionKey for DistributionSpec,
 *  and (PartitionKey + OrderKey) for OrderSpec.
 */
@DependsRules({
    CheckAndStandardizeWindowFunctionAndFrame.class,
    ExtractAndNormalizeWindowExpression.class
})
public class LogicalWindowToPhysicalWindow extends OneImplementationRuleFactory {

    @Override
    public Rule build() {

        return RuleType.LOGICAL_WINDOW_TO_PHYSICAL_WINDOW_RULE.build(
            logicalWindow().when(LogicalWindow::isChecked).then(this::implement)
        );
    }

    /**
     *  main procedure
     */
    private PhysicalWindow implement(LogicalWindow<GroupPlan> logicalWindow) {
        // todo: remove windowExpressions from LogicalWindow and rule ExtractWindowExpressions.
        //  Only add this variable in PhysicalWindow
        List<NamedExpression> windowList = logicalWindow.getWindowExpressions();

        /////////// create three kinds of groups and compute tupleSize of each
        // windowFrameGroup
        List<WindowFrameGroup> windowFrameGroupList = createWindowFrameGroups(windowList);

        // orderKeyGroup
        List<OrderKeyGroup> orderKeyGroupList = createOrderKeyGroups(windowFrameGroupList);
        mergeOrderKeyGroups(orderKeyGroupList);

        // partitionKeyGroup
        List<PartitionKeyGroup> partitionKeyGroupList = createPartitionKeyGroups(orderKeyGroupList);
        // todo: complete the check of NumDistinctValues and open this function
        // mergePartitionKeyGroups(partitionKeyGroupList);

        // sort groups by increasing tupleSize
        sortPartitionKeyGroups(partitionKeyGroupList);

        // todo: optimize the order of partitionKeyGroups when there exist group keys below LogicalWindow

        Plan newRoot = logicalWindow.child();
        for (PartitionKeyGroup partitionKeyGroup : partitionKeyGroupList) {
            for (OrderKeyGroup orderKeyGroup : partitionKeyGroup.groups) {
                // in OrderKeyGroup, create PhysicalWindow for each WindowFrameGroup;
                // each PhysicalWindow contains the same windowExpressions as WindowFrameGroup.groups
                newRoot = createPhysicalPlanNodeForWindowFrameGroup(newRoot, orderKeyGroup);
            }
        }
        return (PhysicalWindow) newRoot;
    }

    /* ********************************************************************************************
     * create PhysicalWindow and PhysicalSort
     * ******************************************************************************************** */

    private Plan createPhysicalPlanNodeForWindowFrameGroup(Plan root, OrderKeyGroup orderKeyGroup) {
        // PhysicalSort node for orderKeys; if there exists no orderKey, newRoot = root
        // Plan newRoot = createPhysicalSortNode(root, orderKeyGroup, ctx);
        Plan newRoot = root;

        // we will not add PhysicalSort in this step, but generate it if necessary with the ability of enforcer by
        // setting RequiredProperties for PhysicalWindow
        List<OrderKey> requiredOrderKeys = generateKeysNeedToBeSorted(orderKeyGroup);

        // PhysicalWindow nodes for each different window frame, so at least one PhysicalWindow node will be added
        for (WindowFrameGroup windowFrameGroup : orderKeyGroup.groups) {
            newRoot = createPhysicalWindow(newRoot, windowFrameGroup, requiredOrderKeys);
        }

        return newRoot;
    }

    private List<OrderKey> generateKeysNeedToBeSorted(OrderKeyGroup orderKeyGroup) {
        // all keys that need to be sorted, which includes BOTH partitionKeys and orderKeys from this group
        List<OrderKey> keysNeedToBeSorted = Lists.newArrayList();

        // used as SortNode.isAnalyticSort, but it is unnecessary to add it in LogicalSort
        if (!orderKeyGroup.partitionKeys.isEmpty()) {
            keysNeedToBeSorted.addAll(orderKeyGroup.partitionKeys.stream().map(partitionKey -> {
                // todo: haven't support isNullFirst, and its default value is false(see AnalyticPlanner,
                //  but in LogicalPlanBuilder, its default value is true)
                return new OrderKey(partitionKey, true, false);
            }).collect(Collectors.toList()));
        }

        if (!orderKeyGroup.orderKeys.isEmpty()) {
            keysNeedToBeSorted.addAll(orderKeyGroup.orderKeys.stream()
                    .map(OrderExpression::getOrderKey)
                    .collect(Collectors.toList())
            );
        }
        return keysNeedToBeSorted;
    }

    private PhysicalWindow<Plan> createPhysicalWindow(Plan root, WindowFrameGroup windowFrameGroup,
                                                      List<OrderKey> requiredOrderKeys) {
        // requiredProperties:
        //  Distribution: partitionKeys
        //  Order: requiredOrderKeys
        LogicalWindow<Plan> tempLogicalWindow = new LogicalWindow<>(windowFrameGroup.groups, root);
        PhysicalWindow<Plan> physicalWindow = new PhysicalWindow<>(
                windowFrameGroup,
                RequireProperties.followParent(),
                tempLogicalWindow.getWindowExpressions(),
                tempLogicalWindow.getLogicalProperties(),
                root);

        if (windowFrameGroup.partitionKeys.isEmpty() && requiredOrderKeys.isEmpty()) {
            return physicalWindow.withRequirePropertiesAndChild(RequireProperties.of(PhysicalProperties.GATHER), root);
        }

        // todo: WFGs in the same OKG only need same RequiredProperties
        PhysicalProperties properties;
        if (windowFrameGroup.partitionKeys.isEmpty()) {
            properties = PhysicalProperties.GATHER.withOrderSpec(new OrderSpec(requiredOrderKeys));
        } else {
            properties = PhysicalProperties.createHash(
                windowFrameGroup.partitionKeys, ShuffleType.REQUIRE);
            // requiredOrderKeys contain partitionKeys, so there is no need to check if requiredOrderKeys.isEmpty()
            properties = properties.withOrderSpec(new OrderSpec(requiredOrderKeys));
        }

        RequireProperties requireProperties = RequireProperties.of(properties);
        return physicalWindow.withRequirePropertiesAndChild(requireProperties, root);
    }

    /* ********************************************************************************************
     * WindowFunctionRelatedGroups
     * ******************************************************************************************** */

    // todo: can we simplify the following three algorithms?
    private List<WindowFrameGroup> createWindowFrameGroups(List<NamedExpression> windowList) {
        List<WindowFrameGroup> windowFrameGroupList = Lists.newArrayList();
        for (int i = 0; i < windowList.size(); i++) {
            NamedExpression windowAlias = windowList.get(i);

            boolean matched = false;
            for (WindowFrameGroup windowFrameGroup : windowFrameGroupList) {
                if (windowFrameGroup.isCompatible(windowAlias)) {
                    windowFrameGroup.addGroup(windowAlias);
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                windowFrameGroupList.add(new WindowFrameGroup(windowAlias));
            }
        }

        for (WindowFrameGroup wfg : windowFrameGroupList) {
            wfg.setTupleSize(wfg.groups.stream()
                    .mapToInt(window -> window.child(0).getDataType().width())
                    .sum());
        }

        return windowFrameGroupList;
    }

    private List<OrderKeyGroup> createOrderKeyGroups(List<WindowFrameGroup> windowFrameGroupList) {
        List<OrderKeyGroup> orderKeyGroupList = Lists.newArrayList();

        for (WindowFrameGroup windowFrameGroup : windowFrameGroupList) {
            boolean matched = false;
            for (OrderKeyGroup orderKeyGroup : orderKeyGroupList) {
                // looks unreadable. An OrderKeyGroup contains a list of WFGs having same partitionKeys and orderKeys.
                // An OKG isCompatible an WFG means this WFG also has same keys.
                if (orderKeyGroup.isCompatible(windowFrameGroup)) {
                    orderKeyGroup.addGroup(windowFrameGroup);
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                orderKeyGroupList.add(new OrderKeyGroup(windowFrameGroup));
            }
        }

        for (OrderKeyGroup okg : orderKeyGroupList) {
            okg.setTupleSize(okg.getGroups().stream()
                    .mapToInt(WindowFrameGroup::getTupleSize)
                    .sum());
        }

        return orderKeyGroupList;
    }

    private List<PartitionKeyGroup> createPartitionKeyGroups(List<OrderKeyGroup> orderKeyGroupList) {
        List<PartitionKeyGroup> partitionKeyGroupList = Lists.newArrayList();

        for (OrderKeyGroup orderKeyGroup : orderKeyGroupList) {
            boolean matched = false;
            for (PartitionKeyGroup partitionKeyGroup : partitionKeyGroupList) {
                if (partitionKeyGroup.isCompatible(orderKeyGroup)) {
                    partitionKeyGroup.addGroup(orderKeyGroup);
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                partitionKeyGroupList.add(new PartitionKeyGroup(orderKeyGroup));
            }
        }

        for (PartitionKeyGroup pkg : partitionKeyGroupList) {
            pkg.setTupleSize(pkg.getGroups().stream()
                    .mapToInt(OrderKeyGroup::getTupleSize)
                    .sum());
        }

        return partitionKeyGroupList;
    }

    private void mergeOrderKeyGroups(List<OrderKeyGroup> orderKeyGroupList) {
        boolean merged = true;

        while (merged) {
            merged = false;
            for (OrderKeyGroup okg1 : orderKeyGroupList) {
                for (OrderKeyGroup okg2 : orderKeyGroupList) {
                    if (okg1 != okg2 && okg2.isPrefixOf(okg1)) {
                        // okg2 ∈ okg1, so we remove okg2, and add all the WFG in okg2 to okg1
                        okg1.absorb(okg2);
                        orderKeyGroupList.remove(okg2);
                        merged = true;
                        break;
                    }
                }
                if (merged) {
                    break;
                }
            }
        }
    }

    private void mergePartitionKeyGroups(List<PartitionKeyGroup> partitionKeyGroupList) {
        boolean merged = true;

        while (merged) {
            merged = false;
            for (PartitionKeyGroup pkg1 : partitionKeyGroupList) {
                for (PartitionKeyGroup pkg2 : partitionKeyGroupList) {
                    if (pkg1 != pkg2) {
                        // todo: check NumDistinctValues like legacy planner
                        pkg1.absorb(pkg2);
                        partitionKeyGroupList.remove(pkg2);
                        merged = true;
                        break;
                    }
                }
                if (merged) {
                    break;
                }
            }
        }
    }

    /**
     * [This comment references AnalyticPlanner.orderGroups()]
     *
     * Order partition groups (and the SortGroups, WindowFrameGroups within them) by increasing
     * totalOutputTupleSize. This minimizes the total volume of data that needs to be
     * repartitioned and sorted.
     *
     * Always move the non-partitioning partition group to the end, if it exists.
     */
    private void sortPartitionKeyGroups(List<PartitionKeyGroup> partitionKeyGroupList) {

        PartitionKeyGroup noPartition = null;
        // after createPartitionKeyGroups(), there will be at most one empty partition.
        for (PartitionKeyGroup pkg : partitionKeyGroupList) {
            if (pkg.partitionKeys.isEmpty()) {
                noPartition = pkg;
                partitionKeyGroupList.remove(noPartition);
                break;
            }
        }

        partitionKeyGroupList.sort((pkg1, pkg2) ->
                Integer.compare(pkg1.getTupleSize() - pkg2.getTupleSize(), 0)
        );

        if (noPartition != null) {
            partitionKeyGroupList.add(noPartition);
        }

        for (PartitionKeyGroup pkg : partitionKeyGroupList) {
            sortOrderKeyGroups(pkg.getGroups());
        }

    }

    private void sortOrderKeyGroups(List<OrderKeyGroup> orderKeyGroupList) {
        orderKeyGroupList.sort((okg1, okg2) ->
                Integer.compare(okg1.getTupleSize() - okg2.getTupleSize(), 0)
        );

        for (OrderKeyGroup okg : orderKeyGroupList) {
            sortWindowFrameGroups(okg.getGroups());
        }
    }

    private void sortWindowFrameGroups(List<WindowFrameGroup> windowFrameGroupList) {
        windowFrameGroupList.sort((wfg1, wfg2) ->
                Integer.compare(wfg1.getTupleSize() - wfg2.getTupleSize(), 0)
        );
    }

    /**
     * Window Functions that have common PartitionKeys, OrderKeys and WindowFrame
     */
    public static class WindowFrameGroup extends WindowFunctionRelatedGroup<NamedExpression> {

        private final Set<Expression> partitionKeys;
        private final List<OrderExpression> orderKeys;
        private final WindowFrame windowFrame;

        public WindowFrameGroup(NamedExpression windowAlias) {
            WindowExpression window = (WindowExpression) (windowAlias.child(0));
            partitionKeys = ImmutableSet.copyOf(window.getPartitionKeys());
            orderKeys = window.getOrderKeys();
            windowFrame = window.getWindowFrame().get();
            groups.add(windowAlias);
        }

        @Override
        public void addGroup(NamedExpression windowAlias) {
            groups.add(windowAlias);
        }

        @Override
        public boolean isCompatible(NamedExpression windowAlias) {
            // The comparison of PartitionKey is irrelevant to key's order,
            // but not in OrderKey' comparison.
            WindowExpression window = (WindowExpression) (windowAlias.child(0));

            Set<Expression> otherPartitionKeys = ImmutableSet.copyOf(window.getPartitionKeys());
            List<OrderExpression> otherOrderKeys = window.getOrderKeys();
            // WindowFrame will not be null after standardizing
            WindowFrame otherWindowFrame = window.getWindowFrame().get();

            // for PartitionKeys, we don't care about the order of each key, so we use isEqualCollection() to compare
            // whether these two lists have same keys; but for OrderKeys, the order of each key also make sense, so
            // we use equals() to compare both the elements and their order in these two lists.

            // CollectionUtils.isEqualCollection() is absolutely equals to Expr.equalSets()
            return CollectionUtils.isEqualCollection(partitionKeys, otherPartitionKeys)
                    && orderKeys.equals(otherOrderKeys)
                    && windowFrame.equals(otherWindowFrame);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("(Funcs=").append(groups.stream()
                    .map(Expression::toString)
                    .collect(Collectors.joining(", ", "[", "], ")));
            sb.append("PartitionKeys=").append(partitionKeys.stream()
                    .map(Expression::toString)
                    .collect(Collectors.joining(", ", "[", "], ")));
            sb.append("OrderKeys=").append(orderKeys.stream()
                    .map(OrderExpression::toString)
                    .collect(Collectors.joining(", ", "[", "], ")));
            sb.append("WindowFrame=").append(windowFrame);
            return sb + ")";
        }

        public Set<Expression> getPartitionKeys() {
            return partitionKeys;
        }

        public List<OrderExpression> getOrderKeys() {
            return orderKeys;
        }

        public WindowFrame getWindowFrame() {
            return windowFrame;
        }

    }

    /**
     * Window Functions that have common PartitionKeys and OrderKeys.
     */
    private static class OrderKeyGroup extends WindowFunctionRelatedGroup<WindowFrameGroup> {

        private final Set<Expression> partitionKeys;
        private final List<OrderExpression> orderKeys;

        public OrderKeyGroup(WindowFrameGroup windowFrameGroup) {
            partitionKeys = windowFrameGroup.partitionKeys;
            orderKeys = windowFrameGroup.orderKeys;
            groups.add(windowFrameGroup);
        }

        @Override
        public boolean isCompatible(WindowFrameGroup windowFrameGroup) {
            return CollectionUtils.isEqualCollection(partitionKeys, windowFrameGroup.partitionKeys)
                && orderKeys.equals(windowFrameGroup.orderKeys);
        }

        /**
         *  check if this okg isPrefixOf other, which means that other okg can cover this
         *  e.g. (a, b) is a prefix of (a, b, c), but is NOT a prefix of (b, a, c) or (b, c)
         */
        public boolean isPrefixOf(OrderKeyGroup otherOkg) {
            if (orderKeys.size() > otherOkg.orderKeys.size()) {
                return false;
            }
            if (!CollectionUtils.isEqualCollection(partitionKeys, otherOkg.partitionKeys)) {
                return false;
            }

            for (int i = 0; i < orderKeys.size(); i++) {
                if (!orderKeys.get(i).equals(otherOkg.orderKeys.get(i))) {
                    return false;
                }
            }
            return true;
        }

        /**
         * add all of otherOkg's WindowFrameGroups to this groupList
         */
        public void absorb(OrderKeyGroup otherOkg) {
            groups.addAll(otherOkg.groups);
            tupleSize += (otherOkg.groups.stream()
                .mapToInt(WindowFrameGroup::getTupleSize)
                .sum());
        }

    }

    /**
     * Window Functions that have common PartitionKeys.
     */
    private static class PartitionKeyGroup extends WindowFunctionRelatedGroup<OrderKeyGroup> {
        public Set<Expression> partitionKeys;

        public PartitionKeyGroup(OrderKeyGroup orderKeyGroup) {
            partitionKeys = orderKeyGroup.partitionKeys;
            groups.add(orderKeyGroup);
        }

        @Override
        public boolean isCompatible(OrderKeyGroup orderKeyGroup) {
            return CollectionUtils.isEqualCollection(partitionKeys, orderKeyGroup.partitionKeys);
        }

        /**
         * absorb other into this:
         * - partitionKeyList will be the intersection of this two group
         * - groupList(orderKeyGroup) will be the union of this two group
         */
        public void absorb(PartitionKeyGroup otherPkg) {
            partitionKeys = partitionKeys.stream()
                    .filter(expression -> otherPkg.partitionKeys.contains(expression))
                    .collect(ImmutableSet.toImmutableSet());
            groups.addAll(otherPkg.groups);
        }
    }

    private abstract static class WindowFunctionRelatedGroup<G> {

        protected List<G> groups = Lists.newArrayList();

        protected int tupleSize;

        public abstract boolean isCompatible(G group);

        public void addGroup(G group) {
            groups.add(group);
        }

        public List<G> getGroups() {
            return groups;
        }

        public int getTupleSize() {
            return tupleSize;
        }

        public void setTupleSize(int tupleSize) {
            this.tupleSize = tupleSize;
        }
    }
}
