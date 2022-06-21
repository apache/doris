package org.apache.doris.nereids.memo;

import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.operators.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.Plans;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class MemoTest implements Plans {
    @Test
    public void testInitialize() {
        UnboundRelation unboundRelation = new UnboundRelation(Lists.newArrayList("test"));
        LogicalProject insideProject = new LogicalProject(
                ImmutableList.of(new SlotReference("name", StringType.INSTANCE, true, ImmutableList.of("test"))));
        LogicalProject rootProject = new LogicalProject(
                ImmutableList.of(new SlotReference("name", StringType.INSTANCE, true, ImmutableList.of("test"))));

        // Project -> Project -> Relation
        Plan root = plan(rootProject, plan(insideProject, plan(unboundRelation)));

        Memo memo = new Memo();
        memo.initialize(root);

        Group rootGroup = memo.getRoot();

        Assert.assertEquals(3, memo.getGroups().size());
        Assert.assertEquals(3, memo.getGroupExpressions().size());

        Assert.assertEquals(OperatorType.LOGICAL_PROJECT, rootGroup.logicalExpressionsAt(0).getOperator().getType());
        Assert.assertEquals(OperatorType.LOGICAL_PROJECT,
                rootGroup.logicalExpressionsAt(0).child(0).logicalExpressionsAt(0).getOperator().getType());
        Assert.assertEquals(OperatorType.LOGICAL_UNBOUND_RELATION,
                rootGroup.logicalExpressionsAt(0).child(0).logicalExpressionsAt(0).child(0).logicalExpressionsAt(0)
                        .getOperator().getType());
    }
}
