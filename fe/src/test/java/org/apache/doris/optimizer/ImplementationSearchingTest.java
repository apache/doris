package org.apache.doris.optimizer;

import com.google.common.collect.Lists;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.optimizer.base.*;
import org.apache.doris.optimizer.cost.SimpleCostModel;
import org.apache.doris.optimizer.utils.MemoStatus;
import org.apache.doris.optimizer.utils.PlansStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ImplementationSearchingTest {
    private static final Logger LOG = LogManager.getLogger(ImplementationSearchingTest.class);
    private List<OptColumnRef> outputColumns;
    private OptColumnRefFactory columnRefFactory;

    @Before
    public void init() {
        outputColumns = Lists.newArrayList();
        final OptColumnRef column1 = new OptColumnRef(1, ScalarType.TINYINT, "t1");
        outputColumns.add(column1);
        columnRefFactory = new OptColumnRefFactory();
    }

    @Test
    public void testSingleJoin() {
        final OptExpression topJoin = Utils.createUtInternal(
                Utils.createUtLeaf(),
                Utils.createUtLeaf());
        LOG.info("init expr=\n{}", topJoin.getExplainString());
        final SearchVariable variable = new SearchVariable();
        variable.setExecuteOptimization(true);
        final QueryContext queryContext = new QueryContext(topJoin,
                RequiredPhysicalProperty.createTestProperty(), outputColumns, variable);
        final Optimizer optimizer = new Optimizer(queryContext, new SimpleCostModel(), columnRefFactory);
        optimizer.addRule(OptUTInternalCommutativityRule.INSTANCE);
        optimizer.addRule(OptUTInternalAssociativityRule.INSTANCE);
        optimizer.addRule(OptUTInternalImplementation.INSTANCE);
        optimizer.addRule(OptUTLeafImplementation.INSTANCE);
        optimizer.optimize();

        Assert.assertTrue(MemoStatus.checkStatusForImplement(optimizer.getMemo()));
        Assert.assertEquals(3, optimizer.getMemo().getGroups().size());
        Assert.assertEquals(4, optimizer.getMemo().getMExprs().size());
        Assert.assertEquals(1, optimizer.getMemo().getGroups().get(0).getMultiExpressions().size());
        Assert.assertEquals(1, optimizer.getMemo().getGroups().get(1).getMultiExpressions().size());
        Assert.assertEquals(2, optimizer.getMemo().getGroups().get(2).getMultiExpressions().size());
        final long planCount = PlansStatus.getPlanCount(optimizer.getRoot());
        LOG.info("Plans:" + planCount);
        Assert.assertEquals(2, planCount);

    }
}
