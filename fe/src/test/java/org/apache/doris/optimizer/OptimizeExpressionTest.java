package org.apache.doris.optimizer;

import com.google.common.collect.Lists;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.util.UnitTestUtil;
import org.apache.doris.optimizer.base.*;
import org.apache.doris.optimizer.cost.SimpleCostModel;
import org.apache.doris.optimizer.operator.OptLogicalScan;
import org.apache.doris.optimizer.rule.implementation.AggToHashAggRule;
import org.apache.doris.optimizer.rule.implementation.OlapScanRule;
import org.apache.doris.optimizer.utils.ExpressionFacotry;
import org.apache.doris.optimizer.utils.MemoStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class OptimizeExpressionTest {
    private static final Logger LOG = LogManager.getLogger(ExplorationSearchingTest.class);
    private OptColumnRefFactory columnRefFactory;
    private OlapTable olapTable;

    @Before
    public void init() {
        columnRefFactory = new OptColumnRefFactory();

        final Database db = UnitTestUtil.createDb(1, 2, 3, 4, 5, 6, 7, 8);
        final List<Table> tables = db.getTables();
        olapTable = (OlapTable) tables.get(0);
    }

    @Test
    public void testLogicalScan() {
        final OptExpression expression = ExpressionFacotry.createLogicalScanExpression(olapTable, columnRefFactory);

        LOG.info("init expr=\n{}", expression.getExplainString());
        final SearchVariable variable = new SearchVariable();
        variable.setExecuteOptimization(false);

        final OptLogicalScan scan = (OptLogicalScan) expression.getOp();
        final RequiredPhysicalProperty requiredPhysicalProperty = RequiredPhysicalProperty.createTestProperty();
        requiredPhysicalProperty.getColumns().include(scan.getOutputColumns());

        final QueryContext queryContext = new QueryContext(expression,
                requiredPhysicalProperty, null, variable);
        final Optimizer exploreOptimizer = new Optimizer(queryContext, new SimpleCostModel(), columnRefFactory);
        exploreOptimizer.addRule(OlapScanRule.INSTANCE);
        exploreOptimizer.optimize();

        final MemoStatus ms = new MemoStatus(exploreOptimizer.getMemo());
        Assert.assertEquals(true, ms.checkStatusForExplore());
        Assert.assertEquals(false, ms.checkLeakInDag());
        Assert.assertEquals(1, exploreOptimizer.getMemo().getGroups().size());
        Assert.assertEquals(1, exploreOptimizer.getMemo().getMExprs().size());

        variable.setExecuteOptimization(true);

        final Optimizer optimizer = new Optimizer(queryContext, new SimpleCostModel(), columnRefFactory);
        optimizer.addRule(OlapScanRule.INSTANCE);
        optimizer.optimize();
        final OptimizationContext rootOptContext = optimizer.getRootOptContext();
        Assert.assertEquals(true, rootOptContext.getBestCostCtx() != null);
        Assert.assertEquals(100.0, rootOptContext.getBestCostCtx().getCost().getValue(), 0.0000001);
    }

    @Test
    public void testLogicalAggregate() {
        final OptExpression scanExpression = ExpressionFacotry.createLogicalScanExpression(olapTable, columnRefFactory);
        final OptLogicalScan scan = (OptLogicalScan) scanExpression.getOp();

        final OptColumnRef groupByCandicateColumns = scan.getOutputColumns().get(0);
        final OptColumnRef groupByColumn = new OptColumnRef(
                groupByCandicateColumns.getId(), groupByCandicateColumns.getType(), groupByCandicateColumns.getName());
        final OptExpression aggExpression =
                ExpressionFacotry.createLogicalAggregateExpression(
                        Lists.newArrayList(groupByColumn),
                        Lists.newArrayList(scanExpression), columnRefFactory);

        LOG.info("init expr=\n{}", aggExpression.getExplainString());
        final SearchVariable variable = new SearchVariable();
        variable.setExecuteOptimization(false);

        final RequiredPhysicalProperty requiredPhysicalProperty = RequiredPhysicalProperty.createTestProperty();
        requiredPhysicalProperty.getColumns().include(scan.getOutputColumns());

        final QueryContext queryContext = new QueryContext(aggExpression,
                requiredPhysicalProperty, null, variable);
        final Optimizer exploreOptimizer = new Optimizer(queryContext, new SimpleCostModel(), columnRefFactory);
        exploreOptimizer.optimize();

        final MemoStatus ms = new MemoStatus(exploreOptimizer.getMemo());
        Assert.assertEquals(true, ms.checkStatusForExplore());
        Assert.assertEquals(false, ms.checkLeakInDag());
        Assert.assertEquals(2, ms.getNonItemGroups().size());
        Assert.assertEquals(3, ms.getItemGroups().size());
        Assert.assertEquals(5, exploreOptimizer.getMemo().getMExprs().size());

        final OptGroup scanGroup = exploreOptimizer.getMemo().getGroups().get(0);
        final OptLogicalProperty scanProperty = (OptLogicalProperty) scanGroup.getProperty();
        Assert.assertEquals(true,
                scanProperty.getOutputColumns().equals(requiredPhysicalProperty.getColumns()));

        variable.setExecuteOptimization(true);
        final Optimizer optimizer = new Optimizer(queryContext, new SimpleCostModel(), columnRefFactory);
        optimizer.addRule(OlapScanRule.INSTANCE);
        optimizer.addRule(AggToHashAggRule.INSTANCE);
        optimizer.optimize();
    }

}
