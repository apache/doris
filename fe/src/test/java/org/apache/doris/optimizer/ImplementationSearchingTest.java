package org.apache.doris.optimizer;

import com.google.common.collect.Lists;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.optimizer.base.*;
import org.apache.doris.optimizer.cost.SimpleCostModel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ImplementationSearchingTest {
    private static final Logger LOG = LogManager.getLogger(ImplementationSearchingTest.class);
    private OptColumnRefFactory columnRefFactory;

    @Before
    public void init() {
        columnRefFactory = new OptColumnRefFactory();
    }

    @Test
    public void testSingleJoin() {
        final List<OptColumnRef> leafOutputColumns1 = Lists.newArrayList();
        final OptColumnRef column1 = columnRefFactory.create("t1", ScalarType.INT);
        final OptColumnRef column2 = columnRefFactory.create("t2", ScalarType.INT);
        leafOutputColumns1.add(column1);
        leafOutputColumns1.add(column2);
        final List<OptColumnRef> leafOutputColumns2 = Lists.newArrayList();
        final OptColumnRef column3 = columnRefFactory.create("t3", ScalarType.INT);
        final OptColumnRef column4 = columnRefFactory.create("t4", ScalarType.INT);
        leafOutputColumns2.add(column3);
        leafOutputColumns2.add(column4);
        final OptExpression topJoin = Utils.createUtInternal(
                Utils.createUtLeaf(leafOutputColumns1),
                Utils.createUtLeaf(leafOutputColumns2));
        LOG.info("init expr=\n{}", topJoin.getExplainString());

        final SearchVariable variable = new SearchVariable();
        variable.setExecuteOptimization(true);
        final RequiredPhysicalProperty requiredPhysicalProperty = RequiredPhysicalProperty.createTestProperty();
        final List<OptColumnRef> outputColumns = Lists.newArrayList();
        outputColumns.add(column1);
        outputColumns.add(column2);
        outputColumns.add(column3);
        outputColumns.add(column4);
        requiredPhysicalProperty.getColumns().include(outputColumns);
        final QueryContext queryContext = new QueryContext(topJoin,
                requiredPhysicalProperty, outputColumns, variable);
        final Optimizer optimizer = new Optimizer(queryContext, new SimpleCostModel(), columnRefFactory);
        optimizer.addRule(OptUTInternalCommutativityRule.INSTANCE);
        optimizer.addRule(OptUTInternalAssociativityRule.INSTANCE);
        optimizer.addRule(OptUTInternalImplementation.INSTANCE);
        optimizer.addRule(OptUTLeafImplementation.INSTANCE);
        optimizer.optimize();

        final OptColumnRefSet outputColumnsSet = new OptColumnRefSet();
        outputColumnsSet.include(outputColumns);
        final UTMemoStatus checker = new UTMemoStatus(
                2, optimizer.getMemo(), optimizer.getRootOptContext(), outputColumnsSet);
        checker.checkImplementStatus();

        Assert.assertEquals(3, optimizer.getMemo().getGroups().size());
        Assert.assertEquals(8, optimizer.getMemo().getMExprs().size());
        Assert.assertEquals(2, optimizer.getMemo().getGroups().get(0).getMultiExpressions().size());
        Assert.assertEquals(2, optimizer.getMemo().getGroups().get(1).getMultiExpressions().size());
        Assert.assertEquals(4, optimizer.getMemo().getGroups().get(2).getMultiExpressions().size());
    }

    @Test
    public void testTwoJoin() {
        final List<OptColumnRef> leafOutputColumns1 = Lists.newArrayList();
        final OptColumnRef column1 = columnRefFactory.create("t1", ScalarType.INT);
        final OptColumnRef column2 = columnRefFactory.create("t2", ScalarType.INT);
        leafOutputColumns1.add(column1);
        leafOutputColumns1.add(column2);
        final List<OptColumnRef> leafOutputColumns2 = Lists.newArrayList();
        final OptColumnRef column3 = columnRefFactory.create("t3", ScalarType.INT);
        final OptColumnRef column4 = columnRefFactory.create("t4", ScalarType.INT);
        leafOutputColumns2.add(column3);
        leafOutputColumns2.add(column4);
        final OptExpression bottomJoin = Utils.createUtInternal(
                Utils.createUtLeaf(leafOutputColumns1),
                Utils.createUtLeaf(leafOutputColumns2));


        final List<OptColumnRef> leafOutputColumns3 = Lists.newArrayList();
        final OptColumnRef column5 = columnRefFactory.create("t5", ScalarType.INT);
        final OptColumnRef column6 = columnRefFactory.create("t6", ScalarType.INT);
        leafOutputColumns3.add(column5);
        leafOutputColumns3.add(column6);
        final OptExpression topJoin = Utils.createUtInternal(
                bottomJoin,
                Utils.createUtLeaf(leafOutputColumns3));
        LOG.info("init expr=\n{}", topJoin.getExplainString());

        final SearchVariable variable = new SearchVariable();
        variable.setExecuteOptimization(true);
        final RequiredPhysicalProperty requiredPhysicalProperty = RequiredPhysicalProperty.createTestProperty();
        final List<OptColumnRef> outputColumns = Lists.newArrayList();
        outputColumns.add(column1);
        outputColumns.add(column2);
        outputColumns.add(column3);
        outputColumns.add(column4);
        outputColumns.add(column5);
        outputColumns.add(column6);
        requiredPhysicalProperty.getColumns().include(outputColumns);
        final QueryContext queryContext = new QueryContext(topJoin,
                requiredPhysicalProperty, outputColumns, variable);
        final Optimizer optimizer = new Optimizer(queryContext, new SimpleCostModel(), columnRefFactory);
        optimizer.addRule(OptUTInternalCommutativityRule.INSTANCE);
        optimizer.addRule(OptUTInternalAssociativityRule.INSTANCE);
        optimizer.addRule(OptUTInternalImplementation.INSTANCE);
        optimizer.addRule(OptUTLeafImplementation.INSTANCE);
        optimizer.optimize();

        final OptColumnRefSet outputColumnsSet = new OptColumnRefSet();
        outputColumnsSet.include(outputColumns);
        final UTMemoStatus checker = new UTMemoStatus(
                3, optimizer.getMemo(), optimizer.getRootOptContext(), outputColumnsSet);
        checker.checkImplementStatus();

        Assert.assertEquals(7, optimizer.getMemo().getGroups().size());
        Assert.assertEquals(30, optimizer.getMemo().getMExprs().size());
        Assert.assertEquals(2, optimizer.getMemo().getGroups().get(0).getMultiExpressions().size());
        Assert.assertEquals(2, optimizer.getMemo().getGroups().get(1).getMultiExpressions().size());
        Assert.assertEquals(4, optimizer.getMemo().getGroups().get(2).getMultiExpressions().size());
        Assert.assertEquals(2, optimizer.getMemo().getGroups().get(3).getMultiExpressions().size());
        Assert.assertEquals(12, optimizer.getMemo().getGroups().get(4).getMultiExpressions().size());
        Assert.assertEquals(4, optimizer.getMemo().getGroups().get(5).getMultiExpressions().size());
        Assert.assertEquals(4, optimizer.getMemo().getGroups().get(6).getMultiExpressions().size());
    }

    @Test
    public void testThreeJoin() {
        final List<OptColumnRef> leafOutputColumns1 = Lists.newArrayList();
        final OptColumnRef column1 = columnRefFactory.create("t1", ScalarType.INT);
        final OptColumnRef column2 = columnRefFactory.create("t2", ScalarType.INT);
        leafOutputColumns1.add(column1);
        leafOutputColumns1.add(column2);
        final List<OptColumnRef> leafOutputColumns2 = Lists.newArrayList();
        final OptColumnRef column3 = columnRefFactory.create("t3", ScalarType.INT);
        final OptColumnRef column4 = columnRefFactory.create("t4", ScalarType.INT);
        leafOutputColumns2.add(column3);
        leafOutputColumns2.add(column4);
        final OptExpression bottomJoin = Utils.createUtInternal(
                Utils.createUtLeaf(leafOutputColumns1),
                Utils.createUtLeaf(leafOutputColumns2));


        final List<OptColumnRef> leafOutputColumns3 = Lists.newArrayList();
        final OptColumnRef column5 = columnRefFactory.create("t5", ScalarType.INT);
        final OptColumnRef column6 = columnRefFactory.create("t6", ScalarType.INT);
        leafOutputColumns3.add(column5);
        leafOutputColumns3.add(column6);
        final OptExpression bottomJoin2 = Utils.createUtInternal(
                bottomJoin,
                Utils.createUtLeaf(leafOutputColumns3));

        final List<OptColumnRef> leafOutputColumns4 = Lists.newArrayList();
        final OptColumnRef column7 = columnRefFactory.create("t7", ScalarType.INT);
        final OptColumnRef column8 = columnRefFactory.create("t8", ScalarType.INT);
        leafOutputColumns4.add(column7);
        leafOutputColumns4.add(column8);
        final OptExpression topJoin = Utils.createUtInternal(
                bottomJoin2,
                Utils.createUtLeaf(leafOutputColumns4));

        LOG.info("init expr=\n{}", topJoin.getExplainString());

        final SearchVariable variable = new SearchVariable();
        variable.setExecuteOptimization(true);
        final RequiredPhysicalProperty requiredPhysicalProperty = RequiredPhysicalProperty.createTestProperty();
        final List<OptColumnRef> outputColumns = Lists.newArrayList();
        outputColumns.add(column1);
        outputColumns.add(column2);
        outputColumns.add(column3);
        outputColumns.add(column4);
        outputColumns.add(column5);
        outputColumns.add(column6);
        outputColumns.add(column7);
        outputColumns.add(column8);
        requiredPhysicalProperty.getColumns().include(outputColumns);
        final QueryContext queryContext = new QueryContext(topJoin,
                requiredPhysicalProperty, outputColumns, variable);
        final Optimizer optimizer = new Optimizer(queryContext, new SimpleCostModel(), columnRefFactory);
        optimizer.addRule(OptUTInternalCommutativityRule.INSTANCE);
        optimizer.addRule(OptUTInternalAssociativityRule.INSTANCE);
        optimizer.addRule(OptUTInternalImplementation.INSTANCE);
        optimizer.addRule(OptUTLeafImplementation.INSTANCE);
        optimizer.optimize();

        final OptColumnRefSet outputColumnsSet = new OptColumnRefSet();
        outputColumnsSet.include(outputColumns);
        final UTMemoStatus checker = new UTMemoStatus(
                4, optimizer.getMemo(), optimizer.getRootOptContext(), outputColumnsSet);
        checker.checkImplementStatus();
    }

    @Test
    public void testFourJoin() {
        final List<OptColumnRef> leafOutputColumns1 = Lists.newArrayList();
        final OptColumnRef column1 = columnRefFactory.create("t1", ScalarType.INT);
        final OptColumnRef column2 = columnRefFactory.create("t2", ScalarType.INT);
        leafOutputColumns1.add(column1);
        leafOutputColumns1.add(column2);
        final List<OptColumnRef> leafOutputColumns2 = Lists.newArrayList();
        final OptColumnRef column3 = columnRefFactory.create("t3", ScalarType.INT);
        final OptColumnRef column4 = columnRefFactory.create("t4", ScalarType.INT);
        leafOutputColumns2.add(column3);
        leafOutputColumns2.add(column4);
        final OptExpression bottomJoin = Utils.createUtInternal(
                Utils.createUtLeaf(leafOutputColumns1),
                Utils.createUtLeaf(leafOutputColumns2));


        final List<OptColumnRef> leafOutputColumns3 = Lists.newArrayList();
        final OptColumnRef column5 = columnRefFactory.create("t5", ScalarType.INT);
        final OptColumnRef column6 = columnRefFactory.create("t6", ScalarType.INT);
        leafOutputColumns3.add(column5);
        leafOutputColumns3.add(column6);
        final OptExpression bottomJoin2 = Utils.createUtInternal(
                bottomJoin,
                Utils.createUtLeaf(leafOutputColumns3));

        final List<OptColumnRef> leafOutputColumns4 = Lists.newArrayList();
        final OptColumnRef column7 = columnRefFactory.create("t7", ScalarType.INT);
        final OptColumnRef column8 = columnRefFactory.create("t8", ScalarType.INT);
        leafOutputColumns4.add(column7);
        leafOutputColumns4.add(column8);
        final OptExpression bottomJoin3 = Utils.createUtInternal(
                bottomJoin2,
                Utils.createUtLeaf(leafOutputColumns4));

        final List<OptColumnRef> leafOutputColumns5 = Lists.newArrayList();
        final OptColumnRef column9 = columnRefFactory.create("t9", ScalarType.INT);
        final OptColumnRef column10 = columnRefFactory.create("t10", ScalarType.INT);
        leafOutputColumns5.add(column9);
        leafOutputColumns5.add(column10);
        final OptExpression topJoin = Utils.createUtInternal(
                bottomJoin3,
                Utils.createUtLeaf(leafOutputColumns5));

        LOG.info("init expr=\n{}", topJoin.getExplainString());

        final SearchVariable variable = new SearchVariable();
        variable.setExecuteOptimization(true);
        final RequiredPhysicalProperty requiredPhysicalProperty = RequiredPhysicalProperty.createTestProperty();
        final List<OptColumnRef> outputColumns = Lists.newArrayList();
        outputColumns.add(column1);
        outputColumns.add(column2);
        outputColumns.add(column3);
        outputColumns.add(column4);
        outputColumns.add(column5);
        outputColumns.add(column6);
        outputColumns.add(column7);
        outputColumns.add(column8);
        outputColumns.add(column9);
        outputColumns.add(column10);
        requiredPhysicalProperty.getColumns().include(outputColumns);
        final QueryContext queryContext = new QueryContext(topJoin,
                requiredPhysicalProperty, outputColumns, variable);
        final Optimizer optimizer = new Optimizer(queryContext, new SimpleCostModel(), columnRefFactory);
        optimizer.addRule(OptUTInternalCommutativityRule.INSTANCE);
        optimizer.addRule(OptUTInternalAssociativityRule.INSTANCE);
        optimizer.addRule(OptUTInternalImplementation.INSTANCE);
        optimizer.addRule(OptUTLeafImplementation.INSTANCE);
        optimizer.optimize();

        final OptColumnRefSet outputColumnsSet = new OptColumnRefSet();
        outputColumnsSet.include(outputColumns);
        final UTMemoStatus checker = new UTMemoStatus(
                5, optimizer.getMemo(), optimizer.getRootOptContext(), outputColumnsSet);
        checker.checkImplementStatus();
    }

    @Test
    public void testSevenJoin() {
        final List<OptColumnRef> leafOutputColumns1 = Lists.newArrayList();
        final OptColumnRef column1 = columnRefFactory.create("t1", ScalarType.INT);
        final OptColumnRef column2 = columnRefFactory.create("t2", ScalarType.INT);
        leafOutputColumns1.add(column1);
        leafOutputColumns1.add(column2);
        final List<OptColumnRef> leafOutputColumns2 = Lists.newArrayList();
        final OptColumnRef column3 = columnRefFactory.create("t3", ScalarType.INT);
        final OptColumnRef column4 = columnRefFactory.create("t4", ScalarType.INT);
        leafOutputColumns2.add(column3);
        leafOutputColumns2.add(column4);
        final OptExpression bottomJoin = Utils.createUtInternal(
                Utils.createUtLeaf(leafOutputColumns1),
                Utils.createUtLeaf(leafOutputColumns2));


        final List<OptColumnRef> leafOutputColumns3 = Lists.newArrayList();
        final OptColumnRef column5 = columnRefFactory.create("t5", ScalarType.INT);
        final OptColumnRef column6 = columnRefFactory.create("t6", ScalarType.INT);
        leafOutputColumns3.add(column5);
        leafOutputColumns3.add(column6);
        final OptExpression bottomJoin2 = Utils.createUtInternal(
                bottomJoin,
                Utils.createUtLeaf(leafOutputColumns3));

        final List<OptColumnRef> leafOutputColumns4 = Lists.newArrayList();
        final OptColumnRef column7 = columnRefFactory.create("t7", ScalarType.INT);
        final OptColumnRef column8 = columnRefFactory.create("t8", ScalarType.INT);
        leafOutputColumns4.add(column7);
        leafOutputColumns4.add(column8);
        final OptExpression bottomJoin3 = Utils.createUtInternal(
                bottomJoin2,
                Utils.createUtLeaf(leafOutputColumns4));

        final List<OptColumnRef> leafOutputColumns5 = Lists.newArrayList();
        final OptColumnRef column9 = columnRefFactory.create("t9", ScalarType.INT);
        final OptColumnRef column10 = columnRefFactory.create("t10", ScalarType.INT);
        leafOutputColumns5.add(column9);
        leafOutputColumns5.add(column10);
        final OptExpression bottomJoin4 = Utils.createUtInternal(
                bottomJoin3,
                Utils.createUtLeaf(leafOutputColumns5));

        final List<OptColumnRef> leafOutputColumns6 = Lists.newArrayList();
        final OptColumnRef column11 = columnRefFactory.create("t11", ScalarType.INT);
        final OptColumnRef column12 = columnRefFactory.create("t12", ScalarType.INT);
        leafOutputColumns6.add(column11);
        leafOutputColumns6.add(column12);
        final OptExpression bottomJoin5 = Utils.createUtInternal(
                bottomJoin4,
                Utils.createUtLeaf(leafOutputColumns6));

        final List<OptColumnRef> leafOutputColumns7 = Lists.newArrayList();
        final OptColumnRef column13 = columnRefFactory.create("t13", ScalarType.INT);
        final OptColumnRef column14 = columnRefFactory.create("t14", ScalarType.INT);
        leafOutputColumns7.add(column13);
        leafOutputColumns7.add(column14);
        final OptExpression bottomJoin6 = Utils.createUtInternal(
                bottomJoin5,
                Utils.createUtLeaf(leafOutputColumns7));

        final List<OptColumnRef> leafOutputColumns8 = Lists.newArrayList();
        final OptColumnRef column15 = columnRefFactory.create("t15", ScalarType.INT);
        final OptColumnRef column16 = columnRefFactory.create("t16", ScalarType.INT);
        leafOutputColumns8.add(column15);
        leafOutputColumns8.add(column16);
        final OptExpression topJoin = Utils.createUtInternal(
                bottomJoin6,
                Utils.createUtLeaf(leafOutputColumns8));

        LOG.info("init expr=\n{}", topJoin.getExplainString());

        final SearchVariable variable = new SearchVariable();
        variable.setExecuteOptimization(true);
        final RequiredPhysicalProperty requiredPhysicalProperty = RequiredPhysicalProperty.createTestProperty();
        final List<OptColumnRef> outputColumns = Lists.newArrayList();
        outputColumns.add(column1);
        outputColumns.add(column2);
        outputColumns.add(column3);
        outputColumns.add(column4);
        outputColumns.add(column5);
        outputColumns.add(column6);
        outputColumns.add(column7);
        outputColumns.add(column8);
        outputColumns.add(column9);
        outputColumns.add(column10);
        outputColumns.add(column11);
        outputColumns.add(column12);
        outputColumns.add(column13);
        outputColumns.add(column14);
        outputColumns.add(column15);
        outputColumns.add(column16);
        requiredPhysicalProperty.getColumns().include(outputColumns);
        final QueryContext queryContext = new QueryContext(topJoin,
                requiredPhysicalProperty, outputColumns, variable);
        final Optimizer optimizer = new Optimizer(queryContext, new SimpleCostModel(), columnRefFactory);
        optimizer.addRule(OptUTInternalCommutativityRule.INSTANCE);
        optimizer.addRule(OptUTInternalAssociativityRule.INSTANCE);
        optimizer.addRule(OptUTInternalImplementation.INSTANCE);
        optimizer.addRule(OptUTLeafImplementation.INSTANCE);
        optimizer.optimize();

        final OptColumnRefSet outputColumnsSet = new OptColumnRefSet();
        outputColumnsSet.include(outputColumns);
        final UTMemoStatus checker = new UTMemoStatus(
                8, optimizer.getMemo(), optimizer.getRootOptContext(), outputColumnsSet);
        checker.checkImplementStatus();
    }
}
