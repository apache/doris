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

package org.apache.doris.planner;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BaseTableRef;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TPartitionType;
import org.apache.doris.thrift.TRuntimeFilterMode;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import mockit.Expectations;
import mockit.Mocked;

public class RuntimeFilterGeneratorTest {
    private Analyzer analyzer;
    private PlanFragment testPlanFragment;
    private HashJoinNode hashJoinNode;
    private OlapScanNode lhsScanNode;
    private OlapScanNode rhsScanNode;
    @Mocked
    private ConnectContext connectContext;

    @Before
    public void setUp() throws UserException {
        Catalog catalog = Deencapsulation.newInstance(Catalog.class);
        analyzer = new Analyzer(catalog, connectContext);
        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default_cluster";
            }
        };
        TableRef tableRef = new TableRef();
        Deencapsulation.setField(tableRef, "isAnalyzed", true);
        Deencapsulation.setField(tableRef, "joinOp", JoinOperator.INNER_JOIN);

        TupleDescriptor lhsTupleDescriptor = new TupleDescriptor(new TupleId(0));
        lhsScanNode = new OlapScanNode(new PlanNodeId(0), lhsTupleDescriptor, "LEFT SCAN");
        TableName lhsTableName = new TableName("default_cluster:test_db", "test_lhs_tbl");
        SlotRef lhsExpr = new SlotRef(lhsTableName, "test_lhs_col");
        SlotDescriptor lhsSlotDescriptor = new SlotDescriptor(new SlotId(0), lhsTupleDescriptor);
        Column k1 = new Column("test_lhs_col", PrimitiveType.BIGINT);
        k1.setIsKey(true);
        k1.setIsAllowNull(false);
        lhsSlotDescriptor.setColumn(k1);
        lhsExpr.setDesc(lhsSlotDescriptor);
        Table lhsTable = new Table(0, "test_lhs_tbl", Table.TableType.OLAP, Lists.newArrayList(k1));
        BaseTableRef lhsTableRef = new BaseTableRef(tableRef, lhsTable, lhsTableName);
        lhsTableRef.analyze(analyzer);

        TupleDescriptor rhsTupleDescriptor = new TupleDescriptor(new TupleId(1));
        rhsScanNode = new OlapScanNode(new PlanNodeId(1), rhsTupleDescriptor, "RIGHT SCAN");
        TableName rhsTableName = new TableName("default_cluster:test_db", "test_rhs_tbl");
        SlotRef rhsExpr = new SlotRef(rhsTableName, "test_rhs_col");
        SlotDescriptor rhsSlotDescriptor = new SlotDescriptor(new SlotId(1), rhsTupleDescriptor);
        Column k2 = new Column("test_rhs_col", PrimitiveType.INT);
        k2.setIsKey(true);
        k2.setIsAllowNull(false);
        rhsSlotDescriptor.setColumn(k2);
        rhsExpr.setDesc(rhsSlotDescriptor);
        Table rhsTable = new Table(0, "test_rhs_tbl", Table.TableType.OLAP, Lists.newArrayList(k2));
        BaseTableRef rhsTableRef = new BaseTableRef(tableRef, rhsTable, rhsTableName);
        rhsTableRef.analyze(analyzer);

        ArrayList<Expr> testJoinExprs = new ArrayList<>();
        BinaryPredicate eqJoinConjunct = new BinaryPredicate(BinaryPredicate.Operator.EQ, lhsExpr, rhsExpr);
        testJoinExprs.add(eqJoinConjunct);

        hashJoinNode = new HashJoinNode(new PlanNodeId(2), lhsScanNode, rhsScanNode, tableRef, testJoinExprs
                , new ArrayList<>());
        testPlanFragment = new PlanFragment(new PlanFragmentId(0), hashJoinNode
                , new DataPartition(TPartitionType.UNPARTITIONED));
        hashJoinNode.setFragment(testPlanFragment);
        lhsScanNode.setFragment(testPlanFragment);
        rhsScanNode.setFragment(testPlanFragment);

        new Expectations() {
            {
                analyzer.getSlotDesc(new SlotId(0));
                result = lhsSlotDescriptor;
                analyzer.getSlotDesc(new SlotId(1));
                result = rhsSlotDescriptor;

                ConnectContext.get().getSessionVariable().getRuntimeFiltersMaxNum();
                result = 8;
                ConnectContext.get().getSessionVariable().getRuntimeBloomFilterMaxSize();
                result = 16777216;
                ConnectContext.get().getSessionVariable().getRuntimeBloomFilterMinSize();
                result = 1048576;
                ConnectContext.get().getSessionVariable().getRuntimeBloomFilterSize();
                result = 2097152;
            }
        };
    }

    private void clearRuntimeFilterState() {
        testPlanFragment.clearRuntimeFilters();
        analyzer.clearAssignedRuntimeFilters();
        hashJoinNode.clearRuntimeFilters();
        lhsScanNode.clearRuntimeFilters();
    }

    @Test
    public void testGenerateRuntimeFiltersMode() {
        clearRuntimeFilterState();
        new Expectations() {
            {
                ConnectContext.get().getSessionVariable().getRuntimeFilterMode();
                result = "GLOBAL";
                ConnectContext.get().getSessionVariable().getRuntimeFilterType();
                result = 15;
            }
        };
        RuntimeFilterGenerator.generateRuntimeFilters(analyzer, hashJoinNode);
        Assert.assertEquals(testPlanFragment.getTargetRuntimeFilterIds().size(), 4);
        Assert.assertEquals(testPlanFragment.getBuilderRuntimeFilterIds().size(), 4);
        Assert.assertEquals(analyzer.getAssignedRuntimeFilter().size(), 4);
        Assert.assertEquals(hashJoinNode.getRuntimeFilters().size(), 4);
        Assert.assertEquals(lhsScanNode.getRuntimeFilters().size(), 4);
        Assert.assertEquals(hashJoinNode.getRuntimeFilterExplainString(true)
                , "RF000[in] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`" +
                        ", RF001[bloom] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`" +
                        ", RF002[min_max] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`" +
                        ", RF003[in_or_bloom] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`\n");
        Assert.assertEquals(lhsScanNode.getRuntimeFilterExplainString(false)
                , "RF000[in] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`" +
                        ", RF001[bloom] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`" +
                        ", RF002[min_max] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`" +
                        ", RF003[in_or_bloom] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`\n");

        clearRuntimeFilterState();
        new Expectations() {
            {
                ConnectContext.get().getSessionVariable().getRuntimeFilterMode();
                result = "LOCAL";
            }
        };
        RuntimeFilterGenerator.generateRuntimeFilters(analyzer, hashJoinNode);
        Assert.assertEquals(testPlanFragment.getTargetRuntimeFilterIds().size(), 4);
        Assert.assertEquals(testPlanFragment.getBuilderRuntimeFilterIds().size(), 4);
        Assert.assertEquals(analyzer.getAssignedRuntimeFilter().size(), 4);
        Assert.assertEquals(hashJoinNode.getRuntimeFilters().size(), 4);
        Assert.assertEquals(lhsScanNode.getRuntimeFilters().size(), 4);
        Assert.assertEquals(hashJoinNode.getRuntimeFilterExplainString(true)
                , "RF000[in] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`" +
                        ", RF001[bloom] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`" +
                        ", RF002[min_max] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`" +
                        ", RF003[in_or_bloom] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`\n");
        Assert.assertEquals(lhsScanNode.getRuntimeFilterExplainString(false)
                , "RF000[in] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`" +
                        ", RF001[bloom] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`" +
                        ", RF002[min_max] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`" +
                        ", RF003[in_or_bloom] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`\n");

        clearRuntimeFilterState();
        new Expectations() {
            {
                ConnectContext.get().getSessionVariable().getRuntimeFilterMode();
                result = "REMOTE";
            }
        };
        RuntimeFilterGenerator.generateRuntimeFilters(analyzer, hashJoinNode);
        Assert.assertEquals(testPlanFragment.getTargetRuntimeFilterIds().size(), 0);
        Assert.assertEquals(testPlanFragment.getBuilderRuntimeFilterIds().size(), 0);
        Assert.assertEquals(analyzer.getAssignedRuntimeFilter().size(), 0);
        Assert.assertEquals(hashJoinNode.getRuntimeFilters().size(), 0);
        Assert.assertEquals(lhsScanNode.getRuntimeFilters().size(), 0);
        Assert.assertEquals(hashJoinNode.getRuntimeFilterExplainString(true), "");
        Assert.assertEquals(lhsScanNode.getRuntimeFilterExplainString(false), "");
    }

    @Test(expected = IllegalStateException.class)
    public void testGenerateRuntimeFiltersModeException() {
        clearRuntimeFilterState();
        new Expectations() {
            {
                ConnectContext.get().getSessionVariable().getRuntimeFilterType();
                result = 16;
            }
        };
        RuntimeFilterGenerator.generateRuntimeFilters(analyzer, hashJoinNode);
    }

    @Test
    public void testGenerateRuntimeFiltersType() {
        clearRuntimeFilterState();
        new Expectations() {
            {
                ConnectContext.get().getSessionVariable().getRuntimeFilterType();
                result = 0;
                ConnectContext.get().getSessionVariable().getRuntimeFilterMode();
                result = "GLOBAL";
            }
        };
        RuntimeFilterGenerator.generateRuntimeFilters(analyzer, hashJoinNode);
        Assert.assertEquals(hashJoinNode.getRuntimeFilterExplainString(true), "");
        Assert.assertEquals(lhsScanNode.getRuntimeFilterExplainString(false), "");
        Assert.assertEquals(testPlanFragment.getTargetRuntimeFilterIds().size(), 0);
        Assert.assertEquals(testPlanFragment.getBuilderRuntimeFilterIds().size(), 0);
        Assert.assertEquals(analyzer.getAssignedRuntimeFilter().size(), 0);
        Assert.assertEquals(hashJoinNode.getRuntimeFilters().size(), 0);
        Assert.assertEquals(lhsScanNode.getRuntimeFilters().size(), 0);

        clearRuntimeFilterState();
        new Expectations() {
            {
                ConnectContext.get().getSessionVariable().getRuntimeFilterType();
                result = 1;
            }
        };
        RuntimeFilterGenerator.generateRuntimeFilters(analyzer, hashJoinNode);
        Assert.assertEquals(hashJoinNode.getRuntimeFilterExplainString(true)
                , "RF000[in] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`\n");
        Assert.assertEquals(lhsScanNode.getRuntimeFilterExplainString(false)
                , "RF000[in] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`\n");
        Assert.assertEquals(testPlanFragment.getTargetRuntimeFilterIds().size(), 1);
        Assert.assertEquals(testPlanFragment.getBuilderRuntimeFilterIds().size(), 1);
        Assert.assertEquals(analyzer.getAssignedRuntimeFilter().size(), 1);
        Assert.assertEquals(hashJoinNode.getRuntimeFilters().size(), 1);
        Assert.assertEquals(lhsScanNode.getRuntimeFilters().size(), 1);

        clearRuntimeFilterState();
        new Expectations() {
            {
                ConnectContext.get().getSessionVariable().getRuntimeFilterType();
                result = 2;
            }
        };
        RuntimeFilterGenerator.generateRuntimeFilters(analyzer, hashJoinNode);
        Assert.assertEquals(hashJoinNode.getRuntimeFilterExplainString(true)
                , "RF000[bloom] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`\n");
        Assert.assertEquals(lhsScanNode.getRuntimeFilterExplainString(false)
                , "RF000[bloom] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`\n");
        Assert.assertEquals(testPlanFragment.getTargetRuntimeFilterIds().size(), 1);
        Assert.assertEquals(testPlanFragment.getBuilderRuntimeFilterIds().size(), 1);
        Assert.assertEquals(analyzer.getAssignedRuntimeFilter().size(), 1);
        Assert.assertEquals(hashJoinNode.getRuntimeFilters().size(), 1);
        Assert.assertEquals(lhsScanNode.getRuntimeFilters().size(), 1);

        clearRuntimeFilterState();
        new Expectations() {
            {
                ConnectContext.get().getSessionVariable().getRuntimeFilterType();
                result = 3;
            }
        };
        RuntimeFilterGenerator.generateRuntimeFilters(analyzer, hashJoinNode);
        Assert.assertEquals(hashJoinNode.getRuntimeFilterExplainString(true)
                , "RF000[in] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`" +
                        ", RF001[bloom] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`\n");
        Assert.assertEquals(lhsScanNode.getRuntimeFilterExplainString(false)
                , "RF000[in] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`" +
                        ", RF001[bloom] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`\n");
        Assert.assertEquals(testPlanFragment.getTargetRuntimeFilterIds().size(), 2);
        Assert.assertEquals(testPlanFragment.getBuilderRuntimeFilterIds().size(), 2);
        Assert.assertEquals(analyzer.getAssignedRuntimeFilter().size(), 2);
        Assert.assertEquals(hashJoinNode.getRuntimeFilters().size(), 2);
        Assert.assertEquals(lhsScanNode.getRuntimeFilters().size(), 2);

        clearRuntimeFilterState();
        new Expectations() {
            {
                ConnectContext.get().getSessionVariable().getRuntimeFilterType();
                result = 4;
            }
        };
        RuntimeFilterGenerator.generateRuntimeFilters(analyzer, hashJoinNode);
        Assert.assertEquals(hashJoinNode.getRuntimeFilterExplainString(true)
                , "RF000[min_max] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`\n");
        Assert.assertEquals(lhsScanNode.getRuntimeFilterExplainString(false)
                , "RF000[min_max] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`\n");
        Assert.assertEquals(testPlanFragment.getTargetRuntimeFilterIds().size(), 1);
        Assert.assertEquals(testPlanFragment.getBuilderRuntimeFilterIds().size(), 1);
        Assert.assertEquals(analyzer.getAssignedRuntimeFilter().size(), 1);
        Assert.assertEquals(hashJoinNode.getRuntimeFilters().size(), 1);
        Assert.assertEquals(lhsScanNode.getRuntimeFilters().size(), 1);

        clearRuntimeFilterState();
        new Expectations() {
            {
                ConnectContext.get().getSessionVariable().getRuntimeFilterType();
                result = 5;
            }
        };
        RuntimeFilterGenerator.generateRuntimeFilters(analyzer, hashJoinNode);
        Assert.assertEquals(hashJoinNode.getRuntimeFilterExplainString(true)
                , "RF000[in] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`" +
                        ", RF001[min_max] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`\n");
        Assert.assertEquals(lhsScanNode.getRuntimeFilterExplainString(false)
                , "RF000[in] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`" +
                        ", RF001[min_max] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`\n");
        Assert.assertEquals(testPlanFragment.getTargetRuntimeFilterIds().size(), 2);
        Assert.assertEquals(testPlanFragment.getBuilderRuntimeFilterIds().size(), 2);
        Assert.assertEquals(analyzer.getAssignedRuntimeFilter().size(), 2);
        Assert.assertEquals(hashJoinNode.getRuntimeFilters().size(), 2);
        Assert.assertEquals(lhsScanNode.getRuntimeFilters().size(), 2);

        clearRuntimeFilterState();
        new Expectations() {
            {
                ConnectContext.get().getSessionVariable().getRuntimeFilterType();
                result = 6;
            }
        };
        RuntimeFilterGenerator.generateRuntimeFilters(analyzer, hashJoinNode);
        Assert.assertEquals(hashJoinNode.getRuntimeFilterExplainString(true)
                , "RF000[bloom] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`" +
                        ", RF001[min_max] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`\n");
        Assert.assertEquals(lhsScanNode.getRuntimeFilterExplainString(false)
                , "RF000[bloom] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`" +
                        ", RF001[min_max] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`\n");
        Assert.assertEquals(testPlanFragment.getTargetRuntimeFilterIds().size(), 2);
        Assert.assertEquals(testPlanFragment.getBuilderRuntimeFilterIds().size(), 2);
        Assert.assertEquals(analyzer.getAssignedRuntimeFilter().size(), 2);
        Assert.assertEquals(hashJoinNode.getRuntimeFilters().size(), 2);
        Assert.assertEquals(lhsScanNode.getRuntimeFilters().size(), 2);

        clearRuntimeFilterState();
        new Expectations() {
            {
                ConnectContext.get().getSessionVariable().getRuntimeFilterType();
                result = 7;
            }
        };
        RuntimeFilterGenerator.generateRuntimeFilters(analyzer, hashJoinNode);
        Assert.assertEquals(hashJoinNode.getRuntimeFilterExplainString(true)
            , "RF000[in] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`" +
                ", RF001[bloom] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`" +
                ", RF002[min_max] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`\n");
        Assert.assertEquals(lhsScanNode.getRuntimeFilterExplainString(false)
            , "RF000[in] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`" +
                ", RF001[bloom] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`" +
                ", RF002[min_max] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`\n");
        Assert.assertEquals(testPlanFragment.getTargetRuntimeFilterIds().size(), 3);
        Assert.assertEquals(testPlanFragment.getBuilderRuntimeFilterIds().size(), 3);
        Assert.assertEquals(analyzer.getAssignedRuntimeFilter().size(), 3);
        Assert.assertEquals(hashJoinNode.getRuntimeFilters().size(), 3);
        Assert.assertEquals(lhsScanNode.getRuntimeFilters().size(), 3);

        clearRuntimeFilterState();
        new Expectations() {
            {
                ConnectContext.get().getSessionVariable().getRuntimeFilterType();
                result = 8;
            }
        };
        RuntimeFilterGenerator.generateRuntimeFilters(analyzer, hashJoinNode);
        Assert.assertEquals(hashJoinNode.getRuntimeFilterExplainString(true)
            , "RF000[in_or_bloom] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`\n");
        Assert.assertEquals(lhsScanNode.getRuntimeFilterExplainString(false)
            , "RF000[in_or_bloom] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`\n");
        Assert.assertEquals(testPlanFragment.getTargetRuntimeFilterIds().size(), 1);
        Assert.assertEquals(testPlanFragment.getBuilderRuntimeFilterIds().size(), 1);
        Assert.assertEquals(analyzer.getAssignedRuntimeFilter().size(), 1);
        Assert.assertEquals(hashJoinNode.getRuntimeFilters().size(), 1);
        Assert.assertEquals(lhsScanNode.getRuntimeFilters().size(), 1);

        clearRuntimeFilterState();
        new Expectations() {
            {
                ConnectContext.get().getSessionVariable().getRuntimeFilterType();
                result = 9;
            }
        };
        RuntimeFilterGenerator.generateRuntimeFilters(analyzer, hashJoinNode);
        Assert.assertEquals(hashJoinNode.getRuntimeFilterExplainString(true)
            , "RF000[in] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`" +
                ", RF001[in_or_bloom] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`\n");
        Assert.assertEquals(lhsScanNode.getRuntimeFilterExplainString(false)
            , "RF000[in] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`" +
                ", RF001[in_or_bloom] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`\n");
        Assert.assertEquals(testPlanFragment.getTargetRuntimeFilterIds().size(), 2);
        Assert.assertEquals(testPlanFragment.getBuilderRuntimeFilterIds().size(), 2);
        Assert.assertEquals(analyzer.getAssignedRuntimeFilter().size(), 2);
        Assert.assertEquals(hashJoinNode.getRuntimeFilters().size(), 2);
        Assert.assertEquals(lhsScanNode.getRuntimeFilters().size(), 2);

        clearRuntimeFilterState();
        new Expectations() {
            {
                ConnectContext.get().getSessionVariable().getRuntimeFilterType();
                result = 10;
            }
        };
        RuntimeFilterGenerator.generateRuntimeFilters(analyzer, hashJoinNode);
        Assert.assertEquals(hashJoinNode.getRuntimeFilterExplainString(true)
            , "RF000[bloom] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`" +
                ", RF001[in_or_bloom] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`\n");
        Assert.assertEquals(lhsScanNode.getRuntimeFilterExplainString(false)
            , "RF000[bloom] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`" +
                ", RF001[in_or_bloom] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`\n");
        Assert.assertEquals(testPlanFragment.getTargetRuntimeFilterIds().size(), 2);
        Assert.assertEquals(testPlanFragment.getBuilderRuntimeFilterIds().size(), 2);
        Assert.assertEquals(analyzer.getAssignedRuntimeFilter().size(), 2);
        Assert.assertEquals(hashJoinNode.getRuntimeFilters().size(), 2);
        Assert.assertEquals(lhsScanNode.getRuntimeFilters().size(), 2);

        clearRuntimeFilterState();
        new Expectations() {
            {
                ConnectContext.get().getSessionVariable().getRuntimeFilterType();
                result = 11;
            }
        };
        RuntimeFilterGenerator.generateRuntimeFilters(analyzer, hashJoinNode);
        Assert.assertEquals(hashJoinNode.getRuntimeFilterExplainString(true)
            , "RF000[in] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`" +
                ", RF001[bloom] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`" +
                ", RF002[in_or_bloom] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`\n");
        Assert.assertEquals(lhsScanNode.getRuntimeFilterExplainString(false)
            , "RF000[in] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`" +
                ", RF001[bloom] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`" +
                ", RF002[in_or_bloom] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`\n");
        Assert.assertEquals(testPlanFragment.getTargetRuntimeFilterIds().size(), 3);
        Assert.assertEquals(testPlanFragment.getBuilderRuntimeFilterIds().size(), 3);
        Assert.assertEquals(analyzer.getAssignedRuntimeFilter().size(), 3);
        Assert.assertEquals(hashJoinNode.getRuntimeFilters().size(), 3);
        Assert.assertEquals(lhsScanNode.getRuntimeFilters().size(), 3);

        clearRuntimeFilterState();
        new Expectations() {
            {
                ConnectContext.get().getSessionVariable().getRuntimeFilterType();
                result = 12;
            }
        };
        RuntimeFilterGenerator.generateRuntimeFilters(analyzer, hashJoinNode);
        Assert.assertEquals(hashJoinNode.getRuntimeFilterExplainString(true)
            , "RF000[min_max] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`" +
                ", RF001[in_or_bloom] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`\n");
        Assert.assertEquals(lhsScanNode.getRuntimeFilterExplainString(false)
            , "RF000[min_max] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`" +
                ", RF001[in_or_bloom] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`\n");
        Assert.assertEquals(testPlanFragment.getTargetRuntimeFilterIds().size(), 2);
        Assert.assertEquals(testPlanFragment.getBuilderRuntimeFilterIds().size(), 2);
        Assert.assertEquals(analyzer.getAssignedRuntimeFilter().size(), 2);
        Assert.assertEquals(hashJoinNode.getRuntimeFilters().size(), 2);
        Assert.assertEquals(lhsScanNode.getRuntimeFilters().size(), 2);

        clearRuntimeFilterState();
        new Expectations() {
            {
                ConnectContext.get().getSessionVariable().getRuntimeFilterType();
                result = 13;
            }
        };
        RuntimeFilterGenerator.generateRuntimeFilters(analyzer, hashJoinNode);
        Assert.assertEquals(hashJoinNode.getRuntimeFilterExplainString(true)
            , "RF000[in] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`" +
                ", RF001[min_max] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`" +
                ", RF002[in_or_bloom] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`\n");
        Assert.assertEquals(lhsScanNode.getRuntimeFilterExplainString(false)
            , "RF000[in] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`" +
                ", RF001[min_max] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`" +
                ", RF002[in_or_bloom] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`\n");
        Assert.assertEquals(testPlanFragment.getTargetRuntimeFilterIds().size(), 3);
        Assert.assertEquals(testPlanFragment.getBuilderRuntimeFilterIds().size(), 3);
        Assert.assertEquals(analyzer.getAssignedRuntimeFilter().size(), 3);
        Assert.assertEquals(hashJoinNode.getRuntimeFilters().size(), 3);
        Assert.assertEquals(lhsScanNode.getRuntimeFilters().size(), 3);


        clearRuntimeFilterState();
        new Expectations() {
            {
                ConnectContext.get().getSessionVariable().getRuntimeFilterType();
                result = 14;
            }
        };
        RuntimeFilterGenerator.generateRuntimeFilters(analyzer, hashJoinNode);
        Assert.assertEquals(hashJoinNode.getRuntimeFilterExplainString(true)
            , "RF000[bloom] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`" +
                ", RF001[min_max] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`" +
                ", RF002[in_or_bloom] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`\n");
        Assert.assertEquals(lhsScanNode.getRuntimeFilterExplainString(false)
            , "RF000[bloom] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`" +
                ", RF001[min_max] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`" +
                ", RF002[in_or_bloom] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`\n");
        Assert.assertEquals(testPlanFragment.getTargetRuntimeFilterIds().size(), 3);
        Assert.assertEquals(testPlanFragment.getBuilderRuntimeFilterIds().size(), 3);
        Assert.assertEquals(analyzer.getAssignedRuntimeFilter().size(), 3);
        Assert.assertEquals(hashJoinNode.getRuntimeFilters().size(), 3);
        Assert.assertEquals(lhsScanNode.getRuntimeFilters().size(), 3);

        clearRuntimeFilterState();
        new Expectations() {
            {
                ConnectContext.get().getSessionVariable().getRuntimeFilterType();
                result = 15;
            }
        };
        RuntimeFilterGenerator.generateRuntimeFilters(analyzer, hashJoinNode);
        Assert.assertEquals(hashJoinNode.getRuntimeFilterExplainString(true)
            , "RF000[in] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`" +
                ", RF001[bloom] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`" +
                ", RF002[min_max] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`" +
                ", RF003[in_or_bloom] <- `default_cluster:test_db`.`test_rhs_tbl`.`test_rhs_col`\n");
        Assert.assertEquals(lhsScanNode.getRuntimeFilterExplainString(false)
            , "RF000[in] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`" +
                ", RF001[bloom] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`" +
                ", RF002[min_max] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`" +
                ", RF003[in_or_bloom] -> `default_cluster:test_db`.`test_lhs_tbl`.`test_lhs_col`\n");
        Assert.assertEquals(testPlanFragment.getTargetRuntimeFilterIds().size(), 4);
        Assert.assertEquals(testPlanFragment.getBuilderRuntimeFilterIds().size(), 4);
        Assert.assertEquals(analyzer.getAssignedRuntimeFilter().size(), 4);
        Assert.assertEquals(hashJoinNode.getRuntimeFilters().size(), 4);
        Assert.assertEquals(lhsScanNode.getRuntimeFilters().size(), 4);
    }

    @Test(expected = IllegalStateException.class)
    public void testGenerateRuntimeFiltersTypeExceptionLess() {
        clearRuntimeFilterState();
        new Expectations() {
            {
                ConnectContext.get().getSessionVariable().getRuntimeFilterType();
                result = -1;
            }
        };
        RuntimeFilterGenerator.generateRuntimeFilters(analyzer, hashJoinNode);
    }

    @Test(expected = IllegalStateException.class)
    public void testGenerateRuntimeFiltersTypeExceptionMore() {
        clearRuntimeFilterState();
        new Expectations() {
            {
                ConnectContext.get().getSessionVariable().getRuntimeFilterType();
                result = 16;
            }
        };
        RuntimeFilterGenerator.generateRuntimeFilters(analyzer, hashJoinNode);
    }

    @Test
    public void testGenerateRuntimeFiltersSize() {
        new Expectations() {
            {
                ConnectContext.get().getSessionVariable().getRuntimeFilterMode();
                result = "GLOBAL";
                ConnectContext.get().getSessionVariable().getRuntimeFilterType();
                result = 2;
            }
        };

        clearRuntimeFilterState();
        new Expectations() {
            {
                ConnectContext.get().getSessionVariable().getRuntimeBloomFilterMaxSize();
                result = 16777216;
                ConnectContext.get().getSessionVariable().getRuntimeBloomFilterMinSize();
                result = 1048576;
                ConnectContext.get().getSessionVariable().getRuntimeBloomFilterSize();
                result = 2097152;
            }
        };
        RuntimeFilterGenerator.generateRuntimeFilters(analyzer, hashJoinNode);
        Assert.assertEquals(analyzer.getAssignedRuntimeFilter().get(0).toThrift().getBloomFilterSizeBytes(), 2097152);

        clearRuntimeFilterState();
        new Expectations() {
            {
                ConnectContext.get().getSessionVariable().getRuntimeBloomFilterMaxSize();
                result = 16777216;
                ConnectContext.get().getSessionVariable().getRuntimeBloomFilterMinSize();
                result = 1048576;
                ConnectContext.get().getSessionVariable().getRuntimeBloomFilterSize();
                result = 1;
            }
        };
        RuntimeFilterGenerator.generateRuntimeFilters(analyzer, hashJoinNode);
        Assert.assertEquals(analyzer.getAssignedRuntimeFilter().get(0).toThrift().getBloomFilterSizeBytes(), 1048576);

        clearRuntimeFilterState();
        new Expectations() {
            {
                ConnectContext.get().getSessionVariable().getRuntimeBloomFilterMaxSize();
                result = 16777216;
                ConnectContext.get().getSessionVariable().getRuntimeBloomFilterMinSize();
                result = 1048576;
                ConnectContext.get().getSessionVariable().getRuntimeBloomFilterSize();
                result = 999999999;
            }
        };
        RuntimeFilterGenerator.generateRuntimeFilters(analyzer, hashJoinNode);
        Assert.assertEquals(analyzer.getAssignedRuntimeFilter().get(0).toThrift().getBloomFilterSizeBytes(), 16777216);

        // Use ndv and fpp to calculate the minimum space required for bloom filter
        Assert.assertEquals(1L <<
                RuntimeFilter.GetMinLogSpaceForBloomFilter(1000000, 0.05), 1048576);
        Assert.assertEquals(1L <<
                RuntimeFilter.GetMinLogSpaceForBloomFilter(1000000, 0.1), 1048576);
        Assert.assertEquals(1L <<
                RuntimeFilter.GetMinLogSpaceForBloomFilter(1000000, 0.3), 524288);
        Assert.assertEquals(1L <<
                RuntimeFilter.GetMinLogSpaceForBloomFilter(10000000, 0.1), 8388608);
        Assert.assertEquals(1L <<
                RuntimeFilter.GetMinLogSpaceForBloomFilter(1000, 0.1), 1024);
    }
}
