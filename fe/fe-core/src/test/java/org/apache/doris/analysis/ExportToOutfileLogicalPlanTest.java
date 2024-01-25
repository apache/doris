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

package org.apache.doris.analysis;

import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.load.ExportJob;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.ExportCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The `Export` sql finally generates the `Outfile` sql.
 * This test is to test whether the generated outfile sql is correct.
 */
public class ExportToOutfileLogicalPlanTest extends TestWithFeService {
    private String dbName = "testDb";
    private String tblName = "table1";

    /**
     * create a database and a table
     *
     * @throws Exception
     */
    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase(dbName);
        useDatabase(dbName);
        createTable("create table " + tblName + "\n" + "(k1 int, k2 int, k3 int) "
                + "PARTITION BY RANGE(k1)\n" + "(\n"
                + "PARTITION p1 VALUES LESS THAN (\"20\"),\n"
                + "PARTITION p2 VALUES [(\"20\"),(\"30\")),\n"
                + "PARTITION p3 VALUES [(\"30\"),(\"40\")),\n"
                + "PARTITION p4 VALUES LESS THAN (\"50\")\n" + ")\n"
                + " distributed by hash(k1) buckets 10\n"
                + "properties(\"replication_num\" = \"1\");");
    }

    /**
     * test normal export, sql:
     *
     * EXPORT TABLE testDb.table1
     * TO "file:///tmp/exp_"
     *
     * @throws UserException
     */
    @Test
    public void testNormal() throws UserException {
        // The origin export sql
        String exportSql = "EXPORT TABLE testDb.table1\n"
                + "TO \"file:///tmp/exp_\";";

        List<Long> currentTablets1 = Arrays.asList(10010L, 10012L, 10014L, 10016L, 10018L, 10020L, 10022L, 10024L,
                10026L, 10028L);
        List<Long> currentTablets2 = Arrays.asList(10030L, 10032L, 10034L, 10036L, 10038L, 10040L, 10042L, 10044L,
                10046L, 10048L);
        List<Long> currentTablets3 = Arrays.asList(10050L, 10052L, 10054L, 10056L, 10058L, 10060L, 10062L, 10064L,
                10066L, 10068L);
        List<Long> currentTablets4 = Arrays.asList(10070L, 10072L, 10074L, 10076L, 10078L, 10080L, 10082L, 10084L,
                10086L, 10088L);
        // generate outfile
        List<List<StatementBase>> outfileSqlPerParallel = getOutfileSqlPerParallel(exportSql);

        // check
        Assert.assertEquals(1, outfileSqlPerParallel.size());
        Assert.assertEquals(4, outfileSqlPerParallel.get(0).size());

        LogicalPlan plan1 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(0).get(0)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan1, false), Lists.newArrayList(), currentTablets1);

        LogicalPlan plan2 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(0).get(1)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan2, false), Lists.newArrayList(), currentTablets2);

        LogicalPlan plan3 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(0).get(2)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan3, false), Lists.newArrayList(), currentTablets3);

        LogicalPlan plan4 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(0).get(3)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan4, false), Lists.newArrayList(), currentTablets4);
    }

    /**
     * test normal parallelism, sql:
     *
     * EXPORT TABLE testDb.table1
     * TO "file:///tmp/exp_"
     * PROPERTIES(
     *     "parallelism" = "4"
     * );
     *
     * @throws UserException
     */
    @Test
    public void testNormalParallelism() throws UserException {
        // The origin export sql
        String exportSql = "EXPORT TABLE testDb.table1\n"
                + "TO \"file:///tmp/exp_\" "
                + "PROPERTIES(\n"
                + "\"parallelism\" = \"4\"\n"
                + ");";

        // This export sql should generate 4 array, and there should be 1 outfile sql in per array.
        // The only difference between them is the TABLET(). They are:
        List<Long> currentTablets1 = Arrays.asList(10010L, 10012L, 10014L, 10016L, 10018L, 10020L, 10022L, 10024L,
                10026L, 10028L);
        List<Long> currentTablets2 = Arrays.asList(10030L, 10032L, 10034L, 10036L, 10038L, 10040L, 10042L, 10044L,
                10046L, 10048L);
        List<Long> currentTablets3 = Arrays.asList(10050L, 10052L, 10054L, 10056L, 10058L, 10060L, 10062L, 10064L,
                10066L, 10068L);
        List<Long> currentTablets4 = Arrays.asList(10070L, 10072L, 10074L, 10076L, 10078L, 10080L, 10082L, 10084L,
                10086L, 10088L);

        // generate outfile
        List<List<StatementBase>> outfileSqlPerParallel = getOutfileSqlPerParallel(exportSql);

        // check
        Assert.assertEquals(4, outfileSqlPerParallel.size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(0).size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(1).size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(2).size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(3).size());

        LogicalPlan plan1 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(0).get(0)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan1, false), Lists.newArrayList(), currentTablets1);

        LogicalPlan plan2 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(1).get(0)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan2, false), Lists.newArrayList(), currentTablets2);

        LogicalPlan plan3 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(2).get(0)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan3, false), Lists.newArrayList(), currentTablets3);

        LogicalPlan plan4 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(3).get(0)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan4, false), Lists.newArrayList(), currentTablets4);
    }

    /**
     * test normal parallelism, sql:
     *
     * EXPORT TABLE testDb.table1
     * TO "file:///tmp/exp_"
     * PROPERTIES(
     *     "parallelism" = "3"
     * );
     *
     * @throws UserException
     */
    @Test
    public void testMultiOutfilePerParalle() throws UserException {
        // The origin export sql
        String exportSql = "EXPORT TABLE testDb.table1\n"
                + "TO \"file:///tmp/exp_\" "
                + "PROPERTIES(\n"
                + "\"parallelism\" = \"3\"\n"
                + ");";

        // This export sql should generate 4 array, and there should be 1 outfile sql in per array.
        // The only difference between them is the TABLET(). They are:
        List<Long> currentTablets1 = Arrays.asList(10010L, 10012L, 10014L, 10016L, 10018L, 10020L, 10022L, 10024L,
                10026L, 10028L);
        List<Long> currentTablets12 = Arrays.asList(10030L, 10032L, 10034L, 10036L);
        List<Long> currentTablets2 = Arrays.asList(10038L, 10040L, 10042L, 10044L, 10046L, 10048L, 10050L, 10052L,
                10054L, 10056L);
        List<Long> currentTablets22 = Arrays.asList(10058L, 10060L, 10062L);
        List<Long> currentTablets3 = Arrays.asList(10064L, 10066L, 10068L, 10070L, 10072L, 10074L, 10076L, 10078L,
                10080L, 10082L);
        List<Long> currentTablets32 = Arrays.asList(10084L, 10086L, 10088L);

        // generate outfile
        List<List<StatementBase>> outfileSqlPerParallel = getOutfileSqlPerParallel(exportSql);

        // check
        Assert.assertEquals(3, outfileSqlPerParallel.size());
        Assert.assertEquals(2, outfileSqlPerParallel.get(0).size());
        Assert.assertEquals(2, outfileSqlPerParallel.get(1).size());
        Assert.assertEquals(2, outfileSqlPerParallel.get(2).size());

        LogicalPlan plan1 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(0).get(0)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan1, false), Lists.newArrayList(), currentTablets1);

        LogicalPlan plan12 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(0).get(1)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan12, false), Lists.newArrayList(), currentTablets12);

        LogicalPlan plan2 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(1).get(0)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan2, false), Lists.newArrayList(), currentTablets2);

        LogicalPlan plan22 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(1).get(1)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan22, false), Lists.newArrayList(), currentTablets22);

        LogicalPlan plan3 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(2).get(0)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan3, false), Lists.newArrayList(), currentTablets3);

        LogicalPlan plan32 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(2).get(1)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan32, false), Lists.newArrayList(), currentTablets32);
    }

    /**
     * test export single partition, sql:
     *
     * EXPORT TABLE testDb.table1 PARTITION (p1)
     * TO "file:///tmp/exp_"
     * PROPERTIES(
     *     "parallelism" = "4"
     * );
     *
     * @throws UserException
     */
    @Test
    public void testPartitionParallelism() throws UserException {
        // The origin export sql
        String exportSql = "EXPORT TABLE testDb.table1 PARTITION (p1)\n"
                + "TO \"file:///tmp/exp_\" "
                + "PROPERTIES(\n"
                + "\"parallelism\" = \"4\"\n"
                + ");";

        // This export sql should generate 4 array, and there should be 1 outfile sql in per array.
        // The only difference between them is the TABLET(). They are:
        List<Long> currentTablets1 = Arrays.asList(10010L, 10012L, 10014L);
        List<Long> currentTablets2 = Arrays.asList(10016L, 10018L, 10020L);
        List<Long> currentTablets3 = Arrays.asList(10022L, 10024L);
        List<Long> currentTablets4 = Arrays.asList(10026L, 10028L);

        List<String> currentPartitions = Arrays.asList("p1");

        // generate outfile
        List<List<StatementBase>> outfileSqlPerParallel = getOutfileSqlPerParallel(exportSql);

        // check
        Assert.assertEquals(4, outfileSqlPerParallel.size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(0).size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(1).size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(2).size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(3).size());


        LogicalPlan plan1 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(0).get(0)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan1, false), currentPartitions, currentTablets1);

        LogicalPlan plan2 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(1).get(0)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan2, false), currentPartitions, currentTablets2);

        LogicalPlan plan3 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(2).get(0)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan3, false), currentPartitions, currentTablets3);

        LogicalPlan plan4 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(3).get(0)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan4, false), currentPartitions, currentTablets4);
    }

    /**
     * test export multiple partition, sql:
     *
     * EXPORT TABLE testDb.table1 PARTITION (p1, p4)
     * TO "file:///tmp/exp_"
     * PROPERTIES(
     *     "parallelism" = "4"
     * );
     *
     * @throws UserException
     */
    @Test
    public void testMultiPartitionParallelism() throws UserException {
        // The origin export sql
        String exportSql = "EXPORT TABLE testDb.table1 PARTITION (p1, p4)\n"
                + "TO \"file:///tmp/exp_\" "
                + "PROPERTIES(\n"
                + "\"parallelism\" = \"4\"\n"
                + ");";

        // This export sql should generate 4 array, and there should be 1 outfile sql in per array.
        // The only difference between them is the TABLET(). They are:
        List<Long> currentTablets1 = Arrays.asList(10010L, 10012L, 10014L, 10016L, 10018L);
        List<Long> currentTablets2 = Arrays.asList(10020L, 10022L, 10024L, 10026L, 10028L);
        List<Long> currentTablets3 = Arrays.asList(10070L, 10072L, 10074L, 10076L, 10078L);
        List<Long> currentTablets4 = Arrays.asList(10080L, 10082L, 10084L, 10086L, 10088L);
        List<String> currentPartitions = Arrays.asList("p1", "p4");

        // generate outfile
        List<List<StatementBase>> outfileSqlPerParallel = getOutfileSqlPerParallel(exportSql);

        // check
        Assert.assertEquals(4, outfileSqlPerParallel.size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(0).size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(1).size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(2).size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(3).size());

        LogicalPlan plan1 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(0).get(0)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan1, false), currentPartitions, currentTablets1);

        LogicalPlan plan2 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(1).get(0)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan2, false), currentPartitions, currentTablets2);

        LogicalPlan plan3 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(2).get(0)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan3, false), currentPartitions, currentTablets3);

        LogicalPlan plan4 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(3).get(0)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan4, false), currentPartitions, currentTablets4);
    }

    /**
     * test parallelism less than tablets, sql:
     *
     * EXPORT TABLE testDb.table1 PARTITION (p1)
     * TO "file:///tmp/exp_"
     * PROPERTIES(
     *     "parallelism" = "20"
     * );
     *
     * @throws UserException
     */
    @Test
    public void testParallelismLessThanTablets() throws UserException {
        // The origin export sql
        String exportSql = "EXPORT TABLE testDb.table1 PARTITION (p1)\n"
                + "TO \"file:///tmp/exp_\" "
                + "PROPERTIES(\n"
                + "\"parallelism\" = \"20\"\n"
                + ");";

        // This export sql should generate 10 array because parallelism is less than the number of tablets,
        // so set parallelism = num(tablets)
        // There should be 1 outfile sql in per array.
        // The only difference between them is the TABLET(). They are:
        List<Long> currentTablets1 = Arrays.asList(10010L);
        List<Long> currentTablets2 = Arrays.asList(10012L);
        List<Long> currentTablets3 = Arrays.asList(10014L);
        List<Long> currentTablets4 = Arrays.asList(10016L);
        List<Long> currentTablets5 = Arrays.asList(10018L);
        List<Long> currentTablets6 = Arrays.asList(10020L);
        List<Long> currentTablets7 = Arrays.asList(10022L);
        List<Long> currentTablets8 = Arrays.asList(10024L);
        List<Long> currentTablets9 = Arrays.asList(10026L);
        List<Long> currentTablets10 = Arrays.asList(10028L);
        List<String> currentPartitions = Arrays.asList("p1");

        // generate outfile
        List<List<StatementBase>> outfileSqlPerParallel = getOutfileSqlPerParallel(exportSql);

        // check
        Assert.assertEquals(10, outfileSqlPerParallel.size());
        for (int i = 0; i < 10; ++i) {
            Assert.assertEquals(1, outfileSqlPerParallel.get(i).size());
        }

        LogicalPlan plan1 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(0).get(0)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan1, false), currentPartitions, currentTablets1);

        LogicalPlan plan2 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(1).get(0)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan2, false), currentPartitions, currentTablets2);

        LogicalPlan plan3 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(2).get(0)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan3, false), currentPartitions, currentTablets3);

        LogicalPlan plan4 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(3).get(0)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan4, false), currentPartitions, currentTablets4);

        LogicalPlan plan5 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(4).get(0)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan5, false), currentPartitions, currentTablets5);

        LogicalPlan plan6 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(5).get(0)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan6, false), currentPartitions, currentTablets6);

        LogicalPlan plan7 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(6).get(0)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan7, false), currentPartitions, currentTablets7);

        LogicalPlan plan8 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(7).get(0)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan8, false), currentPartitions, currentTablets8);

        LogicalPlan plan9 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(8).get(0)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan9, false), currentPartitions, currentTablets9);

        LogicalPlan plan10 = ((LogicalPlanAdapter) outfileSqlPerParallel.get(9).get(0)).getLogicalPlan();
        checkPartitionsAndTablets(getUnboundRelation(plan10, false), currentPartitions, currentTablets10);
    }

    private LogicalPlan parseSql(String exportSql) {
        StatementBase statementBase = new NereidsParser().parseSQL(exportSql).get(0);
        return ((LogicalPlanAdapter) statementBase).getLogicalPlan();
    }

    // need open EnableNereidsPlanner
    private List<List<StatementBase>> getOutfileSqlPerParallel(String exportSql) throws UserException {
        ExportCommand exportCommand = (ExportCommand) parseSql(exportSql);
        List<List<StatementBase>> selectStmtListPerParallel = Lists.newArrayList();
        try {
            Method checkAllParameters = exportCommand.getClass().getDeclaredMethod("checkAllParameters",
                    ConnectContext.class, TableName.class, Map.class);
            checkAllParameters.setAccessible(true);

            Method generateExportJob = exportCommand.getClass().getDeclaredMethod("generateExportJob",
                    ConnectContext.class, Map.class, TableName.class);
            generateExportJob.setAccessible(true);

            // get tblName
            List<String> qualifiedTableName = RelationUtil.getQualifierName(connectContext, exportCommand.getNameParts());
            TableName tblName = new TableName(qualifiedTableName.get(0), qualifiedTableName.get(1),
                    qualifiedTableName.get(2));
            checkAllParameters.invoke(exportCommand, connectContext, tblName, exportCommand.getFileProperties());

            ExportJob job = (ExportJob) generateExportJob.invoke(
                    exportCommand, connectContext, exportCommand.getFileProperties(), tblName);
            selectStmtListPerParallel = job.getSelectStmtListPerParallel();
        } catch (NoSuchMethodException e) {
            throw new UserException(e);
        } catch (InvocationTargetException e) {
            throw new UserException(e);
        } catch (IllegalAccessException e) {
            throw new UserException(e);
        }
        return selectStmtListPerParallel;
    }

    private void checkPartitionsAndTablets(UnboundRelation relation, List<String> currentPartitionNames,
            List<Long> currentTabletIds) {
        List<Long> tabletIdsToCheck = relation.getTabletIds();
        List<String> partNamesToCheck = relation.getPartNames();

        Assert.assertTrue(currentTabletIds.containsAll(tabletIdsToCheck));
        Assert.assertTrue(tabletIdsToCheck.containsAll(currentTabletIds));


        Assert.assertTrue(partNamesToCheck.containsAll(currentPartitionNames));
        Assert.assertTrue(currentPartitionNames.containsAll(partNamesToCheck));
    }

    private UnboundRelation getUnboundRelation(LogicalPlan plan, boolean isHaveFilter) {
        LogicalPlan lastPlan = (LogicalPlan) plan.children().get(0);
        if (isHaveFilter) {
            lastPlan = (LogicalPlan) lastPlan.children().get(0);
        }
        return (UnboundRelation) lastPlan.children().get(0).children().get(0);
    }
}
