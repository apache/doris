package org.apache.doris.statistics;

import mockit.Expectations;
import mockit.Mocked;

import org.apache.doris.analysis.AnalyzeStmt;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;

import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StatisticsJobManagerTest {

    private StatisticsJobManager statisticsJobManagerUnderTest;

    @Before
    public void setUp() throws Exception {
        statisticsJobManagerUnderTest = new StatisticsJobManager();
    }

    @Test
    public void testCreateStatisticsJob1(@Mocked AnalyzeStmt analyzeStmt,
                                         @Mocked PaloAuth auth,
                                         @Mocked Analyzer analyzer) {
        // Setup
        Column col1 = new Column("col1", PrimitiveType.STRING);
        Column col2 = new Column("col2", PrimitiveType.INT);
        OlapTable table = new OlapTable(1L, "tbl", Arrays.asList(col1, col2), KeysType.AGG_KEYS,
                new PartitionInfo(), new HashDistributionInfo());
        Database database = new Database(1L, "db");
        database.createTable(table);

        Catalog catalog = Catalog.getCurrentCatalog();
        Deencapsulation.setField(catalog, "statisticsJobScheduler", new StatisticsJobScheduler());

        ConcurrentHashMap<String, Database> fullNameToDb = new ConcurrentHashMap<>();
        fullNameToDb.put("cluster:db", database);
        Deencapsulation.setField(catalog, "fullNameToDb", fullNameToDb);

        ConcurrentHashMap<Long, Database> idToDb = new ConcurrentHashMap<>();
        idToDb.put(1L, database);
        Deencapsulation.setField(catalog, "idToDb", idToDb);

        UserIdentity userIdentity = new UserIdentity("root", "host", false);

        new Expectations() {
            {
                analyzeStmt.getTableName();
                this.minTimes = 0;
                this.result = new TableName("db", "tbl");

                analyzeStmt.getClusterName();
                this.minTimes = 0;
                this.result = "cluster";

                analyzeStmt.getAnalyzer();
                this.minTimes = 0;
                this.result = analyzer;

                analyzeStmt.getColumnNames();
                this.minTimes = 0;
                this.result = Arrays.asList("col1", "col2");

                analyzeStmt.getUserInfo();
                this.minTimes = 0;
                this.result = userIdentity;

                auth.checkDbPriv(userIdentity, this.anyString, PrivPredicate.SELECT);
                this.minTimes = 0;
                this.result = true;

                auth.checkTblPriv(userIdentity, this.anyString, this.anyString, PrivPredicate.SELECT);
                this.minTimes = 0;
                this.result = true;
            }
        };


        // Run the test and verify the results
        try {
            statisticsJobManagerUnderTest.createStatisticsJob(analyzeStmt);
        } catch (DdlException e) {
            Assert.fail("DdlException throws.");
        }
    }

    @Test(expected = DdlException.class)
    public void testCreateStatisticsJob_ThrowsDdlException(@Mocked AnalyzeStmt analyzeStmt,
                                                           @Mocked PaloAuth auth,
                                                           @Mocked Analyzer analyzer) throws Exception {
        // Setup
        Column col1 = new Column("col1", PrimitiveType.STRING);
        Column col2 = new Column("col2", PrimitiveType.INT);
        OlapTable table = new OlapTable(1L, "tbl", Arrays.asList(col1, col2), KeysType.AGG_KEYS,
                new PartitionInfo(), new HashDistributionInfo());
        Database database = new Database(1L, "db");
        database.createTable(table);

        // add the same table for test
        Map<Long, List<String>> tableIdToColumnName = Maps.newHashMap();
        tableIdToColumnName.put(0L, Arrays.asList("c1", "c2"));
        tableIdToColumnName.put(1L, Arrays.asList("c1", "c2"));
        StatisticsJob statisticsJob = new StatisticsJob(0L, Arrays.asList(0L, 1L), tableIdToColumnName);
        StatisticsJobScheduler statisticsJobScheduler = new StatisticsJobScheduler();
        statisticsJobScheduler.addPendingJob(statisticsJob);

        Catalog catalog = Catalog.getCurrentCatalog();
        Deencapsulation.setField(catalog, "statisticsJobScheduler", statisticsJobScheduler);

        ConcurrentHashMap<String, Database> fullNameToDb = new ConcurrentHashMap<>();
        fullNameToDb.put("cluster:db", database);
        Deencapsulation.setField(catalog, "fullNameToDb", fullNameToDb);

        ConcurrentHashMap<Long, Database> idToDb = new ConcurrentHashMap<>();
        idToDb.put(1L, database);
        Deencapsulation.setField(catalog, "idToDb", idToDb);

        UserIdentity userIdentity = new UserIdentity("root", "host", false);

        new Expectations() {
            {
                analyzeStmt.getTableName();
                this.minTimes = 0;
                this.result = new TableName("db", "tbl");

                analyzeStmt.getClusterName();
                this.minTimes = 0;
                this.result = "cluster";

                analyzeStmt.getAnalyzer();
                this.minTimes = 0;
                this.result = analyzer;

                analyzeStmt.getColumnNames();
                this.minTimes = 0;
                this.result = Arrays.asList("col1", "col2");

                analyzeStmt.getUserInfo();
                this.minTimes = 0;
                this.result = userIdentity;

                auth.checkDbPriv(userIdentity, this.anyString, PrivPredicate.SELECT);
                this.minTimes = 0;
                this.result = true;

                auth.checkTblPriv(userIdentity, this.anyString, this.anyString, PrivPredicate.SELECT);
                this.minTimes = 0;
                this.result = true;
            }
        };

        // Run the test
        statisticsJobManagerUnderTest.createStatisticsJob(analyzeStmt);
    }
}
