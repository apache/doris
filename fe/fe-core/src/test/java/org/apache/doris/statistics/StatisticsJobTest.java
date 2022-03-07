package org.apache.doris.statistics;

import mockit.Expectations;
import mockit.Mocked;
import static org.junit.Assert.assertEquals;

import org.apache.doris.analysis.AnalyzeStmt;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.TableName;
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class StatisticsJobTest {

    private StatisticsJob statisticsJobUnderTest;

    @Before
    public void setUp() throws Exception {
        statisticsJobUnderTest = new StatisticsJob(0L, Arrays.asList(0L, 1L, 1L), new HashMap<>());
    }

    @Test
    public void testRelatedTableId() {
        // Run the test
        final Set<Long> result = statisticsJobUnderTest.relatedTableId();
        // Verify the results
        assertEquals(new HashSet<>(Arrays.asList(0L, 1L)), result);
    }

    @Test
    public void testFromAnalyzeStmtWithDbAndTblName() throws Exception {
        // Setup
        Column col1 = new Column("c1", PrimitiveType.STRING);
        Column col2 = new Column("c2", PrimitiveType.INT);
        OlapTable tbl = new OlapTable(1L, "tbl", Arrays.asList(col1, col2), KeysType.AGG_KEYS,
                new PartitionInfo(), new HashDistributionInfo());
        Database database = new Database(1L, "db");
        database.createTable(tbl);

        Catalog catalog = Catalog.getCurrentCatalog();
        ConcurrentHashMap<String, Database> fullNameToDb = new ConcurrentHashMap<>();
        fullNameToDb.put("cluster:db", database);
        Deencapsulation.setField(catalog, "fullNameToDb", fullNameToDb);

        ConcurrentHashMap<Long, Database> idToDb = new ConcurrentHashMap<>();
        idToDb.put(0L, database);
        Deencapsulation.setField(catalog, "idToDb", idToDb);

        final AnalyzeStmt analyzeStmt = new AnalyzeStmt(new TableName("db", "tbl"), Arrays.asList("c1", "c2"), new HashMap<>());
        analyzeStmt.setClusterName("cluster");

        Map<Long, List<String>> tableIdToColumnName = Maps.newHashMap();
        tableIdToColumnName.put(1L, Arrays.asList("c1", "c2"));

        // Run the test
        final StatisticsJob result = StatisticsJob.fromAnalyzeStmt(analyzeStmt);
        assertEquals(1L, result.getDbId());
        assertEquals(Collections.singletonList(1L), result.getTableIds());
        assertEquals(tableIdToColumnName, result.getTableIdToColumnName());
        assertEquals(0, result.getProgress());
        assertEquals(StatisticsJob.JobState.PENDING, result.getJobState());
        assertEquals(new HashSet<>(Collections.singletonList(1L)), result.relatedTableId());
    }

    @Test
    public void testFromAnalyzeStmtWithoutDbAndTblName(
            @Mocked AnalyzeStmt mockAnalyzeStmt,
            @Mocked Database database,
            @Mocked OlapTable table,
            @Mocked Analyzer analyzer) throws Exception {

        // Setup
        Catalog catalog = Catalog.getCurrentCatalog();
        ConcurrentHashMap<String, Database> fullNameToDb = new ConcurrentHashMap<>();
        fullNameToDb.put("cluster:db", database);
        Deencapsulation.setField(catalog, "fullNameToDb", fullNameToDb);

        ConcurrentHashMap<Long, Database> idToDb = new ConcurrentHashMap<>();
        idToDb.put(0L, database);
        Deencapsulation.setField(catalog, "idToDb", idToDb);

        final AnalyzeStmt analyzeStmt = new AnalyzeStmt(null, null, new HashMap<>());
        analyzeStmt.setClusterName("cluster");

        Map<Long, List<String>> tableIdToColumnName = Maps.newHashMap();
        tableIdToColumnName.put(0L, Arrays.asList("c1", "c2"));

        Column col1 = new Column("c1", PrimitiveType.STRING);
        Column col2 = new Column("c2", PrimitiveType.INT);
        List<Column> baseSchema = Lists.newArrayList();
        baseSchema.add(col1);
        baseSchema.add(col2);

        new Expectations() {
            {
                database.getTables();
                this.minTimes = 0;
                this.result = Collections.singletonList(table);

                table.getBaseSchema();
                this.minTimes = 0;
                this.result = baseSchema;

                mockAnalyzeStmt.getTableName();
                this.minTimes = 0;
                this.result = null;

                mockAnalyzeStmt.getAnalyzer();
                this.minTimes = 0;
                this.result = analyzer;

                analyzer.getDefaultDb();
                this.minTimes = 0;
                this.result = "cluster:db";
            }
        };

        // Run the test
        final StatisticsJob result = StatisticsJob.fromAnalyzeStmt(analyzeStmt);
        assertEquals(0L, result.getDbId());
        assertEquals(Collections.singletonList(0L), result.getTableIds());
        assertEquals(tableIdToColumnName, result.getTableIdToColumnName());
        assertEquals(0, result.getProgress());
        assertEquals(StatisticsJob.JobState.PENDING, result.getJobState());
        assertEquals(new HashSet<>(Collections.singletonList(0L)), result.relatedTableId());
    }

    @Test(expected = DdlException.class)
    public void testFromAnalyzeStmt_ThrowsDdlException() throws Exception {
        // Setup
        final AnalyzeStmt analyzeStmt = new AnalyzeStmt(new TableName("db", "tbl"), Arrays.asList("c1", "c2"), new HashMap<>());

        // Run the test
        StatisticsJob.fromAnalyzeStmt(analyzeStmt);
    }
}
