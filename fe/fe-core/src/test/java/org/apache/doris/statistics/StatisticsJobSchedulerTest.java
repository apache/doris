package org.apache.doris.statistics;

import static org.junit.Assert.assertEquals;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.jmockit.Deencapsulation;

import com.google.common.collect.Maps;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StatisticsJobSchedulerTest {

    private StatisticsJobScheduler statisticsJobSchedulerUnderTest;
    private StatisticsJob statisticsJob;

    @Before
    public void setUp() throws Exception {
        statisticsJobSchedulerUnderTest = new StatisticsJobScheduler();
        Map<Long, List<String>> tableIdToColumnName = Maps.newHashMap();
        tableIdToColumnName.put(0L, Arrays.asList("c1", "c2"));
        tableIdToColumnName.put(1L, Arrays.asList("c1", "c2"));
        statisticsJob = new StatisticsJob(0L, Arrays.asList(0L, 1L), tableIdToColumnName);
        statisticsJobSchedulerUnderTest.addPendingJob(statisticsJob);
    }

    @Test
    public void testRunAfterCatalogReady() throws Exception {
        // Setup
        Column col1 = new Column("c1", PrimitiveType.STRING);
        Column col2 = new Column("c2", PrimitiveType.INT);
        OlapTable tbl1 = new OlapTable(0L, "tbl1", Arrays.asList(col1, col2), KeysType.AGG_KEYS,
                new PartitionInfo(), new HashDistributionInfo());
        OlapTable tbl2 = new OlapTable(1L, "tbl2", Arrays.asList(col1, col2), KeysType.DUP_KEYS,
                new PartitionInfo(), new HashDistributionInfo());
        Database database = new Database(0L, "db");
        database.createTable(tbl1);
        database.createTable(tbl2);

        Catalog catalog = Catalog.getCurrentCatalog();
        ConcurrentHashMap<String, Database> fullNameToDb = new ConcurrentHashMap<>();
        fullNameToDb.put("cluster:db", database);
        Deencapsulation.setField(catalog, "fullNameToDb", fullNameToDb);

        ConcurrentHashMap<Long, Database> idToDb = new ConcurrentHashMap<>();
        idToDb.put(0L, database);
        Deencapsulation.setField(catalog, "idToDb", idToDb);


        // Run the test
        statisticsJobSchedulerUnderTest.runAfterCatalogReady();

        /*
         * Verify the results:
         * mateTask(2):
         *  - col2[avg_len、max_len]
         *  - data_size
         * sqlTask(5):
         *  - row_count
         *  - col1[min_value、max_value、ndv],
         *  - col2[min_value、max_value、ndv]
         *  - col1[num_nulls]
         *  - col2[num_nulls]
         * sampleTask(1):
         *  - col1[max_size、avg_size]
         */
        assertEquals(2 * 8, statisticsJob.getTasks().size());
    }

    @Test
    public void testRunAfterCatalogReadyWithException() {
        statisticsJobSchedulerUnderTest.runAfterCatalogReady();
    }
}
