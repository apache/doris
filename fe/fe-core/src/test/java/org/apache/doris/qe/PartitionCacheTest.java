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

package org.apache.doris.qe;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.privilege.MockedAuth;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.proto.Types;
import org.apache.doris.qe.cache.Cache;
import org.apache.doris.qe.cache.CacheAnalyzer;
import org.apache.doris.qe.cache.CacheAnalyzer.CacheMode;
import org.apache.doris.qe.cache.CacheCoordinator;
import org.apache.doris.qe.cache.PartitionCache;
import org.apache.doris.qe.cache.PartitionRange;
import org.apache.doris.qe.cache.RowBatchBuilder;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.StringReader;
import java.net.UnknownHostException;
import java.util.List;

import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;

public class PartitionCacheTest {
    private static final Logger LOG = LogManager.getLogger(PartitionCacheTest.class);
    public static String clusterName = "testCluster";
    public static String dbName = "testDb";
    public static String fullDbName = "testCluster:testDb";
    public static String tableName = "testTbl";
    public static String userName = "testUser";

    private static ConnectContext context;

    private List<PartitionRange.PartitionSingle> newRangeList;
    private Cache.HitRange hitRange;
    private Analyzer analyzer;
    private Database db;

    @Mocked
    private PaloAuth auth;
    @Mocked
    private SystemInfoService service;
    @Mocked
    private Catalog catalog;
    @Mocked
    private ConnectContext ctx;
    @Mocked
    MysqlChannel channel;
    @Mocked
    ConnectScheduler scheduler;

    @BeforeClass
    public static void start() {
        MetricRepo.init();
        try {
            FrontendOptions.init();
            context = new ConnectContext(null);
            Config.cache_enable_sql_mode = true;
            Config.cache_enable_partition_mode = true;
            context.getSessionVariable().setEnableSqlCache(true);
            context.getSessionVariable().setEnablePartitionCache(true);
            Config.cache_last_version_interval_second = 7200;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
    
    @Before
    public void setUp() {
        MockedAuth.mockedAuth(auth);
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
        new MockUp<Util>() {
            @Mock
            public boolean showHiddenColumns() {
                return true;
            }
        };
        new MockUp<Catalog>() {
            @Mock
            public SystemInfoService getCurrentSystemInfo() {
                return service;
            }
        };
        db = new Database(1L, fullDbName);
            
        OlapTable tbl1 = createOrderTable();
        OlapTable tbl2 = createProfileTable();
        OlapTable tbl3 = createEventTable();
        db.createTable(tbl1);
        db.createTable(tbl2);
        db.createTable(tbl3);

        new Expectations(catalog) {
            {
                catalog.getAuth();
                minTimes = 0;
                result = auth;
   
                catalog.getDb(fullDbName);
                minTimes = 0;
                result = db;

                catalog.getDb(dbName);
                minTimes = 0;
                result = db;

                catalog.getDb(db.getId());
                minTimes = 0;
                result = db;

                catalog.getDbNames();
                minTimes = 0;
                result = Lists.newArrayList(fullDbName);
            }    
        };
        FunctionSet fs = new FunctionSet();
        fs.init();
        Deencapsulation.setField(catalog, "functionSet", fs);
        QueryState state = new QueryState();
        channel.reset();
        
        new Expectations(ctx) {
            {
                ctx.getMysqlChannel();
                minTimes = 0;
                result = channel;

                ctx.getClusterName();
                minTimes = 0;
                result = clusterName;

                ctx.getSerializer();
                minTimes = 0;
                result = MysqlSerializer.newInstance();

                ctx.getCatalog();
                minTimes = 0;
                result = catalog;

                ctx.getState();
                minTimes = 0;
                result = state;

                ctx.getConnectScheduler();
                minTimes = 0;
                result = scheduler;

                ctx.getConnectionId();
                minTimes = 0;
                result = 1;

                ctx.getQualifiedUser();
                minTimes = 0;
                result = userName;

                ctx.getForwardedStmtId();
                minTimes = 0;
                result = 123L;

                ctx.setKilled();
                minTimes = 0;
                ctx.updateReturnRows(anyInt);
                minTimes = 0;
                ctx.setQueryId((TUniqueId) any);
                minTimes = 0;
                
                ctx.queryId();
                minTimes = 0;
                result = new TUniqueId();

                ctx.getStartTime();
                minTimes = 0;
                result = 0L;

                ctx.getDatabase();
                minTimes = 0;
                result = dbName;

                SessionVariable sessionVariable = new SessionVariable();
                ctx.getSessionVariable();
                minTimes = 0;
                result = sessionVariable;

                ctx.setStmtId(anyLong);
                minTimes = 0;
                
                ctx.getStmtId();
                minTimes = 0;
                result = 1L;
            }
        };

        analyzer = new Analyzer(catalog, ctx);
        newRangeList = Lists.newArrayList();
    }

    private OlapTable createOrderTable() {
        Column column1 = new Column("date", ScalarType.INT);
        Column column2 = new Column("id", ScalarType.INT);
        Column column3 = new Column("value", ScalarType.INT);
        List<Column> columns = Lists.newArrayList(column1, column2, column3);

        MaterializedIndex baseIndex = new MaterializedIndex(10001, IndexState.NORMAL);
        RandomDistributionInfo distInfo = new RandomDistributionInfo(10);

        PartitionInfo partInfo = new RangePartitionInfo(Lists.newArrayList(column1));

        Partition part12 = new Partition(20200112, "p20200112", baseIndex, distInfo);
        part12.setVisibleVersion(1,1578762000000L,1);     //2020-01-12 1:00:00
        Partition part13 = new Partition(20200113, "p20200113", baseIndex, distInfo);
        part13.setVisibleVersion(1,1578848400000L,1);     //2020-01-13 1:00:00
        Partition part14 = new Partition(20200114, "p20200114", baseIndex, distInfo);
        part14.setVisibleVersion(1,1578934800000L,1);     //2020-01-14 1:00:00
        Partition part15 = new Partition(20200115, "p20200115", baseIndex, distInfo);
        part15.setVisibleVersion(2,1579053661000L,2);     //2020-01-15 10:01:01

        OlapTable table = new OlapTable(10000L, "order", columns,KeysType.DUP_KEYS, partInfo, distInfo);

        short shortKeyColumnCount = 1;
        table.setIndexMeta(10001, "group1", columns, 1, 1, shortKeyColumnCount,TStorageType.COLUMN, KeysType.DUP_KEYS);

        List<Column> idx_columns = Lists.newArrayList();
        idx_columns.add(column1);
        table.setIndexMeta(new Long(1), "test", idx_columns, 1, 1, shortKeyColumnCount, TStorageType.COLUMN, KeysType.DUP_KEYS);
        Deencapsulation.setField(table, "baseIndexId", 1000);

        table.addPartition(part12);
        table.addPartition(part13);
        table.addPartition(part14);
        table.addPartition(part15);

        return table;
    }

    private ScanNode createOrderScanNode() {
        OlapTable table = createOrderTable();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(10004));
        desc.setTable(table);
        ScanNode node = new OlapScanNode(new PlanNodeId(10008), desc, "ordernode");
        return node;
    }

    private OlapTable createProfileTable() {
        Column column2 = new Column("eventdate", ScalarType.DATE);
        Column column3 = new Column("userid", ScalarType.INT);
        Column column4 = new Column("country", ScalarType.INT);
        List<Column> columns = Lists.newArrayList(column2, column3, column4);

        MaterializedIndex baseIndex = new MaterializedIndex(20001, IndexState.NORMAL);
        RandomDistributionInfo distInfo = new RandomDistributionInfo(10);

        PartitionInfo partInfo = new RangePartitionInfo(Lists.newArrayList(column2));

        Partition part12 = new Partition(2020112, "p20200112", baseIndex, distInfo);
        part12.setVisibleVersion(1,1578762000000L,1);     //2020-01-12 1:00:00
        Partition part13 = new Partition(2020113, "p20200113", baseIndex, distInfo);
        part13.setVisibleVersion(1,1578848400000L,1);     //2020-01-13 1:00:00
        Partition part14 = new Partition(2020114, "p20200114", baseIndex, distInfo);
        part14.setVisibleVersion(1,1578934800000L,1);     //2020-01-14 1:00:00
        Partition part15 = new Partition(2020115, "p20200115", baseIndex, distInfo);
        part15.setVisibleVersion(2,1579021200000L,2);     //2020-01-15 1:00:00

        OlapTable table = new OlapTable(20000L, "userprofile", columns,KeysType.AGG_KEYS, partInfo, distInfo);
        
        short shortKeyColumnCount = 1;
        table.setIndexMeta(20001, "group1", columns, 1, 1, shortKeyColumnCount, TStorageType.COLUMN, KeysType.AGG_KEYS);

        List<Column> idx_columns = Lists.newArrayList();
        idx_columns.add(column2);
        table.setIndexMeta(new Long(2), "test", idx_columns, 1, 1, shortKeyColumnCount, TStorageType.COLUMN, KeysType.AGG_KEYS);
        
        Deencapsulation.setField(table, "baseIndexId", 1000);
        
        table.addPartition(part12);
        table.addPartition(part13);
        table.addPartition(part14);
        table.addPartition(part15);

        return table;
    }

    private ScanNode createProfileScanNode(){
        OlapTable table = createProfileTable();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(20004));
        desc.setTable(table);
        ScanNode node = new OlapScanNode(new PlanNodeId(20008), desc, "userprofilenode");
        return node;
    }

    /**
     * table appevent(date(pk), userid, eventid, eventtime), stream load every 5 miniutes
     */
    private OlapTable createEventTable() {
        Column column1 = new Column("eventdate", ScalarType.DATE);
        Column column2 = new Column("userid", ScalarType.INT);
        Column column3 = new Column("eventid", ScalarType.INT);
        Column column4 = new Column("eventtime", ScalarType.DATETIME);
        List<Column> columns = Lists.newArrayList(column1, column2, column3,column4);
        PartitionInfo partInfo = new RangePartitionInfo(Lists.newArrayList(column1));
        MaterializedIndex baseIndex = new MaterializedIndex(30001, IndexState.NORMAL);
        RandomDistributionInfo distInfo = new RandomDistributionInfo(10);

        Partition part12 = new Partition(20200112, "p20200112", baseIndex, distInfo);
        part12.setVisibleVersion(1,1578762000000L,1);     //2020-01-12 1:00:00
        Partition part13 = new Partition(20200113, "p20200113", baseIndex, distInfo);
        part13.setVisibleVersion(1,1578848400000L,1);     //2020-01-13 1:00:00
        Partition part14 = new Partition(20200114, "p20200114", baseIndex, distInfo);
        part14.setVisibleVersion(1,1578934800000L,1);     //2020-01-14 1:00:00
        Partition part15 = new Partition(20200115, "p20200115", baseIndex, distInfo);
        part15.setVisibleVersion(2,1579053661000L,2);     //2020-01-15 10:01:01

        OlapTable table = new OlapTable(30000L, "appevent", columns,KeysType.DUP_KEYS, partInfo, distInfo);
        
        short shortKeyColumnCount = 1;
        table.setIndexMeta(30001, "group1", columns, 1,1,shortKeyColumnCount,TStorageType.COLUMN, KeysType.AGG_KEYS);

        List<Column> column = Lists.newArrayList();
        column.add(column1);

        table.setIndexMeta(new Long(2), "test", column, 1, 1, shortKeyColumnCount, TStorageType.COLUMN, KeysType.AGG_KEYS);
        Deencapsulation.setField(table, "baseIndexId", 1000);
        
        table.addPartition(part12);
        table.addPartition(part13);
        table.addPartition(part14);
        table.addPartition(part15);

        return table;
    }

    private ScanNode createEventScanNode(){
        OlapTable table = createEventTable();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(30002));
        desc.setTable(table);
        ScanNode node = new OlapScanNode(new PlanNodeId(30004), desc, "appeventnode");
        return node;
    }

    private StatementBase parseSql(String sql){
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(sql)));
        StatementBase parseStmt = null;
        try {
            parseStmt = SqlParserUtils.getFirstStmt(parser);
            parseStmt.analyze(analyzer);
        } catch (AnalysisException e) {
            LOG.warn("Part,an_ex={}", e);
            Assert.fail(e.getMessage());
        } catch (UserException e) {
            LOG.warn("Part,ue_ex={}", e);
            Assert.fail(e.getMessage());
        } catch (Exception e) {
            LOG.warn("Part,cm_ex={}", e);
            Assert.fail(e.getMessage());
        }
        return parseStmt;
    }
   
    @Test
    public void testCacheNode() throws Exception {
        Catalog.getCurrentSystemInfo();
        CacheCoordinator cp = CacheCoordinator.getInstance();
        cp.DebugModel = true;
        Backend bd1 = new Backend(1, "", 1000);
        bd1.updateOnce(0,0,0);
        Backend bd2 = new Backend(2, "", 2000);
        bd2.updateOnce(0,0,0);
        Backend bd3 = new Backend(3, "", 3000);
        bd3.updateOnce(0,0,0);
        cp.addBackend(bd1);
        cp.addBackend(bd2);
        cp.addBackend(bd3);
        
        Types.PUniqueId key1 = Types.PUniqueId.newBuilder().setHi(1L).setLo(1L).build();
        Backend bk = cp.findBackend(key1);
        Assert.assertNotNull(bk);
        Assert.assertEquals(bk.getId(),3);

        key1 = key1.toBuilder().setHi(669560558156283345L).build();
        bk = cp.findBackend(key1);
        Assert.assertNotNull(bk);
        Assert.assertEquals(bk.getId(),1);
    }

    @Test
    public void testCacheModeNone() throws Exception {
        Catalog.getCurrentSystemInfo();
        StatementBase parseStmt = parseSql("select @@version_comment limit 1");
        List<ScanNode> scanNodes = Lists.newArrayList();
        CacheAnalyzer ca = new CacheAnalyzer(context, parseStmt, scanNodes);
        ca.checkCacheMode(0);
        Assert.assertEquals(ca.getCacheMode(), CacheMode.NoNeed);
    }

    @Test
    public void testCacheModeTable() throws Exception {
        Catalog.getCurrentSystemInfo();
        StatementBase parseStmt = parseSql(
            "SELECT country, COUNT(userid) FROM userprofile GROUP BY country"
        );
        List<ScanNode> scanNodes = Lists.newArrayList(createProfileScanNode());
        CacheAnalyzer ca = new CacheAnalyzer(context,parseStmt, scanNodes);
        ca.checkCacheMode(0);
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Sql);
    }
    
    @Test
    public void testWithinMinTime() throws Exception {
        Catalog.getCurrentSystemInfo();
        StatementBase parseStmt = parseSql(
            "SELECT country, COUNT(userid) FROM userprofile GROUP BY country"
        );
        List<ScanNode> scanNodes = Lists.newArrayList(createProfileScanNode());
        CacheAnalyzer ca = new CacheAnalyzer(context,parseStmt, scanNodes);
        ca.checkCacheMode(1579024800000L);  //2020-1-15 02:00:00
        Assert.assertEquals(ca.getCacheMode(), CacheMode.None);
    }

    @Test
    public void testPartitionModel() throws Exception {
        Catalog.getCurrentSystemInfo();
        StatementBase parseStmt = parseSql(
            "SELECT eventdate, COUNT(DISTINCT userid) FROM appevent WHERE eventdate>=\"2020-01-12\" and " +
                    "eventdate<=\"2020-01-15\" GROUP BY eventdate"
        );
        List<ScanNode> scanNodes = Lists.newArrayList(createEventScanNode());
        CacheAnalyzer ca = new CacheAnalyzer(context,parseStmt, scanNodes);
        ca.checkCacheMode(1579053661000L);  //2020-1-15 10:01:01
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Partition);
    }

    @Test
    public void testParseByte() throws Exception {
        Catalog.getCurrentSystemInfo();
        RowBatchBuilder sb = new RowBatchBuilder(CacheMode.Partition);
        byte[] buffer = new byte[]{10, 50, 48, 50, 48, 45, 48, 51, 45, 49, 48, 1, 51, 2, 67, 78};
        PartitionRange.PartitionKeyType key1 = sb.getKeyFromRow(buffer, 0, Type.DATE);
        LOG.info("real value key1 {}",key1.realValue());
        Assert.assertEquals(key1.realValue(), 20200310);
        PartitionRange.PartitionKeyType key2 = sb.getKeyFromRow(buffer, 1, Type.INT);
        LOG.info("real value key2 {}",key2.realValue());
        Assert.assertEquals(key2.realValue(), 3);
    }

    @Test
    public void testPartitionIntTypeSql() throws Exception {
        Catalog.getCurrentSystemInfo();
        StatementBase parseStmt = parseSql(
                "SELECT `date`, COUNT(id) FROM `order` WHERE `date`>=20200112 and `date`<=20200115 GROUP BY date"
        );
        List<ScanNode> scanNodes = Lists.newArrayList(createOrderScanNode());
        CacheAnalyzer ca = new CacheAnalyzer(context,parseStmt, scanNodes);
        ca.checkCacheMode(1579053661000L);                              //2020-1-15 10:01:01
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Partition);    //assert cache model first
        try {
            PartitionCache cache = (PartitionCache) ca.getCache();
            cache.rewriteSelectStmt(null);
            Assert.assertEquals(cache.getNokeyStmt().getWhereClause(), null);

            PartitionRange range = cache.getPartitionRange();
            boolean flag = range.analytics();
            Assert.assertEquals(flag, true);

            int size = range.getPartitionSingleList().size();
            LOG.warn("Rewrite partition range size={}", size);
            Assert.assertEquals(size, 4);

            String sql;
            range.setCacheFlag(20200112L);    //get data from cache
            range.setCacheFlag(20200113L);    //get data from cache

            hitRange = range.buildDiskPartitionRange(newRangeList);
            Assert.assertEquals(hitRange, Cache.HitRange.Left);
            Assert.assertEquals(newRangeList.size(), 2);
            Assert.assertEquals(newRangeList.get(0).getCacheKey().realValue(), 20200114);
            Assert.assertEquals(newRangeList.get(1).getCacheKey().realValue(), 20200115);

            cache.rewriteSelectStmt(newRangeList);
            sql = ca.getRewriteStmt().getWhereClause().toSql();
            Assert.assertEquals(sql, "(`date` >= 20200114) AND (`date` <= 20200115)");
        } catch (Exception e) {
            LOG.warn("ex={}", e);
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testSimpleCacheSql() throws Exception {
        Catalog.getCurrentSystemInfo();
        StatementBase parseStmt = parseSql(
            "SELECT eventdate, COUNT(userid) FROM appevent WHERE eventdate>=\"2020-01-12\" and eventdate<=\"2020-01-15\" GROUP BY eventdate"
        );
        List<ScanNode> scanNodes = Lists.newArrayList(createEventScanNode());
        CacheAnalyzer ca = new CacheAnalyzer(context,parseStmt, scanNodes);
        ca.checkCacheMode(1579053661000L); //2020-1-15 10:01:01
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Partition);      //assert cache model first
        SelectStmt selectStmt = (SelectStmt) parseStmt;

        try{
            PartitionCache cache = (PartitionCache) ca.getCache();
            cache.rewriteSelectStmt(null);
            Assert.assertEquals(cache.getNokeyStmt().getWhereClause(),null);

            PartitionRange range = cache.getPartitionRange();
            boolean flag = range.analytics();
            Assert.assertEquals(flag,true);

            int size = range.getPartitionSingleList().size();
            LOG.warn("Rewrite partition range size={}", size);
            Assert.assertEquals(size, 4);
            
            String sql;        
            range.setCacheFlag(20200112L);    //get data from cache
            range.setCacheFlag(20200113L);    //get data from cache

            hitRange = range.buildDiskPartitionRange(newRangeList);
            cache.rewriteSelectStmt(newRangeList);
            sql = ca.getRewriteStmt().getWhereClause().toSql();
            Assert.assertEquals(sql,"(`eventdate` >= '2020-01-14') AND (`eventdate` <= '2020-01-15')");
        } catch(Exception e){
            LOG.warn("ex={}",e);
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testHitPartPartition() throws Exception {
        Catalog.getCurrentSystemInfo();
        StatementBase parseStmt = parseSql(
                "SELECT eventdate, COUNT(userid) FROM appevent WHERE eventdate>=\"2020-01-12\" and eventdate<=\"2020-01-14\" GROUP BY eventdate"
        );
        List<ScanNode> scanNodes = Lists.newArrayList(createEventScanNode());
        CacheAnalyzer ca = new CacheAnalyzer(context, parseStmt, scanNodes);
        ca.checkCacheMode(1579053661000L); //2020-1-15 10:01:01
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Partition);      //assert cache model first

        try {
            PartitionCache cache = (PartitionCache) ca.getCache();

            cache.rewriteSelectStmt(null);
            Assert.assertEquals(cache.getNokeyStmt().getWhereClause(), null);

            PartitionRange range = cache.getPartitionRange();
            boolean flag = range.analytics();
            Assert.assertEquals(flag, true);

            int size = range.getPartitionSingleList().size();
            LOG.warn("Rewrite partition range size={}", size);
            Assert.assertEquals(size, 3);

            String sql;
            range.setCacheFlag(20200113);
            range.setCacheFlag(20200114);

            hitRange = range.buildDiskPartitionRange(newRangeList);
            Assert.assertEquals(hitRange,Cache.HitRange.Right);
            Assert.assertEquals(newRangeList.size(), 2);
            Assert.assertEquals(newRangeList.get(0).getCacheKey().realValue(), 20200112);
            Assert.assertEquals(newRangeList.get(1).getCacheKey().realValue(), 20200112);

            List<PartitionRange.PartitionSingle> updateRangeList = range.buildUpdatePartitionRange();
            Assert.assertEquals(updateRangeList.size(), 1);
            Assert.assertEquals(updateRangeList.get(0).getCacheKey().realValue(), 20200112);
        } catch (Exception e) {
            LOG.warn("ex={}", e);
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testNoUpdatePartition() throws Exception {
        Catalog.getCurrentSystemInfo();
        StatementBase parseStmt = parseSql(
                "SELECT eventdate, COUNT(userid) FROM appevent WHERE eventdate>=\"2020-01-12\" and eventdate<=\"2020-01-14\" GROUP BY eventdate"
        );
        List<ScanNode> scanNodes = Lists.newArrayList(createEventScanNode());
        CacheAnalyzer ca = new CacheAnalyzer(context, parseStmt, scanNodes);
        ca.checkCacheMode(1579053661000L); //2020-1-15 10:01:01
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Partition);      //assert cache model first

        try {
            PartitionCache cache = (PartitionCache) ca.getCache();

            cache.rewriteSelectStmt(null);
            Assert.assertEquals(cache.getNokeyStmt().getWhereClause(), null);

            PartitionRange range = cache.getPartitionRange();
            boolean flag = range.analytics();
            Assert.assertEquals(flag, true);

            int size = range.getPartitionSingleList().size();
            LOG.warn("Rewrite partition range size={}", size);
            Assert.assertEquals(size, 3);

            String sql;
            range.setCacheFlag(20200112);    //get data from cache
            range.setCacheFlag(20200113);
            range.setCacheFlag(20200114);

            hitRange = range.buildDiskPartitionRange(newRangeList);
            Assert.assertEquals(hitRange, Cache.HitRange.Full);
            Assert.assertEquals(newRangeList.size(), 0);
        } catch (Exception e) {
            LOG.warn("ex={}", e);
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testUpdatePartition() throws Exception {
        Catalog.getCurrentSystemInfo();
        StatementBase parseStmt = parseSql(
                "SELECT eventdate, COUNT(userid) FROM appevent WHERE eventdate>=\"2020-01-12\" and eventdate<=\"2020-01-15\" GROUP BY eventdate"
        );
        List<ScanNode> scanNodes = Lists.newArrayList(createEventScanNode());
        CacheAnalyzer ca = new CacheAnalyzer(context,parseStmt, scanNodes);
        ca.checkCacheMode(1579053661000L); //2020-1-15 10:01:01
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Partition);      //assert cache model first

        try {
            PartitionCache cache = (PartitionCache) ca.getCache();

            cache.rewriteSelectStmt(null);
            Assert.assertEquals(cache.getNokeyStmt().getWhereClause(), null);

            PartitionRange range = cache.getPartitionRange();
            boolean flag = range.analytics();
            Assert.assertEquals(flag, true);

            int size = range.getPartitionSingleList().size();
            LOG.warn("Rewrite partition range size={}", size);
            Assert.assertEquals(size, 4);

            String sql;
            range.setCacheFlag(20200112L);    //get data from cache
            range.setTooNewByKey(20200115);

            range.buildDiskPartitionRange(newRangeList);
            Assert.assertEquals(newRangeList.size(), 2);
            cache.rewriteSelectStmt(newRangeList);

            sql = ca.getRewriteStmt().getWhereClause().toSql();
            Assert.assertEquals(sql, "(`eventdate` >= '2020-01-13') AND (`eventdate` <= '2020-01-15')");

            List<PartitionRange.PartitionSingle> updateRangeList = range.buildUpdatePartitionRange();
            Assert.assertEquals(updateRangeList.size(), 2);
            Assert.assertEquals(updateRangeList.get(0).getCacheKey().realValue(), 20200113);
            Assert.assertEquals(updateRangeList.get(1).getCacheKey().realValue(), 20200114);
        } catch (Exception e) {
            LOG.warn("ex={}", e);
            Assert.fail(e.getMessage());
        }
    }
   
    @Test
    public void testRewriteMultiPredicate1() throws Exception {
        Catalog.getCurrentSystemInfo();
        StatementBase parseStmt = parseSql(
            "SELECT eventdate, COUNT(userid) FROM appevent WHERE eventdate>\"2020-01-11\" and eventdate<\"2020-01-16\"" +
                    " and eventid=1 GROUP BY eventdate"
        );
        List<ScanNode> scanNodes = Lists.newArrayList(createEventScanNode());
        CacheAnalyzer ca = new CacheAnalyzer(context,parseStmt, scanNodes);
        ca.checkCacheMode(1579053661000L); //2020-1-15 10:01:01
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Partition);      //assert cache model first
        try{
            PartitionCache cache = (PartitionCache) ca.getCache();

            cache.rewriteSelectStmt(null);
            LOG.warn("Nokey multi={}", cache.getNokeyStmt().getWhereClause().toSql());
            Assert.assertEquals(cache.getNokeyStmt().getWhereClause().toSql(),"`eventid` = 1");

            PartitionRange range = cache.getPartitionRange();
            boolean flag = range.analytics();
            Assert.assertEquals(flag,true);

            int size = range.getPartitionSingleList().size();
            Assert.assertEquals(size, 4);
            
            String sql;        
            range.setCacheFlag(20200112L);    //get data from cache
            range.setCacheFlag(20200113L);    //get data from cache

            range.buildDiskPartitionRange(newRangeList);

            cache.rewriteSelectStmt(newRangeList);
            sql = ca.getRewriteStmt().getWhereClause().toSql();
            LOG.warn("MultiPredicate={}", sql);                
            Assert.assertEquals(sql,"((`eventdate` > '2020-01-13') AND (`eventdate` < '2020-01-16')) AND (`eventid` = 1)");
        } catch(Exception e){
            LOG.warn("multi ex={}",e);
            Assert.fail(e.getMessage());
        }
    }
    
    @Test
    public void testRewriteJoin() throws Exception {
        Catalog.getCurrentSystemInfo();
        StatementBase parseStmt = parseSql(
            "SELECT appevent.eventdate, country, COUNT(appevent.userid) FROM appevent" +
                    " INNER JOIN userprofile ON appevent.userid = userprofile.userid" +
            " WHERE appevent.eventdate>=\"2020-01-12\" and appevent.eventdate<=\"2020-01-15\"" +
                    " and eventid=1 GROUP BY appevent.eventdate, country"
        );
        List<ScanNode> scanNodes = Lists.newArrayList(createEventScanNode());
        CacheAnalyzer ca = new CacheAnalyzer(context,parseStmt, scanNodes);
        ca.checkCacheMode(1579053661000L); //2020-1-15 10:01:01
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Partition);      //assert cache model first
        try{
            PartitionCache cache = (PartitionCache) ca.getCache();
            cache.rewriteSelectStmt(null);
            LOG.warn("Join nokey={}", cache.getNokeyStmt().getWhereClause().toSql());
            Assert.assertEquals(cache.getNokeyStmt().getWhereClause().toSql(),"`eventid` = 1");

            PartitionRange range = cache.getPartitionRange();
            boolean flag = range.analytics();
            Assert.assertEquals(flag,true);

            int size = range.getPartitionSingleList().size();
            Assert.assertEquals(size, 4);
            
            String sql;        
            range.setCacheFlag(20200112L);    //get data from cache
            range.setCacheFlag(20200113L);    //get data from cache

            range.buildDiskPartitionRange(newRangeList);

            cache.rewriteSelectStmt(newRangeList);
            sql = ca.getRewriteStmt().getWhereClause().toSql();
            LOG.warn("Join rewrite={}", sql);                
            Assert.assertEquals(sql,"((`appevent`.`eventdate` >= '2020-01-14')" +
                    " AND (`appevent`.`eventdate` <= '2020-01-15')) AND (`eventid` = 1)");
        } catch(Exception e){
            LOG.warn("Join ex={}",e);
            Assert.fail(e.getMessage());
        }
    }
    
    @Test
    public void testSubSelect() throws Exception {
        Catalog.getCurrentSystemInfo();
        StatementBase parseStmt = parseSql(
            "SELECT eventdate, sum(pv) FROM (SELECT eventdate, COUNT(userid) AS pv FROM appevent WHERE eventdate>\"2020-01-11\" AND eventdate<\"2020-01-16\"" +
                " AND eventid=1 GROUP BY eventdate) tbl GROUP BY eventdate"
        );
        List<ScanNode> scanNodes = Lists.newArrayList(createEventScanNode());
        CacheAnalyzer ca = new CacheAnalyzer(context,parseStmt, scanNodes);
        ca.checkCacheMode(1579053661000L);                           //2020-1-15 10:01:01
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Partition); //assert cache model first
        try{
            PartitionCache cache = (PartitionCache) ca.getCache();

            cache.rewriteSelectStmt(null);
            LOG.warn("Sub nokey={}", cache.getNokeyStmt().toSql());
            Assert.assertEquals(cache.getNokeyStmt().toSql(),"SELECT <slot 7> `eventdate` AS `eventdate`, <slot 8> sum(`pv`) AS `sum(``pv``)` FROM (" +
                "SELECT <slot 3> `eventdate` AS `eventdate`, <slot 4> count(`userid`) AS `pv` FROM `testCluster:testDb`.`appevent` WHERE `eventid` = 1" +
                " GROUP BY `eventdate`) tbl GROUP BY `eventdate`");

            PartitionRange range = cache.getPartitionRange();
            boolean flag = range.analytics();
            Assert.assertEquals(flag,true);

            int size = range.getPartitionSingleList().size();
            Assert.assertEquals(size, 4);
            
            String sql;        
            range.setCacheFlag(20200112L);    //get data from cache
            range.setCacheFlag(20200113L);    //get data from cache

            range.buildDiskPartitionRange(newRangeList);

            cache.rewriteSelectStmt(newRangeList);
            sql = ca.getRewriteStmt().toSql();
            LOG.warn("Sub rewrite={}", sql);                
            Assert.assertEquals(sql,"SELECT <slot 7> `eventdate` AS `eventdate`, <slot 8> sum(`pv`) AS `sum(``pv``)` FROM (" + 
                "SELECT <slot 3> `eventdate` AS `eventdate`, <slot 4> count(`userid`) AS `pv` FROM `testCluster:testDb`.`appevent` WHERE " + 
                "((`eventdate` > '2020-01-13') AND (`eventdate` < '2020-01-16')) AND (`eventid` = 1) GROUP BY `eventdate`) tbl GROUP BY `eventdate`");
        } catch(Exception e){
            LOG.warn("sub ex={}",e);
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testNotHitPartition() throws Exception {
        Catalog.getCurrentSystemInfo();
        StatementBase parseStmt = parseSql(
                "SELECT eventdate, COUNT(userid) FROM appevent WHERE eventdate>=\"2020-01-12\" and eventdate<=\"2020-01-14\" GROUP BY eventdate"
        );
        List<ScanNode> scanNodes = Lists.newArrayList(createEventScanNode());
        CacheAnalyzer ca = new CacheAnalyzer(context, parseStmt, scanNodes);
        ca.checkCacheMode(1579053661000L); //2020-1-15 10:01:01
        try {
            PartitionCache cache = (PartitionCache) ca.getCache();
            cache.rewriteSelectStmt(null);
            PartitionRange range = cache.getPartitionRange();
            range.analytics();
            hitRange = range.buildDiskPartitionRange(newRangeList);
            Assert.assertEquals(hitRange,Cache.HitRange.None);
            Assert.assertEquals(newRangeList.size(), 2);
            Assert.assertEquals(newRangeList.get(0).getCacheKey().realValue(), 20200112);
            Assert.assertEquals(newRangeList.get(1).getCacheKey().realValue(), 20200114);
        } catch (Exception e) {
            LOG.warn("ex={}", e);
            Assert.fail(e.getMessage());
        }
    }
}

