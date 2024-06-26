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

import org.apache.doris.analysis.AccessTestUtil;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.CreateViewStmt;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.View;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.proto.Types;
import org.apache.doris.qe.cache.Cache;
import org.apache.doris.qe.cache.CacheAnalyzer;
import org.apache.doris.qe.cache.CacheAnalyzer.CacheMode;
import org.apache.doris.qe.cache.CacheCoordinator;
import org.apache.doris.qe.cache.CacheProxy;
import org.apache.doris.qe.cache.PartitionRange;
import org.apache.doris.qe.cache.RowBatchBuilder;
import org.apache.doris.qe.cache.SqlCache;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.StringReader;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class OlapQueryCacheTest {
    private static final Logger LOG = LogManager.getLogger(OlapQueryCacheTest.class);
    public static String fullDbName = "testDb";
    public static String userName = "root";

    private static ConnectContext context;

    private List<PartitionRange.PartitionSingle> newRangeList;
    private Cache.HitRange hitRange;
    private Analyzer analyzer;
    private Database db;
    private Env env;
    private ConnectContext ctx;
    private QueryState state;
    private ConnectScheduler scheduler;
    @Mocked
    private MysqlChannel channel = null;

    @BeforeClass
    public static void start() {
        MetricRepo.init();
        try {
            FeConstants.enableInternalSchemaDb = false;
            FrontendOptions.init();
            context = new ConnectContext();
            Config.cache_enable_sql_mode = true;
            context.getSessionVariable().setEnableSqlCache(true);

            Config.cache_last_version_interval_second = 7200;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void setUp() throws Exception {
        state = new QueryState();
        scheduler = new ConnectScheduler(10);
        ctx = new ConnectContext();

        SessionVariable sessionVariable = new SessionVariable();
        Deencapsulation.setField(sessionVariable, "beNumberForTest", 1);
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        env = AccessTestUtil.fetchAdminCatalog();

        new MockUp<Util>() {
            @Mock
            public boolean showHiddenColumns() {
                return true;
            }
        };
        new MockUp<Env>() {
            @Mock
            Env getCurrentEnv() {
                return env;
            }
        };

        FunctionSet fs = new FunctionSet();
        fs.init();
        Deencapsulation.setField(env, "functionSet", fs);

        channel.reset();

        new Expectations(channel) {
            {
                channel.sendOnePacket((ByteBuffer) any);
                minTimes = 0;

                channel.reset();
                minTimes = 0;

                channel.getSerializer();
                minTimes = 0;
                result = serializer;
            }
        };

        new Expectations(ctx) {
            {
                ctx.getMysqlChannel();
                minTimes = 0;
                result = channel;

                ctx.getEnv();
                minTimes = 0;
                result = env;

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
                result = fullDbName;

                ctx.getSessionVariable();
                minTimes = 0;
                result = sessionVariable;

                ctx.setStmtId(anyLong);
                minTimes = 0;

                ctx.getStmtId();
                minTimes = 0;
                result = 1L;

                ctx.getCurrentCatalog();
                minTimes = 0;
                result = env.getCurrentCatalog();

                ctx.getCatalog(anyString);
                minTimes = 0;
                result = env.getCurrentCatalog();

                ConnectContext.get();
                minTimes = 0;
                result = ctx;

                ctx.getRemoteIP();
                minTimes = 0;
                result = "192.168.1.1";

                ctx.getCurrentUserIdentity();
                minTimes = 0;
                UserIdentity userIdentity = new UserIdentity(userName, "%");
                userIdentity.setIsAnalyzed();
                result = userIdentity;
            }
        };

        analyzer = new Analyzer(env, ctx);
        newRangeList = Lists.newArrayList();

        db = ((InternalCatalog) env.getCurrentCatalog()).getDbNullable(fullDbName);
        // table and view init use analyzer, should init after analyzer build
        OlapTable tbl1 = createOrderTable();
        db.registerTable(tbl1);
        OlapTable tbl2 = createProfileTable();
        db.registerTable(tbl2);
        OlapTable tbl3 = createEventTable();
        db.registerTable(tbl3);

        // build view meta inline sql and create view directly, the originStmt from inline sql
        // should be analyzed by create view statement analyzer and then to sql
        View view1 = createEventView1();
        db.registerTable(view1);
        View view2 = createEventView2();
        db.registerTable(view2);
        View view3 = createEventView3();
        db.registerTable(view3);
        View view4 = createEventNestedView();
        db.registerTable(view4);
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
        part12.setVisibleVersionAndTime(1, 1578762000000L);     //2020-01-12 1:00:00
        setPartitionItem(partInfo, part12, column1, "20200112", "20200113");
        Partition part13 = new Partition(20200113, "p20200113", baseIndex, distInfo);
        part13.setVisibleVersionAndTime(1, 1578848400000L);     //2020-01-13 1:00:00
        setPartitionItem(partInfo, part13, column1, "20200113", "20200114");
        Partition part14 = new Partition(20200114, "p20200114", baseIndex, distInfo);
        part14.setVisibleVersionAndTime(1, 1578934800000L);     //2020-01-14 1:00:00
        setPartitionItem(partInfo, part14, column1, "20200114", "20200115");
        Partition part15 = new Partition(20200115, "p20200115", baseIndex, distInfo);
        part15.setVisibleVersionAndTime(2, 1579053661000L);     //2020-01-15 10:01:01
        setPartitionItem(partInfo, part15, column1, "20200115", "20200116");

        OlapTable table = new OlapTable(10000L, "order", columns, KeysType.DUP_KEYS, partInfo, distInfo);

        short shortKeyColumnCount = 1;
        table.setIndexMeta(10001, "group1", columns, 1, 1, shortKeyColumnCount, TStorageType.COLUMN, KeysType.DUP_KEYS);

        List<Column> idxColumns = Lists.newArrayList();
        idxColumns.add(column1);
        table.setIndexMeta(1L, "test", idxColumns, 1, 1, shortKeyColumnCount, TStorageType.COLUMN,
                KeysType.DUP_KEYS);
        Deencapsulation.setField(table, "baseIndexId", 1000);

        table.addPartition(part12);
        table.addPartition(part13);
        table.addPartition(part14);
        table.addPartition(part15);

        return table;
    }

    private void setPartitionItem(PartitionInfo partInfo, Partition partition, Column column, String rangeKeyLower,
                                  String rangeKeyUpper) {
        try {
            PartitionKey rangeP1Lower =
                    PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue(rangeKeyLower)),
                            Lists.newArrayList(column));
            PartitionKey rangeP1Upper =
                    PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue(rangeKeyUpper)),
                            Lists.newArrayList(column));
            Range<PartitionKey> rangeP1 = Range.closedOpen(rangeP1Lower, rangeP1Upper);
            partInfo.setItem(partition.getId(), false, new RangePartitionItem(rangeP1));
        } catch (AnalysisException e) {
            LOG.warn("Part,an_ex={}", e);
            Assert.fail(e.getMessage());
        }
    }

    private OlapTable createProfileTable() {
        Column column2 = new Column("eventdate", ScalarType.DATE);
        Column column3 = new Column("userid", ScalarType.INT);
        Column column4 = new Column("country", ScalarType.INT);
        List<Column> columns = Lists.newArrayList(column2, column3, column4);

        MaterializedIndex baseIndex = new MaterializedIndex(20001, IndexState.NORMAL);
        RandomDistributionInfo distInfo = new RandomDistributionInfo(10);

        PartitionInfo partInfo = new RangePartitionInfo(Lists.newArrayList(column2));

        Partition part12 = new Partition(20200112, "p20200112", baseIndex, distInfo);
        part12.setVisibleVersionAndTime(1, 1578762000000L);     //2020-01-12 1:00:00
        Partition part13 = new Partition(20200113, "p20200113", baseIndex, distInfo);
        part13.setVisibleVersionAndTime(1, 1578848400000L);     //2020-01-13 1:00:00
        Partition part14 = new Partition(20200114, "p20200114", baseIndex, distInfo);
        part14.setVisibleVersionAndTime(1, 1578934800000L);     //2020-01-14 1:00:00
        Partition part15 = new Partition(20200115, "p20200115", baseIndex, distInfo);
        part15.setVisibleVersionAndTime(2, 1579021200000L);     //2020-01-15 1:00:00

        OlapTable table = new OlapTable(20000L, "userprofile", columns, KeysType.AGG_KEYS, partInfo, distInfo);

        short shortKeyColumnCount = 1;
        table.setIndexMeta(20001, "group1", columns, 1, 1, shortKeyColumnCount, TStorageType.COLUMN, KeysType.AGG_KEYS);

        List<Column> idxColumns = Lists.newArrayList();
        idxColumns.add(column2);
        table.setIndexMeta(2L, "test", idxColumns, 1, 1, shortKeyColumnCount, TStorageType.COLUMN,
                KeysType.AGG_KEYS);

        Deencapsulation.setField(table, "baseIndexId", 1000);

        table.addPartition(part12);
        table.addPartition(part13);
        table.addPartition(part14);
        table.addPartition(part15);

        return table;
    }

    private ScanNode createProfileScanNode(Collection<Long> selectedPartitionIds) {
        OlapTable table = createProfileTable();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(20004));
        desc.setTable(table);
        OlapScanNode node = new OlapScanNode(new PlanNodeId(20008), desc, "userprofilenode");
        node.setSelectedPartitionIds(selectedPartitionIds);
        return node;
    }

    /**
     * table appevent(date(pk), userid, eventid, eventtime, city), stream load every 5 miniutes
     */
    private OlapTable createEventTable() {
        Column column1 = new Column("eventdate", ScalarType.DATE);
        Column column2 = new Column("userid", ScalarType.INT);
        Column column3 = new Column("eventid", ScalarType.INT);
        Column column4 = new Column("eventtime", ScalarType.DATETIME);
        Column column5 = new Column("city", ScalarType.VARCHAR);
        List<Column> columns = Lists.newArrayList(column1, column2, column3, column4, column5);
        PartitionInfo partInfo = new RangePartitionInfo(Lists.newArrayList(column1));
        MaterializedIndex baseIndex = new MaterializedIndex(30001, IndexState.NORMAL);
        RandomDistributionInfo distInfo = new RandomDistributionInfo(10);

        Partition part12 = new Partition(20200112, "p20200112", baseIndex, distInfo);
        part12.setVisibleVersionAndTime(1, 1578762000000L);     //2020-01-12 1:00:00
        setPartitionItem(partInfo, part12, column1, "20200112", "20200113");
        Partition part13 = new Partition(20200113, "p20200113", baseIndex, distInfo);
        part13.setVisibleVersionAndTime(1, 1578848400000L);     //2020-01-13 1:00:00
        setPartitionItem(partInfo, part13, column1, "20200113", "20200114");
        Partition part14 = new Partition(20200114, "p20200114", baseIndex, distInfo);
        part14.setVisibleVersionAndTime(1, 1578934800000L);     //2020-01-14 1:00:00
        setPartitionItem(partInfo, part14, column1, "20200114", "20200115");
        Partition part15 = new Partition(20200115, "p20200115", baseIndex, distInfo);
        part15.setVisibleVersionAndTime(2, 1579053661000L);     //2020-01-15 10:01:01
        setPartitionItem(partInfo, part15, column1, "20200115", "20200116");

        OlapTable table = new OlapTable(30000L, "appevent", columns, KeysType.DUP_KEYS, partInfo, distInfo);

        short shortKeyColumnCount = 1;
        table.setIndexMeta(30001, "group1", columns, 1, 1, shortKeyColumnCount, TStorageType.COLUMN, KeysType.AGG_KEYS);

        List<Column> column = Lists.newArrayList();
        column.add(column1);
        column.add(column2);
        column.add(column3);
        column.add(column4);
        column.add(column5);

        table.setIndexMeta(new Long(2), "test", column, 1, 1, shortKeyColumnCount, TStorageType.COLUMN,
                KeysType.AGG_KEYS);
        Deencapsulation.setField(table, "baseIndexId", 2);

        table.addPartition(part12);
        table.addPartition(part13);
        table.addPartition(part14);
        table.addPartition(part15);

        return table;
    }

    private View createEventView1() {
        String originStmt = "select eventdate, COUNT(userid) FROM appevent WHERE "
                + "eventdate>=\"2020-01-12\" and eventdate<=\"2020-01-14\" GROUP BY eventdate";
        View view = new View(30000L, "view1", null);
        Analyzer createViewAnalyzer = new Analyzer(env, ctx);
        createViewAnalyzer.setRootStatementClazz(CreateViewStmt.class);
        view.setInlineViewDefWithSqlMode(parseSql(originStmt, createViewAnalyzer, true).toSql(), 0L);
        return view;
    }

    private View createEventView2() {
        String originStmt = "select eventdate, userid FROM appevent";
        View view = new View(30001L, "view2", null);
        Analyzer createViewAnalyzer = new Analyzer(env, ctx);
        createViewAnalyzer.setRootStatementClazz(CreateViewStmt.class);
        view.setInlineViewDefWithSqlMode(parseSql(originStmt, createViewAnalyzer, true).toSql(), 0L);
        return view;
    }

    private View createEventView3() {
        String originStmt = "select eventdate, COUNT(userid) FROM appevent WHERE "
                + "eventdate>=\"2020-01-12\" and eventdate<=\"2020-01-15\" GROUP BY eventdate";
        View view = new View(30002L, "view3", null);
        Analyzer createViewAnalyzer = new Analyzer(env, ctx);
        createViewAnalyzer.setRootStatementClazz(CreateViewStmt.class);
        view.setInlineViewDefWithSqlMode(parseSql(originStmt, createViewAnalyzer, true).toSql(), 0L);
        return view;
    }

    private View createEventNestedView() {
        String originStmt = "select eventdate, COUNT(userid) FROM view2 WHERE "
                + "eventdate>=\"2020-01-12\" and eventdate<=\"2020-01-14\" GROUP BY eventdate";
        View view = new View(30003L, "view4", null);
        Analyzer createViewAnalyzer = new Analyzer(env, ctx);
        createViewAnalyzer.setRootStatementClazz(CreateViewStmt.class);
        view.setInlineViewDefWithSqlMode(
                parseSql(originStmt, createViewAnalyzer, true).toSql(), 0L);
        return view;
    }

    private ScanNode createEventScanNode(Collection<Long> selectedPartitionIds) {
        OlapTable table = createEventTable();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(30002));
        desc.setTable(table);
        OlapScanNode node = new OlapScanNode(new PlanNodeId(30004), desc, "appeventnode");
        node.setSelectedPartitionIds(selectedPartitionIds);
        return node;
    }

    private StatementBase parseSqlByNereids(String sql) {
        StatementBase stmt = null;
        try {
            LogicalPlan plan = new NereidsParser().parseSingle(sql);
            OriginStatement originStatement = new OriginStatement(sql, 0);
            StatementContext statementContext = new StatementContext(ctx, originStatement);
            ctx.setStatementContext(statementContext);
            NereidsPlanner nereidsPlanner = new NereidsPlanner(statementContext);
            LogicalPlanAdapter adapter = new LogicalPlanAdapter(plan, statementContext);
            nereidsPlanner.plan(adapter);
            statementContext.setParsedStatement(adapter);
            stmt = adapter;
        } catch (Throwable throwable) {
            LOG.warn("Part,an_ex={}", throwable);
            Assert.fail(throwable.getMessage());
        }
        return stmt;
    }

    private StatementBase parseSql(String sql) {
        return parseSql(sql, null, false);
    }

    private StatementBase parseSql(String sql, Analyzer analyzer, boolean needToSql) {
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(sql)));
        StatementBase parseStmt = null;
        try {
            parseStmt = SqlParserUtils.getFirstStmt(parser);
            if (parseStmt instanceof QueryStmt) {
                ((QueryStmt) parseStmt).setNeedToSql(needToSql);
            }
            parseStmt.analyze(analyzer == null ? this.analyzer : analyzer);
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
        CacheCoordinator cp = CacheCoordinator.getInstance();
        cp.debugModel = true;
        Backend bd1 = new Backend(1, "", 1000);
        bd1.updateOnce(0, 0, 0);
        Backend bd2 = new Backend(2, "", 2000);
        bd2.updateOnce(0, 0, 0);
        Backend bd3 = new Backend(3, "", 3000);
        bd3.updateOnce(0, 0, 0);
        cp.addBackend(bd1);
        cp.addBackend(bd2);
        cp.addBackend(bd3);

        Types.PUniqueId key1 = Types.PUniqueId.newBuilder().setHi(1L).setLo(1L).build();
        Backend bk = cp.findBackend(key1);
        Assert.assertNotNull(bk);
        Assert.assertEquals(bk.getId(), 3);

        key1 = key1.toBuilder().setHi(669560558156283345L).build();
        bk = cp.findBackend(key1);
        Assert.assertNotNull(bk);
        Assert.assertEquals(bk.getId(), 1);
    }

    @Test
    public void testCacheModeNone() throws Exception {
        StatementBase parseStmt = parseSql("select @@version_comment limit 1");
        List<ScanNode> scanNodes = Lists.newArrayList();
        CacheAnalyzer ca = new CacheAnalyzer(context, parseStmt, scanNodes);
        ca.checkCacheMode(0);
        Assert.assertEquals(ca.getCacheMode(), CacheMode.NoNeed);
    }

    @Test
    public void testCacheModeTable() throws Exception {
        StatementBase parseStmt = parseSql(
                "SELECT country, COUNT(userid) FROM userprofile GROUP BY country"
        );
        ArrayList<Long> selectedPartitionIds
                = Lists.newArrayList(20200112L, 20200113L, 20200114L, 20200115L);
        List<ScanNode> scanNodes = Lists.newArrayList(createProfileScanNode(selectedPartitionIds));
        CacheAnalyzer ca = new CacheAnalyzer(context, parseStmt, scanNodes);
        ca.checkCacheMode(0);
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Sql);
    }

    @Test
    public void testWithinMinTime() throws Exception {
        StatementBase parseStmt = parseSql(
                "SELECT country, COUNT(userid) FROM userprofile GROUP BY country"
        );
        ArrayList<Long> selectedPartitionIds
                = Lists.newArrayList(20200112L, 20200113L, 20200114L, 20200115L);
        List<ScanNode> scanNodes = Lists.newArrayList(createProfileScanNode(selectedPartitionIds));
        CacheAnalyzer ca = new CacheAnalyzer(context, parseStmt, scanNodes);
        ca.checkCacheMode(1579024800000L);  //2020-1-15 02:00:00
        Assert.assertEquals(ca.getCacheMode(), CacheMode.None);
    }

    @Test
    public void testParseByte() throws Exception {
        RowBatchBuilder sb = new RowBatchBuilder(CacheMode.Partition);
        byte[] buffer = new byte[]{10, 50, 48, 50, 48, 45, 48, 51, 45, 49, 48, 1, 51, 2, 67, 78};
        PartitionRange.PartitionKeyType key1 = sb.getKeyFromRow(buffer, 0, Type.DATE);
        LOG.info("real value key1 {}", key1.realValue());
        Assert.assertEquals(key1.realValue(), 20200310);
        PartitionRange.PartitionKeyType key2 = sb.getKeyFromRow(buffer, 1, Type.INT);
        LOG.info("real value key2 {}", key2.realValue());
        Assert.assertEquals(key2.realValue(), 3);
    }

    @Test
    public void testHitSqlCache() throws Exception {
        StatementBase parseStmt = parseSql(
                "SELECT eventdate, COUNT(userid) FROM appevent WHERE eventdate>=\"2020-01-12\" and "
                        + "eventdate<=\"2020-01-14\" GROUP BY eventdate"
        );
        ArrayList<Long> selectedPartitionIds
                = Lists.newArrayList(20200112L, 20200113L, 20200114L);
        List<ScanNode> scanNodes = Lists.newArrayList(createEventScanNode(selectedPartitionIds));
        CacheAnalyzer ca = new CacheAnalyzer(context, parseStmt, scanNodes);
        ca.checkCacheMode(1579053661000L); //2020-1-15 10:01:01
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Sql);
    }

    @Test
    public void testSqlCacheKey() {
        StatementBase parseStmt = parseSql(
                "SELECT eventdate, COUNT(userid) FROM appevent WHERE eventdate>=\"2020-01-12\" and "
                        + "eventdate<=\"2020-01-14\" GROUP BY eventdate"
        );
        ArrayList<Long> selectedPartitionIds
                = Lists.newArrayList(20200112L, 20200113L, 20200114L);
        List<ScanNode> scanNodes = Lists.newArrayList(createEventScanNode(selectedPartitionIds));
        CacheAnalyzer ca = new CacheAnalyzer(context, parseStmt, scanNodes);
        ca.checkCacheMode(1579053661000L); //2020-1-15 10:01:01
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Sql);

        SqlCache sqlCache = (SqlCache) ca.getCache();
        String cacheKey = sqlCache.getSqlWithViewStmt();
        Assert.assertEquals(cacheKey, "SELECT `eventdate` AS `eventdate`, count(`userid`) "
                + "AS `count(``userid``)` FROM `testDb`.`appevent` WHERE (`eventdate` >= '2020-01-12') "
                + "AND (`eventdate` <= '2020-01-14') GROUP BY `eventdate`|");
        Assert.assertEquals(selectedPartitionIds.size(), sqlCache.getSumOfPartitionNum());
    }

    @Test
    public void testSqlCacheKeyWithChineseChar() {
        StatementBase parseStmt = parseSql(
                "SELECT eventdate, COUNT(userid) FROM appevent WHERE eventdate>=\"2020-01-12\" and "
                        + "eventdate<=\"2020-01-14\" and city=\"北京\" GROUP BY eventdate"
        );
        ArrayList<Long> selectedPartitionIds
                = Lists.newArrayList(20200112L, 20200113L, 20200114L);
        List<ScanNode> scanNodes = Lists.newArrayList(createEventScanNode(selectedPartitionIds));
        CacheAnalyzer ca = new CacheAnalyzer(context, parseStmt, scanNodes);
        ca.checkCacheMode(1579053661000L); //2020-1-15 10:01:01
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Sql);
        SqlCache sqlCache = (SqlCache) ca.getCache();
        String cacheKey = sqlCache.getSqlWithViewStmt();
        Types.PUniqueId sqlKey2 = CacheProxy.getMd5(cacheKey.replace("北京", "上海"));
        Assert.assertNotEquals(CacheProxy.getMd5(sqlCache.getSqlWithViewStmt()), sqlKey2);
        Assert.assertEquals(selectedPartitionIds.size(), sqlCache.getSumOfPartitionNum());
    }

    @Test
    public void testSqlCacheKeyWithView() {
        StatementBase parseStmt = parseSql("SELECT * from testDb.view1");
        ArrayList<Long> selectedPartitionIds
                = Lists.newArrayList(20200112L, 20200113L, 20200114L);
        List<ScanNode> scanNodes = Lists.newArrayList(createEventScanNode(selectedPartitionIds));
        CacheAnalyzer ca = new CacheAnalyzer(context, parseStmt, scanNodes);
        ca.checkCacheMode(1579053661000L); //2020-1-15 10:01:01
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Sql);

        SqlCache sqlCache = (SqlCache) ca.getCache();
        String cacheKey = sqlCache.getSqlWithViewStmt();
        Assert.assertEquals(cacheKey, "SELECT `testDb`.`view1`.`eventdate` AS `eventdate`, "
                + "`testDb`.`view1`.`__count_1` AS `__count_1` FROM `testDb`.`view1`|"
                + "SELECT `eventdate` AS `eventdate`, count(`userid`) AS `__count_1` FROM "
                + "`testDb`.`appevent` WHERE (`eventdate` >= '2020-01-12') AND "
                + "(`eventdate` <= '2020-01-14') GROUP BY `eventdate`");
        Assert.assertEquals(selectedPartitionIds.size(), sqlCache.getSumOfPartitionNum());
    }

    @Test
    public void testSqlCacheKeyWithViewForNereids() {
        StatementBase parseStmt = parseSqlByNereids("SELECT * from testDb.view1");
        ArrayList<Long> selectedPartitionIds
                = Lists.newArrayList(20200112L, 20200113L, 20200114L);
        List<ScanNode> scanNodes = Lists.newArrayList(createEventScanNode(selectedPartitionIds));
        CacheAnalyzer ca = new CacheAnalyzer(context, parseStmt, scanNodes);
        ca.checkCacheModeForNereids(1579053661000L); //2020-1-15 10:01:01
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Sql);

        SqlCache sqlCache = (SqlCache) ca.getCache();
        String cacheKey = sqlCache.getSqlWithViewStmt();
        Assert.assertEquals(cacheKey, "SELECT * from testDb.view1|SELECT `eventdate` AS `eventdate`, "
                + "count(`userid`) AS `__count_1` FROM `testDb`.`appevent` "
                + "WHERE (`eventdate` >= '2020-01-12') AND (`eventdate` <= '2020-01-14') GROUP BY `eventdate`");
        Assert.assertEquals(selectedPartitionIds.size(), sqlCache.getSumOfPartitionNum());
    }

    @Test
    public void testSqlCacheKeyWithSubSelectView() {
        StatementBase parseStmt = parseSql(
                "select origin.eventdate as eventdate, origin.userid as userid\n"
                        + "from (\n"
                        + "    select view2.eventdate as eventdate, view2.userid as userid \n"
                        + "    from testDb.view2 view2 \n"
                        + "    where view2.eventdate >=\"2020-01-12\" and view2.eventdate <= \"2020-01-14\"\n"
                        + ") origin"
        );
        ArrayList<Long> selectedPartitionIds
                = Lists.newArrayList(20200112L, 20200113L, 20200114L);
        List<ScanNode> scanNodes = Lists.newArrayList(createEventScanNode(selectedPartitionIds));
        CacheAnalyzer ca = new CacheAnalyzer(context, parseStmt, scanNodes);
        ca.checkCacheMode(1579053661000L); //2020-1-15 10:01:01
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Sql);

        SqlCache sqlCache = (SqlCache) ca.getCache();
        String cacheKey = sqlCache.getSqlWithViewStmt();
        Assert.assertEquals(cacheKey, "SELECT `origin`.`eventdate` AS `eventdate`, "
                + "`origin`.`userid` AS `userid` FROM (SELECT `view2`.`eventdate` `eventdate`, "
                + "`view2`.`userid` `userid` FROM `testDb`.`view2` view2 "
                + "WHERE (`view2`.`eventdate` >= '2020-01-12') AND (`view2`.`eventdate` <= '2020-01-14')) origin|"
                + "SELECT `eventdate` AS `eventdate`, `userid` AS `userid` FROM `testDb`.`appevent`");
        Assert.assertEquals(selectedPartitionIds.size(), sqlCache.getSumOfPartitionNum());
    }


    @Test
    public void testSqlCacheKeyWithSubSelectViewForNereids() {
        StatementBase parseStmt = parseSqlByNereids(
                "select origin.eventdate as eventdate, origin.userid as userid\n"
                        + "from (\n"
                        + "    select view2.eventdate as eventdate, view2.userid as userid \n"
                        + "    from testDb.view2 view2 \n"
                        + "    where view2.eventdate >=\"2020-01-12\" and view2.eventdate <= \"2020-01-14\"\n"
                        + ") origin"
        );
        ArrayList<Long> selectedPartitionIds
                = Lists.newArrayList(20200112L, 20200113L, 20200114L);
        List<ScanNode> scanNodes = Lists.newArrayList(createEventScanNode(selectedPartitionIds));
        CacheAnalyzer ca = new CacheAnalyzer(context, parseStmt, scanNodes);
        ca.checkCacheModeForNereids(1579053661000L); //2020-1-15 10:01:01
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Sql);

        SqlCache sqlCache = (SqlCache) ca.getCache();
        String cacheKey = sqlCache.getSqlWithViewStmt();
        Assert.assertEquals(cacheKey, "select origin.eventdate as eventdate, origin.userid as userid\n"
                + "from (\n"
                + "    select view2.eventdate as eventdate, view2.userid as userid \n"
                + "    from testDb.view2 view2 \n"
                + "    where view2.eventdate >=\"2020-01-12\" and view2.eventdate <= \"2020-01-14\"\n"
                + ") origin|SELECT `eventdate` AS `eventdate`, `userid` AS `userid` FROM `testDb`.`appevent`");
        Assert.assertEquals(selectedPartitionIds.size(), sqlCache.getSumOfPartitionNum());
    }

    @Test
    public void testSqlCacheKeyWithNestedView() {
        StatementBase parseStmt = parseSql("SELECT * from testDb.view4");
        ArrayList<Long> selectedPartitionIds
                = Lists.newArrayList(20200112L, 20200113L, 20200114L);
        List<ScanNode> scanNodes = Lists.newArrayList(createEventScanNode(selectedPartitionIds));
        CacheAnalyzer ca = new CacheAnalyzer(context, parseStmt, scanNodes);
        ca.checkCacheMode(1579053661000L); //2020-1-15 10:01:01
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Sql);

        SqlCache sqlCache = (SqlCache) ca.getCache();
        String cacheKey = sqlCache.getSqlWithViewStmt();
        Assert.assertEquals(cacheKey, "SELECT `testDb`.`view4`.`eventdate` AS `eventdate`, "
                + "`testDb`.`view4`.`__count_1` AS `__count_1` FROM `testDb`.`view4`|"
                + "SELECT `eventdate` AS `eventdate`, count(`userid`) AS `__count_1` FROM `testDb`.`view2` "
                + "WHERE (`eventdate` >= '2020-01-12') AND (`eventdate` <= '2020-01-14') GROUP BY `eventdate`|"
                + "SELECT `eventdate` AS `eventdate`, `userid` AS `userid` FROM `testDb`.`appevent`");
        Assert.assertEquals(selectedPartitionIds.size(), sqlCache.getSumOfPartitionNum());
    }

    @Test
    public void testSqlCacheKeyWithNestedViewForNereids() {
        StatementBase parseStmt = parseSqlByNereids("SELECT * from testDb.view4");
        ArrayList<Long> selectedPartitionIds
                = Lists.newArrayList(20200112L, 20200113L, 20200114L);
        List<ScanNode> scanNodes = Lists.newArrayList(createEventScanNode(selectedPartitionIds));
        CacheAnalyzer ca = new CacheAnalyzer(context, parseStmt, scanNodes);
        ca.checkCacheModeForNereids(1579053661000L); //2020-1-15 10:01:01
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Sql);

        SqlCache sqlCache = (SqlCache) ca.getCache();
        String cacheKey = sqlCache.getSqlWithViewStmt();
        Assert.assertEquals(cacheKey, "SELECT * from testDb.view4|SELECT `eventdate` AS `eventdate`, "
                + "count(`userid`) AS `__count_1` FROM `testDb`.`view2` WHERE (`eventdate` >= '2020-01-12') "
                + "AND (`eventdate` <= '2020-01-14') GROUP BY `eventdate`|SELECT `eventdate` AS `eventdate`, "
                + "`userid` AS `userid` FROM `testDb`.`appevent`");
        Assert.assertEquals(selectedPartitionIds.size(), sqlCache.getSumOfPartitionNum());
    }

    @Test
    public void testCacheLocalViewMultiOperand() {
        StatementBase parseStmt = parseSql(
                "SELECT COUNT(userid)\n"
                        + "FROM (\n"
                        + "    (SELECT userid FROM userprofile\n"
                        + "    INTERSECT\n"
                        + "    SELECT userid FROM userprofile)\n"
                        + "    UNION\n"
                        + "    SELECT userid FROM userprofile\n"
                        + ") as tmp"
        );
        ArrayList<Long> selectedPartitionIds
                = Lists.newArrayList(20200112L, 20200113L, 20200114L, 20200115L);
        ScanNode scanNode = createProfileScanNode(selectedPartitionIds);
        List<ScanNode> scanNodes = Lists.newArrayList(scanNode, scanNode, scanNode);
        CacheAnalyzer ca = new CacheAnalyzer(context, parseStmt, scanNodes);
        ca.checkCacheMode(0);
        Assert.assertEquals(ca.getCacheMode(), CacheMode.Sql);
        Assert.assertEquals(selectedPartitionIds.size() * 3, ((SqlCache) ca.getCache()).getSumOfPartitionNum());
    }
}
