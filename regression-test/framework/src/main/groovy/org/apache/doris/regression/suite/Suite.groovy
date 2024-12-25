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

package org.apache.doris.regression.suite

import com.google.common.collect.Maps
import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.MoreExecutors
import com.google.gson.Gson
import groovy.json.JsonSlurper
import com.google.common.collect.ImmutableList
import org.apache.commons.lang3.ObjectUtils
import org.apache.doris.regression.Config
import org.apache.doris.regression.action.BenchmarkAction
import org.apache.doris.regression.action.ProfileAction
import org.apache.doris.regression.action.WaitForAction
import org.apache.doris.regression.util.DataUtils
import org.apache.doris.regression.util.OutputUtils
import org.apache.doris.regression.action.CreateMVAction
import org.apache.doris.regression.action.ExplainAction
import org.apache.doris.regression.action.RestoreAction
import org.apache.doris.regression.action.StreamLoadAction
import org.apache.doris.regression.action.SuiteAction
import org.apache.doris.regression.action.TestAction
import org.apache.doris.regression.action.HttpCliAction
import org.apache.doris.regression.util.JdbcUtils
import org.apache.doris.regression.util.Hdfs
import org.apache.doris.regression.util.SuiteUtils
import org.apache.doris.regression.util.DebugPoint
import org.apache.doris.regression.RunMode
import org.junit.jupiter.api.Assertions

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import groovy.util.logging.Slf4j

import java.sql.Connection
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicBoolean
import java.util.stream.Collectors
import java.util.stream.LongStream
import static org.apache.doris.regression.util.DataUtils.sortByToString

import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSetMetaData
import org.junit.Assert

@Slf4j
class Suite implements GroovyInterceptable {
    final SuiteContext context
    final SuiteCluster cluster
    final DebugPoint debugPoint

    final String name
    final String group
    final Logger logger = LoggerFactory.getLogger(this.class)

    // set this in suite to determine which hive docker to use
    String hivePrefix = "hive2"

    final List<Closure> successCallbacks = new Vector<>()
    final List<Closure> failCallbacks = new Vector<>()
    final List<Closure> finishCallbacks = new Vector<>()
    final List<Throwable> lazyCheckExceptions = new Vector<>()
    final List<Future> lazyCheckFutures = new Vector<>()

    Suite(String name, String group, SuiteContext context, SuiteCluster cluster) {
        this.name = name
        this.group = group
        this.context = context
        this.cluster = cluster;
        this.debugPoint = new DebugPoint(this)
    }

    String getConf(String key, String defaultValue = null) {
        String value = context.config.otherConfigs.get(key)
        return value == null ? defaultValue : value
    }

    String getSuiteConf(String key, String defaultValue = null) {
        return getConf("suites." + name + "." + key, defaultValue)
    }

    Properties getConfs(String prefix) {
        Properties p = new Properties()
        for (String name : context.config.otherConfigs.stringPropertyNames()) {
            if (name.startsWith(prefix + ".")) {
                p.put(name.substring(prefix.length() + 1), context.config.getProperty(name))
            }
        }
        return p
    }

    void onSuccess(Closure callback) {
        successCallbacks.add(callback)
    }

    void onFail(Closure callback) {
        failCallbacks.add(callback)
    }

    void onFinish(Closure callback) {
        finishCallbacks.add(callback)
    }

    LongStream range(long startInclusive, long endExclusive) {
        return LongStream.range(startInclusive, endExclusive)
    }

    LongStream rangeClosed(long startInclusive, long endInclusive) {
        return LongStream.rangeClosed(startInclusive, endInclusive)
    }

    String toCsv(List<Object> rows) {
        StringBuilder sb = new StringBuilder()
        for (int i = 0; i < rows.size(); ++i) {
            Object row = rows.get(i)
            if (!(row instanceof List)) {
                row = ImmutableList.of(row)
            }
            sb.append(OutputUtils.toCsvString(row as List)).append("\n")
        }
        sb.toString()
    }

    Object parseJson(String str) {
        def jsonSlurper = new JsonSlurper()
        return jsonSlurper.parseText(str)
    }

    public <T> T lazyCheck(Closure<T> closure) {
        try {
            T result = closure.call()
            if (result instanceof Future) {
                lazyCheckFutures.add(result)
            }
            return result
        } catch (Throwable t) {
            lazyCheckExceptions.add(t)
            return null
        }
    }

    void doLazyCheck() {
        if (!lazyCheckExceptions.isEmpty()) {
            throw lazyCheckExceptions.get(0)
        }
        lazyCheckFutures.forEach { it.get() }
    }

    public <T> Tuple2<T, Long> timer(Closure<T> actionSupplier) {
        return SuiteUtils.timer(actionSupplier)
    }

    public <T> ListenableFuture<T> extraThread(
            String threadName = null, boolean daemon = false, Closure<T> actionSupplier) {
        def executorService = Executors.newFixedThreadPool(1, new ThreadFactory() {
            @Override
            Thread newThread(Runnable r) {
                def thread = new Thread(r, name)
                thread.setDaemon(daemon)
                return thread
            }
        })

        try {
            def connInfo = context.threadLocalConn.get()
            return MoreExecutors.listeningDecorator(executorService).submit(
                    buildThreadCallable(threadName, connInfo, actionSupplier)
            )
        } finally {
            executorService.shutdown()
        }
    }

    public <T> ListenableFuture<T> thread(String threadName = null, Closure<T> actionSupplier) {
        def connInfo = context.threadLocalConn.get()
        return MoreExecutors.listeningDecorator(context.actionExecutors).submit(
                buildThreadCallable(threadName, connInfo, actionSupplier)
        )
    }

    private <T> Callable<T> buildThreadCallable(String threadName, ConnectionInfo connInfo, Closure<T> actionSupplier) {
        return new Callable<T>() {
            @Override
            T call() throws Exception {
                long startTime = System.currentTimeMillis()
                def originThreadName = Thread.currentThread().name
                try {
                    Thread.currentThread().setName(threadName == null ? originThreadName : threadName)
                    if (connInfo != null) {
                        context.connectTo(connInfo.conn.getMetaData().getURL(), connInfo.username, connInfo.password);
                    }
                    context.scriptContext.eventListeners.each { it.onThreadStarted(context) }

                    return actionSupplier.call()
                } catch (Throwable t) {
                    context.scriptContext.eventListeners.each { it.onThreadFailed(context, t) }
                    throw t
                } finally {
                    try {
                        context.closeThreadLocal()
                    } catch (Throwable t) {
                        logger.warn("Close thread local context failed", t)
                    }
                    long finishTime = System.currentTimeMillis()
                    context.scriptContext.eventListeners.each { it.onThreadFinished(context, finishTime - startTime) }
                    Thread.currentThread().setName(originThreadName)
                }
            }
        };
    }

    public <T> ListenableFuture<T> lazyCheckThread(String threadName = null, Closure<T> actionSupplier) {
        return lazyCheck {
            thread(threadName, actionSupplier)
        }
    }

    public <T> ListenableFuture<T> combineFutures(ListenableFuture<T> ... futures) {
        return Futures.allAsList(futures)
    }

    public <T> ListenableFuture<List<T>> combineFutures(Iterable<? extends ListenableFuture<? extends T>> futures) {
        return Futures.allAsList(futures)
    }

    public <T> T connect(String user = context.config.jdbcUser, String password = context.config.jdbcPassword,
                         String url = context.config.jdbcUrl, Closure<T> actionSupplier) {
        return context.connect(user, password, url, actionSupplier)
    }

    public void docker(ClusterOptions options = new ClusterOptions(), Closure actionSupplier) throws Exception {
        if (context.config.excludeDockerTest) {
            return
        }

        boolean pipelineIsCloud = isCloudMode()
        boolean dockerIsCloud = false
        if (options.cloudMode == null) {
            dockerIsCloud = pipelineIsCloud
        } else {
            dockerIsCloud = options.cloudMode
            if (dockerIsCloud != pipelineIsCloud) {
                return
            }
        }

        try {
            cluster.destroy(true)
            cluster.init(options, dockerIsCloud)

            def user = context.config.jdbcUser
            def password = context.config.jdbcPassword
            Frontend fe = null
            for (def i=0; fe == null && i<30; i++) {
                if (options.connectToFollower) {
                    fe = cluster.getOneFollowerFe()
                } else {
                    fe = cluster.getMasterFe()
                }
                Thread.sleep(1000)
            }

            assertNotNull(fe)
            if (!dockerIsCloud) {
                for (def be : cluster.getAllBackends()) {
                    be_report_disk(be.host, be.httpPort)
                }
            }

            // wait be report
            Thread.sleep(5000)
            def url = String.format(
                    "jdbc:mysql://%s:%s/?useLocalSessionState=false&allowLoadLocalInfile=false",
                    fe.host, fe.queryPort)
            def conn = DriverManager.getConnection(url, user, password)
            def sql = "CREATE DATABASE IF NOT EXISTS " + context.dbName
            logger.info("try create database if not exists {}", context.dbName)
            JdbcUtils.executeToList(conn, sql)
            url = Config.buildUrlWithDb(url, context.dbName)

            logger.info("connect to docker cluster: suite={}, url={}", name, url)
            connect(user, password, url, actionSupplier)
        } finally {
            if (!context.config.dockerEndNoKill) {
                cluster.destroy(context.config.dockerEndDeleteFiles)
            }
        }
    }

    String get_ccr_body(String table, String db = null) {
        if (db == null) {
            db = context.dbName
        }

        Gson gson = new Gson()

        Map<String, String> srcSpec = context.getSrcSpec(db)
        srcSpec.put("table", table)

        Map<String, String> destSpec = context.getDestSpec(db)
        destSpec.put("table", table)

        Map<String, Object> body = Maps.newHashMap()
        body.put("name", context.suiteName + "_" + db + "_" + table)
        body.put("src", srcSpec)
        body.put("dest", destSpec)

        return gson.toJson(body)
    }

    Syncer getSyncer() {
        return context.getSyncer(this)
    }

    List<List<Object>> sql_impl(Connection conn, String sqlStr, boolean isOrder = false) {
        logger.info("Execute ${isOrder ? "order_" : ""}sql: ${sqlStr}".toString())
        def (result, meta) = JdbcUtils.executeToList(conn, sqlStr)
        if (isOrder) {
            result = DataUtils.sortByToString(result)
        }
        return result
    }

    List<List<Object>> jdbc_sql(String sqlStr, boolean isOrder = false) {
        return sql_impl(context.getConnection(), sqlStr, isOrder)
    }

    List<List<Object>> arrow_flight_sql(String sqlStr, boolean isOrder = false) {
        return sql_impl(context.getArrowFlightSqlConnection(), (String) ("USE ${context.dbName};" + sqlStr), isOrder)
    }

    List<List<Object>> sql(String sqlStr, boolean isOrder = false) {
        if (context.useArrowFlightSql()) {
            return arrow_flight_sql(sqlStr, isOrder)
        } else {
            return jdbc_sql(sqlStr, isOrder)
        }
    }

    List<List<Object>> multi_sql(String sqlStr, boolean isOrder = false) {
        String[] sqls = sqlStr.split(";")
        def result = new ArrayList<Object>();
        for (String query : sqls) {
            if (!query.trim().isEmpty()) {
                result.add(sql(query, isOrder));
            }
        }
        return result
    }

    List<List<Object>> arrow_flight_sql_no_prepared (String sqlStr, boolean isOrder = false){
        logger.info("Execute ${isOrder ? "order_" : ""}sql: ${sqlStr}".toString())
        def (result, meta) = JdbcUtils.executeQueryToList(context.getArrowFlightSqlConnection(), (String) ("USE ${context.dbName};" + sqlStr))
        if (isOrder) {
            result = DataUtils.sortByToString(result)
        }
        return result
    }

    List<List<Object>> insert_into_sql_impl(Connection conn, String sqlStr, int num) {
        logger.info("insert into " + num + " records")
        def (result, meta) = JdbcUtils.executeToList(conn, sqlStr)
        return result
    }

    List<List<Object>> jdbc_insert_into_sql(String sqlStr, int num) {
        return insert_into_sql_impl(context.getConnection(), sqlStr, num)
    }

    List<List<Object>> arrow_flight_insert_into_sql(String sqlStr, int num) {
        return insert_into_sql_impl(context.getArrowFlightSqlConnection(), (String) ("USE ${context.dbName};" + sqlStr), num)
    }

    List<List<Object>> insert_into_sql(String sqlStr, int num) {
        if (context.useArrowFlightSql()) {
            return arrow_flight_insert_into_sql(sqlStr, num)
        } else {
            return jdbc_insert_into_sql(sqlStr, num)
        }
    }

    def sql_return_maparray_impl(String sqlStr, Connection conn = null) {        
        logger.info("Execute sql: ${sqlStr}".toString())
        if (conn == null) {
            conn = context.getConnection()
        }
        def (result, meta) = JdbcUtils.executeToList(conn, sqlStr)

        // get all column names as list
        List<String> columnNames = new ArrayList<>()
        for (int i = 0; i < meta.getColumnCount(); i++) {
            columnNames.add(meta.getColumnName(i + 1))
        }

        // add result to res map list, each row is a map with key is column name
        List<Map<String, Object>> res = new ArrayList<>()
        for (int i = 0; i < result.size(); i++) {
            Map<String, Object> row = new HashMap<>()
            for (int j = 0; j < columnNames.size(); j++) {
                row.put(columnNames.get(j), result.get(i).get(j))
            }
            res.add(row)
        }
        return res;
    }

    String getMasterIp(Connection conn) {
        def result = sql_return_maparray_impl("select Host, QueryPort, IsMaster from frontends();", conn)
        logger.info("get master fe: ${result}")

        def masterHost = ""
        for (def row : result) {
            if (row.IsMaster == "true") {
                masterHost = row.Host
                break
            }
        }

        if (masterHost == "") {
            throw new Exception("can not find master fe")
        }
        return masterHost;
    }

    def jdbc_sql_return_maparray(String sqlStr) {
        return sql_return_maparray_impl(sqlStr, context.getConnection())
    }

    def arrow_flight_sql_return_maparray(String sqlStr) {
        return sql_return_maparray_impl((String) ("USE ${context.dbName};" + sqlStr), context.getArrowFlightSqlConnection())
    }

    def sql_return_maparray(String sqlStr) {
        if (context.useArrowFlightSql()) {
            return arrow_flight_sql_return_maparray(sqlStr)
        } else {
            return jdbc_sql_return_maparray(sqlStr)
        }
    }

    List<List<Object>> target_sql(String sqlStr, boolean isOrder = false) {
        logger.info("Execute ${isOrder ? "order_" : ""}target_sql: ${sqlStr}".toString())
        def (result, meta) = JdbcUtils.executeToList(context.getTargetConnection(this), sqlStr)
        if (isOrder) {
            result = DataUtils.sortByToString(result)
        }
        return result
    }

    def target_sql_return_maparray(String sqlStr, boolean isOrder = false) {
        logger.info("Execute ${isOrder ? "order_" : ""}target_sql: ${sqlStr}".toString())
        def (result, meta) = JdbcUtils.executeToList(context.getTargetConnection(this), sqlStr)
        if (isOrder) {
            result = DataUtils.sortByToString(result)
        }

        // get all column names as list
        List<String> columnNames = new ArrayList<>()
        for (int i = 0; i < meta.getColumnCount(); i++) {
            columnNames.add(meta.getColumnName(i + 1))
        }

        // add result to res map list, each row is a map with key is column name
        List<Map<String, Object>> res = new ArrayList<>()
        for (int i = 0; i < result.size(); i++) {
            Map<String, Object> row = new HashMap<>()
            for (int j = 0; j < columnNames.size(); j++) {
                row.put(columnNames.get(j), result.get(i).get(j))
            }
            res.add(row)
        }
        return res
    }

    List<List<String>> sql_meta(String sqlStr, boolean isOrder = false) {
        logger.info("Execute ${isOrder ? "order_" : ""}sql: ${sqlStr}".toString())
        def (tmp, rsmd) = JdbcUtils.executeToList(context.getConnection(), sqlStr)
        int count = rsmd.getColumnCount();
        List<List<String>> result = new ArrayList<>()
        for (int i = 0; i < count; i++) {
            List<String> item = new ArrayList<>()
            String columnName = rsmd.getColumnName(i + 1);
            int columnType = rsmd.getColumnType(i+1);
            String columnTypeName = rsmd.getColumnTypeName(i+1);
            item.add(columnName);
            item.add(columnTypeName);
            result.add(item);
        }
        return result;
    }


    long getTableId(String tableName) {
        def dbInfo = sql "show proc '/dbs'"
        for(List<Object> row : dbInfo) {
            if (row[1].equals(context.dbName)) {
                def tbInfo = sql "show proc '/dbs/${row[0]}' "
                for (List<Object> tb : tbInfo) {
                    if (tb[1].equals(tableName)) {
                        println(tb[0])
                        return tb[0].toLong()
                    }
                }
            }
        }
    }

    long getTableId(String dbName, String tableName) {
        def dbInfo = sql "show proc '/dbs'"
        for(List<Object> row : dbInfo) {
            if (row[1].equals(dbName)) {
                def tbInfo = sql "show proc '/dbs/${row[0]}' "
                for (List<Object> tb : tbInfo) {
                    if (tb[1].equals(tableName)) {
                        return tb[0].toLong()
                    }
                }
            }
        }
    }

    String getCurDbName() {
        return context.dbName
    }

    long getDbId() {
        def dbInfo = sql "show proc '/dbs'"
        for(List<Object> row : dbInfo) {
            if (row[1].equals(context.dbName)) {
                println(row[0])
                return row[0].toLong()
            }
        }
    }

    long getDbId(String dbName) {
        def dbInfo = sql "show proc '/dbs'"
        for (List<Object> row : dbInfo) {
            if (row[1].equals(dbName)) {
                return row[0].toLong()
            }
        }
    }

    long getTableVersion(long dbId, String tableName) {
       def result = sql_return_maparray """show proc '/dbs/${dbId}'"""
        for (def res : result) {
            if(res.TableName.equals(tableName)) {
                log.info(res.toString())
                return res.VisibleVersion.toLong()
            }
        }
    }

    List<List<Object>> order_sql(String sqlStr) {
        return sql(sqlStr,  true)
    }

    List<List<Object>> sortRows(List<List<Object>> result) {
        if (result == null) {
            return null
        }
        return DataUtils.sortByToString(result)
    }

    String selectUnionAll(List list) {
        def toSelectString = { Object value ->
            if (value == null) {
                return "null"
            } else if (value instanceof Number) {
                return value.toString()
            } else {
                return "'${value.toString()}'".toString()
            }
        }
        AtomicBoolean isFirst = new AtomicBoolean(true)
        String sql = list.stream()
            .map({ row ->
                StringBuilder sb = new StringBuilder("SELECT ")
                if (row instanceof List) {
                    if (isFirst.get()) {
                        String columns = row.withIndex().collect({ column, index ->
                            "${toSelectString(column)} AS c${index + 1}"
                        }).join(", ")
                        sb.append(columns)
                        isFirst.set(false)
                    } else {
                        String columns = row.collect({ column ->
                            "${toSelectString(column)}"
                        }).join(", ")
                        sb.append(columns)
                    }
                } else {
                    if (isFirst.get()) {
                        sb.append(toSelectString(row)).append(" AS c1")
                        isFirst.set(false)
                    } else {
                        sb.append(toSelectString(row))
                    }
                }
                return sb.toString()
            }).collect(Collectors.joining("\nUNION ALL\n"))
        return sql
    }

    void explain(Closure actionSupplier) {
        if (context.useArrowFlightSql()) {
            runAction(new ExplainAction(context, "ARROW_FLIGHT_SQL"), actionSupplier)
        } else {
            runAction(new ExplainAction(context), actionSupplier)
        }
    }

    void profile(String tag, Closure<String> actionSupplier) {
        runAction(new ProfileAction(context, tag), actionSupplier)
    }

    void createMV(String sql) {
        (new CreateMVAction(context, sql)).run()
    }

    void createMV(String sql, String expection) {
        (new CreateMVAction(context, sql, expection)).run()
    }

    void test(Closure actionSupplier) {
        runAction(new TestAction(context), actionSupplier)
    }

    void benchmark(Closure actionSupplier) {
        runAction(new BenchmarkAction(context), actionSupplier)
    }

    void waitForSchemaChangeDone(Closure actionSupplier, String insertSql = null, boolean cleanOperator = false,String tbName=null) {
        runAction(new WaitForAction(context), actionSupplier)
        if (ObjectUtils.isNotEmpty(insertSql)){
            sql insertSql
        } else {
            sql "SYNC"
        }
        if (cleanOperator==true){
            if (ObjectUtils.isEmpty(tbName)) throw new RuntimeException("tbName cloud not be null")
            quickTest("", """ SELECT * FROM ${tbName}  """, true)
            sql """ DROP TABLE  ${tbName} """
        }
    }


    void expectException(Closure userFunction, String errorMessage = null) {
        try {
            userFunction()
        } catch (Exception | AssertionError e) {
            if (e.getMessage() != errorMessage) {
                throw e
            }
        }
    }

    void checkTableData(String tbName1 = null, String tbName2 = null, String fieldName = null, String orderByFieldName = null) {
        String orderByName = ""
        if (ObjectUtils.isEmpty(orderByFieldName)){
            orderByName = fieldName;
        }else {
            orderByName = orderByFieldName;
        }
        def tb1Result = sql "select ${fieldName} FROM ${tbName1} order by ${orderByName}"
        def tb2Result = sql "select ${fieldName} FROM ${tbName2} order by ${orderByName}"
        List<Object> tbData1 = new ArrayList<Object>();
        for (List<Object> items:tb1Result){
            tbData1.add(items.get(0))
        }
        List<Object> tbData2 = new ArrayList<Object>();
        for (List<Object> items:tb2Result){
            tbData2.add(items.get(0))
        }
        for (int i =0; i<tbData1.size(); i++) {
            if (ObjectUtils.notEqual(tbData1.get(i),tbData2.get(i)) ){
                throw new RuntimeException("tbData should be same")
            }
        }
    }

    String getRandomBoolean() {
        Random random = new Random()
        boolean randomBoolean = random.nextBoolean()
        return randomBoolean ? "true" : "false"
    }

    void expectExceptionLike(Closure userFunction, String errorMessage = null) {
        try {
            userFunction()
        } catch (Exception e) {
            if (!e.getMessage().contains(errorMessage)) {
                throw e
            }
        }
    }

    String getBrokerName() {
        String brokerName = context.config.otherConfigs.get("brokerName")
        return brokerName
    }

    String getHdfsFs() {
        String hdfsFs = context.config.otherConfigs.get("hdfsFs")
        return hdfsFs
    }

    String getHdfsUser() {
        String hdfsUser = context.config.otherConfigs.get("hdfsUser")
        return hdfsUser
    }

    String getHdfsPasswd() {
        String hdfsPasswd = context.config.otherConfigs.get("hdfsPasswd")
        return hdfsPasswd
    }

    String getHdfsDataDir() {
        String dataDir = context.config.dataPath + "/" + group + "/"
        String hdfsFs = context.config.otherConfigs.get("hdfsFs")
        String hdfsUser = context.config.otherConfigs.get("hdfsUser")
        Hdfs hdfs = new Hdfs(hdfsFs, hdfsUser, dataDir)
        return hdfs.genRemoteDataDir()
    }

    boolean enableHdfs() {
        String enableHdfs = context.config.otherConfigs.get("enableHdfs");
        return enableHdfs.equals("true");
    }

    String uploadToHdfs(String localFile) {
        // as group can be rewrite the origin data file not relate to group
        String dataDir = context.config.dataPath + "/"
        localFile = dataDir + localFile
        String hdfsFs = context.config.otherConfigs.get("hdfsFs")
        String hdfsUser = context.config.otherConfigs.get("hdfsUser")
        Hdfs hdfs = new Hdfs(hdfsFs, hdfsUser, dataDir)
        String remotePath = hdfs.upload(localFile)
        return remotePath;
    }

    String getLoalFilePath(String fileName) {
        if (!new File(fileName).isAbsolute()) {
            fileName = new File(context.dataPath, fileName).getAbsolutePath()
        }
        def file = new File(fileName)
        if (!file.exists()) {
            log.warn("Stream load input file not exists: ${file}".toString())
            throw new IllegalStateException("Stream load input file not exists: ${file}");
        }
        def localFile = file.canonicalPath
        log.info("Set stream load input: ${file.canonicalPath}".toString())
        return localFile;
    }

    boolean enableJdbcTest() {
        String enable = context.config.otherConfigs.get("enableJdbcTest")
        return enable != null && enable.equalsIgnoreCase("true")
    }

    boolean enableBrokerLoad() {
        String enableBrokerLoad = context.config.otherConfigs.get("enableBrokerLoad");
        return (enableBrokerLoad != null && enableBrokerLoad.equals("true"));
    }

    String getS3Provider() {
        String s3Provider = context.config.otherConfigs.get("s3Provider");
        return s3Provider
    }

    String getS3Region() {
        String s3Region = context.config.otherConfigs.get("s3Region");
        return s3Region
    }

    String getS3BucketName() {
        String s3BucketName = context.config.otherConfigs.get("s3BucketName");
        return s3BucketName
    }

    String getS3Endpoint() {
        String s3Endpoint = context.config.otherConfigs.get("s3Endpoint");
        return s3Endpoint
    }

    String getS3AK() {
        String ak = context.config.otherConfigs.get("ak");
        return ak
    }

    String getS3SK() {
        String sk = context.config.otherConfigs.get("sk");
        return sk
    }

    String getS3Url() {
        String s3BucketName = context.config.otherConfigs.get("s3BucketName");
        String s3Endpoint = context.config.otherConfigs.get("s3Endpoint");
        String s3Url = "http://${s3BucketName}.${s3Endpoint}"
        return s3Url
    }

    void scpFiles(String username, String host, String files, String filePath, boolean fromDst=true) {
        String cmd = "scp -r ${username}@${host}:${files} ${filePath}"
        if (!fromDst) {
            cmd = "scp -r ${files} ${username}@${host}:${filePath}"
        }
        logger.info("Execute: ${cmd}".toString())
        Process process = cmd.execute()
        def code = process.waitFor()
        Assert.assertEquals(0, code)
    }

    void mkdirRemote(String username, String host, String path) {
        String cmd = "ssh ${username}@${host} 'mkdir -p ${path}'"
        logger.info("Execute: ${cmd}".toString())
        Process process = cmd.execute()
        def code = process.waitFor()
        Assert.assertEquals(0, code)
    }

    String cmd(String cmd, int timeoutSecond = 0) {
        var processBuilder = new ProcessBuilder()
        processBuilder.command("/bin/bash", "-c", cmd)
        var process = processBuilder.start()
        def outBuf = new StringBuilder()
        def errBuf = new StringBuilder()
        process.consumeProcessOutput(outBuf, errBuf)
        var reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line
        while ((line = reader.readLine()) != null) {
            System.out.println(line)
        }
        // wait until cmd finish
        if (timeoutSecond > 0) {
            process.waitForOrKill(timeoutSecond * 1000)
        } else {
            process.waitFor()
        }
        if (process.exitValue() != 0) {
            println outBuf
            throw new RuntimeException(errBuf.toString())
        }
        return outBuf.toString()
    }

    void sshExec(String username, String host, String cmd, boolean alert=true) {
        String command = "ssh ${username}@${host} '${cmd}'"
        def cmds = ["/bin/bash", "-c", command]
        logger.info("Execute: ${cmds}".toString())
        Process p = cmds.execute()
        def errMsg = new StringBuilder()
        def msg = new StringBuilder()
        p.waitForProcessOutput(msg, errMsg)
        if (alert) {
            assert errMsg.length() == 0: "error occurred!\n" + errMsg
            assert p.exitValue() == 0
        }
    }

    List<String> getFrontendIpHttpPort() {
        return sql_return_maparray("show frontends").collect { it.Host + ":" + it.HttpPort };
    }

    void getBackendIpHttpPort(Map<String, String> backendId_to_backendIP, Map<String, String> backendId_to_backendHttpPort) {
        List<List<Object>> backends = sql("show backends");
        String backend_id;
        for (List<Object> backend : backends) {
            backendId_to_backendIP.put(String.valueOf(backend[0]), String.valueOf(backend[1]));
            backendId_to_backendHttpPort.put(String.valueOf(backend[0]), String.valueOf(backend[4]));
        }
        return;
    }

    void getBackendIpHttpAndBrpcPort(Map<String, String> backendId_to_backendIP,
        Map<String, String> backendId_to_backendHttpPort, Map<String, String> backendId_to_backendBrpcPort) {

        List<List<Object>> backends = sql("show backends");
        for (List<Object> backend : backends) {
            backendId_to_backendIP.put(String.valueOf(backend[0]), String.valueOf(backend[1]));
            backendId_to_backendHttpPort.put(String.valueOf(backend[0]), String.valueOf(backend[4]));
            backendId_to_backendBrpcPort.put(String.valueOf(backend[0]), String.valueOf(backend[5]));
        }
        return;
    }


    int getTotalLine(String filePath) {
        def file = new File(filePath)
        int lines = 0;
        file.eachLine {
            lines++;
        }
        return lines;
    }


    Connection getTargetConnection() {
        return context.getTargetConnection(this)
    }
    
    boolean deleteFile(String filePath) {
        def file = new File(filePath)
        file.delete()
    }

    List<String> downloadExportFromHdfs(String label) {
        String dataDir = context.config.dataPath + "/" + group + "/"
        String hdfsFs = context.config.otherConfigs.get("hdfsFs")
        String hdfsUser = context.config.otherConfigs.get("hdfsUser")
        Hdfs hdfs = new Hdfs(hdfsFs, hdfsUser, dataDir)
        return hdfs.downLoad(label)
    }

    void runStreamLoadExample(String tableName, String coordidateBeHostPort = "") {
        def backends = sql_return_maparray "show backends"
        sql """
                CREATE TABLE IF NOT EXISTS ${tableName} (
                    id int,
                    name varchar(255)
                )
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                  "replication_num" = "${backends.size()}"
                )
            """

        streamLoad {
            table tableName
            set 'column_separator', ','
            file context.config.dataPath + "/demo_p0/streamload_input.csv"
            time 10000
            if (!coordidateBeHostPort.equals("")) {
                def pos = coordidateBeHostPort.indexOf(':')
                def host = coordidateBeHostPort.substring(0, pos)
                def httpPort = coordidateBeHostPort.substring(pos + 1).toInteger()
                directToBe host, httpPort
            }
        }
    }

    void streamLoad(Closure actionSupplier) {
        runAction(new StreamLoadAction(context), actionSupplier)
    }

    void restore(Closure actionSupplier) {
        runAction(new RestoreAction(context), actionSupplier)
    }

    void httpTest(Closure actionSupplier) {
        runAction(new HttpCliAction(context), actionSupplier)
    }

    void runAction(SuiteAction action, Closure actionSupplier) {
        actionSupplier.setDelegate(action)
        actionSupplier.setResolveStrategy(Closure.DELEGATE_FIRST)
        actionSupplier.call(action)
        action.run()
    }

    PreparedStatement prepareStatement(String sql) {
        logger.info("Execute sql: ${sql}".toString())
        return JdbcUtils.prepareStatement(context.getConnection(), sql)
    }

    void setHivePrefix(String hivePrefix) {
        this.hivePrefix = hivePrefix
    }

    List<List<Object>> hive_docker(String sqlStr, boolean isOrder = false) {
        logger.info("Execute hive ql: ${sqlStr}".toString())
        String cleanedSqlStr = sqlStr.replaceAll("\\s*;\\s*\$", "")
        def (result, meta) = JdbcUtils.executeToList(context.getHiveDockerConnection(hivePrefix), cleanedSqlStr)
        if (isOrder) {
            result = DataUtils.sortByToString(result)
        }
        return result
    }

    List<List<Object>> hive_remote(String sqlStr, boolean isOrder = false) {
        String cleanedSqlStr = sqlStr.replaceAll("\\s*;\\s*\$", "")
        def (result, meta) = JdbcUtils.executeToList(context.getHiveRemoteConnection(), cleanedSqlStr)
        if (isOrder) {
            result = DataUtils.sortByToString(result)
        }
        return result
    }

    List<List<Object>> db2_docker(String sqlStr, boolean isOrder = false) {
        String cleanedSqlStr = sqlStr.replaceAll("\\s*;\\s*\$", "")
        def (result, meta) = JdbcUtils.executeToList(context.getDB2DockerConnection(), cleanedSqlStr)
        if (isOrder) {
            result = DataUtils.sortByToString(result)
        }
        return result
    }
    List<List<Object>> exec(Object stmt) {
        logger.info("Execute sql: ${stmt}".toString())
        def (result, meta )= JdbcUtils.executeToList(context.getConnection(),  (PreparedStatement) stmt)
        return result
    }

    void quickRunTest(String tag, Object arg, boolean isOrder = false) {
        if (context.config.generateOutputFile || context.config.forceGenerateOutputFile) {
            Tuple2<List<List<Object>>, ResultSetMetaData> tupleResult = null
            if (arg instanceof PreparedStatement) {
                if (tag.contains("hive_docker")) {
                    tupleResult = JdbcUtils.executeToStringList(context.getHiveDockerConnection(hivePrefix),  (PreparedStatement) arg)
                }else if (tag.contains("hive_remote")) {
                    tupleResult = JdbcUtils.executeToStringList(context.getHiveRemoteConnection(),  (PreparedStatement) arg)
                } else if (tag.contains("arrow_flight_sql") || context.useArrowFlightSql()) {
                    tupleResult = JdbcUtils.executeToStringList(context.getArrowFlightSqlConnection(), (PreparedStatement) arg)
                } else if (tag.contains("target_sql")) {
                    tupleResult = JdbcUtils.executeToStringList(context.getTargetConnection(this), (PreparedStatement) arg)
                } else {
                    tupleResult = JdbcUtils.executeToStringList(context.getConnection(),  (PreparedStatement) arg)
                }
            } else {
                if (tag.contains("hive_docker")) {
                    tupleResult = JdbcUtils.executeToStringList(context.getHiveDockerConnection(hivePrefix), (String) arg)
                }else if (tag.contains("hive_remote")) {
                    tupleResult = JdbcUtils.executeToStringList(context.getHiveRemoteConnection(), (String) arg)
                } else if (tag.contains("arrow_flight_sql") || context.useArrowFlightSql()) {
                    tupleResult = JdbcUtils.executeToStringList(context.getArrowFlightSqlConnection(),
                            (String) ("USE ${context.dbName};" + (String) arg))
                } else if (tag.contains("target_sql")) {
                    tupleResult = JdbcUtils.executeToStringList(context.getTargetConnection(this), (String) arg)
                } else {
                    tupleResult = JdbcUtils.executeToStringList(context.getConnection(),  (String) arg)
                }
            }
            def (result, meta) = tupleResult
            if (isOrder) {
                result = sortByToString(result)
            }
            Iterator<List<Object>> realResults = result.iterator()
            // generate and save to .out file
            def writer = context.getOutputWriter(context.config.forceGenerateOutputFile)
            writer.write(realResults, tag)
        } else {
            if (!context.outputFile.exists()) {
                throw new IllegalStateException("Missing outputFile: ${context.outputFile.getAbsolutePath()}")
            }

            if (!context.getOutputIterator().hasNextTagBlock(tag)) {
                throw new IllegalStateException("Missing output block for tag '${tag}': ${context.outputFile.getAbsolutePath()}")
            }

            OutputUtils.TagBlockIterator expectCsvResults = context.getOutputIterator().next()
            Tuple2<List<List<Object>>, ResultSetMetaData> tupleResult = null
            if (arg instanceof PreparedStatement) {
                if (tag.contains("hive_docker")) {
                    tupleResult = JdbcUtils.executeToStringList(context.getHiveDockerConnection(hivePrefix),  (PreparedStatement) arg)
                }else if (tag.contains("hive_remote")) {
                    tupleResult = JdbcUtils.executeToStringList(context.getHiveRemoteConnection(),  (PreparedStatement) arg)
                } else if (tag.contains("arrow_flight_sql") || context.useArrowFlightSql()) {
                    tupleResult = JdbcUtils.executeToStringList(context.getArrowFlightSqlConnection(), (PreparedStatement) arg)
                } else if (tag.contains("target_sql")) {
                    tupleResult = JdbcUtils.executeToStringList(context.getTargetConnection(this), (PreparedStatement) arg)
                } else {
                    tupleResult = JdbcUtils.executeToStringList(context.getConnection(),  (PreparedStatement) arg)
                }
            } else {
                if (tag.contains("hive_docker")) {
                    tupleResult = JdbcUtils.executeToStringList(context.getHiveDockerConnection(hivePrefix), (String) arg)
                }else if (tag.contains("hive_remote")) {
                    tupleResult = JdbcUtils.executeToStringList(context.getHiveRemoteConnection(), (String) arg)
                } else if (tag.contains("arrow_flight_sql") || context.useArrowFlightSql()) {
                    tupleResult = JdbcUtils.executeToStringList(context.getArrowFlightSqlConnection(),
                            (String) ("USE ${context.dbName};" + (String) arg))
                } else if (tag.contains("target_sql")) {
                    tupleResult = JdbcUtils.executeToStringList(context.getTargetConnection(this), (String) arg)
                } else {
                    tupleResult = JdbcUtils.executeToStringList(context.getConnection(),  (String) arg)
                }
            }
            def (realResults, meta) = tupleResult
            if (isOrder) {
                realResults = sortByToString(realResults)
            }

            Iterator<List<Object>> realResultsIter = realResults.iterator()
            def realWriter = context.getRealOutputWriter(true)
            realWriter.write(realResultsIter, tag)

            String errorMsg = null
            try {
                errorMsg = OutputUtils.checkOutput(expectCsvResults, realResults.iterator(),
                    { row -> OutputUtils.toCsvString(row as List<Object>) },
                    { row ->  OutputUtils.toCsvString(row) },
                    "Check tag '${tag}' failed", meta)
            } catch (Throwable t) {
                throw new IllegalStateException("Check tag '${tag}' failed, sql:\n${arg}", t)
            }
            if (errorMsg != null) {
                String csvRealResult = realResults.stream()
                    .map {row -> OutputUtils.toCsvString(row)}
                    .collect(Collectors.joining("\n"))
                def outputFilePath = context.outputFile.getCanonicalPath().substring(context.config.dataPath.length() + 1)
                def line = expectCsvResults.currentLine()
                logger.warn("expect results in file: ${outputFilePath}, line: ${line}\nrealResults:\n" + csvRealResult)
                throw new IllegalStateException("Check tag '${tag}' failed:\n${errorMsg}\n\nsql:\n${arg}")
            }
        }
    }

    void quickTest(String tag, String sql, boolean isOrder = false) {
        logger.info("Execute tag: ${tag}, ${isOrder ? "order_" : ""}sql: ${sql}".toString())
        if (tag.contains("hive_docker")) {
            String cleanedSqlStr = sql.replaceAll("\\s*;\\s*\$", "")
            sql = cleanedSqlStr
        }
        if (tag.contains("hive_remote")) {
            String cleanedSqlStr = sql.replaceAll("\\s*;\\s*\$", "")
            sql = cleanedSqlStr
        }
        quickRunTest(tag, sql, isOrder)
    }

    void quickExecute(String tag, PreparedStatement stmt) {
        logger.info("Execute tag: ${tag}, sql: ${stmt}".toString())
        quickRunTest(tag, stmt)
    }

    @Override
    Object invokeMethod(String name, Object args) {
        // qt: quick test
        if (name.startsWith("qt_")) {
            return quickTest(name.substring("qt_".length()), (args as Object[])[0] as String)
        } else if (name.startsWith("order_qt_")) {
            return quickTest(name.substring("order_qt_".length()), (args as Object[])[0] as String, true)
        } else if (name.startsWith("qe_")) {
            return quickExecute(name.substring("qe_".length()), (args as Object[])[0] as PreparedStatement)
        } else if (name.startsWith("assert") && name.length() > "assert".length()) {
            // delegate to junit Assertions dynamically
            return Assertions."$name"(*args) // *args: spread-dot
        } else if (name.startsWith("try_")) {
            String realMethod = name.substring("try_".length())
            try {
                return this."$realMethod"(*args)
            } catch (Throwable t) {
                // do nothing
                return null
            }
        } else {
            // invoke origin method
            return metaClass.invokeMethod(this, name, args)
        }
    }

    Boolean checkSnapshotFinish() {
        String checkSQL = "SHOW BACKUP FROM " + context.dbName
        int size = sql(checkSQL).size()
        logger.info("Now size is ${size}")
        List<Object> row = sql(checkSQL)[size-1]
        logger.info("Now row is ${row}")

        return (row[3] as String) == "FINISHED"
    }

    Boolean checkRestoreFinish() {
        String checkSQL = "SHOW RESTORE FROM " + context.dbName
        int size = sql(checkSQL).size()
        logger.info("Now size is ${size}")
        List<Object> row = sql(checkSQL)[size-1]
        logger.info("Now row is ${row}")

        return (row[4] as String) == "FINISHED"
    }

    String getServerPrepareJdbcUrl(String jdbcUrl, String database) {
        String urlWithoutSchema = jdbcUrl.substring(jdbcUrl.indexOf("://") + 3)
        def sql_ip = getMasterIp()
        def sql_port
        if (urlWithoutSchema.indexOf("/") >= 0) {
            // e.g: jdbc:mysql://locahost:8080/?a=b
            sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1, urlWithoutSchema.indexOf("/"))
        } else {
            // e.g: jdbc:mysql://locahost:8080
            sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1)
        }
        // set server side prepared statement url
        return "jdbc:mysql://" + sql_ip + ":" + sql_port + "/" + database + "?&useServerPrepStmts=true"
    }

    DebugPoint GetDebugPoint() {
        return debugPoint
    }

    void waitingMTMVTaskFinishedByMvName(String mvName) {
        Thread.sleep(2000);
        String showTasks = "select TaskId,JobId,JobName,MvId,Status,MvName,MvDatabaseName,ErrorMsg from tasks('type'='mv') where MvName = '${mvName}' order by CreateTime ASC"
        String status = "NULL"
        List<List<Object>> result
        long startTime = System.currentTimeMillis()
        long timeoutTimestamp = startTime + 5 * 60 * 1000 // 5 min
        do {
            result = sql(showTasks)
            logger.info("result: " + result.toString())
            if (!result.isEmpty()) {
                status = result.last().get(4)
            }
            logger.info("The state of ${showTasks} is ${status}")
            Thread.sleep(1000);
        } while (timeoutTimestamp > System.currentTimeMillis() && (status == 'PENDING' || status == 'RUNNING'  || status == 'NULL'))
        if (status != "SUCCESS") {
            logger.info("status is not success")
        }
        Assert.assertEquals("SUCCESS", status)
        logger.info("waitingMTMVTaskFinished analyze mv name is " + result.last().get(5))
        sql "analyze table ${result.last().get(6)}.${mvName} with sync;"
    }

    void waitingMTMVTaskFinishedByMvNameAllowCancel(String mvName) {
        Thread.sleep(2000);
        String showTasks = "select TaskId,JobId,JobName,MvId,Status,MvName,MvDatabaseName,ErrorMsg from tasks('type'='mv') where MvName = '${mvName}' order by CreateTime ASC"
        String status = "NULL"
        List<List<Object>> result
        long startTime = System.currentTimeMillis()
        long timeoutTimestamp = startTime + 5 * 60 * 1000 // 5 min
        do {
            result = sql(showTasks)
            logger.info("result: " + result.toString())
            if (!result.isEmpty()) {
                status = result.last().get(4)
            }
            logger.info("The state of ${showTasks} is ${status}")
            Thread.sleep(1000);
        } while (timeoutTimestamp > System.currentTimeMillis() && (status == 'PENDING' || status == 'RUNNING'  || status == 'NULL' || status == 'CANCELED'))
        if (status != "SUCCESS") {
            logger.info("status is not success")
            assertTrue(result.toString().contains("same table"))
        }
        // Need to analyze materialized view for cbo to choose the materialized view accurately
        logger.info("waitingMTMVTaskFinished analyze mv name is " + result.last().get(5))
        sql "analyze table ${result.last().get(6)}.${mvName} with sync;"
    }

    void waitingPartitionIsExpected(String tableName, String partitionName, boolean expectedStatus) {
        Thread.sleep(2000);
        String showPartitions = "show partitions from ${tableName}"
        Boolean status = null;
        List<List<Object>> result
        long startTime = System.currentTimeMillis()
        long timeoutTimestamp = startTime + 1 * 60 * 1000 // 1 min
        do {
            result = sql(showPartitions)
            if (!result.isEmpty()) {
                for (List<Object> row : result) {
                    def existPartitionName = row.get(1).toString()
                    if (Objects.equals(existPartitionName, partitionName)) {
                        def statusStr = row.get(row.size() - 2).toString()
                        status = Boolean.valueOf(statusStr)
                    }
                }
            }
            Thread.sleep(500);
        } while (timeoutTimestamp > System.currentTimeMillis() && !Objects.equals(status, expectedStatus))
        if (!Objects.equals(status, expectedStatus)) {
            logger.info("partition status is not expected")
        }
        Assert.assertEquals(expectedStatus, status)
    }

    void waitingMTMVTaskFinished(String jobName) {
        Thread.sleep(2000);
        String showTasks = "select TaskId,JobId,JobName,MvId,Status,MvName,MvDatabaseName,ErrorMsg from tasks('type'='mv') where JobName = '${jobName}' order by CreateTime ASC"
        String status = "NULL"
        List<List<Object>> result
        long startTime = System.currentTimeMillis()
        long timeoutTimestamp = startTime + 5 * 60 * 1000 // 5 min
        do {
            result = sql(showTasks)
            logger.info("result: " + result.toString())
            if (!result.isEmpty()) {
                status = result.last().get(4)
            }
            logger.info("The state of ${showTasks} is ${status}")
            Thread.sleep(1000);
        } while (timeoutTimestamp > System.currentTimeMillis() && (status == 'PENDING' || status == 'RUNNING' || status == 'NULL'))
        if (status != "SUCCESS") {
            logger.info("status is not success")
        }
        Assert.assertEquals("SUCCESS", status)
        logger.info("waitingMTMVTaskFinished analyze mv name is " + result.last().get(5))
        sql "analyze table ${result.last().get(6)}.${result.last().get(5)} with sync;"
    }

    void waitingMTMVTaskFinishedNotNeedSuccess(String jobName) {
        Thread.sleep(2000);
        String showTasks = "select TaskId,JobId,JobName,MvId,Status,MvName,MvDatabaseName,ErrorMsg from tasks('type'='mv') where JobName = '${jobName}' order by CreateTime ASC"
        String status = "NULL"
        List<List<Object>> result
        long startTime = System.currentTimeMillis()
        long timeoutTimestamp = startTime + 5 * 60 * 1000 // 5 min
        do {
            result = sql(showTasks)
            logger.info("result: " + result.toString())
            if (!result.isEmpty()) {
                status = result.last().get(4)
            }
            logger.info("The state of ${showTasks} is ${status}")
            Thread.sleep(1000);
        } while (timeoutTimestamp > System.currentTimeMillis() && (status == 'PENDING' || status == 'RUNNING' || status == 'NULL'))
        if (status != "SUCCESS") {
            logger.info("status is not success")
        }
    }

    def getMVJobState = { tableName, rollUpName  ->
        def jobStateResult = sql """ SHOW ALTER TABLE ROLLUP WHERE TableName='${tableName}' and IndexName = '${rollUpName}' ORDER BY CreateTime DESC limit 1"""
        if (jobStateResult == null || jobStateResult.isEmpty()) {
            logger.info("show alter table roll is empty" + jobStateResult)
            return "NOT_READY"
        }
        logger.info("getMVJobState jobStateResult is " + jobStateResult.toString())
        if (!jobStateResult[0][8].equals("FINISHED")) {
            return "NOT_READY"
        }
        return "FINISHED";
    }
    def waitForRollUpJob =  (tbName, rollUpName, timeoutMillisecond) -> {

        long startTime = System.currentTimeMillis()
        long timeoutTimestamp = startTime + timeoutMillisecond

        String result
        while (timeoutTimestamp > System.currentTimeMillis()){
            result = getMVJobState(tbName, rollUpName)
            if (result == "FINISHED") {
                sleep(200)
                return
            } else {
                sleep(200)
            }
        }
        Assert.assertEquals("FINISHED", result)
    }

    void testFoldConst(String foldSql) {
        String openFoldConstant = "set debug_skip_fold_constant=false";
        sql(openFoldConstant)
        logger.info(foldSql)
        List<List<Object>> resultByFoldConstant = sql(foldSql)
        logger.info("result by fold constant: " + resultByFoldConstant.toString())
        String closeFoldConstant = "set debug_skip_fold_constant=true";
        sql(closeFoldConstant)
        logger.info(foldSql)
        List<List<Object>> resultExpected = sql(foldSql)
        logger.info("result expected: " + resultExpected.toString())
        Assert.assertEquals(resultExpected, resultByFoldConstant)
    }

    String getJobName(String dbName, String mtmvName) {
        String showMTMV = "select JobName from mv_infos('database'='${dbName}') where Name = '${mtmvName}'";
	    logger.info(showMTMV)
        List<List<Object>> result = sql(showMTMV)
        logger.info("result: " + result.toString())
        if (result.isEmpty()) {
            Assert.fail();
        }
        return result.last().get(0);
    }

    boolean isCloudMode() {
        // APPR: in 2.1 branch not have cloud, so just return false
        return false
    }

    String getFeConfig(String key) {
        return sql_return_maparray("SHOW FRONTEND CONFIG LIKE '${key}'")[0].Value
    }

    void setFeConfig(String key, Object value) {
        sql "ADMIN SET FRONTEND CONFIG ('${key}' = '${value}')"
    }

    void setFeConfigTemporary(Map<String, Object> tempConfig, Closure actionSupplier) {
        def oldConfig = tempConfig.keySet().collectEntries { [it, getFeConfig(it)] }

        def updateConfig = { conf ->
            conf.each { key, value -> setFeConfig(key, value) }
        }

        try {
            updateConfig tempConfig
            actionSupplier()
        } finally {
            updateConfig oldConfig
        }
    }

    void waiteCreateTableFinished(String tableName) {
        Thread.sleep(2000);
        String showCreateTable = "SHOW CREATE TABLE ${tableName}"
        String createdTableName = "";
        List<List<Object>> result
        long startTime = System.currentTimeMillis()
        long timeoutTimestamp = startTime + 1 * 60 * 1000 // 1 min
        do {
            result = sql(showCreateTable)
            if (!result.isEmpty()) {
                createdTableName = result.last().get(0)
            }
            logger.info("create table result of ${showCreateTable} is ${createdTableName}")
            Thread.sleep(500);
        } while (timeoutTimestamp > System.currentTimeMillis() && createdTableName.isEmpty())
        if (createdTableName.isEmpty()) {
            logger.info("create table is not success")
        }
        Assert.assertEquals(true, !createdTableName.isEmpty())
    }

    String[][] deduplicate_tablets(String[][] tablets) {
        def result = [:]

        tablets.each { row ->
            def tablet_id = row[0]
            if (!result.containsKey(tablet_id)) {
                result[tablet_id] = row
            }
        }

        return result.values().toList()
    }

    ArrayList deduplicate_tablets(ArrayList tablets) {
        def result = [:]

        tablets.each { row ->
            def tablet_id
            if (row.containsKey("TabletId")) {
                tablet_id = row.TabletId
            } else {
                tablet_id = row[0]
            }
            if (!result.containsKey(tablet_id)) {
                result[tablet_id] = row
            }
        }

        return result.values().toList()
    }

    // enable_sync_mv_cost_based_rewrite is true or not
    boolean enable_sync_mv_cost_based_rewrite () {
        def showVariable = "show variables like 'enable_sync_mv_cost_based_rewrite';"
        List<List<Object>> result = sql(showVariable)
        logger.info("enable_sync_mv_cost_based_rewrite = " + result)
        if (result.isEmpty()) {
            return false;
        }
        return Boolean.parseBoolean(result.get(0).get(1));
    }

    // Given tables to decide whether the table partition row count statistic is ready or not
    boolean is_partition_statistics_ready(db, tables)  {
        boolean isReady = true;
        for (String table : tables) {
            if (!isReady) {
                logger.info("is_partition_statistics_ready " + db + " " + table + " " + isReady)
                return false;
            }
            String showPartitions = "show partitions from ${db}.${table}"
            String showData = "show data from ${db}.${table}"

            List<List<Object>> partitions = sql(showPartitions)
            logger.info("is_partition_statistics_ready partitions = " + partitions)
            if (partitions.size() == 1) {
                // If only one partition, the table row count is same with partition row count, so is ready
                continue
            }

            List<List<Object>> data = sql(showData)
            logger.info("is_partition_statistics_ready data = " + data)
            for (List<Object> row : data) {
                if (row.size() >= 2 && Objects.equals(row.get(1), table)) {
                    isReady = isReady && Double.parseDouble(row.get(4)) > 0
                    break
                }
            }
        }
        logger.info("is_partition_statistics_ready " + db + " " + tables + " " + isReady)
        return isReady
    }

    def create_async_mv = { db, mv_name, mv_sql ->

        sql """DROP MATERIALIZED VIEW IF EXISTS ${db}.${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${db}.${mv_name} 
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS ${mv_sql}
        """
        def job_name = getJobName(db, mv_name);
        waitingMTMVTaskFinished(job_name)
        sql "analyze table ${db}.${mv_name} with sync;"
    }

    def create_async_partition_mv = { db, mv_name, mv_sql, partition_col ->

        sql """DROP MATERIALIZED VIEW IF EXISTS ${db}.${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${db}.${mv_name} 
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL 
        PARTITION BY ${partition_col} 
        DISTRIBUTED BY RANDOM BUCKETS 2 
        PROPERTIES ('replication_num' = '1')  
        AS ${mv_sql}
        """
        def job_name = getJobName(db, mv_name);
        waitingMTMVTaskFinished(job_name)
        sql "analyze table ${db}.${mv_name} with sync;"
    }

    // mv not part in rewrite process
    def mv_not_part_in = { query_sql, mv_name ->
        logger.info("query_sql = " + query_sql + ", mv_names = " + mv_name)
        explain {
            sql(" memo plan ${query_sql}")
            check { result ->
                boolean success = !result.contains("${mv_name} chose") && !result.contains("${mv_name} not chose")
                && !result.contains("${mv_name} fail")
                Assert.assertEquals(true, success)
            }
        }
    }

    // multi mv all not part in rewrite process
    def mv_all_not_part_in = { query_sql, mv_names ->
        logger.info("query_sql = " + query_sql + ", mv_names = " + mv_names)
        explain {
            sql(" memo plan ${query_sql}")
            check { result ->
                boolean success = true;
                for (String mv_name : mv_names) {
                    success = !result.contains("${mv_name} chose") && !result.contains("${mv_name} not chose")
                            && !result.contains("${mv_name} fail")
                }
                Assert.assertEquals(true, success)
            }
        }
    }

    // mv part in rewrite process, rewrte success and chosen by cbo
    // sync_cbo_rewrite is the bool value which control sync mv is use cbo based mv rewrite
    // is_partition_statistics_ready is the bool value which identifying if partition row count is valid or not
    // if true, check if chosen by cbo or doesn't check
    void mv_rewrite_success(query_sql, mv_name, sync_cbo_rewrite = enable_sync_mv_cost_based_rewrite(),
                               is_partition_statistics_ready = true) {
        logger.info("query_sql = " + query_sql + ", mv_name = " + mv_name + ", sync_cbo_rewrite = " +sync_cbo_rewrite
                + ", is_partition_statistics_ready = " + is_partition_statistics_ready)
        if (!is_partition_statistics_ready) {
            // If partition statistics is no ready, degrade to without check cbo chosen
            mv_rewrite_success_without_check_chosen(query_sql, mv_name, sync_cbo_rewrite)
            return
        }
        if (!sync_cbo_rewrite) {
            explain {
                sql("${query_sql}")
                contains("(${mv_name})")
            }
            return
        }
        explain {
            sql(" memo plan ${query_sql}")
            contains("${mv_name} chose")
        }
    }

    // multi mv part in rewrite process, all rewrte success and chosen by cbo
    // sync_cbo_rewrite is the bool value which control sync mv is use cbo based mv rewrite
    // is_partition_statistics_ready is the bool value which identifying if partition row count is valid or not
    // if true, check if chosen by cbo or doesn't check
    void mv_rewrite_all_success( query_sql, mv_names, sync_cbo_rewrite = enable_sync_mv_cost_based_rewrite(),
                                   is_partition_statistics_ready = true) {
        logger.info("query_sql = " + query_sql + ", mv_names = " + mv_names + ", sync_cbo_rewrite = " +sync_cbo_rewrite
                + ", is_partition_statistics_ready = " + is_partition_statistics_ready)
        if (!is_partition_statistics_ready) {
            // If partition statistics is no ready, degrade to without check cbo chosen
            mv_rewrite_all_success_without_check_chosen(query_sql, mv_names, sync_cbo_rewrite)
            return
        }
        if (!sync_cbo_rewrite) {
            explain {
                sql("${query_sql}")
                check { result ->
                    boolean success = true;
                    for (String mv_name : mv_names) {
                        success = success && result.contains("(${mv_name})")
                    }
                    if (!success) {
                        logger.info("mv_rewrite_all_success fail =" + result)
                    }
                    Assert.assertEquals(true, success)
                }
            }
            return
        }
        explain {
            sql(" memo plan ${query_sql}")
            check { result ->
                boolean success = true;
                for (String mv_name : mv_names) {
                    def contains = result.contains("${mv_name} chose")
                    if (!contains) {
                        logger.info("mv_rewrite_all_success fail =" + result)
                    }
                    Assert.assertEquals(true, contains)
                }
            }
        }
    }

    // multi mv part in rewrite process, any of them rewrte success and chosen by cbo
    // sync_cbo_rewrite is the bool value which control sync mv is use cbo based mv rewrite
    // is_partition_statistics_ready is the bool value which identifying if partition row count is valid or not
    // if true, check if chosen by cbo or doesn't check
    void mv_rewrite_any_success(query_sql, mv_names, sync_cbo_rewrite = enable_sync_mv_cost_based_rewrite(),
                                   is_partition_statistics_ready = true) {
        logger.info("query_sql = " + query_sql + ", mv_names = " + mv_names + ", sync_cbo_rewrite = " +sync_cbo_rewrite
                + ", is_partition_statistics_ready = " + is_partition_statistics_ready)
        if (!is_partition_statistics_ready) {
            // If partition statistics is no ready, degrade to without check cbo chosen
            mv_rewrite_any_success_without_check_chosen(query_sql, mv_names, sync_cbo_rewrite)
            return
        }
        if (!sync_cbo_rewrite) {
            explain {
                sql("${query_sql}")
                check { result ->
                    boolean success = false;
                    for (String mv_name : mv_names) {
                        success = success || result.contains("(${mv_name})")
                    }
                    if (!success) {
                        logger.info("mv_rewrite_any_success fail =" + result)
                    }
                    Assert.assertEquals(true, success)
                }
            }
            return
        }
        explain {
            sql(" memo plan ${query_sql}")
            check { result ->
                boolean success = false;
                for (String mv_name : mv_names) {
                    success = success || result.contains("${mv_name} chose")
                }
                if (!success) {
                    logger.info("mv_rewrite_any_success fail =" + result)
                }
                Assert.assertEquals(true, success)
            }
        }
    }

    // multi mv part in rewrite process, all rewrte success without check if chosen by cbo
    // sync_cbo_rewrite is the bool value which control sync mv is use cbo based mv rewrite
    void mv_rewrite_all_success_without_check_chosen(query_sql, mv_names,
                                                        sync_cbo_rewrite = enable_sync_mv_cost_based_rewrite()){
        logger.info("query_sql = " + query_sql + ", mv_names = " + mv_names)
        if (!sync_cbo_rewrite) {
            explain {
                sql("${query_sql}")
                check { result ->
                    boolean success = true;
                    for (String mv_name : mv_names) {
                        def splitResult = result.split("MaterializedViewRewriteFail")
                        def each_result =  splitResult.length == 2 ? splitResult[0].contains(mv_name) : false
                        success = success && (result.contains("(${mv_name})") || each_result)
                    }
                    if (!success) {
                        logger.info("mv_rewrite_all_success_without_check_chosen fail =" + result)
                    }
                    Assert.assertEquals(true, success)
                }
            }
            return
        }
        explain {
            sql(" memo plan ${query_sql}")
            check {result ->
                boolean success = true
                for (String mv_name : mv_names) {
                    boolean stepSuccess = result.contains("${mv_name} chose") || result.contains("${mv_name} not chose")
                    success = success && stepSuccess
                }
                if (!success) {
                    logger.info("mv_rewrite_all_success_without_check_chosen fail =" + result)
                }
                Assert.assertEquals(true, success)
            }
        }
    }

    // multi mv part in rewrite process, any of them rewrte success without check if chosen by cbo or not
    // sync_cbo_rewrite is the bool value which control sync mv is use cbo based mv rewrite
    void mv_rewrite_any_success_without_check_chosen(query_sql, mv_names,
                                                     sync_cbo_rewrite = enable_sync_mv_cost_based_rewrite()) {
        logger.info("query_sql = " + query_sql + ", mv_names = " + mv_names)
        if (!sync_cbo_rewrite) {
            explain {
                sql("${query_sql}")
                check { result ->
                    boolean success = false;
                    for (String mv_name : mv_names) {
                        def splitResult = result.split("MaterializedViewRewriteFail")
                        def each_result =  splitResult.length == 2 ? splitResult[0].contains(mv_name) : false
                        success = success || (result.contains("(${mv_name})") || each_result)
                    }
                    if (!success) {
                        logger.info("mv_rewrite_any_success_without_check_chosen fail =" + result)
                    }
                    Assert.assertEquals(true, success)
                }
            }
            return
        }
        explain {
            sql(" memo plan ${query_sql}")
            check { result ->
                boolean success = false
                for (String mv_name : mv_names) {
                    success = success || result.contains("${mv_name} chose") || result.contains("${mv_name} not chose")
                }
                if (!success) {
                    logger.info("mv_rewrite_any_success_without_check_chosen fail =" + result)
                }
                Assert.assertEquals(true, success)
            }
        }
    }

    // multi mv part in rewrite process, rewrte success without check if chosen by cbo or not
    // sync_cbo_rewrite is the bool value which control sync mv is use cbo based mv rewrite
    void mv_rewrite_success_without_check_chosen(query_sql, mv_name,
                                                 sync_cbo_rewrite = enable_sync_mv_cost_based_rewrite()) {
        logger.info("query_sql = " + query_sql + ", mv_name = " + mv_name)
        if (!sync_cbo_rewrite) {
            explain {
                sql("${query_sql}")
                check { result ->
                    def splitResult = result.split("MaterializedViewRewriteFail")
                    result.contains("(${mv_name})") || (splitResult.length == 2 ? splitResult[0].contains(mv_name) : false)
                }
            }
            return
        }
        explain {
            sql(" memo plan ${query_sql}")
            check { result ->
                result.contains("${mv_name} chose") || result.contains("${mv_name} not chose")
            }
        }
    }

    // single mv part in rewrite process, rewrte fail
    // sync_cbo_rewrite is the bool value which control sync mv is use cbo based mv rewrite
    void mv_rewrite_fail(query_sql, mv_name, sync_cbo_rewrite = enable_sync_mv_cost_based_rewrite()) {
        logger.info("query_sql = " + query_sql + ", mv_name = " + mv_name)
        if (!sync_cbo_rewrite) {
            explain {
                sql("${query_sql}")
                notContains("(${mv_name})")
            }
            return
        }
        explain {
            sql(" memo plan ${query_sql}")
            contains("${mv_name} fail")
        }
    }

    // multi mv part in rewrite process, all rewrte fail
    // sync_cbo_rewrite is the bool value which control sync mv is use cbo based mv rewrite
    void mv_rewrite_all_fail(query_sql, mv_names, sync_cbo_rewrite = enable_sync_mv_cost_based_rewrite()) {
        logger.info("query_sql = " + query_sql + ", mv_names = " + mv_names)
        if (!sync_cbo_rewrite) {
            explain {
                sql("${query_sql}")
                check { result ->
                    boolean fail = true
                    for (String mv_name : mv_names) {
                        boolean stepFail = !result.contains("(${mv_name})")
                        fail = fail && stepFail
                    }
                    if (!fail) {
                        logger.info("mv_rewrite_all_fail =" + result)
                    }
                    Assert.assertEquals(true, fail)
                }
            }
            return
        }
        explain {
            sql(" memo plan ${query_sql}")
            check {result ->
                boolean fail = true
                for (String mv_name : mv_names) {
                    boolean stepFail = result.contains("${mv_name} fail")
                    fail = fail && stepFail
                }
                if (!fail) {
                    logger.info("mv_rewrite_all_fail =" + result)
                }
                Assert.assertEquals(true, fail)
            }
        }
    }

    // multi mv part in rewrite process, any rewrte fail
    // sync_cbo_rewrite is the bool value which control sync mv is use cbo based mv rewrite
    void mv_rewrite_any_fail (query_sql, mv_names, sync_cbo_rewrite = enable_sync_mv_cost_based_rewrite()) {
        logger.info("query_sql = " + query_sql + ", mv_names = " + mv_names)
        if (!sync_cbo_rewrite) {
            explain {
                sql("${query_sql}")
                check { result ->
                    boolean fail = false
                    for (String mv_name : mv_names) {
                        fail = fail || !result.contains("(${mv_name})")
                    }
                    if (!fail) {
                        logger.info("mv_rewrite_any_fail =" + result)
                    }
                    Assert.assertEquals(true, fail)
                }
            }
            return
        }
        explain {
            sql(" memo plan ${query_sql}")
            check { result ->
                boolean fail = false
                for (String mv_name : mv_names) {
                    fail = fail || result.contains("${mv_name} fail")
                }
                if (!fail) {
                    logger.info("mv_rewrite_any_fail =" + result)
                }
                Assert.assertEquals(true, fail)
            }
        }
    }

    def async_mv_rewrite_success = { db, mv_sql, query_sql, mv_name ->

        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS ${mv_sql}
        """
        def job_name = getJobName(db, mv_name);
        waitingMTMVTaskFinished(job_name)
        mv_rewrite_success(query_sql, mv_name, true)
    }

    def async_mv_rewrite_success_without_check_chosen = { db, mv_sql, query_sql, mv_name ->

        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS ${mv_sql}
        """

        def job_name = getJobName(db, mv_name);
        waitingMTMVTaskFinished(job_name)
        mv_rewrite_success_without_check_chosen(query_sql, mv_name, true)
    }


    def async_mv_rewrite_fail = { db, mv_sql, query_sql, mv_name ->

        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS ${mv_sql}
        """

        def job_name = getJobName(db, mv_name);
        waitingMTMVTaskFinished(job_name)
        mv_rewrite_fail(query_sql, mv_name, true)
    }

    def scp_udf_file_to_all_be = { udf_file_path ->
        if (!new File(udf_file_path).isAbsolute()) {
            udf_file_path = new File(udf_file_path).getAbsolutePath()
        }
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)
        if(backendId_to_backendIP.size() == 1) {
            logger.info("Only one backend, skip scp udf file")
            return
        }

        def udf_file_dir = new File(udf_file_path).parent
        backendId_to_backendIP.values().each { be_ip ->
            sshExec("root", be_ip, "ssh-keygen -f '/root/.ssh/known_hosts' -R \"${be_ip}\"", false)
            sshExec("root", be_ip, "ssh -o StrictHostKeyChecking=no root@${be_ip} \"mkdir -p ${udf_file_dir}\"", false)
            scpFiles("root", be_ip, udf_file_path, udf_file_path, false)
        }
    }

}
