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

package org.pentaho.di.trans.steps.dorisstreamloader;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaBigNumber;
import org.pentaho.di.core.row.value.ValueMetaBoolean;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.row.value.ValueMetaTimestamp;
import org.pentaho.di.junit.rules.RestorePDIEngineEnvironment;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.dorisstreamloader.load.DorisRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;

/**
 * Test the deletion and writing process of kettle,
 * temporarily using the doris environment passed in by the user.
 * mvn clean package -Ddoris_host=127.0.0.1 -Ddoris_http_port=8030 -Ddoris_passwd= -Ddoris_query_port=9030 -Ddoris_user=root
 */
public class DorisStreamLoaderTest {

    @ClassRule
    public static RestorePDIEngineEnvironment env = new RestorePDIEngineEnvironment();
    private static final Logger LOG = LoggerFactory.getLogger(DorisStreamLoaderTest.class);
    DorisStreamLoaderMeta lmeta;
    DorisStreamLoaderData ldata;
    DorisStreamLoader lder;
    StepMeta smeta;

    private static final String JDBC_URL = "jdbc:mysql://%s:%s";
    private static final String DATABASE = "test_kettle";
    private static final String TABLE_INSERT = "tbl_insert";
    private static final String TABLE_DELETE = "tbl_delete";
    private static String HOST;
    private static String QUERY_PORT;
    private static String HTTP_PORT;
    private static String USER;
    private static String PASSWD;

    @BeforeClass
    public static void initEnvironment() throws Exception {
        checkParams();
        KettleEnvironment.init();
    }

    private static void checkParams() {
        Preconditions.checkArgument(
            System.getProperty("doris_host") != null, "doris_host is required.");
        Preconditions.checkArgument(
            System.getProperty("doris_query_port") != null, "doris_query_port is required.");
        Preconditions.checkArgument(
            System.getProperty("doris_http_port") != null, "doris_http_port is required.");
        Preconditions.checkArgument(
            System.getProperty("doris_user") != null, "doris_user is required.");
        Preconditions.checkArgument(
            System.getProperty("doris_passwd") != null, "doris_passwd is required.");
        HOST = System.getProperty("doris_host");
        QUERY_PORT = System.getProperty("doris_query_port");
        HTTP_PORT = System.getProperty("doris_http_port");
        USER = System.getProperty("doris_user");
        PASSWD = System.getProperty("doris_passwd");
    }

    @Test
    public void testWrite() throws KettleException, ParseException, InterruptedException {
        TransMeta transMeta = new TransMeta();
        transMeta.setName( "DorisStreamLoader" );

        Map<String, String> vars = new HashMap<String, String>();
        vars.put( "fenodes", HOST + ":" + HTTP_PORT);
        vars.put( "database", DATABASE );
        vars.put( "table", TABLE_INSERT );
        vars.put( "username", USER );
        vars.put( "password", PASSWD );
        vars.put( "bufferFlushMaxRows", "10000");
        vars.put( "bufferFlushMaxBytes", 10 * 1024 * 1024 + "");
        vars.put( "streamLoadProp", "format:json;read_json_by_line:true" );
        vars.put( "maxRetries", "3" );
        vars.put( "deletable", "false" );
        transMeta.injectVariables(vars);

        lmeta = new DorisStreamLoaderMeta();
        lmeta.setFenodes(transMeta.environmentSubstitute("${fenodes}"));
        lmeta.setDatabase(transMeta.environmentSubstitute("${database}"));
        lmeta.setTable(transMeta.environmentSubstitute("${table}"));
        lmeta.setUsername(transMeta.environmentSubstitute("${username}"));
        lmeta.setPassword(transMeta.environmentSubstitute("${password}"));
        lmeta.setBufferFlushMaxRows(Long.parseLong(transMeta.environmentSubstitute("${bufferFlushMaxRows}")));
        lmeta.setBufferFlushMaxBytes(Long.parseLong(transMeta.environmentSubstitute("${bufferFlushMaxBytes}")));
        lmeta.setMaxRetries(Integer.parseInt(transMeta.environmentSubstitute("${maxRetries}")));
        lmeta.setStreamLoadProp(transMeta.environmentSubstitute("${streamLoadProp}"));
        lmeta.setDeletable(Boolean.parseBoolean(transMeta.environmentSubstitute("${deletable}")));
        lmeta.setFieldStream(new String[]{"id", "c1", "c2", "c3","c4","c5","c6"});
        lmeta.setFieldTable(new String[]{"id", "c1", "c2", "c3","c4","c5","c6"});

        ldata = new DorisStreamLoaderData();
        PluginRegistry plugReg = PluginRegistry.getInstance();
        String mblPid = plugReg.getPluginId( StepPluginType.class, lmeta );
        smeta = new StepMeta( mblPid, "DorisStreamLoader", lmeta );
        Trans trans = new Trans( transMeta );
        transMeta.addStep( smeta );
        lder = Mockito.spy(new DorisStreamLoader( smeta, ldata, 1, transMeta, trans ));

        RowMeta rm = new RowMeta();
        rm.addValueMeta(new ValueMetaInteger("id"));
        rm.addValueMeta(new ValueMetaBoolean("c1"));
        rm.addValueMeta(new ValueMetaBigNumber("c2"));
        rm.addValueMeta(new ValueMetaNumber("c3"));
        rm.addValueMeta(new ValueMetaDate("c4"));
        rm.addValueMeta(new ValueMetaTimestamp("c5"));
        rm.addValueMeta(new ValueMetaString("c6"));
        lder.setInputRowMeta(rm);

        lder.copyVariablesFrom( transMeta );
        initializeTable(TABLE_INSERT);
        lder.init(lmeta, ldata);

        Object[] data = new Object[]{
            1L,
            true,
            BigDecimal.valueOf(123456789L),
            12345.6789,
            new SimpleDateFormat("yyyy-MM-dd").parse("2024-11-18"),
            Timestamp.valueOf("2024-11-18 15:30:45"),
            "First Row"};
        doReturn(data).when(lder).getRow();
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                lder.dispose(lmeta, ldata);
                return null;
            }
        }).when(lder).putRow(any(), any());
        lder.processRow(lmeta, ldata);
        lder.dispose(lmeta, ldata);

        while (lder.getStreamLoad() != null && lder.getStreamLoad().isLoadThreadAlive()){
            Thread.sleep(1000);
            LOG.info("Waiting for load thread to finish.");
        }
        List<String> expected = Arrays.asList("1,true,123456789,12345.6789,2024-11-18,2024-11-18T15:30:45,First Row");
        String query =
            String.format("select * from %s.%s order by 1", DATABASE, TABLE_INSERT);
        checkResult(getQueryConnection(), LOG, expected, query, 7);
    }


    @Test
    public void testDelete() throws KettleException, ParseException, InterruptedException {
        TransMeta transMeta = new TransMeta();
        transMeta.setName( "DorisStreamLoaderDelete" );

        Map<String, String> vars = new HashMap<String, String>();
        vars.put( "fenodes", HOST + ":" + HTTP_PORT);
        vars.put( "database", DATABASE );
        vars.put( "table", TABLE_DELETE );
        vars.put( "username", USER );
        vars.put( "password", PASSWD );
        vars.put( "bufferFlushMaxRows", "10000");
        vars.put( "bufferFlushMaxBytes", 10 * 1024 * 1024 + "");
        vars.put( "streamLoadProp", "format:json;read_json_by_line:true" );
        vars.put( "maxRetries", "3" );
        vars.put( "deletable", "true" );
        transMeta.injectVariables(vars);

        lmeta = new DorisStreamLoaderMeta();
        lmeta.setFenodes(transMeta.environmentSubstitute("${fenodes}"));
        lmeta.setDatabase(transMeta.environmentSubstitute("${database}"));
        lmeta.setTable(transMeta.environmentSubstitute("${table}"));
        lmeta.setUsername(transMeta.environmentSubstitute("${username}"));
        lmeta.setPassword(transMeta.environmentSubstitute("${password}"));
        lmeta.setBufferFlushMaxRows(Long.parseLong(transMeta.environmentSubstitute("${bufferFlushMaxRows}")));
        lmeta.setBufferFlushMaxBytes(Long.parseLong(transMeta.environmentSubstitute("${bufferFlushMaxBytes}")));
        lmeta.setMaxRetries(Integer.parseInt(transMeta.environmentSubstitute("${maxRetries}")));
        lmeta.setStreamLoadProp(transMeta.environmentSubstitute("${streamLoadProp}"));
        lmeta.setDeletable(Boolean.parseBoolean(transMeta.environmentSubstitute("${deletable}")));
        lmeta.setFieldStream(new String[]{"id", "c1", "c2", "c3","c4","c5","c6"});
        lmeta.setFieldTable(new String[]{"id", "c1", "c2", "c3","c4","c5","c6"});

        ldata = new DorisStreamLoaderData();
        PluginRegistry plugReg = PluginRegistry.getInstance();
        String mblPid = plugReg.getPluginId( StepPluginType.class, lmeta );
        smeta = new StepMeta( mblPid, "DorisStreamLoaderDelete", lmeta );
        Trans trans = new Trans( transMeta );
        transMeta.addStep( smeta );
        lder = Mockito.spy(new DorisStreamLoader( smeta, ldata, 1, transMeta, trans ));

        RowMeta rm = new RowMeta();
        rm.addValueMeta(new ValueMetaInteger("id"));
        rm.addValueMeta(new ValueMetaBoolean("c1"));
        rm.addValueMeta(new ValueMetaBigNumber("c2"));
        rm.addValueMeta(new ValueMetaNumber("c3"));
        rm.addValueMeta(new ValueMetaDate("c4"));
        rm.addValueMeta(new ValueMetaTimestamp("c5"));
        rm.addValueMeta(new ValueMetaString("c6"));
        lder.setInputRowMeta(rm);

        lder.copyVariablesFrom( transMeta );
        initializeTable(TABLE_DELETE);
        mockDorisData(TABLE_DELETE);
        lder.init(lmeta, ldata);

        Object[] data = new Object[]{
            1L,
            true,
            BigDecimal.valueOf(123456789L),
            12345.6789,
            new SimpleDateFormat("yyyy-MM-dd").parse("2024-11-18"),
            Timestamp.valueOf("2024-11-18 15:30:45"),
            "First Row"};
        doReturn(data).when(lder).getRow();
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                lder.dispose(lmeta, ldata);
                return null;
            }
        }).when(lder).putRow(any(), any());
        lder.processRow(lmeta, ldata);
        lder.dispose(lmeta, ldata);

        while (lder.getStreamLoad() != null && lder.getStreamLoad().isLoadThreadAlive()){
            Thread.sleep(1000);
            LOG.info("Waiting for load thread to finish.");
        }
        List<String> expected = Arrays.asList("2,false,987654321,98765.4321,2024-11-17,2024-11-17T08:15:30,Second Row");
        String query =
            String.format("select * from %s.%s order by 1", DATABASE, TABLE_DELETE);
        checkResult(getQueryConnection(), LOG, expected, query, 7);
    }


    private void initializeTable(String table) {
        executeSQLStatement(
            getQueryConnection(),
            LOG,
            String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE),
            String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, table),
            String.format(
                "CREATE TABLE %s.%s (\n" +
                    "  `id` int, \n" +
                    "  `c1` boolean, \n" +
                    "  `c2` bigint,\n" +
                    "  `c3` decimal(12,4),\n" +
                    "  `c4` date,\n" +
                    "  `c5` datetime,\n" +
                    "  `c6` string\n" +
                    ") ENGINE=OLAP\n" +
                    "UNIQUE KEY(`id`)\n" +
                    "COMMENT 'OLAP'\n" +
                    "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
                    "PROPERTIES (\n" +
                    "\"replication_allocation\" = \"tag.location.default: 1\"\n" +
                    "); \n",
                DATABASE, table));
    }

    public void mockDorisData(String table){
        executeSQLStatement(
            getQueryConnection(),
            LOG,
            String.format("INSERT INTO %s.%s VALUES \n" +
                "(1, TRUE, 123456789, 12345.6789, '2024-11-18', '2024-11-18 15:30:45', 'First Row')," +
                "(2, FALSE, 987654321, 98765.4321, '2024-11-17', '2024-11-17 08:15:30', 'Second Row')",
                DATABASE, table));
    }


    public Connection getQueryConnection() {
        LOG.info("Try to get query connection from doris.");
        String jdbcUrl =
            String.format(
                JDBC_URL,
                System.getProperty("doris_host"),
                System.getProperty("doris_query_port"));
        try {
            return DriverManager.getConnection(jdbcUrl, USER, PASSWD);
        } catch (SQLException e) {
            LOG.info("Failed to get doris query connection. jdbcUrl={}", jdbcUrl, e);
            throw new DorisRuntimeException(e);
        }
    }

    public static void executeSQLStatement(Connection connection, Logger logger, String... sql) {
        if (Objects.isNull(sql) || sql.length == 0) {
            return;
        }
        try (Statement statement = connection.createStatement()) {
            for (String s : sql) {
                if (StringUtils.isNotEmpty(s)) {
                    logger.info("start to execute sql={}", s);
                    statement.execute(s);
                }
            }
        } catch (SQLException e) {
            throw new DorisRuntimeException(e);
        }
    }

    public static void checkResult(
        Connection connection,
        Logger logger,
        List<String> expected,
        String query,
        int columnSize) {
        List<String> actual = new ArrayList<>();
        try (Statement statement = connection.createStatement()) {
            ResultSet sinkResultSet = statement.executeQuery(query);
            while (sinkResultSet.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= columnSize; i++) {
                    Object value = sinkResultSet.getObject(i);
                    if (value == null) {
                        row.add("null");
                    } else {
                        row.add(value.toString());
                    }
                }
                actual.add(StringUtils.join(row, ","));
            }
        } catch (SQLException e) {
            logger.info(
                "Failed to check query result. expected={}, actual={}",
                String.join(",", expected),
                String.join(",", actual),
                e);
            throw new DorisRuntimeException(e);
        }
        logger.info(
            "checking test result. expected={}, actual={}",
            String.join(",", expected),
            String.join(",", actual));
        Assert.assertArrayEquals(expected.toArray(), actual.toArray());
    }
}
