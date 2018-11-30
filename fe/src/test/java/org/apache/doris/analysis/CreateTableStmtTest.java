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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.MockedAuth;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import mockit.Mocked;
import mockit.internal.startup.Startup;

public class CreateTableStmtTest {
    private static final Logger LOG = LoggerFactory.getLogger(CreateTableStmtTest.class);
    
    // used to get default db
    private TableName tblName;
    private TableName tblNameNoDb;
    private List<ColumnDef> cols;
    private List<ColumnDef> invalidCols;
    private List<String> colsName;
    private List<String> invalidColsName;
    private Analyzer analyzer;
    
    @Mocked
    private PaloAuth auth;
    @Mocked
    private ConnectContext ctx;

    static {
        Startup.initializeIfPossible();
    }

    // set default db is 'db1'
    // table name is table1
    // Column: [col1 int; col2 string]
    @Before
    public void setUp() {
        // analyzer
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
        // table name
        tblName = new TableName("db1", "table1");
        tblNameNoDb = new TableName("", "table1");
        // col
        cols = Lists.newArrayList();
        cols.add(new ColumnDef("col1", new TypeDef(ScalarType.createType(PrimitiveType.INT))));
        cols.add(new ColumnDef("col2", new TypeDef(ScalarType.createChar(10))));
        colsName = Lists.newArrayList();
        colsName.add("col1");
        colsName.add("col2");
        // invalid col
        invalidCols = Lists.newArrayList();
        invalidCols.add(new ColumnDef("col1", new TypeDef(ScalarType.createType(PrimitiveType.INT))));
        invalidCols.add(new ColumnDef("col2", new TypeDef(ScalarType.createChar(10))));
        invalidCols.add(new ColumnDef("col2", new TypeDef(ScalarType.createChar(10))));
        invalidColsName = Lists.newArrayList();
        invalidColsName.add("col1");
        invalidColsName.add("col2");
        invalidColsName.add("col2");

        MockedAuth.mockedAuth(auth);
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
    }
    
    @Test
    public void testNormal() throws UserException, AnalysisException {
        CreateTableStmt stmt = new CreateTableStmt(false, false, tblName, cols, "olap",
                new KeysDesc(KeysType.AGG_KEYS, colsName), null,
                new HashDistributionDesc(10, Lists.newArrayList("col1")), null, null);
        stmt.analyze(analyzer);
        Assert.assertEquals("testCluster:db1", stmt.getDbName());
        Assert.assertEquals("table1", stmt.getTableName());
        Assert.assertNull(stmt.getProperties());
    }
    
    @Test
    public void testDefaultDbNormal() throws UserException, AnalysisException {
        CreateTableStmt stmt = new CreateTableStmt(false, false, tblNameNoDb, cols, "olap",
                new KeysDesc(KeysType.AGG_KEYS, colsName), null,
                new HashDistributionDesc(10, Lists.newArrayList("col1")), null, null);
        stmt.analyze(analyzer);
        Assert.assertEquals("testDb", stmt.getDbName());
        Assert.assertEquals("table1", stmt.getTableName());
        Assert.assertNull(stmt.getPartitionDesc());
        Assert.assertNull(stmt.getProperties());
    }
    
    @Test(expected = AnalysisException.class)
    public void testNoDb() throws UserException, AnalysisException {
        // make defalut db return empty;
        analyzer = EasyMock.createMock(Analyzer.class);
        EasyMock.expect(analyzer.getDefaultDb()).andReturn("").anyTimes();
        EasyMock.expect(analyzer.getClusterName()).andReturn("cluster").anyTimes();
        EasyMock.replay(analyzer);
        CreateTableStmt stmt = new CreateTableStmt(false, false, tblNameNoDb, cols, "olap",
                new KeysDesc(KeysType.AGG_KEYS, colsName), null,
                new RandomDistributionDesc(10), null, null);
        stmt.analyze(analyzer);
    }
    
    @Test(expected = AnalysisException.class)
    public void testEmptyCol() throws UserException, AnalysisException {
        // make defalut db return empty;
        List<ColumnDef> emptyCols = Lists.newArrayList();
        CreateTableStmt stmt = new CreateTableStmt(false, false, tblNameNoDb, emptyCols, "olap",
                new KeysDesc(), null,
                new RandomDistributionDesc(10), null, null);
        stmt.analyze(analyzer);
    }
    
    @Test(expected = AnalysisException.class)
    public void testDupCol() throws UserException, AnalysisException {
        // make defalut db return empty;
        CreateTableStmt stmt = new CreateTableStmt(false, false, tblNameNoDb, invalidCols, "olap",
                new KeysDesc(KeysType.AGG_KEYS, invalidColsName), null,
                new RandomDistributionDesc(10), null, null);
        stmt.analyze(analyzer);
    }
}