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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class TruncateTableStmtTest {

    @Mocked
    private Env env;

    @Mocked
    private ConnectContext connectContext;

    @Mocked
    private AccessControllerManager accessManager;

    private Analyzer analyzer;

    @Before
    public void setUp() {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessManager;

                ConnectContext.get();
                minTimes = 0;
                result = connectContext;

                connectContext.getDatabase();
                minTimes = 0;
                result = "testDb";

                connectContext.getQualifiedUser();
                minTimes = 0;
                result = "root";

                connectContext.getCurrentUserIdentity();
                minTimes = 0;
                result = null;
            }
        };
        analyzer = new Analyzer(env, connectContext);
    }

    private TableRef createTableRef(String db, String table, String alias, PartitionNames partitionNames) {
        TableName tableName = new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, db, table);
        TableRef tableRef = new TableRef(tableName, alias, partitionNames);
        return tableRef;
    }

    @Test
    public void testConstructorWithoutIfExists() {
        TableRef tableRef = createTableRef("testDb", "testTbl", null, null);
        TruncateTableStmt stmt = new TruncateTableStmt(tableRef);

        Assert.assertNotNull(stmt.getTblRef());
        Assert.assertFalse(stmt.isIfExists());
    }

    @Test
    public void testConstructorWithIfExists() {
        TableRef tableRef = createTableRef("testDb", "testTbl", null, null);
        TruncateTableStmt stmt = new TruncateTableStmt(tableRef, true);

        Assert.assertNotNull(stmt.getTblRef());
        Assert.assertTrue(stmt.isIfExists());
    }

    @Test
    public void testConstructorWithIfExistsFalse() {
        TableRef tableRef = createTableRef("testDb", "testTbl", null, null);
        TruncateTableStmt stmt = new TruncateTableStmt(tableRef, false);

        Assert.assertNotNull(stmt.getTblRef());
        Assert.assertFalse(stmt.isIfExists());
    }

    @Test
    public void testGetTblRef() {
        TableRef tableRef = createTableRef("testDb", "testTbl", null, null);
        TruncateTableStmt stmt = new TruncateTableStmt(tableRef);

        Assert.assertEquals(tableRef, stmt.getTblRef());
    }

    @Test
    public void testToSqlWithoutIfExistsAndPartitions() {
        TableRef tableRef = createTableRef("testDb", "testTbl", null, null);
        TruncateTableStmt stmt = new TruncateTableStmt(tableRef);

        String sql = stmt.toSql();
        Assert.assertTrue(sql.contains("TRUNCATE TABLE"));
        Assert.assertFalse(sql.contains("IF EXISTS"));
    }

    @Test
    public void testToSqlWithIfExists() {
        TableRef tableRef = createTableRef("testDb", "testTbl", null, null);
        TruncateTableStmt stmt = new TruncateTableStmt(tableRef, true);

        String sql = stmt.toSql();
        Assert.assertTrue(sql.contains("TRUNCATE TABLE"));
        Assert.assertTrue(sql.contains("IF EXISTS"));
    }

    @Test
    public void testToSqlWithPartitions() {
        List<String> partitions = Lists.newArrayList("p1", "p2");
        PartitionNames partitionNames = new PartitionNames(false, partitions);
        TableRef tableRef = createTableRef("testDb", "testTbl", null, partitionNames);
        TruncateTableStmt stmt = new TruncateTableStmt(tableRef);

        String sql = stmt.toSql();
        Assert.assertTrue(sql.contains("TRUNCATE TABLE"));
        Assert.assertTrue(sql.contains("PARTITION"));
    }

    @Test
    public void testToSqlWithIfExistsAndPartitions() {
        List<String> partitions = Lists.newArrayList("p1", "p2");
        PartitionNames partitionNames = new PartitionNames(false, partitions);
        TableRef tableRef = createTableRef("testDb", "testTbl", null, partitionNames);
        TruncateTableStmt stmt = new TruncateTableStmt(tableRef, true);

        String sql = stmt.toSql();
        Assert.assertTrue(sql.contains("TRUNCATE TABLE"));
        Assert.assertTrue(sql.contains("IF EXISTS"));
        Assert.assertTrue(sql.contains("PARTITION"));
    }

    @Test
    public void testToSqlWithoutTable() {
        TableRef tableRef = createTableRef("testDb", "testTbl", null, null);
        TruncateTableStmt stmt = new TruncateTableStmt(tableRef);

        String sql = stmt.toSqlWithoutTable();
        Assert.assertEquals("", sql);
    }

    @Test(expected = AnalysisException.class)
    public void testAnalyzeWithAlias() throws UserException {
        TableRef tableRef = createTableRef("testDb", "testTbl", "alias", null);
        TruncateTableStmt stmt = new TruncateTableStmt(tableRef);

        stmt.analyze(analyzer);
    }

    @Test(expected = AnalysisException.class)
    public void testAnalyzeWithTempPartition() throws UserException {
        List<String> partitions = Lists.newArrayList("tp1");
        PartitionNames partitionNames = new PartitionNames(true, partitions);
        TableRef tableRef = createTableRef("testDb", "testTbl", null, partitionNames);
        TruncateTableStmt stmt = new TruncateTableStmt(tableRef);

        stmt.analyze(analyzer);
    }

    @Test
    public void testAnalyzeSuccess() throws UserException {
        new Expectations() {
            {
                accessManager.checkTblPriv((ConnectContext) any, anyString, anyString, anyString,
                        (PrivPredicate) any);
                minTimes = 0;
                result = true;
            }
        };

        TableRef tableRef = createTableRef("testDb", "testTbl", null, null);
        TruncateTableStmt stmt = new TruncateTableStmt(tableRef);

        stmt.analyze(analyzer);
        Assert.assertNotNull(stmt.getTblRef());
    }

    @Test
    public void testAnalyzeWithPartitionsSuccess() throws UserException {
        new Expectations() {
            {
                accessManager.checkTblPriv((ConnectContext) any, anyString, anyString, anyString,
                        (PrivPredicate) any);
                minTimes = 0;
                result = true;
            }
        };

        List<String> partitions = Lists.newArrayList("p1", "p2");
        PartitionNames partitionNames = new PartitionNames(false, partitions);
        TableRef tableRef = createTableRef("testDb", "testTbl", null, partitionNames);
        TruncateTableStmt stmt = new TruncateTableStmt(tableRef);

        stmt.analyze(analyzer);
        Assert.assertNotNull(stmt.getTblRef());
    }

    @Test(expected = AnalysisException.class)
    public void testAnalyzeNoPrivilege() throws UserException {
        new Expectations() {
            {
                accessManager.checkTblPriv((ConnectContext) any, anyString, anyString, anyString,
                        (PrivPredicate) any);
                minTimes = 0;
                result = false;
            }
        };

        TableRef tableRef = createTableRef("testDb", "testTbl", null, null);
        TruncateTableStmt stmt = new TruncateTableStmt(tableRef);

        stmt.analyze(analyzer);
    }
}
