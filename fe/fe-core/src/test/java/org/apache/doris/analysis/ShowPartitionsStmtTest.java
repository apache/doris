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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalDatabase;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import mockit.Delegate;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;


public class ShowPartitionsStmtTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;

    private Analyzer analyzer;

    private ConnectContext ctx = new ConnectContext();

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Mocked
    private HMSExternalCatalog hmsExternalCatalog;

    @Mocked
    private HMSExternalDatabase hmsExternalDatabase;

    @Mocked
    private HMSExternalTable hmsExternalTable;

    @Mocked
    private IcebergExternalCatalog icebergExternalCatalog;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        ctx.setSessionVariable(new SessionVariable());
        ctx.setThreadLocalInfo();
    }

    @After
    public void tearDown() {
        ConnectContext.remove();
    }

    @Test
    public void testNormal() throws UserException {
        ShowPartitionsStmt stmt = new ShowPartitionsStmt(new TableName(internalCtl, "testDb", "testTable"), null, null, null, false);
        stmt.analyzeImpl(analyzer);
        Assert.assertEquals("SHOW PARTITIONS FROM `testDb`.`testTable`", stmt.toString());
    }

    @Test
    public void testShowPartitionsStmtWithBinaryPredicate() throws UserException {
        SlotRef slotRef = new SlotRef(null, "LastConsistencyCheckTime");
        StringLiteral stringLiteral = new StringLiteral("2019-12-22 10:22:11");
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.GT, slotRef, stringLiteral);
        ShowPartitionsStmt stmt = new ShowPartitionsStmt(new TableName(internalCtl, "testDb", "testTable"), binaryPredicate, null, null, false);
        stmt.analyzeImpl(analyzer);
        Assert.assertEquals("SHOW PARTITIONS FROM `testDb`.`testTable` WHERE (`LastConsistencyCheckTime` > '2019-12-22 10:22:11')", stmt.toString());
    }

    @Test
    public void testShowPartitionsStmtWithLikePredicate() throws UserException {
        SlotRef slotRef = new SlotRef(null, "PartitionName");
        StringLiteral stringLiteral = new StringLiteral("%p2019%");
        LikePredicate likePredicate = new LikePredicate(LikePredicate.Operator.LIKE, slotRef, stringLiteral);
        ShowPartitionsStmt stmt = new ShowPartitionsStmt(new TableName(internalCtl, "testDb", "testTable"), likePredicate, null, null, false);
        stmt.analyzeImpl(analyzer);
        Assert.assertEquals("SHOW PARTITIONS FROM `testDb`.`testTable` WHERE `PartitionName` LIKE '%p2019%'", stmt.toString());
    }

    @Test
    public void testShowParitionsStmtOrderByAndLimit() throws UserException {
        SlotRef slotRef = new SlotRef(null, "PartitionId");
        OrderByElement orderByElement = new OrderByElement(slotRef, true, false);
        LimitElement limitElement = new LimitElement(10);
        ShowPartitionsStmt stmt = new ShowPartitionsStmt(new TableName(internalCtl, "testDb", "testTable"), null, Arrays.asList(orderByElement), limitElement, false);
        stmt.analyzeImpl(analyzer);
        Assert.assertEquals("SHOW PARTITIONS FROM `testDb`.`testTable` ORDER BY `PartitionId` ASC LIMIT 10", stmt.toString());
    }

    @Test
    public void testUnsupportFilter() throws UserException {
        SlotRef slotRef = new SlotRef(null, "DataSize");
        StringLiteral stringLiteral = new StringLiteral("3.2 GB");
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef, stringLiteral);
        ShowPartitionsStmt stmt = new ShowPartitionsStmt(new TableName(internalCtl, "testDb", "testTable"), binaryPredicate, null, null, false);
        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("Only the columns of PartitionId/PartitionName/"
                + "State/Buckets/ReplicationNum/LastConsistencyCheckTime are supported.");
        stmt.analyzeImpl(analyzer);
    }

    @Test
    public void testTablePrivilege() throws UserException {
        new MockUp<AccessControllerManager>() {
            @Mock
            public boolean checkTblPriv(ConnectContext ctx, String qualifiedCtl,
                    String qualifiedDb, String tbl, PrivPredicate wanted) {
                return false;
            }
        };

        SlotRef slotRef = new SlotRef(null, "Buckets");
        StringLiteral stringLiteral = new StringLiteral("33");
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef, stringLiteral);

        ShowPartitionsStmt stmt = new ShowPartitionsStmt(new TableName(internalCtl, "testDb", "testTable"),
                binaryPredicate, null, null, false);
        Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
    }

    @Test
    public void testExternalTable() throws UserException {
        new MockUp<AccessControllerManager>() {
            @Mock
            public boolean checkGlobalPriv(ConnectContext ctx, PrivPredicate wanted) {
                return true;
            }

            @Mock
            public boolean checkTblPriv(ConnectContext ctx, String qualifiedCtl,
                    String qualifiedDb, String tbl, PrivPredicate wanted) {
                return true;
            }
        };

        new MockUp<CatalogMgr>() {
            @Mock
            public CatalogIf getCatalog(String name) {
                switch (name) {
                    case "hms":
                        return hmsExternalCatalog;
                    case "ice":
                        return icebergExternalCatalog;
                    default:
                        return null;
                }
            }
        };

        AtomicBoolean isView = new AtomicBoolean(true);
        AtomicBoolean isPartitioned = new AtomicBoolean(false);
        new Expectations() {
            {
                hmsExternalCatalog.getDbNullable(anyString);
                result = new Delegate<DatabaseIf>() {
                    DatabaseIf catalogResult(String name) {
                        switch (name) {
                            case "hms":
                                return hmsExternalDatabase;
                            default:
                                return null;
                        }
                    }
                };

                hmsExternalTable.isView();
                result = new Delegate<Boolean>() {
                    boolean isView() {
                        return isView.get();
                    }
                };

                hmsExternalTable.getPartitionColumns();
                result = new Delegate<List>() {
                    List<Column> data() {
                        if (isPartitioned.get()) {
                            return Arrays.asList(new Column("test", PrimitiveType.INT));
                        }

                        return Arrays.asList();
                    }
                };
            }
        };

        SlotRef slotRef = new SlotRef(null, "PartitionName");
        StringLiteral stringLiteral = new StringLiteral("part=part1");
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef, stringLiteral);

        /* case 0: Catalog - iceberg */ {
            ShowPartitionsStmt stmt = new ShowPartitionsStmt(new TableName("ice", "null", "testTable"),
                    binaryPredicate, null, null, false);
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
        }

        /* case 1: Catalog - null */ {
            ShowPartitionsStmt stmt = new ShowPartitionsStmt(new TableName("null", "null", "testTable"),
                    binaryPredicate, null, null, false);
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
        }

        /* case 2: HMSExternalTable null */ {
            ShowPartitionsStmt stmt = new ShowPartitionsStmt(new TableName("hms", "null", "testTable"),
                    binaryPredicate, null, null, false);
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
        }

        /* case 3: HMSExternalTable : isView */ {
            ShowPartitionsStmt stmt = new ShowPartitionsStmt(new TableName("hms", "hms", "testTable"),
                    binaryPredicate, null, null, false);
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
            isView.set(false);
        }

        /* case 4: HMSExternalTable : notPartitioned */ {
            ShowPartitionsStmt stmt = new ShowPartitionsStmt(new TableName("hms", "hms", "testTable"),
                    binaryPredicate, null, null, false);
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
        }

        /* case 5: HMSExternalTable : partitioned */ {
            isPartitioned.set(true);
            ShowPartitionsStmt stmt = new ShowPartitionsStmt(new TableName("hms", "hms", "testTable"),
                    binaryPredicate, null, null, false);
            Assertions.assertEquals(stmt.toSql(), "SHOW PARTITIONS FROM `hms`.`testTable` "
                    + "WHERE (`PartitionName` = 'part=part1')");
        }
    }
}
