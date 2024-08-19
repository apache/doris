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


import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.MockedAuth;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class DeleteStmtTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;

    Analyzer analyzer;

    @Mocked
    private AccessControllerManager accessManager;
    @Mocked
    private ConnectContext ctx;

    @Before
    public void setUp() {
        MockedAuth.mockedAccess(accessManager);
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
    }

    @Test
    public void getMethodTest() {
        BinaryPredicate wherePredicate = new BinaryPredicate(Operator.EQ, new SlotRef(null, "k1"),
                                                             new StringLiteral("abc"));
        DeleteStmt deleteStmt = new DeleteStmt(new TableName(internalCtl, "testDb", "testTbl"),
                new PartitionNames(false, Lists.newArrayList("partition")), wherePredicate);

        Assert.assertEquals("testDb", deleteStmt.getDbName());
        Assert.assertEquals("testTbl", deleteStmt.getTableName());
        Assert.assertEquals(Lists.newArrayList("partition"), deleteStmt.getPartitionNames());
        Assert.assertEquals("DELETE FROM `testDb`.`testTbl` PARTITION (partition) WHERE (`k1` = 'abc')",
                            deleteStmt.toSql());

        deleteStmt = new DeleteStmt(new TableName(internalCtl, "testDb", "testTbl"), null, wherePredicate);
        Assert.assertEquals("DELETE FROM `testDb`.`testTbl` WHERE (`k1` = 'abc')",
                deleteStmt.toSql());
    }

    @Test
    public void testAnalyze() {
        // case 1
        LikePredicate likePredicate = new LikePredicate(org.apache.doris.analysis.LikePredicate.Operator.LIKE,
                                                        new SlotRef(null, "k1"),
                                                        new StringLiteral("abc"));
        DeleteStmt deleteStmt = new DeleteStmt(new TableName(internalCtl, "testDb", "testTbl"),
                new PartitionNames(false, Lists.newArrayList("partition")), likePredicate);
        try {
            deleteStmt.analyzePredicate(likePredicate, analyzer);
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Where clause only supports compound predicate, binary predicate, is_null predicate or in predicate"));
        }

        // case 2
        BinaryPredicate binaryPredicate = new BinaryPredicate(Operator.EQ, new SlotRef(null, "k1"),
                                                              new StringLiteral("abc"));
        CompoundPredicate compoundPredicate =
                new CompoundPredicate(org.apache.doris.analysis.CompoundPredicate.Operator.OR, binaryPredicate,
                                      binaryPredicate);

        deleteStmt = new DeleteStmt(new TableName(internalCtl, "testDb", "testTbl"),
                new PartitionNames(false, Lists.newArrayList("partition")), compoundPredicate);

        try {
            deleteStmt.analyzePredicate(compoundPredicate, analyzer);
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("should be AND"));
        }

        // case 3
        compoundPredicate = new CompoundPredicate(org.apache.doris.analysis.CompoundPredicate.Operator.AND,
                                                  binaryPredicate,
                                                  likePredicate);

        deleteStmt = new DeleteStmt(new TableName(internalCtl, "testDb", "testTbl"),
                new PartitionNames(false, Lists.newArrayList("partition")), compoundPredicate);
        try {
            deleteStmt.analyzePredicate(compoundPredicate, analyzer);
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(
                    "Unknown column"));
        }

        // case 4
        binaryPredicate = new BinaryPredicate(Operator.EQ, new SlotRef(null, "k1"),
                                              new SlotRef(null, "k1"));
        compoundPredicate = new CompoundPredicate(org.apache.doris.analysis.CompoundPredicate.Operator.AND,
                                                  binaryPredicate,
                                                  binaryPredicate);

        deleteStmt = new DeleteStmt(new TableName(internalCtl, "testDb", "testTbl"),
                new PartitionNames(false, Lists.newArrayList("partition")), compoundPredicate);
        try {
            deleteStmt.analyzePredicate(compoundPredicate, analyzer);
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("Unknown column"));
        }

        // case 5
        binaryPredicate = new BinaryPredicate(Operator.EQ, new StringLiteral("abc"),
                                              new SlotRef(null, "k1"));
        compoundPredicate = new CompoundPredicate(org.apache.doris.analysis.CompoundPredicate.Operator.AND,
                                                  binaryPredicate,
                                                  binaryPredicate);

        deleteStmt = new DeleteStmt(new TableName(internalCtl, "testDb", "testTbl"),
                new PartitionNames(false, Lists.newArrayList("partition")), compoundPredicate);
        try {
            deleteStmt.analyzePredicate(compoundPredicate, analyzer);
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("Unknown column"));
        }

        // case 6 partition is null
        binaryPredicate = new BinaryPredicate(Operator.EQ, new SlotRef(null, "k1"), new StringLiteral("abc"));
        compoundPredicate = new CompoundPredicate(org.apache.doris.analysis.CompoundPredicate.Operator.AND,
                                                  binaryPredicate,
                                                  binaryPredicate);

        deleteStmt = new DeleteStmt(new TableName(internalCtl, "testDb", "testTbl"), null, compoundPredicate);
        try {
            deleteStmt.analyzePredicate(compoundPredicate, analyzer);
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("Unknown column"));
        }

        // normal
        binaryPredicate = new BinaryPredicate(Operator.EQ, new SlotRef(null, "k1"),
                                              new StringLiteral("abc"));
        List<Expr> inList = Lists.newArrayList();
        inList.add(new StringLiteral("2323"));
        inList.add(new StringLiteral("1518"));
        inList.add(new StringLiteral("5768"));
        inList.add(new StringLiteral("6725"));
        InPredicate inPredicate = new InPredicate(new SlotRef(null, "k2"), inList, true);
        CompoundPredicate compoundPredicate2 =
                new CompoundPredicate(org.apache.doris.analysis.CompoundPredicate.Operator.AND,
                                      binaryPredicate,
                                      inPredicate);
        compoundPredicate = new CompoundPredicate(org.apache.doris.analysis.CompoundPredicate.Operator.AND,
                                                  binaryPredicate,
                                                  compoundPredicate2);

        deleteStmt = new DeleteStmt(new TableName(internalCtl, "testDb", "testTbl"),
                new PartitionNames(false, Lists.newArrayList("partition")), compoundPredicate);
        try {
            deleteStmt.analyzePredicate(compoundPredicate, analyzer);
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("Unknown column"));
        }

        // multi partition
        deleteStmt = new DeleteStmt(new TableName(internalCtl, "testDb", "testTbl"),
                new PartitionNames(false, Lists.newArrayList("partition1", "partiton2")), compoundPredicate);
        Assert.assertEquals(Lists.newArrayList("partition1", "partiton2"), deleteStmt.getPartitionNames());
    }
}
