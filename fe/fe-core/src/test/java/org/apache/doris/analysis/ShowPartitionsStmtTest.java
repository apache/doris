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

import mockit.Expectations;
import mockit.Mocked;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.FakeCatalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;


public class ShowPartitionsStmtTest {
    @Mocked
    private Analyzer analyzer;
    private Catalog catalog;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Before
    public void setUp() {
        catalog = AccessTestUtil.fetchAdminCatalog();
        FakeCatalog.setCatalog(catalog);

        new Expectations() {
            {
                analyzer.getDefaultDb();
                minTimes = 0;
                result = "testDb";

                analyzer.getCatalog();
                minTimes = 0;
                result = catalog;

                analyzer.getClusterName();
                minTimes = 0;
                result = "testCluster";
            }
        };

    }

    @Test
    public void testNormal() throws UserException {
        ShowPartitionsStmt stmt = new ShowPartitionsStmt(new TableName("testDb", "testTable"), null, null, null, false);
        stmt.analyzeImpl(analyzer);
        Assert.assertEquals("SHOW PARTITIONS FROM `testCluster:testDb`.`testTable`", stmt.toString());
    }

    @Test
    public void testShowPartitionsStmtWithBinaryPredicate() throws UserException {
        SlotRef slotRef = new SlotRef(null, "LastConsistencyCheckTime");
        StringLiteral stringLiteral = new StringLiteral("2019-12-22 10:22:11");
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.GT, slotRef, stringLiteral);
        ShowPartitionsStmt stmt = new ShowPartitionsStmt(new TableName("testDb", "testTable"), binaryPredicate, null, null, false);
        stmt.analyzeImpl(analyzer);
        Assert.assertEquals("SHOW PARTITIONS FROM `testCluster:testDb`.`testTable` WHERE `LastConsistencyCheckTime` > '2019-12-22 10:22:11'", stmt.toString());
    }

    @Test
    public void testShowPartitionsStmtWithLikePredicate() throws UserException {
        SlotRef slotRef = new SlotRef(null, "PartitionName");
        StringLiteral stringLiteral = new StringLiteral("%p2019%");
        LikePredicate likePredicate = new LikePredicate(LikePredicate.Operator.LIKE, slotRef, stringLiteral);
        ShowPartitionsStmt stmt = new ShowPartitionsStmt(new TableName("testDb", "testTable"), likePredicate, null, null, false);
        stmt.analyzeImpl(analyzer);
        Assert.assertEquals("SHOW PARTITIONS FROM `testCluster:testDb`.`testTable` WHERE `PartitionName` LIKE '%p2019%'", stmt.toString());
    }

    @Test
    public void testShowParitionsStmtOrderByAndLimit() throws UserException {
        SlotRef slotRef = new SlotRef(null, "PartitionId");
        OrderByElement orderByElement = new OrderByElement(slotRef, true, false);
        LimitElement limitElement = new LimitElement(10);
        ShowPartitionsStmt stmt = new ShowPartitionsStmt(new TableName("testDb", "testTable"), null, Arrays.asList(orderByElement), limitElement, false);
        stmt.analyzeImpl(analyzer);
        Assert.assertEquals("SHOW PARTITIONS FROM `testCluster:testDb`.`testTable` ORDER BY `PartitionId` ASC LIMIT 10", stmt.toString());
    }

    @Test
    public void testUnsupportFilter() throws UserException {
        SlotRef slotRef = new SlotRef(null, "DataSize");
        StringLiteral stringLiteral = new StringLiteral("3.2 GB");
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef, stringLiteral);
        ShowPartitionsStmt stmt = new ShowPartitionsStmt(new TableName("testDb", "testTable"), binaryPredicate, null, null, false);
        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("Only the columns of PartitionId/PartitionName/" +
                "State/Buckets/ReplicationNum/LastConsistencyCheckTime are supported.");
        stmt.analyzeImpl(analyzer);
    }

}
