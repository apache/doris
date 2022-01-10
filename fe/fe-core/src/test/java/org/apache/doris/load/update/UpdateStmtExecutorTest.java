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

package org.apache.doris.load.update;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.UpdateStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.cluster.Cluster;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.GlobalTransactionMgr;

import java.util.List;

import com.clearspring.analytics.util.Lists;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class UpdateStmtExecutorTest {

    @Test
    public void testCommitAndPublishTxn(@Injectable Analyzer analyzer,
                                        @Injectable Coordinator coordinator,
                                        @Mocked GlobalTransactionMgr globalTransactionMgr) {
        Cluster test_cluster = new Cluster("test_cluster", 0);
        Database test_db = new Database(1, "test_db");
        test_db.setClusterName("test_cluster");
        Catalog.getCurrentCatalog().addCluster(test_cluster);
        Catalog.getCurrentCatalog().unprotectCreateDb(test_db);
        UpdateStmtExecutor updateStmtExecutor = new UpdateStmtExecutor();
        Deencapsulation.setField(updateStmtExecutor, "dbId", 1);
        Deencapsulation.setField(updateStmtExecutor, "effectRows", 0);
        Deencapsulation.setField(updateStmtExecutor, "analyzer", analyzer);
        Deencapsulation.setField(updateStmtExecutor, "coordinator", coordinator);
        Deencapsulation.invoke(updateStmtExecutor, "commitAndPublishTxn");
    }

    @Test
    public void testFromUpdateStmt(@Injectable OlapTable olapTable,
                                   @Mocked Catalog catalog,
                                   @Injectable Database db,
                                   @Injectable Analyzer analyzer) throws AnalysisException {
        TableName tableName = new TableName("db", "test");
        List<Expr> setExprs = Lists.newArrayList();
        SlotRef slotRef = new SlotRef(tableName, "v1");
        IntLiteral intLiteral = new IntLiteral(1);
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                slotRef, intLiteral);
        setExprs.add(binaryPredicate);
        SlotRef keySlotRef = new SlotRef(tableName, "k1");
        Expr whereExpr = new BinaryPredicate(BinaryPredicate.Operator.EQ, keySlotRef, intLiteral);
        UpdateStmt updateStmt = new UpdateStmt(tableName, setExprs, whereExpr);
        Deencapsulation.setField(updateStmt, "targetTable", olapTable);
        Deencapsulation.setField(updateStmt, "analyzer", analyzer);
        new Expectations() {
            {
                db.getId();
                result = 1;
                analyzer.getContext().queryId();
                result = new TUniqueId(1, 2);
                analyzer.getContext().getSessionVariable().getQueryTimeoutS();
                result = 1000;
                olapTable.getId();
                result = 2;
            }
        };
        UpdateStmtExecutor executor = UpdateStmtExecutor.fromUpdateStmt(updateStmt);
        Assert.assertEquals(new Long(2), new Long(executor.getTargetTableId()));
        Assert.assertEquals(whereExpr, Deencapsulation.getField(executor, "whereExpr"));
        Assert.assertEquals(setExprs, Deencapsulation.getField(executor, "setExprs"));
        Assert.assertEquals(new Long(1), Deencapsulation.getField(executor, "dbId"));
    }
}
