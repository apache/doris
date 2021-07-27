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

import org.apache.doris.common.UserException;

import java.util.List;

import com.clearspring.analytics.util.Lists;
import mockit.Expectations;
import mockit.Injectable;
import org.junit.Assert;
import org.junit.Test;

public class UpdateStmtTest {

    @Test
    public void testAnalyze(@Injectable Analyzer analyzer) {
        TableName tableName = new TableName("db", "table");
        IntLiteral intLiteral = new IntLiteral(1);
        SlotRef slotRef = new SlotRef(tableName, "c1");
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, intLiteral, slotRef);
        List<Expr> setExprs = Lists.newArrayList();
        setExprs.add(binaryPredicate);

        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
            }
        };
        UpdateStmt updateStmt = new UpdateStmt(tableName, Lists.newArrayList(setExprs), null);
        try {
            updateStmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            System.out.println(e.getMessage());
        }
    }
}
