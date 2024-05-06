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
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class DropMaterializedViewStmtTest {

    @Mocked
    Analyzer analyzer;
    @Mocked
    AccessControllerManager accessManager;

    @Test
    public void testEmptyMVName(@Injectable TableName tableName) {
        DropMaterializedViewStmt stmt = new DropMaterializedViewStmt(false, "", tableName);
        try {
            stmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("could not be empty"));
        }
    }

    @Test
    public void testNoPermission(@Injectable TableName tableName) {
        new Expectations() {
            {
                accessManager.checkTblPriv(ConnectContext.get(), tableName.getCtl(), tableName.getDb(),
                        tableName.getTbl(), PrivPredicate.ALTER);
                result = false;
            }
        };
        DropMaterializedViewStmt stmt = new DropMaterializedViewStmt(false, "test", tableName);
        try {
            stmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Access denied;"));
        }

    }
}
