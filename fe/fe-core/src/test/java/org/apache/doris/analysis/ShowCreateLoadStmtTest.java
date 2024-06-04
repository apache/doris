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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.ShowResultSetMetaData;

import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ShowCreateLoadStmtTest {
    private Analyzer analyzer;
    private ConnectContext ctx = new ConnectContext();

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
    public void testAnalyse() throws Exception {
        AtomicBoolean privilege = new AtomicBoolean(false);
        AtomicReference<PrivPredicate> predicate = new AtomicReference<>(PrivPredicate.SHOW);
        new MockUp<AccessControllerManager>() {
            @Mock
            public boolean checkGlobalPriv(ConnectContext ctx, PrivPredicate wanted) {
                if (predicate.get().equals(wanted)) {
                    return privilege.get();
                }
                return false;
            }
        };

        /* case 1: Privilege */ {
            ShowCreateLoadStmt stmt = new ShowCreateLoadStmt(new LabelName());
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));

            predicate.set(PrivPredicate.ADMIN);
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
        }

        /* case 2: labelName format */ {
            ShowCreateLoadStmt stmt = new ShowCreateLoadStmt(new LabelName("", "12#3"));
            predicate.set(PrivPredicate.ADMIN);
            privilege.set(true);
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
        }

        /* case 3: success */ {
            ShowCreateLoadStmt stmt = new ShowCreateLoadStmt(new LabelName("", "ab12_3"));
            predicate.set(PrivPredicate.ADMIN);
            privilege.set(true);
            stmt.analyze(analyzer);
            Assertions.assertEquals(stmt.getDb(), "testDb");
            Assertions.assertEquals(stmt.getLabel(), "ab12_3");
        }
    }

    @Test
    public void getMetaData() {
        ShowCreateLoadStmt stmt = new ShowCreateLoadStmt(null);
        ShowResultSetMetaData result = stmt.getMetaData();
        Assertions.assertEquals(result.getColumnCount(), 2);
    }
}
