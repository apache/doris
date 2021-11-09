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

import com.google.common.collect.Lists;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.qe.ConnectContext;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class BackupTest {

    private Analyzer analyzer;

    private Catalog catalog;

    @Mocked
    private ConnectContext ctx;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        catalog = AccessTestUtil.fetchAdminCatalog();
        new MockUp<Catalog>() {
            @Mock
            public Catalog getCurrentCatalog() {
                return catalog;
            }
        };
    }

    public BackupStmt createStmt(boolean caseSensitive) {
        BackupStmt stmt;
        List<TableRef> tblRefs = Lists.newArrayList();
        String testDB = "test_db";
        tblRefs.add(new TableRef(new TableName(null, "table1"), null));
        if (caseSensitive) {
            // case sensitive
            tblRefs.add(new TableRef(new TableName(null, "Table1"), null));
        }
        AbstractBackupTableRefClause tableRefClause = new AbstractBackupTableRefClause(false, tblRefs);
        stmt = new BackupStmt(new LabelName(testDB, "label1"), "repo",
                tableRefClause, null);
        return stmt;
    }

    @Test
    public void caseSensitiveTest() throws Exception {
        BackupStmt stmt = createStmt(true);
        stmt.analyze(analyzer);
    }
}
