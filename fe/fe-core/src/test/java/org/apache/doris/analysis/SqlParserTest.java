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

import org.apache.doris.common.util.SqlParserUtils;

import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;

public class SqlParserTest {

    @Test
    public void testIndex() {
        String[] alterStmts = {
                "alter table example_db.table1 add index idx_user(username) comment 'username index';",
                "alter table example_db.table1 add index idx_user_bitmap(username) using bitmap comment 'username bitmap index';",
                "alter table example_db.table1 add index idx_user_ngrambf(username) using NGRAM_BF(3, 256) comment 'username ngrambf_v1 index';",
                "create index idx_ngrambf on example_db.table1(username) using NGRAM_BF(3, 256) comment 'username ngram_bf index';"
        };
        for (String alterStmt : alterStmts) {
            SqlParser parser = new SqlParser(new SqlScanner(new StringReader(alterStmt)));
            StatementBase alterQueryStmt = null;
            try {
                alterQueryStmt = SqlParserUtils.getFirstStmt(parser);
            } catch (Error e) {
                Assert.fail(e.getMessage());
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
            Assert.assertTrue(alterQueryStmt instanceof AlterTableStmt);
        }
    }
}
