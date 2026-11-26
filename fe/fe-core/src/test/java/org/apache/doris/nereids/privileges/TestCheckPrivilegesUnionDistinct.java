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

package org.apache.doris.nereids.privileges;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCheckPrivilegesUnionDistinct extends TestWithFeService {
    private static final String TABLE = "test_union_priv.t";
    private static final String USER = "test_union_priv_user";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test_union_priv");
        createTable("create table " + TABLE
                + " (id int, name varchar(32), secret bigint) duplicate key(id)"
                + " distributed by hash(id) buckets 1 properties('replication_num' = '1')");
        addUser(USER, true);
        grantPriv("grant select_priv(id) on " + TABLE + " to " + USER);
    }

    @Test
    public void testUnionDistinctChecksEveryOutputColumn() throws Exception {
        useUser(USER);
        try {
            // The granted column must stay readable through UNION DISTINCT.
            assertAllowed("select id from " + TABLE + " union select id from " + TABLE);
            assertAllowed("select count(*) from (select id from " + TABLE
                    + " union select id from " + TABLE + ") q");
            assertAllowed("select count(id) from " + TABLE + " union select count(id) from " + TABLE);

            // Every branch output column is read by a DISTINCT union, so unauthorized columns
            // must be rejected in all of these shapes.
            for (String sql : ImmutableList.of(
                    "select name, id from " + TABLE + " union select name, id from " + TABLE,
                    "select secret, id from " + TABLE + " union select secret, id from " + TABLE,
                    "select id, name from " + TABLE + " union select id, name from " + TABLE,
                    "select * from (select name, id from " + TABLE
                            + " union select name, id from " + TABLE + ") q",
                    "select name from (select name, id from " + TABLE
                            + " union select name, id from " + TABLE + ") q",
                    "select count(*) from (select name, id from " + TABLE
                            + " union select name, id from " + TABLE + ") q",
                    "select name, id from " + TABLE + " union select 'x', 1",
                    "select cast(name as varchar(64)) n, id from " + TABLE
                            + " union select name, id from " + TABLE,
                    "with c as (select name, id from " + TABLE
                            + " union select name, id from " + TABLE + ") select * from c",
                    // The other set operators must keep rejecting unauthorized columns.
                    "select name, id from " + TABLE + " union all select name, id from " + TABLE,
                    "select name, id from " + TABLE + " intersect select name, id from " + TABLE,
                    "select name, id from " + TABLE + " except select name, id from " + TABLE)) {
                assertDenied(sql);
            }
        } finally {
            connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        }
    }

    private void assertAllowed(String sql) {
        PlanChecker.from(connectContext).parse(sql).analyze().rewrite();
    }

    private void assertDenied(String sql) {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).parse(sql).analyze().rewrite());
        Assertions.assertTrue(exception.getMessage().contains("Permission denied"), exception.getMessage());
    }
}
