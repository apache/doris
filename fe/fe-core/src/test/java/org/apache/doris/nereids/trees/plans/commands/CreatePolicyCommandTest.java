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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Covers apache/doris#62729: CREATE ROW POLICY with a correlated subquery in the USING
 * clause used to be silently accepted and silently discarded instead of erroring out.
 */
public class CreatePolicyCommandTest extends TestWithFeService {

    private static final String DB_NAME = "create_policy_command_test";

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabaseAndUse(DB_NAME);
        createTable("create table main_table (id int, ref_id int, owner varchar(100)) "
                + "distributed by hash(id) buckets 1 properties('replication_num' = '1')");
        createTable("create table lookup_table (ref_id int, allowed_user varchar(100)) "
                + "distributed by hash(ref_id) buckets 1 properties('replication_num' = '1')");
        addUser("jack", true);

        AccessControllerManager spyAcm = Mockito.spy(Env.getCurrentEnv().getAccessManager());
        Mockito.doReturn(true).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.GRANT));
        Deencapsulation.setField(Env.getCurrentEnv(), "accessManager", spyAcm);
    }

    private CreatePolicyCommand parse(String sql) {
        return (CreatePolicyCommand) new NereidsParser().parseSingle(sql);
    }

    @Test
    public void testSimplePredicateAllowed() throws Exception {
        CreatePolicyCommand command = parse(
                "create row policy p_simple on main_table as restrictive to jack using (owner = 'alice')");
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
    }

    @Test
    public void testUncorrelatedExistsAllowed() throws Exception {
        CreatePolicyCommand command = parse(
                "create row policy p_uncorrelated on main_table as restrictive to jack using "
                        + "(exists (select 1 from lookup_table where allowed_user = 'alice'))");
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
    }

    @Test
    public void testCorrelatedExistsRejected() {
        CreatePolicyCommand command = parse(
                "create row policy p_correlated on main_table as restrictive to jack using "
                        + "(exists (select 1 from lookup_table l where l.allowed_user = 'alice' "
                        + "and l.ref_id = main_table.ref_id))");
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext));
    }
}
