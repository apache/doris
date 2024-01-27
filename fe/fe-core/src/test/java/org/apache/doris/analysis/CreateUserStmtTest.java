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
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CreateUserStmtTest {

    @Before
    public void setUp() {
        ConnectContext ctx = new ConnectContext();
        ctx.setQualifiedUser("root");
        ctx.setRemoteIP("192.168.1.1");
        UserIdentity currentUserIdentity = new UserIdentity("root", "192.168.1.1");
        currentUserIdentity.setIsAnalyzed();
        ctx.setCurrentUserIdentity(currentUserIdentity);
        ctx.setThreadLocalInfo();
    }

    @Test
    public void testToString(@Injectable Analyzer analyzer,
            @Mocked AccessControllerManager accessManager) throws UserException {

        new Expectations() {
            {
                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.GRANT);
                result = true;
            }
        };

        CreateUserStmt stmt = new CreateUserStmt(new UserDesc(new UserIdentity("user", "%"), "passwd", true));
        stmt.analyze(analyzer);

        Assert.assertEquals("CREATE USER 'user'@'%' IDENTIFIED BY '*XXX'", stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");

        stmt = new CreateUserStmt(
                new UserDesc(new UserIdentity("user", "%"), "*59c70da2f3e3a5bdf46b68f5c8b8f25762bccef0", false));
        stmt.analyze(analyzer);
        Assert.assertEquals("user", stmt.getUserIdent().getQualifiedUser());

        Assert.assertEquals("CREATE USER 'user'@'%' IDENTIFIED BY PASSWORD '*59c70da2f3e3a5bdf46b68f5c8b8f25762bccef0'",
                stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");

        stmt = new CreateUserStmt(new UserDesc(new UserIdentity("user", "%"), "", false));
        stmt.analyze(analyzer);

        Assert.assertEquals("CREATE USER 'user'@'%'", stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "");
    }

    @Test(expected = AnalysisException.class)
    public void testEmptyUser(@Injectable Analyzer analyzer) throws UserException, AnalysisException {
        CreateUserStmt stmt = new CreateUserStmt(new UserDesc(new UserIdentity("", "%"), "passwd", true));
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testBadPass(@Injectable Analyzer analyzer) throws UserException, AnalysisException {
        CreateUserStmt stmt = new CreateUserStmt(new UserDesc(new UserIdentity("", "%"), "passwd", false));
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }
}
