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
import org.apache.doris.common.Pair;
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

import java.util.Collections;

public class AlterUserStmtTest {

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
    public void testTlsRequireNoneOnly(@Injectable Analyzer analyzer,
            @Mocked AccessControllerManager accessManager) throws UserException, AnalysisException {
        new Expectations() {
            {
                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.GRANT);
                result = true;
            }
        };

        AlterUserStmt stmt = new AlterUserStmt(false, new UserDesc(new UserIdentity("tls_user", "%")),
                null, null, null, TlsOptions.requireNone());
        stmt.analyze(analyzer);

        Assert.assertEquals("ALTER USER 'tls_user'@'%' REQUIRE NONE", stmt.toString());
        Assert.assertEquals(AlterUserStmt.OpType.SET_TLS_REQUIRE, stmt.getOpType());
        UserIdentity userIdent = stmt.getUserIdent();
        Assert.assertFalse(userIdent.hasTlsRequirements());
        Assert.assertNull(userIdent.getSan());
        Assert.assertNull(userIdent.getIssuer());
        Assert.assertNull(userIdent.getSubject());
        Assert.assertNull(userIdent.getCipher());
    }

    @Test
    public void testTlsRequireSanOnly(@Injectable Analyzer analyzer,
            @Mocked AccessControllerManager accessManager) throws UserException, AnalysisException {
        new Expectations() {
            {
                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.GRANT);
                result = true;
            }
        };

        TlsOptions tlsOptions = TlsOptions.of(Collections.singletonList(Pair.of("SAN", "DNS:example.com")));
        AlterUserStmt stmt = new AlterUserStmt(false, new UserDesc(new UserIdentity("tls_user", "%")),
                null, null, null, tlsOptions);
        stmt.analyze(analyzer);

        Assert.assertEquals("ALTER USER 'tls_user'@'%' REQUIRE SAN 'DNS:example.com'", stmt.toString());
        Assert.assertEquals(AlterUserStmt.OpType.SET_TLS_REQUIRE, stmt.getOpType());
        UserIdentity userIdent = stmt.getUserIdent();
        Assert.assertEquals("DNS:example.com", userIdent.getSan());
        Assert.assertNull(userIdent.getIssuer());
        Assert.assertNull(userIdent.getSubject());
        Assert.assertNull(userIdent.getCipher());
    }

    @Test(expected = AnalysisException.class)
    public void testTlsWithPasswordChangeNotAllowed(@Injectable Analyzer analyzer,
            @Mocked AccessControllerManager accessManager) throws UserException, AnalysisException {
        new Expectations() {
            {
                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.GRANT);
                result = true;
                minTimes = 0;
            }
        };

        TlsOptions tlsOptions = TlsOptions.of(Collections.singletonList(Pair.of("SAN", "DNS:example.com")));
        AlterUserStmt stmt = new AlterUserStmt(false,
                new UserDesc(new UserIdentity("tls_user", "%"), "passwd", true),
                null, null, null, tlsOptions);
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testTlsRequireSanEmptyValue(@Injectable Analyzer analyzer) throws UserException, AnalysisException {
        TlsOptions tlsOptions = TlsOptions.of(Collections.singletonList(Pair.of("SAN", "")));
        AlterUserStmt stmt = new AlterUserStmt(false, new UserDesc(new UserIdentity("tls_user", "%")),
                null, null, null, tlsOptions);
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testTlsUnsupportedOption(@Injectable Analyzer analyzer) throws UserException, AnalysisException {
        TlsOptions tlsOptions = TlsOptions.of(Collections.singletonList(Pair.of("ISSUER", "ca")));
        AlterUserStmt stmt = new AlterUserStmt(false, new UserDesc(new UserIdentity("tls_user", "%")),
                null, null, null, tlsOptions);
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testMultipleNonTlsOpsAreRejected(@Injectable Analyzer analyzer) throws UserException, AnalysisException {
        AlterUserStmt stmt = new AlterUserStmt(false,
                new UserDesc(new UserIdentity("tls_user", "%"), "passwd", true),
                null, null, "new comment", TlsOptions.notSpecified());
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testNoOpsAreRejected(@Injectable Analyzer analyzer) throws UserException, AnalysisException {
        AlterUserStmt stmt = new AlterUserStmt(false, new UserDesc(new UserIdentity("tls_user", "%")),
                null, null, null, TlsOptions.notSpecified());
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }
}
