// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.qe;

import com.baidu.palo.analysis.AccessTestUtil;
import com.baidu.palo.analysis.Analyzer;
import com.baidu.palo.analysis.IntLiteral;
import com.baidu.palo.analysis.SetNamesVar;
import com.baidu.palo.analysis.SetPassVar;
import com.baidu.palo.analysis.SetStmt;
import com.baidu.palo.analysis.SetVar;
import com.baidu.palo.analysis.UserIdentity;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.common.UserException;
import com.baidu.palo.mysql.privilege.PaloAuth;
import com.baidu.palo.mysql.privilege.PrivPredicate;

import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

import mockit.Mocked;
import mockit.NonStrictExpectations;
import mockit.internal.startup.Startup;

public class SetExecutorTest {
    private Analyzer analyzer;
    private ConnectContext ctx;

    @Mocked
    private PaloAuth auth;

    static {
        Startup.initializeIfPossible();
    }

    @Before
    public void setUp() throws DdlException {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
        ctx = new ConnectContext(null);
        ctx.setCatalog(AccessTestUtil.fetchAdminCatalog());
        ctx.setQualifiedUser("root");
        ctx.setRemoteIP("192.168.1.1");

        new NonStrictExpectations() {
            {
                auth.checkGlobalPriv((ConnectContext) any, (PrivPredicate) any);
                result = true;

                auth.checkDbPriv((ConnectContext) any, anyString, (PrivPredicate) any);
                result = true;

                auth.checkTblPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                result = true;

                auth.setPassword((SetPassVar) any);
                minTimes = 0;
            }
        };
    }

    @Test
    public void testNormal() throws UserException, AnalysisException, DdlException {
        List<SetVar> vars = Lists.newArrayList();
        vars.add(new SetPassVar(new UserIdentity("testUser", "%"), "*88EEBA7D913688E7278E2AD071FDB5E76D76D34B"));
        vars.add(new SetNamesVar("utf8"));
        vars.add(new SetVar("query_timeout", new IntLiteral(10L)));

        SetStmt stmt = new SetStmt(vars);
        stmt.analyze(analyzer);
        SetExecutor executor = new SetExecutor(ctx, stmt);

        executor.execute();
    }
}