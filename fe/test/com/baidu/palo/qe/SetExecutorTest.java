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
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.common.InternalException;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class SetExecutorTest {
    private Analyzer analyzer;
    private ConnectContext ctx;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
        ctx = new ConnectContext(null);
        ctx.setCatalog(AccessTestUtil.fetchAdminCatalog());
    }

    @Test
    public void testNormal() throws InternalException, AnalysisException, DdlException {
        List<SetVar> vars = Lists.newArrayList();
        vars.add(new SetPassVar("testUser", "*88EEBA7D913688E7278E2AD071FDB5E76D76D34B"));
        vars.add(new SetNamesVar("utf8"));
        vars.add(new SetVar("query_timeout", new IntLiteral(10L)));

        SetStmt stmt = new SetStmt(vars);
        stmt.analyze(analyzer);
        SetExecutor executor = new SetExecutor(ctx, stmt);

        executor.execute();
    }

    @Test(expected = DdlException.class)
    public void testNoPriv() throws InternalException, AnalysisException, DdlException {
        List<SetVar> vars = Lists.newArrayList();
        vars.add(new SetPassVar("root", "*88EEBA7D913688E7278E2AD071FDB5E76D76D34B"));

        SetStmt stmt = new SetStmt(vars);
        stmt.analyze(analyzer);
        SetExecutor executor = new SetExecutor(ctx, stmt);

        executor.execute();
        Assert.fail("No exception throws");
    }

    @Test
    public void testEmpty() {
    }
}