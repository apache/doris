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

package com.baidu.palo.analysis;

import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.InternalException;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class LoadStmtTest {
    private DataDescription desc;
    private List<DataDescription> dataDescriptions;
    private Analyzer analyzer;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        dataDescriptions = Lists.newArrayList();
        desc = EasyMock.createMock(DataDescription.class);
        EasyMock.expect(desc.toSql()).andReturn("XXX");
        dataDescriptions.add(desc);
    }

    @Test
    public void testNormal() throws InternalException, AnalysisException {
        desc.analyze();
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(desc);

        LoadStmt stmt = new LoadStmt(new LabelName("testDb", "testLabel"), dataDescriptions, null, null);
        stmt.analyze(analyzer);
        Assert.assertEquals("testCluster:testDb", stmt.getLabel().getDbName());
        Assert.assertEquals(dataDescriptions, stmt.getDataDescriptions());
        Assert.assertNull(stmt.getProperties());

        Assert.assertEquals("LOAD LABEL `testCluster:testDb`.`testLabel`\n"
                + "(XXX)", stmt.toString());
    }

    @Test(expected = AnalysisException.class)
    public void testNoPriv() throws InternalException, AnalysisException {
        desc.analyze();
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(desc);

        LoadStmt stmt = new LoadStmt(new LabelName("testDb", "testLabel"), dataDescriptions, null, null);
        stmt.analyze(AccessTestUtil.fetchBlockAnalyzer());

        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testNoData() throws InternalException, AnalysisException {
        desc.analyze();
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(desc);

        LoadStmt stmt = new LoadStmt(new LabelName("testDb", "testLabel"), null, null, null);
        stmt.analyze(analyzer);

        Assert.fail("No exception throws.");
    }

}