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

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.InternalException;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("org.apache.log4j.*")
@PrepareForTest(Catalog.class)
public class ShowDataStmtTest {

    private Analyzer analyzer;
    private Catalog catalog;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        catalog = AccessTestUtil.fetchAdminCatalog();
        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        PowerMock.replay(Catalog.class);
    }

    @Test
    public void testNormal() throws AnalysisException, InternalException {
        ShowDataStmt stmt = new ShowDataStmt(null, null);
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW DATA FROM `testCluster:testDb`", stmt.toString());
        Assert.assertEquals(2, stmt.getMetaData().getColumnCount());
        Assert.assertEquals(false, stmt.hasTable());
        
        stmt = new ShowDataStmt("testDb", "testTbl");
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW DATA FROM `testCluster:testDb`.`testTbl`", stmt.toString());
        Assert.assertEquals(3, stmt.getMetaData().getColumnCount());
        Assert.assertEquals(true, stmt.hasTable());
    }
}
