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

package org.apache.doris.qe;

import org.apache.doris.analysis.AccessTestUtil;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.DdlException;

import com.google.common.collect.Lists;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "org.apache.log4j.*", "javax.management.*" })
@PrepareForTest(Catalog.class)
public class MultiLoadMgrTest {
    private Catalog catalog;

    @Before
    public void setUp() {
        catalog = AccessTestUtil.fetchAdminCatalog();

        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        PowerMock.replay(Catalog.class);
    }

    @Test
    public void testStartNormal() throws DdlException {
        MultiLoadMgr mgr = new MultiLoadMgr();
        mgr.startMulti("testDb", "1", null);
        mgr.startMulti("testDb", "2", null);
        mgr.startMulti("testDb", "3", null);
        mgr.startMulti("testDb", "4", null);

        List<String> labels = Lists.newArrayList();
        mgr.list("testDb", labels);
        Assert.assertEquals(4, labels.size());

        mgr.abort("testDb", "1");
        labels.clear();
        mgr.list("testDb", labels);
        Assert.assertEquals(3, labels.size());
    }

    @Test(expected = DdlException.class)
    public void testStartDup() throws DdlException {
        MultiLoadMgr mgr = new MultiLoadMgr();
        mgr.startMulti("testDb", "1", null);
        mgr.startMulti("testDb", "1", null);
    }
}
