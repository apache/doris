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

import org.apache.doris.backup.CatalogMocker;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.DdlException;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;


public class MultiLoadMgrTest {
    @Mocked
    private Catalog catalog;
    @Mocked
    private ConnectContext context;
    @Mocked
    private SystemInfoService systemInfoService;
    @Before
    public void setUp() {
        new Expectations() {
            {
                ConnectContext.get();
                minTimes = 0;
                result = context;
            }
        };
        new Expectations(context) {
            {
                context.getClusterName();
                minTimes = 0;
                result = "default";
            }
        };
        new Expectations() {
            {
                systemInfoService.seqChooseBackendIdsByStorageMediumAndTag(anyInt, anyBoolean, anyBoolean, anyString,
                        (TStorageMedium) any, (Tag) any);
                minTimes = 0;
                result = new Delegate() {
                    public synchronized List<Long> seqChooseBackendIdsByStorageMediumAndTag(int backendNum, boolean needAlive,
                                                                                            boolean isCreate, String clusterName, TStorageMedium medium,
                                                                                            Tag tag) {
                        List<Long> beIds = Lists.newArrayList();
                        beIds.add(CatalogMocker.BACKEND1_ID);
                        beIds.add(CatalogMocker.BACKEND2_ID);
                        beIds.add(CatalogMocker.BACKEND3_ID);
                        return beIds;
                    }
                };
            }
        };
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
