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
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;

public class LoadStmtTest {
    private DataDescription desc;
    private List<DataDescription> dataDescriptions;
    private Analyzer analyzer;

    @Mocked
    private PaloAuth auth;
    @Mocked
    private ConnectContext ctx;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        dataDescriptions = Lists.newArrayList();
        desc = EasyMock.createMock(DataDescription.class);
        EasyMock.expect(desc.toSql()).andReturn("XXX");
        dataDescriptions.add(desc);

        new Expectations() {
            {
                ConnectContext.get();
                minTimes = 0;
                result = ctx;

                ctx.getQualifiedUser();
                minTimes = 0;
                result = "default_cluster:user";
            }
        };
    }

    @Test
    public void testNormal(@Injectable DataDescription desc) throws UserException, AnalysisException {
        List<DataDescription> dataDescriptionList = Lists.newArrayList();
        dataDescriptionList.add(desc);

        new Expectations(){
            {
                desc.toSql();
                minTimes = 0;
                result = "XXX";
            }
        };

        LoadStmt stmt = new LoadStmt(new LabelName("testDb", "testLabel"), dataDescriptionList, null, null, null);
        stmt.analyze(analyzer);
        Assert.assertEquals("testCluster:testDb", stmt.getLabel().getDbName());
        Assert.assertEquals(dataDescriptionList, stmt.getDataDescriptions());
        Assert.assertNull(stmt.getProperties());

        Assert.assertEquals("LOAD LABEL `testCluster:testDb`.`testLabel`\n"
                + "(XXX)", stmt.toString());
    }

    @Test(expected = AnalysisException.class)
    public void testNoData() throws UserException, AnalysisException {
        desc.analyze(EasyMock.anyString());
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(desc);

        LoadStmt stmt = new LoadStmt(new LabelName("testDb", "testLabel"), null, null, null, null);
        stmt.analyze(analyzer);

        Assert.fail("No exception throws.");
    }

}