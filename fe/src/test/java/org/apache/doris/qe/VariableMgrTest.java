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

import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.SysVariableDesc;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.persist.EditLog;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "org.apache.log4j.*", "javax.management.*" })
@PrepareForTest({ Catalog.class })
public class VariableMgrTest {
    private static final Logger LOG = LoggerFactory.getLogger(VariableMgrTest.class);
    private Catalog catalog;

    @Before
    public void setUp() {
        catalog = EasyMock.createNiceMock(Catalog.class);
        // mock editLog
        EditLog editLog = EasyMock.createMock(EditLog.class);
        EasyMock.expect(catalog.getEditLog()).andReturn(editLog).anyTimes();
        editLog.logGlobalVariable(EasyMock.anyObject(SessionVariable.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(editLog);
        // mock static getInstance
        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        PowerMock.replay(Catalog.class);

        EasyMock.replay(catalog);
    }

    @Test
    public void testNormal() throws IllegalAccessException, DdlException, NoSuchFieldException, AnalysisException {
        SessionVariable var = VariableMgr.newSessionVariable();
        Assert.assertEquals(2147483648L, var.getMaxExecMemByte());
        Assert.assertEquals(300, var.getQueryTimeoutS());
        Assert.assertEquals(false, var.isReportSucc());

        List<List<String>> rows = VariableMgr.dump(SetType.SESSION, var, null);
        Assert.assertTrue(rows.size() > 5);
        for (List<String> row : rows) {
            if (row.get(0).equalsIgnoreCase("exec_mem_limit")) {
                Assert.assertEquals("2147483648", row.get(1));
            } else if (row.get(0).equalsIgnoreCase("is_report_success")) {
                Assert.assertEquals("false", row.get(1));
            } else if (row.get(0).equalsIgnoreCase("query_timeout")) {
                Assert.assertEquals("300", row.get(1));
            } else if (row.get(0).equalsIgnoreCase("sql_mode")) {
                Assert.assertEquals("", row.get(1));
            }
        }

        // Set global variable
        SetVar setVar = new SetVar(SetType.GLOBAL, "exec_mem_limit", new IntLiteral(1234L));
        VariableMgr.setVar(var, setVar);
        Assert.assertEquals(2147483648L, var.getMaxExecMemByte());
        var = VariableMgr.newSessionVariable();
        Assert.assertEquals(1234L, var.getMaxExecMemByte());

        SetVar setVar2 = new SetVar(SetType.GLOBAL, "parallel_fragment_exec_instance_num", new IntLiteral(5L));
        VariableMgr.setVar(var, setVar2);
        Assert.assertEquals(1L, var.getParallelExecInstanceNum());
        var = VariableMgr.newSessionVariable();
        Assert.assertEquals(5L, var.getParallelExecInstanceNum());

        SetVar setVar3 = new SetVar(SetType.GLOBAL, "time_zone", new StringLiteral("Asia/Shanghai"));
        VariableMgr.setVar(var, setVar3);
        Assert.assertEquals("CST", var.getTimeZone());
        var = VariableMgr.newSessionVariable();
        Assert.assertEquals("Asia/Shanghai", var.getTimeZone());

        // Set session variable
        setVar = new SetVar(SetType.GLOBAL, "exec_mem_limit", new IntLiteral(1234L));
        VariableMgr.setVar(var, setVar);
        Assert.assertEquals(1234L, var.getMaxExecMemByte());

        setVar3 = new SetVar(SetType.SESSION, "time_zone", new StringLiteral("Asia/Jakarta"));
        VariableMgr.setVar(var, setVar3);
        Assert.assertEquals("Asia/Jakarta", var.getTimeZone());

        // Get from name
        SysVariableDesc desc = new SysVariableDesc("exec_mem_limit");
        Assert.assertEquals(var.getMaxExecMemByte() + "", VariableMgr.getValue(var, desc));

        // Test checkTimeZoneValidAndStandardize
        SetVar setVar5 = new SetVar(SetType.GLOBAL, "time_zone", new StringLiteral("+8:00"));
        VariableMgr.setVar(var, setVar5);
        Assert.assertEquals("+08:00", VariableMgr.newSessionVariable().getTimeZone());

        SetVar setVar6 = new SetVar(SetType.GLOBAL, "time_zone", new StringLiteral("8:00"));
        VariableMgr.setVar(var, setVar6);
        Assert.assertEquals("+08:00", VariableMgr.newSessionVariable().getTimeZone());

        SetVar setVar7 = new SetVar(SetType.GLOBAL, "time_zone", new StringLiteral("-8:00"));
        VariableMgr.setVar(var, setVar7);
        Assert.assertEquals("-08:00", VariableMgr.newSessionVariable().getTimeZone());
    }

    @Test(expected = DdlException.class)
    public void testInvalidType() throws DdlException {
        // Set global variable
        SetVar setVar = new SetVar(SetType.GLOBAL, "exec_mem_limit", new StringLiteral("abc"));
        SessionVariable var = VariableMgr.newSessionVariable();
        try {
            VariableMgr.setVar(var, setVar);
        } catch (DdlException e) {
            LOG.warn("VariableMgr throws", e);
            throw e;
        }
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testInvalidTimeZoneRegion() throws DdlException {
        // Set global variable
        SetVar setVar = new SetVar(SetType.GLOBAL, "time_zone", new StringLiteral("Hongkong"));
        SessionVariable var = VariableMgr.newSessionVariable();
        try {
            VariableMgr.setVar(var, setVar);
        } catch (DdlException e) {
            LOG.warn("VariableMgr throws", e);
            throw e;
        }
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testInvalidTimeZoneOffset() throws DdlException {
        // Set global variable
        SetVar setVar = new SetVar(SetType.GLOBAL, "time_zone", new StringLiteral("+15:00"));
        SessionVariable var = VariableMgr.newSessionVariable();
        try {
            VariableMgr.setVar(var, setVar);
        } catch (DdlException e) {
            LOG.warn("VariableMgr throws", e);
            throw e;
        }
        Assert.fail("No exception throws.");
    }


    @Test(expected = DdlException.class)
    public void testReadOnly() throws AnalysisException, DdlException {
        SysVariableDesc desc = new SysVariableDesc("version_comment");
        LOG.info(VariableMgr.getValue(null, desc));

        // Set global variable
        SetVar setVar = new SetVar(SetType.GLOBAL, "version_comment", null);
        VariableMgr.setVar(null, setVar);

        Assert.fail("No exception throws.");
    }
}
