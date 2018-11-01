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

package org.apache.doris.common.proc;

import org.apache.doris.common.AnalysisException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ProcServiceTest {
    private class EmptyProcNode implements ProcNodeInterface {
        @Override
        public ProcResult fetchResult() {
            return null;
        }
    }

    // 构造用于测试的文件结构，具体的结构如下
    // - palo
    // | - be
    //   | - src
    //   | - deps
    // | - fe
    //   | - src
    //   | - conf
    //   | - build.sh
    // | - common
    @Before
    public void beforeTest() {
        ProcService procService = ProcService.getInstance();

        BaseProcDir paloDir = new BaseProcDir();
        Assert.assertTrue(procService.register("palo", paloDir));

        BaseProcDir beDir = new BaseProcDir();
        Assert.assertTrue(paloDir.register("be", beDir));
        Assert.assertTrue(beDir.register("src", new BaseProcDir()));
        Assert.assertTrue(beDir.register("deps", new BaseProcDir()));

        BaseProcDir feDir = new BaseProcDir();
        Assert.assertTrue(paloDir.register("fe", feDir));
        Assert.assertTrue(feDir.register("src", new BaseProcDir()));
        Assert.assertTrue(feDir.register("conf", new BaseProcDir()));
        Assert.assertTrue(feDir.register("build.sh", new EmptyProcNode()));

        Assert.assertTrue(paloDir.register("common", new BaseProcDir()));
    }

    @After
    public void afterTest() {
        ProcService.destroy();
    }

    @Test
    public void testRegisterNormal() {
        ProcService procService = ProcService.getInstance();
        String name = "test";
        BaseProcDir dir = new BaseProcDir();

        Assert.assertTrue(procService.register(name, dir));
    }

    // register second time
    @Test
    public void testRegisterSecond() {
        ProcService procService = ProcService.getInstance();
        String name = "test";
        BaseProcDir dir = new BaseProcDir();

        Assert.assertTrue(procService.register(name, dir));
        Assert.assertFalse(procService.register(name, dir));
    }

    // register invalid
    @Test
    public void testRegisterInvalidInput() {
        ProcService procService = ProcService.getInstance();
        String name = "test";
        BaseProcDir dir = new BaseProcDir();

        Assert.assertFalse(procService.register(null, dir));
        Assert.assertFalse(procService.register("", dir));
        Assert.assertFalse(procService.register(name, null));
    }

    @Test
    public void testOpenNormal() throws AnalysisException {
        ProcService procService = ProcService.getInstance();

        // assert root
        Assert.assertNotNull(procService.open("/"));
        Assert.assertNotNull(procService.open("/palo"));
        Assert.assertNotNull(procService.open("/palo/be"));
        Assert.assertNotNull(procService.open("/palo/be/src"));
        Assert.assertNotNull(procService.open("/palo/be/deps"));
        Assert.assertNotNull(procService.open("/palo/fe"));
        Assert.assertNotNull(procService.open("/palo/fe/src"));
        Assert.assertNotNull(procService.open("/palo/fe/conf"));
        Assert.assertNotNull(procService.open("/palo/fe/build.sh"));
        Assert.assertNotNull(procService.open("/palo/common"));
    }

    @Test
    public void testOpenSapceNormal() throws AnalysisException {
        ProcService procService = ProcService.getInstance();

        // assert space
        Assert.assertNotNull(procService.open(" \r/"));
        Assert.assertNotNull(procService.open(" \r/ "));
        Assert.assertNotNull(procService.open("  /palo \r\n"));
        Assert.assertNotNull(procService.open("\n\r\t /palo/be \n\r"));

        // assert last '/'
        Assert.assertNotNull(procService.open(" /palo/be/"));
        Assert.assertNotNull(procService.open(" /palo/fe/  "));

        ProcNodeInterface node = procService.open("/dbs");
        Assert.assertNotNull(node);
        Assert.assertTrue(node instanceof DbsProcDir);
    }

    @Test
    public void testOpenFail() {
        ProcService procService = ProcService.getInstance();

        // assert no path
        int errCount = 0;
        try {
            procService.open("/abc");
        } catch (AnalysisException e) {
            ++errCount;
        }
        try {
            Assert.assertNull(procService.open("/palo/b e"));
        } catch (AnalysisException e) {
            ++errCount;
        }
        try {
            Assert.assertNull(procService.open("/palo/fe/build.sh/"));
        } catch (AnalysisException e) {
            ++errCount;
        }

        // assert no root
        try {
            Assert.assertNull(procService.open("palo"));
        } catch (AnalysisException e) {
            ++errCount;
        }
        try {
            Assert.assertNull(procService.open(" palo"));
        } catch (AnalysisException e) {
            ++errCount;
        }

        Assert.assertEquals(5, errCount);
    }

}