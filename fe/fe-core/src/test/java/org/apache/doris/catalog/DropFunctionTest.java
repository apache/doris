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

package org.apache.doris.catalog;

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateFunctionStmt;
import org.apache.doris.analysis.DropFunctionStmt;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.DorisAssert;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.UUID;


public class DropFunctionTest {

    private static String runningDir = "fe/mocked/DropFunctionTest/" + UUID.randomUUID().toString() + "/";
    private static ConnectContext connectContext;
    private static DorisAssert dorisAssert;

    @BeforeClass
    public static void setup() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);
        FeConstants.runningUnitTest = true;
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @AfterClass
    public static void teardown() {
        File file = new File("fe/mocked/DropFunctionTest/");
        file.delete();
    }

    @Test
    public void testDropGlobalFunction() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        // 1. create database db1
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt("create database db1;", ctx);
        Env.getCurrentEnv().createDb(createDbStmt);

        String createFuncStr
                = "create global alias function id_masking(bigint) with parameter(id) as concat(left(id,3),'****',right(id,4));";
        CreateFunctionStmt createFunctionStmt = (CreateFunctionStmt) UtFrameUtils.parseAndAnalyzeStmt(createFuncStr,
                ctx);
        Env.getCurrentEnv().createFunction(createFunctionStmt);

        List<Function> functions = Env.getCurrentEnv().getGlobalFunctionMgr().getFunctions();
        Assert.assertEquals(1, functions.size());
        // drop global function
        String dropFuncStr = "drop global function id_masking(bigint)";

        DropFunctionStmt dropFunctionStmt = (DropFunctionStmt) UtFrameUtils.parseAndAnalyzeStmt(dropFuncStr, ctx);

        Env.getCurrentEnv().dropFunction(dropFunctionStmt);

        functions = Env.getCurrentEnv().getGlobalFunctionMgr().getFunctions();
        Assert.assertEquals(0, functions.size());
    }
}
