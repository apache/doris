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

package com.baidu.palo.common.proc;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.easymock.EasyMock;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.baidu.palo.catalog.AccessPrivilege;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.UserProperty;
import com.baidu.palo.catalog.UserPropertyMgr;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.persist.EditLog;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("org.apache.log4j.*")
@PrepareForTest(Catalog.class)
public class UserPropertyProcTest {
    private static UserPropertyMgr service;
    private static EditLog edits;

    private static Catalog catalog;
    private static Database db = new Database(10000, "testDb");

    @BeforeClass
    public static void setUp() throws DdlException, IOException {
        catalog = EasyMock.createMock(Catalog.class);

        EasyMock.expect(catalog.getDb(EasyMock.isA(String.class))).andReturn(db).anyTimes();
        EasyMock.replay(catalog);

        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        PowerMock.replay(Catalog.class);

        edits = EasyMock.createMock(EditLog.class);
        edits.logAlterAccess(EasyMock.isA(UserProperty.class));
        EasyMock.expectLastCall().anyTimes();
        edits.logDropUser(EasyMock.isA(String.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(edits);

        service = new UserPropertyMgr();
        service.setEditLog(edits);
        service.addUser("cluster", "cluster:userA", "passwdA".getBytes(), false);
        service.addUser("cluster", "cluster:userB", "passwdB".getBytes(), false);
        service.grant("cluster:userA", "cluster:DBA", AccessPrivilege.READ_ONLY);
        service.grant("cluster:userA", "cluster:DBB", AccessPrivilege.READ_WRITE);
        service.grant("cluster:userB", "cluster:DBB", AccessPrivilege.READ_ONLY);
    }

    @Test
    public void testAccessResourceProcNodeFetchResult() throws AnalysisException {
        AccessResourceProcDir node = new AccessResourceProcDir(service);
        ProcResult result = node.fetchResult();
        // the result
        // [UserName, Password, IsAdmin, MaxConn, Privilege]
        // userA passwdA false 100 DBA(READ_ONLY), DBB(READ_WRITE)
        // userB passwdB false 100 DBB(READ_ONLY)
        // root true 100

        // check result
        List<String> actual = Arrays.asList("UserName", "Password", "IsAdmin", 
            "IsSuperuser", "MaxConn", "Privilege");
        Assert.assertEquals(result.getColumnNames().toString(), actual.toString());
        Assert.assertEquals(4, result.getRows().size());
        String resultA = Arrays.asList("cluster:userA", "passwdA", "false", "false", "100",
                "cluster:information_schema(READ_ONLY)", "cluster:DBB(READ_WRITE), cluster:DBA(READ_ONLY)").toString();
        String resultB = Arrays.asList("cluster:userB", "passwdB", "false", "false", "100",
                "cluster:information_schema(READ_ONLY)", "cluster:DBB(READ_ONLY)").toString();

        String resultC = Arrays.asList("root", "", "true", "true", "100", "").toString();
        String row0 = result.getRows().get(0).toString();
        String row1 = result.getRows().get(1).toString();
        String row2 = result.getRows().get(2).toString();

        System.out.println("row0 : " + row0);
        System.out.println("row1 : " + row1);
        System.out.println("row2 : " + row2);
        Assert.assertTrue(compareString(row0, resultA, resultB, resultC));
        Assert.assertTrue(compareString(row1, resultA, resultB, resultC));
        Assert.assertTrue(compareString(row2, resultA, resultB, resultC));
        Assert.assertFalse(row0.equals(row1));
        Assert.assertFalse(row0.equals(row2));
        Assert.assertFalse(row1.equals(row2));
    }

    boolean compareString(String src, String rst1, String rst2, String rst3) {
        if (src.equals(rst1) || src.equals(rst2) || src.equals(rst3)) {
            return true;
        }
        return false;
    }
}
