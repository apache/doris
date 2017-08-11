// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

package com.baidu.palo.persist;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.baidu.palo.catalog.AccessPrivilege;
import com.baidu.palo.catalog.UserProperty;
import com.baidu.palo.catalog.UserPropertyMgr;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.common.FeConstants;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("org.apache.log4j.*")
@PrepareForTest(Catalog.class)
public class AccessTest {
    private static File file = new File("./AccessTest");
    private static UserProperty userProperty1 = new UserProperty();
    private static UserProperty userProperty2 = new UserProperty();
    private static byte[] passwd = new byte[0];
    private static EditLog edits;
    private static Catalog catalog;

    static UserProperty invokeGetAccessResourceFunction(UserPropertyMgr userPropertyMgr, String userName)
            throws IllegalAccessException,
            IllegalArgumentException, InvocationTargetException, NoSuchFieldException, 
            SecurityException, NoSuchMethodException {
        Method method = userPropertyMgr.getClass().getDeclaredMethod("getAccessResource", String.class);
        method.setAccessible(true);
        Object object = method.invoke(userPropertyMgr, userName);
        return (UserProperty) object;
    }
    
    @BeforeClass
    public static void setUpClass() throws IOException {
        file.createNewFile();
        passwd = "passwordIsLong".getBytes();
        // ordinary user
        userProperty1.setUser("userName");
        userProperty1.setAccess("db1", AccessPrivilege.READ_ONLY);
        userProperty1.setAccess("db2", AccessPrivilege.READ_WRITE);
        userProperty1.setPassword(passwd);
        userProperty1.setIsAdmin(false);
        // adminstrator user
        userProperty2.setUser("root");
        userProperty2.setPassword(new byte[0]);
        userProperty2.setIsAdmin(true);
        
        edits = EasyMock.createMock(EditLog.class);
        edits.logAlterAccess(EasyMock.isA(UserProperty.class));
        EasyMock.expectLastCall().anyTimes();
        edits.logDropUser(EasyMock.isA(String.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(edits);

    }

    @Before
    public void setUp() {
        catalog = EasyMock.createMock(Catalog.class);

        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        EasyMock.expect(Catalog.getCurrentCatalogJournalVersion()).andReturn(FeConstants.meta_version).anyTimes();
        PowerMock.replay(Catalog.class);
    }
    
    @AfterClass
    public static void tearDownClass() {
        file.delete();
    }
    
    @Test
    public void testAccessResource() throws Exception {
        // write ordinary user information to snapshot
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        userProperty1.write(dos);
        dos.flush();
        dos.close();
        
        // read snapshot
        UserProperty result = new UserProperty();
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        result.readFields(dis);
        dis.close();
        
        // check result
        Assert.assertEquals(result.getUser(), "userName");
        Assert.assertTrue(Arrays.equals(result.getPassword(), passwd));
        Assert.assertEquals(result.isAdmin(), false);
        Assert.assertEquals(result.getMaxConn(), 100);
        Assert.assertTrue(result.checkAccess("db1", AccessPrivilege.READ_ONLY));
        Assert.assertFalse(result.checkAccess("db1", AccessPrivilege.READ_WRITE));
        Assert.assertTrue(result.checkAccess("db2", AccessPrivilege.READ_ONLY));
        Assert.assertTrue(result.checkAccess("db2", AccessPrivilege.READ_WRITE));
        Assert.assertFalse(result.checkAccess("no_exists_db", AccessPrivilege.READ_ONLY));
        Assert.assertFalse(result.checkAccess("no_exists_db", AccessPrivilege.READ_WRITE));
    }
    
    @Test
    public void testAccessService() throws Exception {
        file.delete();
        file.createNewFile();
        UserPropertyMgr result = new UserPropertyMgr();
        result.unprotectAlterAccess(userProperty1);
        result.unprotectAlterAccess(userProperty2);
        
        // write snapshot
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        result.write(dos);
        dos.flush();
        dos.close();        
        // read snapshot
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        result.readFields(dis);
        dis.close();
        // check root resource
        UserProperty rootResource = invokeGetAccessResourceFunction(result, "root");
        Assert.assertEquals(rootResource.getUser(), "root");
        Assert.assertEquals(rootResource.isAdmin(), true);
        Assert.assertEquals(rootResource.getMaxConn(), 100);
        Assert.assertTrue(Arrays.equals(rootResource.getPassword(), new byte[0]));
        Assert.assertTrue(rootResource.checkAccess("db1", AccessPrivilege.READ_WRITE));
        Assert.assertTrue(rootResource.checkAccess("db2", AccessPrivilege.READ_WRITE));
        
        UserProperty ordinaryResource = invokeGetAccessResourceFunction(result, "userName");
        Assert.assertEquals(ordinaryResource.getUser(), "userName");
        Assert.assertTrue(Arrays.equals(ordinaryResource.getPassword(), passwd));
        Assert.assertEquals(ordinaryResource.isAdmin(), false);
        Assert.assertEquals(ordinaryResource.getMaxConn(), 100);
        Assert.assertTrue(ordinaryResource.checkAccess("db1", AccessPrivilege.READ_ONLY));
        Assert.assertFalse(ordinaryResource.checkAccess("db1", AccessPrivilege.READ_WRITE));
        Assert.assertTrue(ordinaryResource.checkAccess("db2", AccessPrivilege.READ_ONLY));
        Assert.assertTrue(ordinaryResource.checkAccess("db2", AccessPrivilege.READ_WRITE));
    }
    
    // add user
    @Test(expected = DdlException.class)
    public void testAddUserExceptionUserIsEmpty() throws Exception {
        UserPropertyMgr service = new UserPropertyMgr();
        service.addUser("", "", new byte[0], false);
        Assert.fail("No Exception throws.");
    }
    
    @Test(expected = DdlException.class)
    public void testAddUserExceptionUserIsNull() throws Exception {
        UserPropertyMgr service = new UserPropertyMgr();
        service.addUser(null, null, new byte[0], false);
        Assert.fail("No Exception throws.");
    }
    
    @Test
    public void testAddUserSuccess() throws Exception {
        UserPropertyMgr service = new UserPropertyMgr();
        service.setEditLog(edits);
        service.addUser("cluster", "user", new byte[0], false);
        Assert.assertNotNull(invokeGetAccessResourceFunction(service, "user"));
            
        Assert.assertTrue(Arrays.equals(invokeGetAccessResourceFunction(service, "user")
                .getPassword(), new byte[0]));
    }  
    
    @Test(expected = DdlException.class)
    public void testAddUserTwoTimes() throws Exception { 
        UserPropertyMgr service = new UserPropertyMgr();
        service.setEditLog(edits);
        service.addUser("cluster", "user", "pi".getBytes(), false);
        service.addUser("cluster", "user", "pi".getBytes(), false);
        Assert.fail("No Exception throws.");
    }
    
    // set Passwd 
    @Test(expected = DdlException.class)
    public void testSetPasswdExceptionUserIsEmpty() throws Exception {
        UserPropertyMgr service = new UserPropertyMgr();
        service.setPasswd("user", new byte[0]);
        Assert.fail("No Exception throws.");
    }
    
    @Test
    public void testSetPasswdSuccess() throws Exception {
        UserPropertyMgr service = new UserPropertyMgr();
        service.setEditLog(edits);
        service.addUser("cluster", "user", new byte[0], false);
        Assert.assertTrue(Arrays.equals(service.getPassword("user"), new byte[0]));
        
        byte[] newPasswd = "*B6BDA741F59FE8066344FE3E118291C5D7DD12AD".getBytes();
        service.setPasswd("user", newPasswd);
        Assert.assertTrue(Arrays.equals(service.getPassword("user"), newPasswd));
    }
    
    @Test(expected = DdlException.class)
    public void testGrandExceptionUserIsEmpty() throws Exception {
        UserPropertyMgr service = new UserPropertyMgr();
        service.setEditLog(edits);
        service.grant("user", "db", AccessPrivilege.READ_ONLY);
        Assert.fail("No Exception throws.");
    }
    
    // grant
    @Ignore("Not Ready to Run")
    @Test(expected = DdlException.class)
    public void testGrandExceptionDbIsEmpty() throws Exception {
        UserPropertyMgr service = new UserPropertyMgr();
        service.setEditLog(edits);
        service.addUser("cluster", "user", new byte[0], false);
        service.grant("user", "db_not_exists", AccessPrivilege.READ_ONLY);
        Assert.fail("No Exception throws.");
    }
   
    @Test
    public void testGrandSuccess() throws Exception {
        catalog = EasyMock.createMock(Catalog.class);
        EasyMock.expect(catalog.getDb("db_exists")).andReturn(new Database()).anyTimes();   
        EasyMock.replay(catalog);
        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        PowerMock.replay(Catalog.class);
        
        UserPropertyMgr service = new UserPropertyMgr();
        service.setEditLog(edits);
        service.addUser("cluster", "user", new byte[0], false);
        service.grant("user", "db_exists", AccessPrivilege.READ_ONLY);
        Assert.assertFalse(service.checkAccess("user", "db_not_exists", AccessPrivilege.READ_ONLY));
        Assert.assertTrue(service.checkAccess("user", "db_exists", AccessPrivilege.READ_ONLY));
    }
    
    // drop User
    @Test(expected = DdlException.class)
    public void testDropUserExceptionUserNotExist() throws Exception {
        UserPropertyMgr service = new UserPropertyMgr();
        service.setEditLog(edits);
        service.dropUser("user");
        Assert.fail("No Exception throws.");
    }
    
    @Test
    public void testDropUserSuccess() throws Exception {
        UserPropertyMgr service = new UserPropertyMgr();
        service.setEditLog(edits);
        service.addUser("cluster", "user", new byte[0], false);
        Assert.assertNotNull(invokeGetAccessResourceFunction(service, "user"));
        service.dropUser("user");
        Assert.assertNull(invokeGetAccessResourceFunction(service, "user"));
    }
}
