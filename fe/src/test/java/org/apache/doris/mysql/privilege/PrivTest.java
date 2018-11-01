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

package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeMetaVersion;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import mockit.Mock;
import mockit.MockUp;

public class PrivTest {

    private PaloAuth auth;
    private byte[] passwd = new byte[] { 'a', 'c' };

    private Method grantGlobalPrivsM;
    private Method grantDbPrivsM;
    private Method grantTblPrivsM;

    @Before
    public void setUp() {
        auth = new PaloAuth();
       
        Method[] methods = PaloAuth.class.getDeclaredMethods();
        for (Method method : methods) {
            if (method.getName().equals("grantGlobalPrivs")) {
                method.setAccessible(true);
                grantGlobalPrivsM = method;
            } else if (method.getName().equals("grantDbPrivs")) {
                method.setAccessible(true);
                grantDbPrivsM = method;
            } else if (method.getName().equals("grantTblPrivs")) {
                method.setAccessible(true);
                grantTblPrivsM = method;
            }
        }

        new MockUp<Catalog>() {
            @Mock
            public int getCurrentCatalogJournalVersion() {
                return FeMetaVersion.VERSION_43;
            }
        };
    }

    public void grantGlobalPrivs(Object... params)
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        grantGlobalPrivsM.invoke(auth, params);
    }

    public void grantDbPrivs(Object... params)
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        grantDbPrivsM.invoke(auth, params);
    }

    public void grantTblPrivs(Object... params)
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        grantTblPrivsM.invoke(auth, params);
    }


    @Test
    public void testGlobalPriv()
            throws DdlException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        // exact match
        grantGlobalPrivs("192.168.1.1", "cmy", passwd, false, false, false,
                         PrivBitSet.of(PaloPrivilege.GRANT_PRIV));
        Assert.assertFalse(auth.checkGlobalPriv("192.168.1.2", "cmy",
                                                PrivPredicate.of(PrivBitSet.of(PaloPrivilege.GRANT_PRIV),
                                                                 Operator.OR)));
        Assert.assertFalse(auth.checkGlobalPriv("192.168.1.1", "cmy2",
                                                PrivPredicate.of(PrivBitSet.of(PaloPrivilege.GRANT_PRIV),
                                                                 Operator.OR)));
        Assert.assertFalse(auth.checkGlobalPriv("192.168.1.1", "cmy",
                                                PrivPredicate.of(PrivBitSet.of(PaloPrivilege.NODE_PRIV),
                                                                 Operator.OR)));
        Assert.assertTrue(auth.checkGlobalPriv("192.168.1.1", "cmy",
                                               PrivPredicate.of(PrivBitSet.of(PaloPrivilege.GRANT_PRIV),
                                                                Operator.OR)));
        // fuzzy match
        auth.clear();
        grantGlobalPrivs("192.168._.%", "cmy\\_%", passwd, false, false, false,
                         PrivBitSet.of(PaloPrivilege.GRANT_PRIV,
                                                PaloPrivilege.NODE_PRIV));
        Assert.assertFalse(auth.checkGlobalPriv("192.168.1.1", "cmy",
                                                PrivPredicate.of(PrivBitSet.of(PaloPrivilege.GRANT_PRIV),
                                                                 Operator.OR)));
        Assert.assertFalse(auth.checkGlobalPriv("192.168.1.200", "cmy_123",
                                                PrivPredicate.of(PrivBitSet.of(PaloPrivilege.SELECT_PRIV),
                                                                 Operator.OR)));
        Assert.assertTrue(auth.checkGlobalPriv("192.168.1.200", "cmy_123",
                                               PrivPredicate.of(PrivBitSet.of(PaloPrivilege.GRANT_PRIV,
                                                                                       PaloPrivilege.NODE_PRIV),
                                                                Operator.OR)));
        Assert.assertTrue(auth.checkGlobalPriv("192.168.1.200", "cmy_",
                                               PrivPredicate.of(PrivBitSet.of(PaloPrivilege.NODE_PRIV),
                                                                Operator.OR)));

        auth.clear();
        grantGlobalPrivs("192.168.%", ".cmy", passwd, false, false, false,
                         PrivBitSet.of(PaloPrivilege.GRANT_PRIV));
        Assert.assertFalse(auth.checkGlobalPriv("192.10.1.1", ".cmy",
                                                PrivPredicate.of(PrivBitSet.of(PaloPrivilege.GRANT_PRIV),
                                                                 Operator.OR)));
        Assert.assertTrue(auth.checkGlobalPriv("192.168.1.200", ".cmy",
                                               PrivPredicate.of(PrivBitSet.of(PaloPrivilege.GRANT_PRIV),
                                                                Operator.OR)));

        // multi priv entries
        auth.clear();
        grantGlobalPrivs("%", "cmy", passwd, false, false, false, PrivBitSet.of(PaloPrivilege.GRANT_PRIV));
        grantGlobalPrivs("localhost", "cmy", passwd, false, false, false,
                         PrivBitSet.of(PaloPrivilege.NODE_PRIV));
        grantGlobalPrivs("127.0.0.1", "cmy", passwd, false, false, false,
                         PrivBitSet.of(PaloPrivilege.SELECT_PRIV));

        Assert.assertTrue(auth.checkGlobalPriv("127.0.0.1", "cmy",
                                               PrivPredicate.of(PrivBitSet.of(PaloPrivilege.SELECT_PRIV),
                                                                Operator.OR)));
        Assert.assertFalse(auth.checkGlobalPriv("127.0.0.1", "cmy",
                                                PrivPredicate.of(PrivBitSet.of(PaloPrivilege.GRANT_PRIV),
                                                                 Operator.OR)));
        Assert.assertTrue(auth.checkGlobalPriv("localhost", "cmy",
                                               PrivPredicate.of(PrivBitSet.of(PaloPrivilege.NODE_PRIV),
                                                                Operator.OR)));
        Assert.assertFalse(auth.checkGlobalPriv("localhost", "cmy",
                                                PrivPredicate.of(PrivBitSet.of(PaloPrivilege.GRANT_PRIV),
                                                                 Operator.OR)));
        Assert.assertTrue(auth.checkGlobalPriv("192.168.1.1", "cmy",
                                               PrivPredicate.of(PrivBitSet.of(PaloPrivilege.GRANT_PRIV),
                                                                Operator.OR)));

        // test persist
        auth = testPersist(auth);
        Assert.assertTrue(auth.checkGlobalPriv("127.0.0.1", "cmy",
                                               PrivPredicate.of(PrivBitSet.of(PaloPrivilege.SELECT_PRIV),
                                                                Operator.OR)));
        Assert.assertFalse(auth.checkGlobalPriv("127.0.0.1", "cmy",
                                                PrivPredicate.of(PrivBitSet.of(PaloPrivilege.GRANT_PRIV),
                                                                 Operator.OR)));
        Assert.assertTrue(auth.checkGlobalPriv("localhost", "cmy",
                                               PrivPredicate.of(PrivBitSet.of(PaloPrivilege.NODE_PRIV),
                                                                Operator.OR)));
        Assert.assertFalse(auth.checkGlobalPriv("localhost", "cmy",
                                                PrivPredicate.of(PrivBitSet.of(PaloPrivilege.GRANT_PRIV),
                                                                 Operator.OR)));
        Assert.assertTrue(auth.checkGlobalPriv("192.168.1.1", "cmy",
                                               PrivPredicate.of(PrivBitSet.of(PaloPrivilege.GRANT_PRIV),
                                                                Operator.OR)));
    }

    @Test
    public void testDbPriv()
            throws DdlException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        // normal
        grantDbPrivs("192.168.1.%", "my\\_database", "cmy", false, false, false,
                     PrivBitSet.of(PaloPrivilege.SELECT_PRIV,
                                            PaloPrivilege.ALTER_PRIV));
        Assert.assertTrue(auth.checkDbPriv("192.168.1.1", "my_database", "cmy",
                                           PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV),
                                                            Operator.OR)));
        Assert.assertTrue(auth.checkDbPriv("192.168.1.1", "my_database", "cmy",
                                           PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV,
                                                                                   PaloPrivilege.SELECT_PRIV),
                                                            Operator.OR)));
        Assert.assertFalse(auth.checkDbPriv("192.168.1.2", "my_database", "cmy",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV,
                                                                                    PaloPrivilege.LOAD_PRIV),
                                                             Operator.AND)));
        Assert.assertTrue(auth.checkDbPriv("192.168.1.2", "my_database", "cmy",
                                           PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV,
                                                                                   PaloPrivilege.LOAD_PRIV),
                                                            Operator.OR)));
        Assert.assertFalse(auth.checkDbPriv("192.168.1.1", "my_database2", "cmy",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV),
                                                             Operator.OR)));
        Assert.assertFalse(auth.checkDbPriv("192.168.2.1", "my_database2", "cmy",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV),
                                                             Operator.OR)));
        Assert.assertFalse(auth.checkDbPriv("192.168.1.1", "my_database2", "cmy2",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV),
                                                             Operator.OR)));

        // add global priv
        auth.clear();
        grantGlobalPrivs("%", "cmy", passwd, false, false, false, PrivBitSet.of(PaloPrivilege.SELECT_PRIV));
        grantDbPrivs("192.168.1.%", "database", "cmy", false, false, false,
                     PrivBitSet.of(PaloPrivilege.ALTER_PRIV));

        Assert.assertTrue(auth.checkDbPriv("192.168.1.1", "database", "cmy",
                                           PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV,
                                                                                   PaloPrivilege.SELECT_PRIV),
                                                            Operator.OR)));

        Assert.assertTrue(auth.checkDbPriv("192.168.1.1", "database2", "cmy",
                                           PrivPredicate.of(PrivBitSet.of(PaloPrivilege.SELECT_PRIV),
                                                            Operator.OR)));

        Assert.assertFalse(auth.checkDbPriv("192.168.2.1", "database", "cmy",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV),
                                                             Operator.OR)));

        Assert.assertFalse(auth.checkDbPriv("192.168.1.1", "database2", "cmy",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV),
                                                             Operator.OR)));

        // test persist
        auth = testPersist(auth);
        Assert.assertTrue(auth.checkDbPriv("192.168.1.1", "database", "cmy",
                                           PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV,
                                                                                   PaloPrivilege.SELECT_PRIV),
                                                            Operator.OR)));

        Assert.assertTrue(auth.checkDbPriv("192.168.1.1", "database2", "cmy",
                                           PrivPredicate.of(PrivBitSet.of(PaloPrivilege.SELECT_PRIV),
                                                            Operator.OR)));

        Assert.assertFalse(auth.checkDbPriv("192.168.2.1", "database", "cmy",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV),
                                                             Operator.OR)));

        Assert.assertFalse(auth.checkDbPriv("192.168.1.1", "database2", "cmy",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV),
                                                             Operator.OR)));
    }

    @Test
    public void testTblPriv()
            throws DdlException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        // normal
        grantTblPrivs("192.%.1.1", "db\\_%", "cmy%", "tbl%", false, false, false,
                      PrivBitSet.of(PaloPrivilege.SELECT_PRIV,
                                             PaloPrivilege.LOAD_PRIV));
        Assert.assertTrue(auth.checkTblPriv("192.168.1.1", "db_1", "cmy", "tbl",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.LOAD_PRIV,
                                                                                    PaloPrivilege.SELECT_PRIV),
                                                             Operator.OR)));
        Assert.assertFalse(auth.checkTblPriv("192.168.1.1", "db_1", "cmy", "tbl",
                                             PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV),
                                                              Operator.OR)));

        // add db priv
        grantDbPrivs("192.%", "db\\_123", "cmy", false, false, false, PrivBitSet.of(PaloPrivilege.ALTER_PRIV));
        Assert.assertTrue(auth.checkTblPriv("192.168.1.1", "db_123", "cmy", "tbl",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV),
                                                             Operator.OR)));

        Assert.assertTrue(auth.checkTblPriv("192.168.1.1", "db_123", "cmy", "tbl",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.SELECT_PRIV),
                                                             Operator.OR)));

        Assert.assertFalse(auth.checkTblPriv("10.168.1.1", "db_123", "cmy", "tbl",
                                             PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV),
                                                              Operator.OR)));

        // add global priv
        grantGlobalPrivs("192.168.2.1", "cmy\\_admin", passwd, false, false, false,
                         PrivBitSet.of(PaloPrivilege.DROP_PRIV));
        Assert.assertTrue(auth.checkTblPriv("192.168.2.1", "db_123", "cmy_admin", "tbl",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.DROP_PRIV),
                                                             Operator.OR)));

        Assert.assertFalse(auth.checkTblPriv("192.168.1.1", "db_123", "cmy_admin", "tbl",
                                             PrivPredicate.of(PrivBitSet.of(PaloPrivilege.DROP_PRIV),
                                                              Operator.OR)));

        // test persist
        auth = testPersist(auth);
        Assert.assertTrue(auth.checkTblPriv("192.168.2.1", "db_123", "cmy_admin", "tbl",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.DROP_PRIV),
                                                             Operator.OR)));

        Assert.assertFalse(auth.checkTblPriv("192.168.1.1", "db_123", "cmy_admin", "tbl",
                                             PrivPredicate.of(PrivBitSet.of(PaloPrivilege.DROP_PRIV),
                                                              Operator.OR)));

        // add global priv
        grantGlobalPrivs("%", "cmy2", passwd, false, false, false,
                         PrivBitSet.of(PaloPrivilege.DROP_PRIV));
        Assert.assertTrue(auth.checkTblPriv("", "db_123", "cmy2", "tbl",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.DROP_PRIV),
                                                             Operator.OR)));
        Assert.assertTrue(auth.checkTblPriv(null, "db_123", "cmy2", "tbl",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.DROP_PRIV),
                                                             Operator.OR)));

    }

    private PaloAuth testPersist(PaloAuth auth) {
        // 1. Write objects to file
        File file = new File("./paloAuth");
        try {
            file.createNewFile();
            DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

            auth.write(dos);

            dos.flush();
            dos.close();

            // 2. Read objects from file
            DataInputStream dis = new DataInputStream(new FileInputStream(file));

            PaloAuth replayed = PaloAuth.read(dis);
            
            System.out.println(replayed.toString());
            
            return replayed;

        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            file.delete();
        }
        return null;
    }

}
