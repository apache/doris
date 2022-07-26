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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MysqlTableTest {
    private List<Column> columns;
    private Map<String, String> properties;

    @Mocked
    private Env env;

    private FakeEnv fakeEnv;

    @Before
    public void setUp() {
        columns = Lists.newArrayList();
        Column column = new Column("col1", PrimitiveType.BIGINT);
        column.setIsKey(true);
        columns.add(column);

        properties = Maps.newHashMap();
        properties.put("host", "127.0.0.1");
        properties.put("port", "3306");
        properties.put("user", "root");
        properties.put("password", "root");
        properties.put("database", "db");
        properties.put("table", "tbl");

        fakeEnv = new FakeEnv();
        FakeEnv.setEnv(env);
        FakeEnv.setMetaVersion(FeConstants.meta_version);
    }

    @Test
    public void testNormal() throws DdlException, IOException {
        MysqlTable mysqlTable = new MysqlTable(1000, "mysqlTable", columns, properties);
        Assert.assertEquals("tbl", mysqlTable.getMysqlTableName());

        String dirString = "mysqlTableFamilyGroup";
        File dir = new File(dirString);
        if (!dir.exists()) {
            dir.mkdir();
        } else {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    file.delete();
                }
            }
        }

        File file = new File(dir, "image");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        mysqlTable.write(dos);
        dos.close();

        DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
        MysqlTable table1 = (MysqlTable) Table.read(dis);

        Assert.assertEquals(mysqlTable.toThrift(), table1.toThrift());

        dis.close();

        dir = new File(dirString);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            for (File aFile : files) {
                if (aFile.isFile()) {
                    aFile.delete();
                }
            }
            dir.delete();
        }
    }

    @Test(expected = DdlException.class)
    public void testNoHost() throws DdlException {
        Map<String, String> pro = Maps.filterKeys(properties, new Predicate<String>() {
            @Override
            public boolean apply(String s) {
                if (s.equalsIgnoreCase("host")) {
                    return false;
                } else {
                    return true;
                }
            }
        });
        new MysqlTable(1000, "mysqlTable", columns, pro);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testNoPort() throws DdlException {
        Map<String, String> pro = Maps.filterKeys(properties, new Predicate<String>() {
            @Override
            public boolean apply(String s) {
                if (s.equalsIgnoreCase("port")) {
                    return false;
                } else {
                    return true;
                }
            }
        });
        new MysqlTable(1000, "mysqlTable", columns, pro);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testPortNotNumber() throws DdlException {
        Map<String, String> pro = Maps.transformEntries(properties,
                new Maps.EntryTransformer<String, String, String>() {
                    @Override
                    public String transformEntry(String s, String s2) {
                        if (s.equalsIgnoreCase("port")) {
                            return "abc";
                        }
                        return s2;
                    }
                });
        new MysqlTable(1000, "mysqlTable", columns, pro);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testNoUser() throws DdlException {
        Map<String, String> pro = Maps.filterKeys(properties, new Predicate<String>() {
            @Override
            public boolean apply(String s) {
                if (s.equalsIgnoreCase("user")) {
                    return false;
                } else {
                    return true;
                }
            }
        });
        new MysqlTable(1000, "mysqlTable", columns, pro);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testNoPass() throws DdlException {
        Map<String, String> pro = Maps.filterKeys(properties, new Predicate<String>() {
            @Override
            public boolean apply(String s) {
                if (s.equalsIgnoreCase("password")) {
                    return false;
                } else {
                    return true;
                }
            }
        });
        new MysqlTable(1000, "mysqlTable", columns, pro);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testNoDb() throws DdlException {
        Map<String, String> pro = Maps.filterKeys(properties, new Predicate<String>() {
            @Override
            public boolean apply(String s) {
                if (s.equalsIgnoreCase("database")) {
                    return false;
                } else {
                    return true;
                }
            }
        });
        new MysqlTable(1000, "mysqlTable", columns, pro);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testNoTbl() throws DdlException {
        Map<String, String> pro = Maps.filterKeys(properties, new Predicate<String>() {
            @Override
            public boolean apply(String s) {
                if (s.equalsIgnoreCase("table")) {
                    return false;
                } else {
                    return true;
                }
            }
        });
        new MysqlTable(1000, "mysqlTable", columns, pro);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testNoPro() throws DdlException {
        new MysqlTable(1000, "mysqlTable", columns, null);
        Assert.fail("No exception throws.");
    }
}
