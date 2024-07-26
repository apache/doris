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

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

public class PartitionKeyTest {

    private static List<Column> allColumns;
    private static Column tinyInt;
    private static Column smallInt;
    private static Column int32;
    private static Column bigInt;
    private static Column largeInt;
    private static Column date;
    private static Column datetime;
    private static Column charString;
    private static Column varchar;
    private static Column bool;

    private Env env;

    @BeforeClass
    public static void setUp() {
        TimeZone tz = TimeZone.getTimeZone("ETC/GMT-0");
        TimeZone.setDefault(tz);

        tinyInt = new Column("tinyint", PrimitiveType.TINYINT);
        smallInt = new Column("smallint", PrimitiveType.SMALLINT);
        int32 = new Column("int32", PrimitiveType.INT);
        bigInt = new Column("bigint", PrimitiveType.BIGINT);
        largeInt = new Column("largeint", PrimitiveType.LARGEINT);
        date = new Column("date", PrimitiveType.DATE);
        datetime = new Column("datetime", PrimitiveType.DATETIME);
        charString = new Column("char", PrimitiveType.CHAR);
        varchar = new Column("varchar", PrimitiveType.VARCHAR);
        bool = new Column("bool", PrimitiveType.BOOLEAN);

        allColumns = Arrays.asList(tinyInt, smallInt, int32, bigInt, largeInt, date, datetime);
    }

    @Test
    public void compareTest() throws AnalysisException {
        PartitionKey pk1;
        PartitionKey pk2;

        // case1
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("127"), new PartitionValue("32767")),
                                              Arrays.asList(tinyInt, smallInt));
        pk2 = PartitionKey.createInfinityPartitionKey(Arrays.asList(tinyInt, smallInt), true);
        Assert.assertTrue(pk1.hashCode() != pk2.hashCode());
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == -1);

        // case2
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("127")),
                                              Arrays.asList(tinyInt, smallInt));
        pk2 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("127"), new PartitionValue("-32768")),
                                              Arrays.asList(tinyInt, smallInt));
        Assert.assertTrue(pk1.hashCode() == pk2.hashCode());
        Assert.assertTrue(pk1.equals(pk2) && pk1.compareTo(pk2) == 0);

        // case3
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("127")),
                                              Arrays.asList(int32, bigInt));
        pk2 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("128"), new PartitionValue("-32768")),
                                              Arrays.asList(int32, bigInt));
        Assert.assertTrue(pk1.hashCode() != pk2.hashCode());
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == -1);

        // case4
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("127"), new PartitionValue("12345")),
                                              Arrays.asList(largeInt, bigInt));
        pk2 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("127"), new PartitionValue("12346")),
                                              Arrays.asList(largeInt, bigInt));
        Assert.assertTrue(pk1.hashCode() != pk2.hashCode());
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == -1);

        // case5
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("2014-12-12"), new PartitionValue("2014-12-12 10:00:00")),
                                              Arrays.asList(date, datetime));
        pk2 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("2014-12-12"), new PartitionValue("2014-12-12 10:00:01")),
                                              Arrays.asList(date, datetime));
        Assert.assertTrue(pk1.hashCode() != pk2.hashCode());
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == -1);

        // case6
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("-128")),
                                              Arrays.asList(tinyInt, smallInt));
        pk2 = PartitionKey.createInfinityPartitionKey(Arrays.asList(tinyInt, smallInt), false);
        Assert.assertTrue(pk1.hashCode() == pk2.hashCode());
        Assert.assertTrue(pk1.equals(pk2) && pk1.compareTo(pk2) == 0);

        // case7
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("127")),
                                              Arrays.asList(tinyInt, smallInt));
        pk2 = PartitionKey.createInfinityPartitionKey(Arrays.asList(tinyInt, smallInt), true);
        Assert.assertTrue(pk1.hashCode() != pk2.hashCode());
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == -1);

        // case7
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("127"), new PartitionValue("32767")),
                                              Arrays.asList(tinyInt, smallInt));
        pk2 = PartitionKey.createInfinityPartitionKey(Arrays.asList(tinyInt, smallInt), true);
        Assert.assertTrue(pk1.hashCode() != pk2.hashCode());
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == -1);

        // case8
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("127"), new PartitionValue("32767"),
                new PartitionValue("2147483647"), new PartitionValue("9223372036854775807"),
                new PartitionValue("170141183460469231731687303715884105727"),
                new PartitionValue("9999-12-31"), new PartitionValue("9999-12-31 23:59:59")),
                allColumns);
        pk2 = PartitionKey.createInfinityPartitionKey(allColumns, true);
        Assert.assertTrue(pk1.hashCode() != pk2.hashCode());
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == -1);

        // case9
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("-128"), new PartitionValue("-32768"),
                new PartitionValue("-2147483648"), new PartitionValue("-9223372036854775808"),
                new PartitionValue("-170141183460469231731687303715884105728"),
                new PartitionValue("0000-01-01"), new PartitionValue("0000-01-01 00:00:00")),
                allColumns);
        pk2 = PartitionKey.createInfinityPartitionKey(allColumns, false);
        Assert.assertTrue(pk1.hashCode() == pk2.hashCode());
        Assert.assertTrue(pk1.equals(pk2) && pk1.compareTo(pk2) == 0);

        // case10
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("-128"), new PartitionValue("-32768"),
                new PartitionValue("0"), new PartitionValue("-9223372036854775808"),
                new PartitionValue("0"), new PartitionValue("1970-01-01"), new PartitionValue("1970-01-01 00:00:00")),
                allColumns);
        pk2 = PartitionKey.createInfinityPartitionKey(allColumns, false);
        Assert.assertTrue(pk1.hashCode() != pk2.hashCode());
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == 1);

        // case11
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("beijing"), new PartitionValue("shanghai")),
                Arrays.asList(charString, varchar));
        pk2 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("beijing"), new PartitionValue("shanghai")),
                Arrays.asList(charString, varchar));
        Assert.assertTrue(pk1.hashCode() == pk2.hashCode());
        Assert.assertTrue(pk1.equals(pk2) && pk1.compareTo(pk2) == 0);

        // case12
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("beijing"), new PartitionValue("shanghai")),
                Arrays.asList(charString, varchar));
        pk2 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("shijiazhuang"), new PartitionValue("tianjin")),
                Arrays.asList(charString, varchar));
        Assert.assertTrue(pk1.hashCode() != pk2.hashCode());
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == -1);

        // case13
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("beijing"), new PartitionValue("shanghai")),
                Arrays.asList(charString, varchar));
        pk2 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("beijing"), new PartitionValue("tianjin")),
                Arrays.asList(charString, varchar));
        Assert.assertTrue(pk1.hashCode() != pk2.hashCode());
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == -1);

        // case14
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("true")),
                Arrays.asList(bool));
        pk2 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("false")),
                Arrays.asList(bool));
        Assert.assertTrue(pk1.hashCode() != pk2.hashCode());
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == 1);

        // case15
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("true")),
                Arrays.asList(bool));
        pk2 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("true")),
                Arrays.asList(bool));
        Assert.assertTrue(pk1.hashCode() == pk2.hashCode());
        Assert.assertTrue(pk1.equals(pk2) && pk1.compareTo(pk2) == 0);

        // case16
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("false")),
                Arrays.asList(bool));
        pk2 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("false")),
                Arrays.asList(bool));
        Assert.assertTrue(pk1.hashCode() == pk2.hashCode());
        Assert.assertTrue(pk1.equals(pk2) && pk1.compareTo(pk2) == 0);
    }

    @Test
    public void testSerialization() throws Exception {
        FakeEnv fakeEnv = new FakeEnv(); // CHECKSTYLE IGNORE THIS LINE
        FakeEnv.setMetaVersion(FeConstants.meta_version);

        // 1. Write objects to file
        Path path = Files.createFile(Paths.get("./keyRangePartition"));
        DataOutputStream dos = new DataOutputStream(Files.newOutputStream(path));

        PartitionKey keyEmpty = new PartitionKey();
        keyEmpty.write(dos);

        List<PartitionValue> keys = new ArrayList<PartitionValue>();
        List<Column> columns = new ArrayList<Column>();
        keys.add(new PartitionValue("100"));
        columns.add(new Column("column2", ScalarType.createType(PrimitiveType.TINYINT), true, null, "", ""));
        keys.add(new PartitionValue("101"));
        columns.add(new Column("column3", ScalarType.createType(PrimitiveType.SMALLINT), true, null, "", ""));
        keys.add(new PartitionValue("102"));
        columns.add(new Column("column4", ScalarType.createType(PrimitiveType.INT), true, null, "", ""));
        keys.add(new PartitionValue("103"));
        columns.add(new Column("column5", ScalarType.createType(PrimitiveType.BIGINT), true, null, "", ""));
        keys.add(new PartitionValue("2014-12-26"));
        columns.add(new Column("column10", ScalarType.createType(PrimitiveType.DATE), true, null, "", ""));
        keys.add(new PartitionValue("2014-12-27 11:12:13"));
        columns.add(new Column("column11", ScalarType.createType(PrimitiveType.DATETIME), true, null, "", ""));
        keys.add(new PartitionValue("beijing"));
        columns.add(new Column("column12", ScalarType.createType(PrimitiveType.VARCHAR), true, null, "", ""));
        keys.add(new PartitionValue("shanghai"));
        columns.add(new Column("column13", ScalarType.createType(PrimitiveType.CHAR), true, null, "", ""));
        keys.add(new PartitionValue("true"));
        columns.add(new Column("column14", ScalarType.createType(PrimitiveType.BOOLEAN), true, null, "", ""));
        keys.add(new PartitionValue("false"));
        columns.add(new Column("column15", ScalarType.createType(PrimitiveType.BOOLEAN), true, null, "", ""));

        PartitionKey key = PartitionKey.createPartitionKey(keys, columns);
        key.write(dos);

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(Files.newInputStream(path));
        PartitionKey rKeyEmpty = PartitionKey.read(dis);
        Assert.assertEquals(keyEmpty, rKeyEmpty);

        PartitionKey rKey = PartitionKey.read(dis);
        Assert.assertEquals(key, rKey);
        Assert.assertEquals(key, key);
        Assert.assertNotEquals(key, this);

        // 3. delete files
        dis.close();
        Files.deleteIfExists(path);
    }
}
