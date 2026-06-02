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

import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.LiteralExprUtils;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;

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
    private static Column timestampTz;
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
        timestampTz = new Column("timestamptz", ScalarType.createTimeStampTzType(6), true, null, "", "");
        charString = new Column("char", PrimitiveType.CHAR);
        varchar = new Column("varchar", PrimitiveType.VARCHAR);
        bool = new Column("bool", PrimitiveType.BOOLEAN);

        allColumns = Arrays.asList(tinyInt, smallInt, int32, bigInt, largeInt, date, datetime);
    }

    private static PartitionValue partitionValue(String value, Column column) throws AnalysisException {
        return partitionValue(value, column.getType());
    }

    private static PartitionValue partitionValue(String value, Type type) throws AnalysisException {
        return new PartitionValue(LiteralExprUtils.createLiteral(value, type));
    }

    @Test
    public void compareTest() throws AnalysisException {
        PartitionKey pk1;
        PartitionKey pk2;

        // case1
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(partitionValue("127", tinyInt), partitionValue("32767", smallInt)),
                                              Arrays.asList(tinyInt, smallInt));
        pk2 = PartitionKey.createInfinityPartitionKey(Arrays.asList(tinyInt, smallInt), true);
        Assert.assertTrue(pk1.hashCode() != pk2.hashCode());
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == -1);

        // case2
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(partitionValue("127", tinyInt)),
                                              Arrays.asList(tinyInt, smallInt));
        pk2 = PartitionKey.createPartitionKey(Arrays.asList(partitionValue("127", tinyInt), partitionValue("-32768", smallInt)),
                                              Arrays.asList(tinyInt, smallInt));
        Assert.assertTrue(pk1.hashCode() == pk2.hashCode());
        Assert.assertTrue(pk1.equals(pk2) && pk1.compareTo(pk2) == 0);

        // case3
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(partitionValue("127", int32)),
                                              Arrays.asList(int32, bigInt));
        pk2 = PartitionKey.createPartitionKey(Arrays.asList(partitionValue("128", int32), partitionValue("-32768", bigInt)),
                                              Arrays.asList(int32, bigInt));
        Assert.assertTrue(pk1.hashCode() != pk2.hashCode());
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == -1);

        // case4
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(partitionValue("127", largeInt), partitionValue("12345", bigInt)),
                                              Arrays.asList(largeInt, bigInt));
        pk2 = PartitionKey.createPartitionKey(Arrays.asList(partitionValue("127", largeInt), partitionValue("12346", bigInt)),
                                              Arrays.asList(largeInt, bigInt));
        Assert.assertTrue(pk1.hashCode() != pk2.hashCode());
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == -1);

        // case5
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(partitionValue("2014-12-12", date), partitionValue("2014-12-12 10:00:00", datetime)),
                                              Arrays.asList(date, datetime));
        pk2 = PartitionKey.createPartitionKey(Arrays.asList(partitionValue("2014-12-12", date), partitionValue("2014-12-12 10:00:01", datetime)),
                                              Arrays.asList(date, datetime));
        Assert.assertTrue(pk1.hashCode() != pk2.hashCode());
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == -1);

        // case6
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(partitionValue("-128", tinyInt)),
                                              Arrays.asList(tinyInt, smallInt));
        pk2 = PartitionKey.createInfinityPartitionKey(Arrays.asList(tinyInt, smallInt), false);
        Assert.assertTrue(pk1.hashCode() == pk2.hashCode());
        Assert.assertTrue(pk1.equals(pk2) && pk1.compareTo(pk2) == 0);

        // case7
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(partitionValue("127", tinyInt)),
                                              Arrays.asList(tinyInt, smallInt));
        pk2 = PartitionKey.createInfinityPartitionKey(Arrays.asList(tinyInt, smallInt), true);
        Assert.assertTrue(pk1.hashCode() != pk2.hashCode());
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == -1);

        // case7
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(partitionValue("127", tinyInt), partitionValue("32767", smallInt)),
                                              Arrays.asList(tinyInt, smallInt));
        pk2 = PartitionKey.createInfinityPartitionKey(Arrays.asList(tinyInt, smallInt), true);
        Assert.assertTrue(pk1.hashCode() != pk2.hashCode());
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == -1);

        // case8
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(partitionValue("127", tinyInt), partitionValue("32767", smallInt),
                partitionValue("2147483647", int32), partitionValue("9223372036854775807", bigInt),
                partitionValue("170141183460469231731687303715884105727", largeInt),
                partitionValue("9999-12-31", date), partitionValue("9999-12-31 23:59:59", datetime)),
                allColumns);
        pk2 = PartitionKey.createInfinityPartitionKey(allColumns, true);
        Assert.assertTrue(pk1.hashCode() != pk2.hashCode());
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == -1);

        // case9
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(partitionValue("-128", tinyInt), partitionValue("-32768", smallInt),
                partitionValue("-2147483648", int32), partitionValue("-9223372036854775808", bigInt),
                partitionValue("-170141183460469231731687303715884105728", largeInt),
                partitionValue("0000-01-01", date), partitionValue("0000-01-01 00:00:00", datetime)),
                allColumns);
        pk2 = PartitionKey.createInfinityPartitionKey(allColumns, false);
        Assert.assertTrue(pk1.hashCode() == pk2.hashCode());
        Assert.assertTrue(pk1.equals(pk2) && pk1.compareTo(pk2) == 0);

        // case10
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(partitionValue("-128", tinyInt), partitionValue("-32768", smallInt),
                partitionValue("0", int32), partitionValue("-9223372036854775808", bigInt),
                partitionValue("0", largeInt), partitionValue("1970-01-01", date), partitionValue("1970-01-01 00:00:00", datetime)),
                allColumns);
        pk2 = PartitionKey.createInfinityPartitionKey(allColumns, false);
        Assert.assertTrue(pk1.hashCode() != pk2.hashCode());
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == 1);

        // case11
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(partitionValue("beijing", charString), partitionValue("shanghai", varchar)),
                Arrays.asList(charString, varchar));
        pk2 = PartitionKey.createPartitionKey(Arrays.asList(partitionValue("beijing", charString), partitionValue("shanghai", varchar)),
                Arrays.asList(charString, varchar));
        Assert.assertTrue(pk1.hashCode() == pk2.hashCode());
        Assert.assertTrue(pk1.equals(pk2) && pk1.compareTo(pk2) == 0);

        // case12
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(partitionValue("beijing", charString), partitionValue("shanghai", varchar)),
                Arrays.asList(charString, varchar));
        pk2 = PartitionKey.createPartitionKey(Arrays.asList(partitionValue("shijiazhuang", charString), partitionValue("tianjin", varchar)),
                Arrays.asList(charString, varchar));
        Assert.assertTrue(pk1.hashCode() != pk2.hashCode());
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == -1);

        // case13
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(partitionValue("beijing", charString), partitionValue("shanghai", varchar)),
                Arrays.asList(charString, varchar));
        pk2 = PartitionKey.createPartitionKey(Arrays.asList(partitionValue("beijing", charString), partitionValue("tianjin", varchar)),
                Arrays.asList(charString, varchar));
        Assert.assertTrue(pk1.hashCode() != pk2.hashCode());
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == -1);

        // case14
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(partitionValue("true", bool)),
                Arrays.asList(bool));
        pk2 = PartitionKey.createPartitionKey(Arrays.asList(partitionValue("false", bool)),
                Arrays.asList(bool));
        Assert.assertTrue(pk1.hashCode() != pk2.hashCode());
        Assert.assertTrue(!pk1.equals(pk2) && pk1.compareTo(pk2) == 1);

        // case15
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(partitionValue("true", bool)),
                Arrays.asList(bool));
        pk2 = PartitionKey.createPartitionKey(Arrays.asList(partitionValue("true", bool)),
                Arrays.asList(bool));
        Assert.assertTrue(pk1.hashCode() == pk2.hashCode());
        Assert.assertTrue(pk1.equals(pk2) && pk1.compareTo(pk2) == 0);

        // case16
        pk1 = PartitionKey.createPartitionKey(Arrays.asList(partitionValue("false", bool)),
                Arrays.asList(bool));
        pk2 = PartitionKey.createPartitionKey(Arrays.asList(partitionValue("false", bool)),
                Arrays.asList(bool));
        Assert.assertTrue(pk1.hashCode() == pk2.hashCode());
        Assert.assertTrue(pk1.equals(pk2) && pk1.compareTo(pk2) == 0);
    }

    @Test
    public void testCompareDoublePartitionKeysWithDifferentLiteralTypes() throws AnalysisException {
        Column doubleColumn = new Column("double_col", PrimitiveType.DOUBLE);

        PartitionKey small = PartitionKey.createPartitionKey(
                Arrays.asList(partitionValue("1", doubleColumn)),
                Arrays.asList(doubleColumn));
        PartitionKey big = PartitionKey.createPartitionKey(
                Arrays.asList(partitionValue("1.2345678901234567", doubleColumn)),
                Arrays.asList(doubleColumn));

        Assert.assertTrue(small.compareTo(big) < 0);
        Assert.assertTrue(big.compareTo(small) > 0);
    }

    @Test
    public void testCompareDecimalPartitionKeysWithDifferentLiteralTypes() throws AnalysisException {
        Column decimalColumn = new Column("decimal_col", ScalarType.createDecimalV3Type(10, 2), true,
                null, "", "");

        PartitionKey small = PartitionKey.createPartitionKey(
                Arrays.asList(partitionValue("1", decimalColumn)),
                Arrays.asList(decimalColumn));
        PartitionKey big = PartitionKey.createPartitionKey(
                Arrays.asList(partitionValue("10.00", decimalColumn)),
                Arrays.asList(decimalColumn));

        Assert.assertTrue(small.compareTo(big) < 0);
        Assert.assertTrue(big.compareTo(small) > 0);
    }

    @Test
    public void testSerialization() throws Exception {
        FakeEnv fakeEnv = new FakeEnv(); // CHECKSTYLE IGNORE THIS LINE
        FakeEnv.setMetaVersion(FeConstants.meta_version);

        // 1. Write objects to file
        Path dir = Paths.get("./keyRangePartition");
        Files.deleteIfExists(dir);
        Path path = Files.createFile(dir);
        DataOutputStream dos = new DataOutputStream(Files.newOutputStream(path));

        PartitionKey keyEmpty = new PartitionKey();
        keyEmpty.write(dos);

        List<PartitionValue> keys = new ArrayList<PartitionValue>();
        List<Column> columns = new ArrayList<Column>();
        keys.add(partitionValue("100", ScalarType.createType(PrimitiveType.TINYINT)));
        columns.add(new Column("column2", ScalarType.createType(PrimitiveType.TINYINT), true, null, "", ""));
        keys.add(partitionValue("101", ScalarType.createType(PrimitiveType.SMALLINT)));
        columns.add(new Column("column3", ScalarType.createType(PrimitiveType.SMALLINT), true, null, "", ""));
        keys.add(partitionValue("102", ScalarType.createType(PrimitiveType.INT)));
        columns.add(new Column("column4", ScalarType.createType(PrimitiveType.INT), true, null, "", ""));
        keys.add(partitionValue("103", ScalarType.createType(PrimitiveType.BIGINT)));
        columns.add(new Column("column5", ScalarType.createType(PrimitiveType.BIGINT), true, null, "", ""));
        keys.add(partitionValue("2014-12-26", ScalarType.createType(PrimitiveType.DATE)));
        columns.add(new Column("column10", ScalarType.createType(PrimitiveType.DATE), true, null, "", ""));
        keys.add(partitionValue("2014-12-27 11:12:13", ScalarType.createType(PrimitiveType.DATETIME)));
        columns.add(new Column("column11", ScalarType.createType(PrimitiveType.DATETIME), true, null, "", ""));
        keys.add(partitionValue("beijing", ScalarType.createType(PrimitiveType.VARCHAR)));
        columns.add(new Column("column12", ScalarType.createType(PrimitiveType.VARCHAR), true, null, "", ""));
        keys.add(partitionValue("shanghai", ScalarType.createType(PrimitiveType.CHAR)));
        columns.add(new Column("column13", ScalarType.createType(PrimitiveType.CHAR), true, null, "", ""));
        keys.add(partitionValue("true", ScalarType.createType(PrimitiveType.BOOLEAN)));
        columns.add(new Column("column14", ScalarType.createType(PrimitiveType.BOOLEAN), true, null, "", ""));
        keys.add(partitionValue("false", ScalarType.createType(PrimitiveType.BOOLEAN)));
        columns.add(new Column("column15", ScalarType.createType(PrimitiveType.BOOLEAN), true, null, "", ""));

        PartitionKey key = PartitionKey.createPartitionKey(keys, columns);
        key.write(dos);

        keys.clear();
        List<Type> types = new ArrayList<Type>();
        types.add(ScalarType.createType(PrimitiveType.INT));
        PartitionKey defaultKey = PartitionKey.createListPartitionKeyWithTypes(keys, types, false);
        Assert.assertTrue(defaultKey.isDefaultListPartitionKey());
        defaultKey.write(dos);

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

        PartitionKey rDefaultKey = PartitionKey.read(dis);
        Assert.assertEquals(defaultKey, rDefaultKey);
        Assert.assertTrue(rDefaultKey.isDefaultListPartitionKey());

        // 3. delete files
        dis.close();
        Files.deleteIfExists(path);
    }

    @Test
    public void testMaxValueToSql() throws Exception {
        PartitionKey key = PartitionKey.createInfinityPartitionKey(allColumns, true);
        Assert.assertEquals("(MAXVALUE, MAXVALUE, MAXVALUE, MAXVALUE, MAXVALUE, MAXVALUE, MAXVALUE)", key.toSql());
    }

    @Test
    public void testTimestampTzPartitionKeyKeepsExplicitOffset() throws Exception {
        boolean originalRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        try {
            context.getSessionVariable().setTimeZone("America/New_York");

            PartitionKey key = PartitionKey.createPartitionKey(
                    Arrays.asList(partitionValue("2024-01-15 12:00:00 +00:00", timestampTz)),
                    Arrays.asList(timestampTz));

            DateLiteral literal = (DateLiteral) key.getKeys().get(0);
            Assert.assertEquals(2024, literal.getYear());
            Assert.assertEquals(1, literal.getMonth());
            Assert.assertEquals(15, literal.getDay());
            Assert.assertEquals(12, literal.getHour());
            Assert.assertEquals(0, literal.getMinute());
            Assert.assertEquals(0, literal.getSecond());
            Assert.assertEquals(0, literal.getMicrosecond());
            Assert.assertTrue(literal.getStringValue().startsWith("2024-01-15 12:00:00"));
            Assert.assertTrue(literal.getStringValue().endsWith("+00:00"));
        } finally {
            ConnectContext.remove();
            FeConstants.runningUnitTest = originalRunningUnitTest;
        }
    }

    @Test
    public void testTimestampTzPartitionKeyAcceptsNamedTimezone() throws Exception {
        PartitionKey key = PartitionKey.createPartitionKey(
                                Arrays.asList(partitionValue("2024-01-15 20:00:00Asia/Shanghai", timestampTz)),
                Arrays.asList(timestampTz));

        DateLiteral literal = (DateLiteral) key.getKeys().get(0);
        Assert.assertEquals(2024, literal.getYear());
        Assert.assertEquals(1, literal.getMonth());
        Assert.assertEquals(15, literal.getDay());
        Assert.assertEquals(12, literal.getHour());
        Assert.assertEquals(0, literal.getMinute());
        Assert.assertEquals(0, literal.getSecond());
        Assert.assertEquals(0, literal.getMicrosecond());
        Assert.assertTrue(literal.getStringValue().startsWith("2024-01-15 12:00:00"));
        Assert.assertTrue(literal.getStringValue().endsWith("+00:00"));
    }

    @Test
    public void testTimestampTzPartitionKeyAcceptsLowercaseTimezone() throws Exception {
        PartitionKey key = PartitionKey.createPartitionKey(
                                Arrays.asList(partitionValue("2024-01-15 12:00:00    uTc", timestampTz)),
                Arrays.asList(timestampTz));

        DateLiteral literal = (DateLiteral) key.getKeys().get(0);
        Assert.assertEquals(2024, literal.getYear());
        Assert.assertEquals(1, literal.getMonth());
        Assert.assertEquals(15, literal.getDay());
        Assert.assertEquals(12, literal.getHour());
        Assert.assertEquals(0, literal.getMinute());
        Assert.assertEquals(0, literal.getSecond());
        Assert.assertEquals(0, literal.getMicrosecond());
        Assert.assertTrue(literal.getStringValue().startsWith("2024-01-15 12:00:00"));
        Assert.assertTrue(literal.getStringValue().endsWith("+00:00"));
    }

    @Test
    public void testTimestampTzPartitionKeyUsesSessionTimezoneWithoutExplicitOffset() throws Exception {
        boolean originalRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        try {
            context.getSessionVariable().setTimeZone("America/New_York");

            PartitionKey key = PartitionKey.createPartitionKey(
                    Arrays.asList(partitionValue("2024-01-15 12:00:00", timestampTz)),
                    Arrays.asList(timestampTz));

            DateLiteral literal = (DateLiteral) key.getKeys().get(0);
            Assert.assertEquals(2024, literal.getYear());
            Assert.assertEquals(1, literal.getMonth());
            Assert.assertEquals(15, literal.getDay());
            Assert.assertEquals(17, literal.getHour());
            Assert.assertEquals(0, literal.getMinute());
            Assert.assertEquals(0, literal.getSecond());
            Assert.assertEquals(0, literal.getMicrosecond());
            Assert.assertTrue(literal.getStringValue().startsWith("2024-01-15 17:00:00"));
            Assert.assertTrue(literal.getStringValue().endsWith("+00:00"));
        } finally {
            ConnectContext.remove();
            FeConstants.runningUnitTest = originalRunningUnitTest;
        }
    }
}
