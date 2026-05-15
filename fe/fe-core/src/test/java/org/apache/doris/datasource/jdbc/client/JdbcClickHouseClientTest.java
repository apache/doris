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

package org.apache.doris.datasource.jdbc.client;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.jdbc.util.JdbcFieldSchema;

import mockit.Expectations;
import mockit.Injectable;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.lang.reflect.Method;

public class JdbcClickHouseClientTest {

    @Test
    public void testIsNewClickHouseDriver() {
        try {
            Method method = JdbcClickHouseClient.class.getDeclaredMethod("isNewClickHouseDriver", String.class);
            method.setAccessible(true);

            // Valid test cases
            Assert.assertTrue((boolean) method.invoke(null, "0.5.0")); // Major version 0, Minor version 5
            Assert.assertTrue((boolean) method.invoke(null, "1.0.0")); // Major version 1
            Assert.assertTrue((boolean) method.invoke(null, "0.6.3 (revision: a6a8a22)")); // Major version 0, Minor version 6
            Assert.assertFalse((boolean) method.invoke(null, "0.4.2 (revision: 1513b27)")); // Major version 0, Minor version 4

            // Invalid version formats
            try {
                method.invoke(null, "invalid.version"); // Invalid version format
                Assert.fail("Expected JdbcClientException for invalid version 'invalid.version'");
            } catch (Exception e) {
                Assert.assertTrue(e.getCause() instanceof JdbcClientException);
                Assert.assertTrue(e.getCause().getMessage().contains("Invalid clickhouse driver version format"));
            }

            try {
                method.invoke(null, ""); // Empty version
                Assert.fail("Expected JdbcClientException for empty version");
            } catch (Exception e) {
                Assert.assertTrue(e.getCause() instanceof JdbcClientException);
                Assert.assertTrue(e.getCause().getMessage().contains("Invalid clickhouse driver version format"));
            }

            try {
                method.invoke(null, (Object) null); // Null version
                Assert.fail("Expected JdbcClientException for null version");
            } catch (Exception e) {
                Assert.assertTrue(e.getCause() instanceof JdbcClientException);
                Assert.assertTrue(e.getCause().getMessage().contains("Driver version cannot be null"));
            }
        } catch (Exception e) {
            Assert.fail("Exception occurred while testing isNewClickHouseDriver: " + e.getMessage());
        }
    }

    @Test
    public void testDecimalType(@Injectable JdbcFieldSchema fieldSchema) {
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "Decimal(10, 2)";
            }
        };
        JdbcClickHouseClient client = new JdbcClickHouseClient();
        String ckType = fieldSchema.getDataTypeName().orElse("unknown");
        String[] accuracy = ckType.substring(8, ckType.length() - 1).split(", ");
        int precision = Integer.parseInt(accuracy[0]);
        int scale = Integer.parseInt(accuracy[1]);
        Type type = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(ScalarType.createDecimalV3Type(precision, scale), type);
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "Decimal(40, 39)";
            }
        };
        Type type1 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.STRING, type1);
    }

    @Test
    public void testStringType(@Injectable JdbcFieldSchema fieldSchema) {
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "String";
            }
        };
        JdbcClickHouseClient client = new JdbcClickHouseClient();
        Type type = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.STRING, type);
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "Enum8";
            }
        };
        Type type1 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.STRING, type1);
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "Enum16";
            }
        };
        Type type2 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.STRING, type2);
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "IPv4";
            }
        };
        Type type3 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.STRING, type3);
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "IPv6";
            }
        };
        Type type4 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.STRING, type4);
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "UUID";
            }
        };
        Type type5 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.STRING, type5);
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "FixedString";
            }
        };
        Type type6 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.STRING, type6);
    }

    @Test
    public void testDateTime64Type(@Injectable JdbcFieldSchema fieldSchema) {
        // test DateTime
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "DateTime";
            }
        };
        JdbcClickHouseClient client = new JdbcClickHouseClient();
        Type type1 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.DEFAULT_DATETIMEV2, type1);
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "DateTime()";
            }
        };
        Type type2 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.DEFAULT_DATETIMEV2, type2);
        final int expectedPrecision6 = 6;
        // test DateTime64
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "DateTime64(" + expectedPrecision6 + ")";
            }
        };
        Type type3 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.DATETIMEV2_WITH_MAX_SCALAR, type3);
        final int expectedPrecisio7 = 7;
        // test DateTime64
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "DateTime64(" + expectedPrecisio7 + ")";
            }
        };
        Type type4 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.DATETIMEV2_WITH_MAX_SCALAR, type4);
        // test DateTime64
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "DateTime64(6, 'Asia/Shanghai')";
            }
        };
        Type type5 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.DATETIMEV2_WITH_MAX_SCALAR, type5);
    }

    @Test
    public void testArrayType(@Injectable JdbcFieldSchema fieldSchema) {
        // test Array
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "Int32";
            }
        };
        JdbcClickHouseClient client = new JdbcClickHouseClient();
        Type type = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.INT, type);
    }

    @Test
    public void testBooleanType(@Injectable JdbcFieldSchema fieldSchema) {
        // test Bool
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "Bool";
            }
        };
        JdbcClickHouseClient client = new JdbcClickHouseClient();
        Type type = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.BOOLEAN, type);
    }

    @Test
    public void testTinyIntType(@Injectable JdbcFieldSchema fieldSchema) {
        // test Nullable(Nothing)
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "Nullable(Nothing)";
            }
        };
        JdbcClickHouseClient client = new JdbcClickHouseClient();
        Type type1 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.TINYINT, type1);
        // test Int8
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "Int8";
            }
        };
        Type type2 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.TINYINT, type2);
    }

    @Test
    public void testSmallIntType(@Injectable JdbcFieldSchema fieldSchema) {
        // test Int16
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "Int16";
            }
        };
        JdbcClickHouseClient client = new JdbcClickHouseClient();
        Type type1 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.SMALLINT, type1);
        // test UInt8
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "UInt8";
            }
        };
        Type type2 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.SMALLINT, type2);
    }

    @Test
    public void testIntType(@Injectable JdbcFieldSchema fieldSchema) {
        // test Int32
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "Int32";
            }
        };
        JdbcClickHouseClient client = new JdbcClickHouseClient();
        Type type1 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.INT, type1);
        // test UInt16
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "UInt16";
            }
        };
        Type type2 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.INT, type2);
    }

    @Test
    public void testBigIntType(@Injectable JdbcFieldSchema fieldSchema) {
        // test Int64
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "Int64";
            }
        };
        JdbcClickHouseClient client = new JdbcClickHouseClient();
        Type type1 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.BIGINT, type1);
        // test UInt32
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "UInt32";
            }
        };
        Type type2 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.BIGINT, type2);
    }

    @Test
    public void testLargeIntType(@Injectable JdbcFieldSchema fieldSchema) {
        // test Int128
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "Int128";
            }
        };
        JdbcClickHouseClient client = new JdbcClickHouseClient();
        Type type1 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.LARGEINT, type1);
        // test UInt64
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "UInt64";
            }
        };
        Type type2 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.LARGEINT, type2);
    }

    @Test
    public void testScalarStringType(@Injectable JdbcFieldSchema fieldSchema) {
        // test Int256
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "Int256";
            }
        };
        JdbcClickHouseClient client = new JdbcClickHouseClient();
        Type type1 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.STRING, type1);
        // test UInt128
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "UInt128";
            }
        };
        Type type2 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.STRING, type2);
        // test UInt256
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "UInt256";
            }
        };
        Type type3 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.STRING, type3);
    }

    @Test
    public void testFloatType(@Injectable JdbcFieldSchema fieldSchema) {
        // test Float32
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "Float32";
            }
        };
        JdbcClickHouseClient client = new JdbcClickHouseClient();
        Type type = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.FLOAT, type);
    }

    @Test
    public void testDoubleType(@Injectable JdbcFieldSchema fieldSchema) {
        // test Float64
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "Float64";
            }
        };
        JdbcClickHouseClient client = new JdbcClickHouseClient();
        Type type = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.DOUBLE, type);
    }

    @Test
    public void testScalarDateType(@Injectable JdbcFieldSchema fieldSchema) {
        // test Date
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "Date";
            }
        };
        JdbcClickHouseClient client = new JdbcClickHouseClient();
        Type type1 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.DATEV2, type1);
        // test Date32
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "Date32";
            }
        };
        Type type2 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.DATEV2, type2);
    }

    @Test
    public void testUnsupportedType(@Injectable JdbcFieldSchema fieldSchema) {
        // test others
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "Tuple(Int8, Int8)";
            }
        };
        JdbcClickHouseClient client = new JdbcClickHouseClient();
        Type type = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.UNSUPPORTED, type);
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "Nested(Int8)";
            }
        };
        Type type1 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.UNSUPPORTED, type1);
        new Expectations() {
            {
                fieldSchema.getDataTypeName();
                minTimes = 1;
                result = "Null";
            }
        };
        Type type2 = client.jdbcTypeToDoris(fieldSchema);
        Assertions.assertEquals(Type.UNSUPPORTED, type2);
    }
}

