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

package org.apache.doris.nereids.types;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class VarBinaryTypeTest {

    @Test
    public void testSingletonsAndFactory() {
        // INSTANCE should use MAX length
        Assertions.assertEquals(VarBinaryType.MAX_VARBINARY_LENGTH, VarBinaryType.INSTANCE.len);

        // createVarBinaryType behavior
        VarBinaryType t1 = VarBinaryType.createVarBinaryType(128);
        Assertions.assertEquals(128, t1.len);
        VarBinaryType t2 = VarBinaryType.createVarBinaryType(VarBinaryType.MAX_VARBINARY_LENGTH);
        Assertions.assertSame(VarBinaryType.MAX_VARBINARY_TYPE, t2);
        VarBinaryType t3 = VarBinaryType.createVarBinaryType(-1);
        Assertions.assertSame(VarBinaryType.MAX_VARBINARY_TYPE, t3);
    }

    @Test
    public void testEqualsHashCode() {
        VarBinaryType a = new VarBinaryType(32);
        VarBinaryType b = new VarBinaryType(32);
        VarBinaryType c = new VarBinaryType(64);
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        Assertions.assertNotEquals(a, c);
        Assertions.assertNotEquals(a.hashCode(), c.hashCode());
    }

    @Test
    public void testAcceptsTypeAndDefaultConcrete() {
        Assertions.assertTrue(VarBinaryType.INSTANCE.acceptsType(new VarBinaryType(10)));
        Assertions.assertFalse(VarBinaryType.INSTANCE.acceptsType(VarcharType.createVarcharType(10)));
        Assertions.assertSame(VarBinaryType.INSTANCE, VarBinaryType.INSTANCE.defaultConcreteType());
    }

    @Test
    public void testToCatalogDataTypeAndWidth() {
        VarBinaryType t = new VarBinaryType(256);
        Type catalog = t.toCatalogDataType();
        Assertions.assertTrue(catalog instanceof ScalarType);
        ScalarType sc = (ScalarType) catalog;
        Assertions.assertEquals(256, sc.getLength());
        Assertions.assertEquals(256, t.width());
        // byte size synced with length
        Assertions.assertEquals(256, sc.getByteSize());
    }

    @Test
    public void testToSqlAndSimpleString() {
        VarBinaryType t = new VarBinaryType(77);
        Assertions.assertEquals("VARBINARY(77)", t.toSql());
        Assertions.assertEquals("varbinary", t.simpleString());

        Assertions.assertEquals("VARBINARY(" + VarBinaryType.MAX_VARBINARY_LENGTH + ")",
                VarBinaryType.createVarBinaryType(-1).toSql());
    }

    @Test
    public void testIsWildcard() {
        Assertions.assertTrue(VarBinaryType.createVarBinaryType(-1).isWildcardVarBinary());
        Assertions.assertTrue(VarBinaryType.createVarBinaryType(VarBinaryType.MAX_VARBINARY_LENGTH).isWildcardVarBinary());
        Assertions.assertFalse(new VarBinaryType(10).isWildcardVarBinary());
    }
}
