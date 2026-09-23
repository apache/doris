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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DictGetManyTest {
    @Test
    public void testRejectNullQueryKeyStruct() {
        StructType structType = new StructType(ImmutableList.of(
                new StructField("k", IntegerType.INSTANCE, true, "")));
        DictGetMany function = new DictGetMany(new StringLiteral("db.dict"),
                new ArrayLiteral(ImmutableList.of(new StringLiteral("value"))),
                new Cast(new NullLiteral(), structType));

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                function::checkLegalityBeforeTypeCoercion);
        Assertions.assertEquals("dict_get_many() query_key_values argument cannot be NULL",
                exception.getMessage());
    }

    @Test
    public void testAllowNullFieldInNonNullQueryKeyStruct() {
        DictGetMany function = new DictGetMany(new StringLiteral("db.dict"),
                new ArrayLiteral(ImmutableList.of(new StringLiteral("value"))),
                new StructLiteral(ImmutableList.of(new NullLiteral(IntegerType.INSTANCE))));

        Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion);
    }

    @Test
    public void testRejectNonArrayValueColumnNames() {
        DictGetMany function = new DictGetMany(new StringLiteral("db.dict"), new StringLiteral("value"),
                new StructLiteral(ImmutableList.of(new BigIntLiteral(1))));

        assertRejected(function, "second argument must be a constant ARRAY<VARCHAR>");
    }

    @Test
    public void testRejectNonStructQueryKeyValues() {
        DictGetMany function = new DictGetMany(new StringLiteral("db.dict"),
                new ArrayLiteral(ImmutableList.of(new StringLiteral("value"))), new BigIntLiteral(1));

        assertRejected(function, "third argument must be a STRUCT");
    }

    @Test
    public void testRejectNonStringValueColumnNames() {
        DictGetMany function = new DictGetMany(new StringLiteral("db.dict"),
                new ArrayLiteral(ImmutableList.of(new BigIntLiteral(1))),
                new StructLiteral(ImmutableList.of(new BigIntLiteral(1))));

        assertRejected(function, "second argument must be a constant ARRAY<VARCHAR>");
    }

    private void assertRejected(DictGetMany function, String expectedMessage) {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                function::checkLegalityBeforeTypeCoercion);
        Assertions.assertTrue(exception.getMessage().contains(expectedMessage), exception.getMessage());
    }
}
