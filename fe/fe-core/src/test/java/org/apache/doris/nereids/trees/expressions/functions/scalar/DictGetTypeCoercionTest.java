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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.DdlException;
import org.apache.doris.dictionary.Dictionary;
import org.apache.doris.dictionary.DictionaryManager;
import org.apache.doris.dictionary.LayoutType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BitmapType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

public class DictGetTypeCoercionTest {
    private Dictionary dictionary;
    private MockedStatic<Env> mockedEnv;

    @BeforeEach
    public void setUp() throws DdlException {
        Env env = Mockito.mock(Env.class);
        DictionaryManager manager = Mockito.mock(DictionaryManager.class);
        dictionary = Mockito.mock(Dictionary.class);
        Mockito.when(env.getDictionaryManager()).thenReturn(manager);
        Mockito.when(manager.getDictionary("db", "dict")).thenReturn(dictionary);
        Mockito.when(dictionary.getLayout()).thenReturn(LayoutType.HASH_MAP);
        Mockito.when(dictionary.getDicColumns()).thenReturn(List.of());
        Mockito.when(dictionary.getColumnType("value")).thenReturn(IntegerType.INSTANCE);
        Mockito.when(dictionary.getOriginColumn("value")).thenReturn(new Column("value", Type.INT));
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
    }

    @AfterEach
    public void tearDown() {
        mockedEnv.close();
    }

    @Test
    public void testUnsupportedKeyCastsAreRejected() {
        assertRejectedKeyCast(BitmapType.INSTANCE, IntegerType.INSTANCE);
    }

    @Test
    public void testTimestampNsKeysCannotBypassImplicitConversionRestrictions() {
        assertRejectedKeyCast(TimeStampNsType.INSTANCE, DateTimeV2Type.MAX);
        assertRejectedKeyCast(TimeStampNsType.INSTANCE, BigIntType.INSTANCE);
    }

    @Test
    public void testDictGetKeepsSupportedKeyCasts() {
        assertDictGetCast(BigIntType.INSTANCE, IntegerType.INSTANCE);
        assertDictGetCast(DateTimeV2Type.of(3), TimeStampTzType.of(3));
        assertDictGetCast(DateV2Type.INSTANCE, TimeStampNsType.INSTANCE);
    }

    @Test
    public void testDictGetManyCastsEachKeyToItsDictionaryType() {
        Mockito.when(dictionary.getKeyColumnTypes()).thenReturn(
                List.of(IntegerType.INSTANCE, TimeStampTzType.of(3), TimeStampNsType.INSTANCE));
        StructType queryType = new StructType(List.of(
                new StructField("id", BigIntType.INSTANCE, true, ""),
                new StructField("time", DateTimeV2Type.of(3), true, ""),
                new StructField("ns", TimeStampNsType.INSTANCE, true, "")));
        Expression result = TypeCoercionUtils.processBoundFunction(dictGetMany(queryType));

        Assertions.assertInstanceOf(Cast.class, result.child(2));
        StructType converted = (StructType) result.child(2).getDataType();
        Assertions.assertEquals(IntegerType.INSTANCE, converted.getFields().get(0).getDataType());
        Assertions.assertEquals(TimeStampTzType.of(3), converted.getFields().get(1).getDataType());
        Assertions.assertEquals(TimeStampNsType.INSTANCE, converted.getFields().get(2).getDataType());
        Assertions.assertTrue(result.checkInputDataTypes().success());
    }

    private void assertRejectedKeyCast(DataType sourceType, DataType targetType) {
        Mockito.when(dictionary.getKeyColumnTypes()).thenReturn(List.of(targetType));
        AnalysisException singleError = Assertions.assertThrows(AnalysisException.class,
                () -> TypeCoercionUtils.processBoundFunction(dictGet(sourceType)));
        Assertions.assertEquals("dict_get() cannot implicitly cast key from " + sourceType + " to " + targetType,
                singleError.getMessage());

        StructType queryType = new StructType(List.of(new StructField("key", sourceType, true, "")));
        AnalysisException manyError = Assertions.assertThrows(AnalysisException.class,
                () -> TypeCoercionUtils.processBoundFunction(dictGetMany(queryType)));
        Assertions.assertEquals("dict_get_many() cannot implicitly cast key at position 1 from "
                + sourceType + " to " + targetType, manyError.getMessage());
    }

    private void assertDictGetCast(DataType sourceType, DataType targetType) {
        Mockito.when(dictionary.getKeyColumnTypes()).thenReturn(List.of(targetType));
        Expression result = TypeCoercionUtils.processBoundFunction(dictGet(sourceType));
        Assertions.assertInstanceOf(Cast.class, result.child(2));
        Assertions.assertEquals(targetType, result.child(2).getDataType());
        Assertions.assertTrue(result.checkInputDataTypes().success());
    }

    private DictGet dictGet(DataType sourceType) {
        return new DictGet(new StringLiteral("db.dict"), new StringLiteral("value"),
                new SlotReference("key", sourceType));
    }

    private DictGetMany dictGetMany(StructType sourceType) {
        return new DictGetMany(new StringLiteral("db.dict"), new ArrayLiteral(List.of(new StringLiteral("value"))),
                new SlotReference("keys", sourceType));
    }
}
