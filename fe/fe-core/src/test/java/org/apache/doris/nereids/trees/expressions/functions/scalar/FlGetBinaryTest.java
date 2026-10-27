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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.RewriteWhenAnalyze;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.FileType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarBinaryType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

class FlGetBinaryTest {
    @Test
    void testNullableBinarySignature() {
        for (Expression input : Arrays.asList(new SlotReference("file", FileType.INSTANCE),
                new NullLiteral(FileType.INSTANCE), new NullLiteral())) {
            Expression function = TypeCoercionUtils.processBoundFunction(new FlGetBinary(input));
            Assertions.assertEquals(VarBinaryType.INSTANCE, function.getDataType());
            Assertions.assertTrue(function.nullable());
            Assertions.assertFalse(function instanceof RewriteWhenAnalyze);
        }
    }

    @Test
    void testRejectsOtherArgumentTypes() {
        for (DataType type : Arrays.asList(IntegerType.INSTANCE, StringType.INSTANCE, JsonType.INSTANCE,
                VariantType.INSTANCE, FileType.INSTANCE.publicStructType(), ArrayType.of(FileType.INSTANCE))) {
            Assertions.assertThrows(AnalysisException.class,
                    () -> TypeCoercionUtils.processBoundFunction(new FlGetBinary(new SlotReference("value", type))));
        }
    }

    @Test
    void testFileInlineSelectorRejected() {
        for (Expression input : Arrays.asList(new SlotReference("file", FileType.INSTANCE),
                new NullLiteral(FileType.INSTANCE))) {
            for (String name : Arrays.asList("inline", "INLINE", "InLiNe")) {
                AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                        () -> TypeCoercionUtils.processBoundFunction(new ElementAt(input, new StringLiteral(name))));
                Assertions.assertTrue(error.getMessage().contains("use FL_GET_BINARY"));
            }
        }
        Expression struct = new SlotReference("value", FileType.INSTANCE.publicStructType());
        Assertions.assertEquals(VarBinaryType.INSTANCE,
                TypeCoercionUtils.processBoundFunction(new ElementAt(struct, new StringLiteral("inline")))
                        .getDataType());
    }
}
