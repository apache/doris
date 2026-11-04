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
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.FileType;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ToFileTest {
    @Test
    void rejectsResourceExpressionsAndImplicitUriParsing() {
        for (Expression resource : ImmutableList.of(NullLiteral.INSTANCE, new IntegerLiteral(1),
                new SlotReference("resource", StringType.INSTANCE))) {
            Assertions.assertThrows(AnalysisException.class,
                    () -> new ToFile(resource, new StringLiteral("s3://bucket/key"))
                            .checkLegalityBeforeTypeCoercion());
        }
        Assertions.assertThrows(AnalysisException.class,
                () -> new ToFile(new StringLiteral("s3_resource"), new IntegerLiteral(1))
                        .checkLegalityBeforeTypeCoercion());
    }

    @Test
    void leavesUriValuesUnchangedForTheFilesystem() {
        for (String uri : ImmutableList.of("", "relative path", "ftp://host/key#fragment",
                "S3://Bucket/a/../b%2f?token=value")) {
            StringLiteral argument = new StringLiteral(uri);
            ToFile function = new ToFile(new VarcharLiteral("resource"), argument);
            function.checkLegalityBeforeTypeCoercion();
            Assertions.assertSame(argument, function.getArgument(1));
            Assertions.assertEquals(uri, ((StringLiteral) function.getArgument(1)).getStringValue());
        }
    }

    @Test
    void preservesVolatilityAndNullPropagationAcrossRewrites() {
        StringLiteral resource = new StringLiteral("resource");
        ToFile function = new ToFile(resource, new StringLiteral("s3://bucket/key"));
        Assertions.assertEquals(FileType.INSTANCE, function.getDataType());
        Assertions.assertFalse(function.nullable());
        Assertions.assertFalse(function.foldable());
        Assertions.assertFalse(function.isDeterministic());
        Assertions.assertNotEquals(function, new ToFile(resource, function.getArgument(1)));
        ToFile nullable = function.withChildren(ImmutableList.of(resource, NullLiteral.INSTANCE));
        nullable.checkLegalityBeforeTypeCoercion();
        Assertions.assertTrue(nullable.nullable());
        Assertions.assertEquals(function.getVolatileIdentity(), nullable.getVolatileIdentity());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> function.withChildren(ImmutableList.of(resource)));
    }
}
