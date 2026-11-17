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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.functions.window.LastValue;
import org.apache.doris.nereids.trees.expressions.functions.window.RowNumber;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.FileType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class FileKeyInputTypeTest {
    private static final List<DataType> FILE_TYPES = ImmutableList.of(
            FileType.INSTANCE, ArrayType.of(FileType.INSTANCE),
            MapType.of(StringType.INSTANCE, FileType.INSTANCE),
            new StructType(ImmutableList.of(new StructField("f", FileType.INSTANCE, true, ""))),
            new StructType(ImmutableList.of(new StructField("f", ArrayType.of(FileType.INSTANCE), true, ""))));

    @Test
    public void testInSubqueryRejectsFileOnEitherSide() {
        for (DataType type : FILE_TYPES) {
            InSubquery compareFile = inSubquery(new SlotReference("f", type), IntegerType.INSTANCE);
            assertRejected(compareFile, "IN does not support FILE");
            InSubquery outputFile = inSubquery(new SlotReference("n", IntegerType.INSTANCE), type);
            assertRejected(outputFile, "IN does not support FILE");
        }
    }

    @Test
    public void testWindowRejectsFilePartitionAndOrderKeys() {
        for (DataType type : FILE_TYPES) {
            SlotReference key = new SlotReference("f", type);
            WindowExpression partition = new WindowExpression(new RowNumber(),
                    ImmutableList.of(key), ImmutableList.of());
            assertRejected(partition, "window PARTITION BY does not support FILE");
            WindowExpression order = new WindowExpression(new RowNumber(), ImmutableList.of(),
                    ImmutableList.of(new OrderExpression(new OrderKey(key, true, true))));
            assertRejected(order, "window ORDER BY does not support FILE");
        }
    }

    @Test
    public void testRecursiveValidationRejectsFileKeys() {
        SlotReference file = new SlotReference("f", FileType.INSTANCE);
        Alias subquery = new Alias(inSubquery(file, FileType.INSTANCE), "matched");
        assertRejected(subquery, "IN does not support FILE");
        Alias window = new Alias(new WindowExpression(new RowNumber(),
                ImmutableList.of(file), ImmutableList.of()), "ranked");
        assertRejected(window, "window PARTITION BY does not support FILE");
    }

    @Test
    public void testOrdinaryKeysAndFileWindowResultsRemainSupported() {
        for (DataType type : ImmutableList.of(IntegerType.INSTANCE, StringType.INSTANCE)) {
            SlotReference key = new SlotReference("k", type);
            Assertions.assertTrue(inSubquery(key, type).checkInputDataTypes().success());
            WindowExpression window = new WindowExpression(new LastValue(new SlotReference("f", FileType.INSTANCE)),
                    ImmutableList.of(key), ImmutableList.of(new OrderExpression(new OrderKey(key, true, true))));
            Assertions.assertTrue(window.checkInputDataTypes().success());
            Assertions.assertEquals(FileType.INSTANCE, window.getDataType());
        }
    }

    private void assertRejected(Expression expression, String expectedMessage) {
        AnalysisException error = Assertions.assertThrows(AnalysisException.class, expression::checkInputDataTypes);
        Assertions.assertTrue(error.getMessage().startsWith(expectedMessage), error.getMessage());
    }

    private InSubquery inSubquery(Expression compare, DataType outputType) {
        LogicalOneRowRelation query = new LogicalOneRowRelation(StatementScopeIdGenerator.newRelationId(),
                ImmutableList.of(new Alias(new SlotReference("output", outputType), "output")));
        return new InSubquery(compare, query, false);
    }
}
