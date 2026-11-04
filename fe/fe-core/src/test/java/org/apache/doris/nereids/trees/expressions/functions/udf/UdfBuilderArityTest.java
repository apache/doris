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

package org.apache.doris.nereids.trees.expressions.functions.udf;

import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class UdfBuilderArityTest {

    @Test
    void testVariadicMetadataDoesNotEnableVariableArity() {
        JavaUdf javaUdf = Mockito.mock(JavaUdf.class);
        Mockito.when(javaUdf.hasVarArguments()).thenReturn(true);
        Mockito.when(javaUdf.arity()).thenReturn(2);
        assertFixedArity(new JavaUdfBuilder(javaUdf));

        JavaUdaf javaUdaf = Mockito.mock(JavaUdaf.class);
        Mockito.when(javaUdaf.hasVarArguments()).thenReturn(true);
        Mockito.when(javaUdaf.arity()).thenReturn(2);
        assertFixedArity(new JavaUdafBuilder(javaUdaf));

        JavaUdtf javaUdtf = Mockito.mock(JavaUdtf.class);
        Mockito.when(javaUdtf.hasVarArguments()).thenReturn(true);
        Mockito.when(javaUdtf.arity()).thenReturn(2);
        assertFixedArity(new JavaUdtfBuilder(javaUdtf));

        PythonUdf pythonUdf = Mockito.mock(PythonUdf.class);
        Mockito.when(pythonUdf.hasVarArguments()).thenReturn(true);
        Mockito.when(pythonUdf.arity()).thenReturn(2);
        assertFixedArity(new PythonUdfBuilder(pythonUdf));

        PythonUdaf pythonUdaf = Mockito.mock(PythonUdaf.class);
        Mockito.when(pythonUdaf.hasVarArguments()).thenReturn(true);
        Mockito.when(pythonUdaf.arity()).thenReturn(2);
        assertFixedArity(new PythonUdafBuilder(pythonUdaf));

        PythonUdtf pythonUdtf = Mockito.mock(PythonUdtf.class);
        Mockito.when(pythonUdtf.hasVarArguments()).thenReturn(true);
        Mockito.when(pythonUdtf.arity()).thenReturn(2);
        assertFixedArity(new PythonUdtfBuilder(pythonUdtf));
    }

    private void assertFixedArity(UdfBuilder builder) {
        Assertions.assertFalse(builder.canApply(ImmutableList.of(new IntegerLiteral(1))));
        Assertions.assertTrue(builder.canApply(ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(2))));
        Assertions.assertFalse(builder.canApply(
                ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(2), new IntegerLiteral(3))));
    }
}
