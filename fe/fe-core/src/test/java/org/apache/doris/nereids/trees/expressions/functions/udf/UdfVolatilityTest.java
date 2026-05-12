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

import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.catalog.FunctionVolatility;
import org.apache.doris.nereids.rules.rewrite.AddProjectForUniqueFunction;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.VolatileIdentity;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class UdfVolatilityTest {

    @Test
    void testImmutablePythonUdfIsNotVolatileExpression() {
        PythonUdf udf = pythonUdf(FunctionVolatility.IMMUTABLE, VolatileIdentity.NON_VOLATILE);

        Assertions.assertTrue(udf.isDeterministic());
        Assertions.assertFalse(udf.containsVolatileExpression());
        Assertions.assertEquals(PythonUdf.class, new PythonUdfBuilder(udf).functionClass());
    }

    @Test
    void testVolatilePythonUdfUsesUniqueIdentity() {
        PythonUdf first = pythonUdf(FunctionVolatility.VOLATILE, VolatileIdentity.newVolatileIdentity());
        PythonUdf second = pythonUdf(FunctionVolatility.VOLATILE, VolatileIdentity.newVolatileIdentity());

        Assertions.assertFalse(first.isDeterministic());
        Assertions.assertTrue(first.containsVolatileExpression());
        Assertions.assertNotEquals(first, second);

        Expression ignoredFirst = ExpressionUtils.setIgnoreUniqueIdForUniqueFunc(first, true);
        Expression ignoredSecond = ExpressionUtils.setIgnoreUniqueIdForUniqueFunc(second, true);
        Assertions.assertEquals(ignoredFirst, ignoredSecond);
    }

    @Test
    void testVolatileAndImmutableUdfAreNotEqual() {
        PythonUdf immutable = pythonUdf(FunctionVolatility.IMMUTABLE, VolatileIdentity.NON_VOLATILE);
        PythonUdf volatileUdf = pythonUdf(FunctionVolatility.VOLATILE, VolatileIdentity.newVolatileIdentity());

        Assertions.assertNotEquals(immutable, volatileUdf);
        Assertions.assertNotEquals(volatileUdf, immutable);
    }

    @Test
    void testAddProjectForRepeatedVolatileUdf() {
        PythonUdf udf = pythonUdf(FunctionVolatility.VOLATILE, VolatileIdentity.newVolatileIdentity());
        List<NamedExpression> aliases = new AddProjectForUniqueFunction()
                .tryGenUniqueFunctionAlias(ImmutableList.of(udf, udf));

        Assertions.assertEquals(1, aliases.size());
        Assertions.assertEquals(udf, aliases.get(0).child(0));
    }

    @Test
    void testJavaUdfVolatility() {
        JavaUdf udf = javaUdf(FunctionVolatility.STABLE, VolatileIdentity.NON_VOLATILE);

        Assertions.assertFalse(udf.isDeterministic());
        Assertions.assertFalse(udf.containsVolatileExpression());
    }

    private PythonUdf pythonUdf(FunctionVolatility volatility, VolatileIdentity volatileIdentity) {
        return new PythonUdf("py_fn", 1, "db1", Function.BinaryType.PYTHON_UDF, signature(),
                NullableMode.ALWAYS_NULLABLE, volatility, volatileIdentity,
                null, "evaluate", null, null, "", false, 360, "3.10.2", "",
                new IntegerLiteral(1));
    }

    private JavaUdf javaUdf(FunctionVolatility volatility, VolatileIdentity volatileIdentity) {
        return new JavaUdf("java_fn", 1, "db1", Function.BinaryType.JAVA_UDF, signature(),
                NullableMode.ALWAYS_NULLABLE, volatility, volatileIdentity,
                null, "evaluate", null, null, "", false, 360, new IntegerLiteral(1));
    }

    private FunctionSignature signature() {
        return FunctionSignature.ret(IntegerType.INSTANCE).args(IntegerType.INSTANCE);
    }
}
