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

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.expression.rules.NormalizeElementAt;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.List;

class StructOrdinalTest {
    @Test
    void normalizationMustNotBindWrappedOrdinals() {
        StructType type = new StructType(List.of(new StructField("a", IntegerType.INSTANCE, true, "")));
        SlotReference slot = new SlotReference("s", type);
        // Call the rewrite directly. SQL analysis rejects these values before this rule runs.
        for (String text : List.of("4294967297", "-4294967295",
                "18446744073709551617", "-18446744073709551615")) {
            ElementAt expression = new ElementAt(slot, new LargeIntLiteral(new BigInteger(text)));
            Expression rewritten = Deencapsulation.invoke(NormalizeElementAt.class, "normalize", expression);
            Assertions.assertSame(expression, rewritten);
        }
        Expression legal = Deencapsulation.invoke(NormalizeElementAt.class, "normalize",
                new ElementAt(slot, new LargeIntLiteral(BigInteger.ONE)));
        Assertions.assertEquals(new StringLiteral("a"), legal.child(1));
    }

    @Test
    void preserveSelectorPrecision() {
        // One field is enough. These out-of-range values have low 32 or 64 bits equal to 1.
        StructType type = new StructType(List.of(new StructField("a", IntegerType.INSTANCE, true, "")));
        SlotReference slot = new SlotReference("s", type);
        for (String text : List.of("4294967297", "-4294967295",
                "18446744073709551617", "-18446744073709551615")) {
            IntegerLikeLiteral index = new LargeIntLiteral(new BigInteger(text));
            Assertions.assertNull(type.getFieldByOrdinal(index.getBigDecimalValue()));
            Assertions.assertThrows(AnalysisException.class, () -> new ElementAt(slot, index).getDataType());
        }
        Assertions.assertThrows(AnalysisException.class,
                () -> new ElementAt(slot, new BigIntLiteral(4294967297L)).getDataType());
        // A legal LARGEINT selector must retain the same field type.
        Assertions.assertEquals(IntegerType.INSTANCE,
                new ElementAt(slot, new LargeIntLiteral(BigInteger.ONE)).getDataType());
    }
}
