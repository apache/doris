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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.VarcharType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FixedRangePartitionTest {

    @Test
    public void testSingleColumnIntBounds() {
        List<DataType> types = Collections.singletonList(IntegerType.INSTANCE);
        Expression lower = new IntegerLiteral((byte) 1);
        Expression upper = new IntegerLiteral((byte) 10);

        FixedRangePartition p = new FixedRangePartition(false, "p1",
                Collections.singletonList(lower), Collections.singletonList(upper));
        p.setPartitionTypes(types);
        p.validate(Collections.emptyMap());

        Assertions.assertEquals(1, p.getLowerBounds().size());
        Assertions.assertTrue(p.getLowerBounds().get(0) instanceof IntegerLiteral);
        Assertions.assertEquals(1, ((IntegerLiteral) p.getLowerBounds().get(0)).getValue());
        Assertions.assertTrue(p.getUpperBounds().get(0) instanceof IntegerLiteral);
    }

    @Test
    public void testExtraLowerBoundsRejected() {
        List<DataType> types = Collections.singletonList(IntegerType.INSTANCE);
        // 2 lower values for a single-column table
        List<Expression> lower = new ArrayList<>();
        lower.add(new IntegerLiteral((byte) 1));
        lower.add(new IntegerLiteral((byte) 2));
        List<Expression> upper = Collections.singletonList(new IntegerLiteral((byte) 10));

        FixedRangePartition p = new FixedRangePartition(false, "p1", lower, upper);
        p.setPartitionTypes(types);
        Assertions.assertThrows(AnalysisException.class,
                () -> p.validate(Collections.emptyMap()));
    }

    @Test
    public void testExtraUpperBoundsRejected() {
        List<DataType> types = Collections.singletonList(IntegerType.INSTANCE);
        List<Expression> lower = Collections.singletonList(new IntegerLiteral((byte) 1));
        // 2 upper values for a single-column table
        List<Expression> upper = new ArrayList<>();
        upper.add(new IntegerLiteral((byte) 10));
        upper.add(new IntegerLiteral((byte) 20));

        FixedRangePartition p = new FixedRangePartition(false, "p1", lower, upper);
        p.setPartitionTypes(types);
        Assertions.assertThrows(AnalysisException.class,
                () -> p.validate(Collections.emptyMap()));
    }

    @Test
    public void testDecimalScaleOverflowRejected() {
        // DECIMAL(10,2) column with a bound that has 3 decimal places
        List<DataType> types = Collections.singletonList(DecimalV3Type.createDecimalV3Type(10, 2));
        DecimalV3Literal lower = new DecimalV3Literal(DecimalV3Type.createDecimalV3Type(10, 3), new BigDecimal("10.005"));
        DecimalV3Literal upper = new DecimalV3Literal(DecimalV3Type.createDecimalV3Type(10, 2), new BigDecimal("20.00"));

        FixedRangePartition p = new FixedRangePartition(false, "p1",
                Collections.<Expression>singletonList(lower),
                Collections.<Expression>singletonList(upper));
        p.setPartitionTypes(types);
        Assertions.assertThrows(AnalysisException.class,
                () -> p.validate(Collections.emptyMap()));
    }

    @Test
    public void testDecimalExactScaleAccepted() {
        // DECIMAL(10,2) column with exact-scale bounds (trailing zero ok)
        List<DataType> types = Collections.singletonList(DecimalV3Type.createDecimalV3Type(10, 2));
        DecimalV3Literal lower = new DecimalV3Literal(DecimalV3Type.createDecimalV3Type(10, 2), new BigDecimal("10.00"));
        DecimalV3Literal upper = new DecimalV3Literal(DecimalV3Type.createDecimalV3Type(10, 2), new BigDecimal("20.50"));

        FixedRangePartition p = new FixedRangePartition(false, "p1",
                Collections.<Expression>singletonList(lower),
                Collections.<Expression>singletonList(upper));
        p.setPartitionTypes(types);
        p.validate(Collections.emptyMap());

        Assertions.assertEquals(1, p.getLowerBounds().size());
    }

    @Test
    public void testTypeCoercionDoubleToDecimal() {
        // Double literal coerced to DECIMAL column type
        List<DataType> types = Collections.singletonList(DecimalV3Type.createDecimalV3Type(10, 0));
        DoubleLiteral lower = new DoubleLiteral(1.0);
        DoubleLiteral upper = new DoubleLiteral(10.0);

        FixedRangePartition p = new FixedRangePartition(false, "p1",
                Collections.<Expression>singletonList(lower),
                Collections.<Expression>singletonList(upper));
        p.setPartitionTypes(types);
        p.validate(Collections.emptyMap());

        Assertions.assertEquals(1, p.getLowerBounds().size());
    }

    @Test
    public void testMultiColumnBounds() {
        List<DataType> types = new ArrayList<>();
        types.add(IntegerType.INSTANCE);
        types.add(VarcharType.createVarcharType(100));

        List<Expression> lowers = new ArrayList<>();
        lowers.add(new IntegerLiteral((byte) 1));
        lowers.add(new VarcharLiteral("a"));
        List<Expression> uppers = new ArrayList<>();
        uppers.add(new IntegerLiteral((byte) 10));
        uppers.add(new VarcharLiteral("z"));

        FixedRangePartition p = new FixedRangePartition(false, "p1", lowers, uppers);
        p.setPartitionTypes(types);
        p.validate(Collections.emptyMap());

        Assertions.assertEquals(2, p.getLowerBounds().size());
        Assertions.assertEquals(2, p.getUpperBounds().size());
    }
}
