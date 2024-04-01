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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TestIcebergPredict {

    public static Schema schema;

    @BeforeClass
    public static void before() throws AnalysisException {
        schema = new Schema(
            Types.NestedField.required(1, "c_int", Types.IntegerType.get()),
            Types.NestedField.required(2, "c_long", Types.LongType.get()),
            Types.NestedField.required(3, "c_bool", Types.BooleanType.get()),
            Types.NestedField.required(4, "c_float", Types.FloatType.get()),
            Types.NestedField.required(5, "c_double", Types.DoubleType.get()),
            Types.NestedField.required(6, "c_dec", Types.DecimalType.of(20, 10)),
            Types.NestedField.required(7, "c_date", Types.DateType.get()),
            Types.NestedField.required(8, "c_ts", Types.TimestampType.withoutZone()),
            Types.NestedField.required(10, "c_str", Types.StringType.get())
        );
    }

    @Test
    public void testBinaryPredicate() throws AnalysisException {
        List<LiteralExpr> literalList = new ArrayList<LiteralExpr>() {{
                add(new BoolLiteral(true));
                add(new DateLiteral("2023-01-02", Type.DATEV2));
                add(new DateLiteral("2024-01-02 12:34:56.123456", Type.DATETIMEV2));
                add(new DecimalLiteral(new BigDecimal("1.23")));
                add(new FloatLiteral(1.23, Type.FLOAT));
                add(new FloatLiteral(3.456, Type.DOUBLE));
                add(new IntLiteral(1, Type.TINYINT));
                add(new IntLiteral(1, Type.SMALLINT));
                add(new IntLiteral(1, Type.INT));
                add(new IntLiteral(1, Type.BIGINT));
                add(new StringLiteral("abc"));
                add(new StringLiteral("2023-01-02"));
                add(new StringLiteral("2023-01-02 01:02:03.456789"));
            }};

        List<SlotRef> slotRefs = new ArrayList<SlotRef>() {{
                add(new SlotRef(new TableName(), "c_int"));
                add(new SlotRef(new TableName(), "c_long"));
                add(new SlotRef(new TableName(), "c_bool"));
                add(new SlotRef(new TableName(), "c_float"));
                add(new SlotRef(new TableName(), "c_double"));
                add(new SlotRef(new TableName(), "c_dec"));
                add(new SlotRef(new TableName(), "c_date"));
                add(new SlotRef(new TableName(), "c_ts"));
                add(new SlotRef(new TableName(), "c_str"));
            }};

        // true indicates support for pushdown
        Boolean[][] expects = new Boolean[][] {
            { // int
                false, false, false, false, false, false, true, true, true, true, false, false, false
            },
            { // long
                false, false, false, false, false, false, true, true, true, true, false, false, false
            },
            { // boolean
                true, false, false, false, false, false, false, false, false, false, false, false, false
            },
            { // float
                false, false, false, false, true, false, true, true, true, true, false, false, false
            },
            { // double
                false, false, false, true, true, true, true, true, true, true, false, false, false
            },
            { // decimal
                false, false, false, true, true, true, true, true, true, true, false, false, false
            },
            { // date
                false, false, false, false, false, false, true, true, true, true, false, true, false
            },
            { // timestamp
                false, true, true, false, false, false, false, false, false, true, false, false, false
            },
            { // string
                true, true, true, true, false, false, false, false, false, false, true, true, true
            }
        };

        for (int i = 0; i < slotRefs.size(); i++) {
            final int loc = i;
            List<Boolean> ret = literalList.stream().map(literal -> {
                BinaryPredicate expr = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRefs.get(loc), literal);
                Expression expression = IcebergUtils.convertToIcebergExpr(expr, schema);
                return expression != null;
            }).collect(Collectors.toList());
            Assert.assertArrayEquals(expects[i], ret.toArray());
        }
    }
}
