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

import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Arithmetic on a decimal wider than decimal128 (a decimal256 column, or the output of a view created with
 * enable_decimal256 on) while enable_decimal256 is off.
 */
public class ViewDecimal256OperandTest extends TestWithFeService {

    private static final DecimalV3Type DECIMAL128 = DecimalV3Type.createDecimalV3Type(38, 10);

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("view_decimal256_operand");
        connectContext.setDatabase("view_decimal256_operand");
        connectContext.getSessionVariable().enableDecimal256 = true;
        createTables(
                "CREATE TABLE IF NOT EXISTS t_view_dec256 (\n"
                        + "    k int,\n"
                        + "    d decimal(38, 10),\n"
                        + "    d256 decimal(76, 10),\n"
                        + "    m map<string, decimal(38, 10)>\n"
                        + ")\n"
                        + "DUPLICATE KEY(k)\n"
                        + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n"
        );
        createView("CREATE VIEW v_dec256_sum AS SELECT sum(d) AS s FROM t_view_dec256");
        createView("CREATE VIEW v_dec256_sum_map AS SELECT sum_map(m) AS sm FROM t_view_dec256");
        createView("CREATE VIEW v_dec256_avg_map AS SELECT avg_map(m) AS am FROM t_view_dec256");
        connectContext.getSessionVariable().enableDecimal256 = false;
    }

    private DataType outputType(String sql) {
        return PlanChecker.from(connectContext).analyze(sql).getPlan().getOutput().get(0).getDataType();
    }

    @Test
    public void testArithmeticOnWideOperandInDecimal128Mode() {
        Assertions.assertEquals(DECIMAL128, outputType(
                "SELECT element_at(map_values(sm), 1) + cast(1 as decimal(38, 10)) FROM v_dec256_sum_map"));
        Assertions.assertEquals(DECIMAL128, outputType(
                "SELECT element_at(map_values(am), 1) + cast(1 as decimal(38, 10)) FROM v_dec256_avg_map"));
        Assertions.assertEquals(DECIMAL128, outputType(
                "SELECT s - cast(1 as decimal(38, 10)) FROM v_dec256_sum"));
        Assertions.assertEquals(DECIMAL128, outputType("SELECT d256 + 1 FROM t_view_dec256"));
        Assertions.assertEquals(DECIMAL128, outputType(
                "SELECT d256 % cast(1 as decimal(38, 10)) FROM t_view_dec256"));
        // the view output itself keeps the type analyzed when the view was created
        Assertions.assertEquals(DecimalV3Type.createDecimalV3TypeNoCheck(76, 10),
                ((MapType) outputType("SELECT sm FROM v_dec256_sum_map")).getValueType());
    }

    @Test
    public void testArithmeticOnWideOperandInDecimal256Mode() {
        connectContext.getSessionVariable().enableDecimal256 = true;
        try {
            Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(76, 10),
                    outputType("SELECT d256 + 1 FROM t_view_dec256"));
        } finally {
            connectContext.getSessionVariable().enableDecimal256 = false;
        }
    }
}
