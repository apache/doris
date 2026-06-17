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

package org.apache.doris.analysis;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DefaultValueExpressionValidatorTest extends TestWithFeService {

    @Override
    public void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
    }

    @Test
    public void testValidateDefaultValueExpressionSqlOk() throws AnalysisException {
        ColumnDef.validateDefaultValue(ScalarType.DATEV2,
                "to_date(now())",
                DefaultValueExprDef.fromSql("to_date(now())"));

        ColumnDef.validateDefaultValue(ScalarType.createDatetimeV2Type(3),
                "now(3)",
                DefaultValueExprDef.fromSql("now(3)"));
    }

    @Test
    public void testValidateDefaultValueExpressionSqlRejectsColumnReference() {
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> ColumnDef.validateDefaultValue(Type.INT,
                        "k1 + 1",
                        DefaultValueExprDef.fromSql("k1 + 1")));
        Assertions.assertTrue(ex.getMessage().contains("column reference"));
    }

    @Test
    public void testValidateDefaultValueExpressionSqlRejectsRand() {
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> ColumnDef.validateDefaultValue(Type.DOUBLE,
                        "rand()",
                        DefaultValueExprDef.fromSql("rand()")));
        Assertions.assertTrue(ex.getMessage().toLowerCase().contains("non-deterministic"));
    }

    @Test
    public void testValidateDefaultValueExpressionSqlRejectsAggregateFunction() {
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> ColumnDef.validateDefaultValue(Type.BIGINT,
                        "sum(1)",
                        DefaultValueExprDef.fromSql("sum(1)")));
        String msg = ex.getMessage().toLowerCase();
        Assertions.assertTrue(msg.contains("aggregate") || msg.contains("sum"));
    }
}
