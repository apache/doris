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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.parser.ParseDialect;
import org.apache.doris.nereids.parser.ParserContext;
import org.apache.doris.nereids.parser.trino.TrinoParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;

/**
 * Plan parse checker for trino.
 * It supports equals or contain pattern match assert and so on.
 */
public class TrinoDialectPlanParseChecker extends ParseChecker {

    private final Supplier<LogicalPlan> parsedPlanSupplier;

    public TrinoDialectPlanParseChecker(String sql) {
        super(sql);
        this.parsedPlanSupplier =
                Suppliers.memoize(() -> TrinoParser.parseSingle(sql, new ParserContext(ParseDialect.TRINO_395)));
    }

    public TrinoDialectPlanParseChecker assertEquals(LogicalPlan plan) {
        LogicalPlan target = parsedPlanSupplier.get();
        Assertions.assertEquals(plan, target);
        return this;
    }

    public TrinoDialectPlanParseChecker assertContains(String... expects) {
        LogicalPlan logicalPlan = parsedPlanSupplier.get();
        Assertions.assertNotNull(logicalPlan);
        String targetPlanString = logicalPlan.toString();
        for (String expected : expects) {
            Assertions.assertTrue(StringUtils.containsIgnoreCase(targetPlanString.toLowerCase(), expected),
                    "expected contain is: " + expected + " but plan is \n" + targetPlanString);
        }
        return this;
    }
}
