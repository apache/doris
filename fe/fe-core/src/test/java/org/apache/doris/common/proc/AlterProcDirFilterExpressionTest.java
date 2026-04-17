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

package org.apache.doris.common.proc;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.types.DateTimeV2Type;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class AlterProcDirFilterExpressionTest {

    @Test
    public void testSchemaChangeFilterResultExpressionWithAnd() throws AnalysisException {
        SchemaChangeProcDir schemaChangeProcDir = new SchemaChangeProcDir(null, null);
        HashMap<String, Expression> filter = buildCreateTimeRangeFilter();

        Assert.assertTrue(schemaChangeProcDir.filterResultExpression(
                "CreateTime", "2026-04-17 10:44:34.380", filter));
        Assert.assertFalse(schemaChangeProcDir.filterResultExpression(
                "CreateTime", "2026-04-17 10:44:23.070", filter));
    }

    @Test
    public void testRollupFilterResultExpressionWithAnd() throws AnalysisException {
        RollupProcDir rollupProcDir = new RollupProcDir(null, null);
        HashMap<String, Expression> filter = buildCreateTimeRangeFilter();

        Assert.assertTrue(rollupProcDir.filterResultExpression(
                "CreateTime", "2026-04-17 10:44:34.380", filter));
        Assert.assertFalse(rollupProcDir.filterResultExpression(
                "CreateTime", "2026-04-17 10:44:23.070", filter));
    }

    private HashMap<String, Expression> buildCreateTimeRangeFilter() {
        Expression rangeFilter = new And(
                new GreaterThanEqual(new UnboundSlot("CreateTime"),
                        new DateTimeV2Literal(DateTimeV2Type.MAX, "2026-04-17 10:44:34")),
                new LessThanEqual(new UnboundSlot("CreateTime"),
                        new DateTimeV2Literal(DateTimeV2Type.MAX, "2026-04-17 10:44:36")));
        HashMap<String, Expression> filter = new HashMap<>();
        filter.put("createtime", rangeFilter);
        return filter;
    }
}
