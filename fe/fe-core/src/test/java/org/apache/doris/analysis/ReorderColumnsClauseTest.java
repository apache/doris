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

import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class ReorderColumnsClauseTest {
    private static Analyzer analyzer;

    @BeforeClass
    public static void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
    }

    @Test
    public void testNormal() throws AnalysisException {
        List<String> cols = Lists.newArrayList("col1", "col2");
        ReorderColumnsClause clause = new ReorderColumnsClause(cols, "", null);
        clause.analyze(analyzer);
        Assert.assertEquals("ORDER BY `col1`, `col2`", clause.toString());
        Assert.assertEquals(cols, clause.getColumnsByPos());
        Assert.assertNull(clause.getProperties());
        Assert.assertNull(clause.getRollupName());

        clause = new ReorderColumnsClause(cols, "rollup", null);
        clause.analyze(analyzer);
        Assert.assertEquals("ORDER BY `col1`, `col2` IN `rollup`", clause.toString());
    }

    @Test(expected = AnalysisException.class)
    public void testNoCol() throws AnalysisException {
        ReorderColumnsClause clause = new ReorderColumnsClause(null, "", null);
        clause.analyze(analyzer);
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testColEmpty() throws AnalysisException {
        List<String> cols = Lists.newArrayList("col1", "");
        ReorderColumnsClause clause = new ReorderColumnsClause(cols, "", null);
        clause.analyze(analyzer);
        Assert.fail("No exception throws.");
    }
}
