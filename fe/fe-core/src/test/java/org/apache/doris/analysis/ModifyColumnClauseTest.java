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

import mockit.Expectations;
import mockit.Mocked;
import org.apache.doris.catalog.PrimitiveType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;

public class ModifyColumnClauseTest {
    private static Analyzer analyzer;

    @BeforeClass
    public static void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
    }

    @Test
    public void testNormal(@Mocked ColumnDef definition) throws AnalysisException {
        Column column = new Column("tsetCol", PrimitiveType.INT);
        new Expectations() {
            {
                definition.analyze(true);
                minTimes = 0;

                definition.toSql();
                minTimes = 0;
                result = "`testCol` INT";

                definition.toColumn();
                minTimes = 0;
                result = column;
            }
        };

        ModifyColumnClause clause = new ModifyColumnClause(definition, null, null, null);
        clause.analyze(analyzer);
        Assert.assertEquals("MODIFY COLUMN `testCol` INT", clause.toString());
        Assert.assertEquals(column, clause.getColumn());
        Assert.assertNull(clause.getProperties());
        Assert.assertNull(clause.getColPos());
        Assert.assertNull(clause.getRollupName());

        clause = new ModifyColumnClause(definition, ColumnPosition.FIRST, null, null);
        clause.analyze(analyzer);
        Assert.assertEquals("MODIFY COLUMN `testCol` INT FIRST", clause.toString());
        Assert.assertEquals(ColumnPosition.FIRST, clause.getColPos());

        clause = new ModifyColumnClause(definition, new ColumnPosition("testCol2"), null, null);
        clause.analyze(analyzer);
        Assert.assertEquals("MODIFY COLUMN `testCol` INT AFTER `testCol2`", clause.toString());

        clause = new ModifyColumnClause(definition, new ColumnPosition("testCol2"), "testRollup", null);
        clause.analyze(analyzer);
        Assert.assertEquals("MODIFY COLUMN `testCol` INT AFTER `testCol2` IN `testRollup`", clause.toString());
    }

    @Test(expected = AnalysisException.class)
    public void testNoColDef() throws AnalysisException {
        ModifyColumnClause clause = new ModifyColumnClause(null, null, null, null);
        clause.analyze(analyzer);
        Assert.fail("No exception throws.");
    }
}