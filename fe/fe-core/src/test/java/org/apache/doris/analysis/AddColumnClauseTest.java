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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.common.AnalysisException;

public class AddColumnClauseTest {
    private static Analyzer analyzer;

    @Mocked
    ColumnDef definition;

    @BeforeClass
    public static void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
    }

    @Test
    public void testNormal() throws AnalysisException {
        Column column = new Column("testCol", ScalarType.createType(PrimitiveType.INT));
        new Expectations() {
            {
                definition.analyze(true);
                minTimes = 0;

                definition.toSql();
                minTimes = 0;
                result = "`testCol` INT";

                definition.getDefaultValue();
                minTimes = 0;
                result = "";

                definition.getAggregateType();
                minTimes = 0;
                result = null;

                definition.isAllowNull();
                minTimes = 0;
                result = false;

                definition.toColumn();
                minTimes = 0;
                result = column;
            }
        };

        AddColumnClause clause = new AddColumnClause(definition, null, null, null);
        clause.analyze(analyzer);
        Assert.assertEquals("ADD COLUMN `testCol` INT", clause.toString());

        clause = new AddColumnClause(definition, ColumnPosition.FIRST, null, null);
        clause.analyze(analyzer);
        Assert.assertEquals("ADD COLUMN `testCol` INT FIRST", clause.toString());

        clause = new AddColumnClause(definition, new ColumnPosition("testCol2"), null, null);
        clause.analyze(analyzer);
        Assert.assertEquals("ADD COLUMN `testCol` INT AFTER `testCol2`", clause.toString());

        clause = new AddColumnClause(definition, new ColumnPosition("testCol2"), "testRollup", null);
        clause.analyze(analyzer);
        Assert.assertEquals("ADD COLUMN `testCol` INT AFTER `testCol2` IN `testRollup`", clause.toString());
        Assert.assertNull(clause.getProperties());
        Assert.assertEquals(new ColumnPosition("testCol2").toString(), clause.getColPos().toSql());
        Assert.assertEquals("testRollup", clause.getRollupName());
    }

    @Test(expected = AnalysisException.class)
    public void testNoColDef() throws AnalysisException {
        AddColumnClause clause = new AddColumnClause(null, null, null, null);
        clause.analyze(analyzer);
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testNoDefault() throws AnalysisException {
        new Expectations() {
            {
                definition.analyze(true);
                minTimes = 0;

                definition.toSql();
                minTimes = 0;
                result = "`testCol` INT";

                definition.getDefaultValue();
                minTimes = 0;
                result = null;

                definition.getAggregateType();
                minTimes = 0;
                result = null;

                definition.getName();
                minTimes = 0;
                result = "testCol";

                definition.isAllowNull();
                minTimes = 0;
                result = false;
            }
        };
        AddColumnClause clause = new AddColumnClause(definition, null, null, null);
        clause.analyze(analyzer);
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testAggPos() throws AnalysisException {
        new Expectations() {
            {
                definition.analyze(true);
                minTimes = 0;

                definition.toSql();
                minTimes = 0;
                result = "`testCol` INT";

                definition.getDefaultValue();
                minTimes = 0;
                result = null;

                definition.getAggregateType();
                minTimes = 0;
                result = AggregateType.SUM;

                definition.getName();
                minTimes = 0;
                result = "testCol";

                definition.isAllowNull();
                minTimes = 0;
                result = false;
            }
        };
        AddColumnClause clause = new AddColumnClause(definition, ColumnPosition.FIRST, null, null);
        clause.analyze(analyzer);
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testAddValueToFirst() throws AnalysisException {
        new Expectations() {
            {
                definition.analyze(true);
                minTimes = 0;

                definition.toSql();
                minTimes = 0;
                result = "`testCol` INT";

                definition.getDefaultValue();
                minTimes = 0;
                result = "2";

                definition.getAggregateType();
                minTimes = 0;
                result = AggregateType.SUM;

                definition.getName();
                minTimes = 0;
                result = "testCol";

                definition.isAllowNull();
                minTimes = 0;
                result = false;
            }
        };
        AddColumnClause clause = new AddColumnClause(definition, ColumnPosition.FIRST, null, null);
        clause.analyze(analyzer);
        Assert.fail("No exception throws.");
    }
}
