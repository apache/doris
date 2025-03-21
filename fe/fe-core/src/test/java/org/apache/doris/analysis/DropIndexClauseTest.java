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
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DropIndexClauseTest {

    private static Analyzer analyzer;

    @BeforeClass
    public static void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
    }

    @Test
    public void testNormal() throws UserException {
        DropIndexClause clause = new DropIndexClause("index1", false,
                new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, "db", "table"), false);
        clause.analyze(analyzer);
        Assert.assertEquals("DROP INDEX `index1` ON `db`.`table`", clause.toSql());
    }

    @Test
    public void testAlter() throws UserException {
        DropIndexClause clause = new DropIndexClause("index1", false,
                new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, "db", "table"), true);
        clause.analyze(analyzer);
        Assert.assertEquals("DROP INDEX `index1`", clause.toSql());
    }

    @Test(expected = AnalysisException.class)
    public void testNoIndex() throws UserException {
        DropIndexClause clause = new DropIndexClause("", false,
                new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, "db", "table"), false);
        clause.analyze(analyzer);
    }
}
