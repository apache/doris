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

import org.apache.doris.analysis.ColumnDef.DefaultValue;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class AddColumnCommentTest {
    private static Analyzer analyzer;

    @BeforeClass
    public static void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
    }

    @Test
    public void testNormal() throws AnalysisException {
        List<ColumnDef> columns = Lists.newArrayList();
        ColumnDef definition = new ColumnDef("col1", new TypeDef(ScalarType.createType(PrimitiveType.VARCHAR)),
                true,
                null, false,
                new DefaultValue(true, "col1_default"), "a");
        definition.setKeysType(KeysType.DUP_KEYS);
        columns.add(definition);
        definition = new ColumnDef("col2", new TypeDef(ScalarType.createType(PrimitiveType.INT)),
                false,
                null, false,
                new DefaultValue(true, "0"), "[\"a\", \"b\"]");
        columns.add(definition);
        definition = new ColumnDef("col3", new TypeDef(ScalarType.createType(PrimitiveType.VARCHAR)),
                false,
                null, false,
                new DefaultValue(true, "col3_default"), "['a', 'b']");
        columns.add(definition);
        definition = new ColumnDef("col4", new TypeDef(ScalarType.createType(PrimitiveType.VARCHAR)),
                false,
                null, false,
                new DefaultValue(true, "col4_default"), "'[123, 456]' and '[789, 10]'");
        columns.add(definition);
        AddColumnsClause clause = new AddColumnsClause(columns, null, null);
        clause.analyze(analyzer);
        System.out.println(clause.toString());
        Assert.assertEquals(
                "ADD COLUMN (`col1` varchar(65533) NOT NULL DEFAULT \"col1_default\" COMMENT \"a\", "
                        + "`col2` int NOT NULL DEFAULT \"0\" COMMENT \"[\\\"a\\\", \\\"b\\\"]\", "
                        + "`col3` varchar(65533) NOT NULL DEFAULT \"col3_default\" COMMENT \"['a', 'b']\", "
                        + "`col4` varchar(65533) NOT NULL DEFAULT \"col4_default\" COMMENT \"'[123, 456]' and '[789, 10]'\")",
                clause.toString());
    }
}
