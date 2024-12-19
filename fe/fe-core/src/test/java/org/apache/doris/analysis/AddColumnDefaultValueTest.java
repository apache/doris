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
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class AddColumnDefaultValueTest {
    private static Analyzer analyzer;

    @BeforeClass
    public static void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
    }

    @Test
    public void testNormal() throws AnalysisException {
        List<ColumnDef> columns = Lists.newArrayList();
        ColumnDef definition = new ColumnDef("col1", new TypeDef(ScalarType.createType(PrimitiveType.DATETIME)),
                true,
                null, false,
                DefaultValue.CURRENT_TIMESTAMP_DEFAULT_VALUE, "");
        definition.setKeysType(KeysType.DUP_KEYS);
        columns.add(definition);
        definition = new ColumnDef("col2", new TypeDef(ScalarType.createType(PrimitiveType.INT)),
                false,
                null, false,
                new DefaultValue(true, "1"), "");
        columns.add(definition);
        definition = new ColumnDef("col3", new TypeDef(ScalarType.createType(PrimitiveType.VARCHAR)),
                false,
                null, false,
                new DefaultValue(true, "varchar"), "");
        columns.add(definition);
        definition = new ColumnDef("col4", new TypeDef(ScalarType.createType(PrimitiveType.BITMAP)),
                false,
                null, false,
                DefaultValue.BITMAP_EMPTY_DEFAULT_VALUE, "");
        definition.setKeysType(KeysType.DUP_KEYS);
        columns.add(definition);
        definition = new ColumnDef("col5", new TypeDef(ScalarType.createType(PrimitiveType.VARCHAR)),
                false,
                null, false,
                new DefaultValue(true, "xxx\"\"xxx"), "");
        columns.add(definition);
        definition = new ColumnDef("col6", new TypeDef(ScalarType.createType(PrimitiveType.HLL)),
                false,
                AggregateType.HLL_UNION, false,
                null, "");
        columns.add(definition);
        AddColumnsClause clause = new AddColumnsClause(columns, null, null);
        clause.analyze(analyzer);
        System.out.println(clause.toString());
        Assert.assertEquals(
                "ADD COLUMN (`col1` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT \"\", "
                        + "`col2` int NOT NULL DEFAULT \"1\" COMMENT \"\", "
                        + "`col3` varchar(65533) NOT NULL DEFAULT \"varchar\" COMMENT \"\", "
                        + "`col4` bitmap NOT NULL DEFAULT BITMAP_EMPTY COMMENT \"\", "
                        + "`col5` varchar(65533) NOT NULL DEFAULT \"xxx\\\"\\\"xxx\" COMMENT \"\", "
                        + "`col6` hll HLL_UNION NOT NULL COMMENT \"\")",
                clause.toString());
    }
}
