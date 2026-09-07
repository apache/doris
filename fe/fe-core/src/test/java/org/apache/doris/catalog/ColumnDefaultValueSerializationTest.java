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

package org.apache.doris.catalog;

import org.apache.doris.analysis.DefaultValueExprDef;
import org.apache.doris.proto.OlapFile;
import org.apache.doris.thrift.TColumn;

import org.junit.Assert;
import org.junit.Test;

public class ColumnDefaultValueSerializationTest {
    private static final String DEFAULT_EXPR = "CURRENT_TIMESTAMP(6)";
    private static final String BACKFILL_VALUE = "2026-08-29 23:05:05.110622";

    private Column createSchemaChangeColumn() {
        return new Column("created_at", ScalarType.createDatetimeV2Type(6), false,
                AggregateType.NONE, true, -1, DEFAULT_EXPR, "", true,
                new DefaultValueExprDef("now", 6L), 1, BACKFILL_VALUE);
    }

    @Test
    public void testThriftKeepsBackfillValueAndDefaultExpression() {
        TColumn tColumn = ColumnToThrift.toThrift(createSchemaChangeColumn());

        Assert.assertEquals(BACKFILL_VALUE, tColumn.getDefaultValue());
        Assert.assertTrue(tColumn.isSetDefaultValueExpr());
        Assert.assertEquals(DEFAULT_EXPR, tColumn.getDefaultValueExpr());
    }

    @Test
    public void testProtobufKeepsBackfillValueAndDefaultExpression() throws Exception {
        OlapFile.ColumnPB columnPb = ColumnToProtobuf.toPb(createSchemaChangeColumn(), null, null);

        Assert.assertTrue(columnPb.hasDefaultValue());
        Assert.assertEquals(BACKFILL_VALUE, columnPb.getDefaultValue().toStringUtf8());
        Assert.assertTrue(columnPb.hasDefaultValueExpr());
        Assert.assertEquals(DEFAULT_EXPR, columnPb.getDefaultValueExpr().toStringUtf8());
    }
}
