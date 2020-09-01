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

import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class IndexSchemaProcNodeTest {

    @Test
    public void testFetchResult() throws AnalysisException {
        List<Column> columnList = Lists.newArrayList();
        Column column1 = new Column("k1", Type.INT, true, null, true, "", "");
        Column column2 = new Column("mv_bitmap_union_v1", Type.BITMAP, false, AggregateType.BITMAP_UNION, true, "", "");
        TableName tableName = new TableName("db1", "t1");
        SlotRef slotRef = new SlotRef(tableName, "v1");
        FunctionCallExpr functionCallExpr = new FunctionCallExpr("to_bitmap", Lists.newArrayList(slotRef));
        column2.setDefineExpr(functionCallExpr);
        columnList.add(column1);
        columnList.add(column2);
        IndexSchemaProcNode indexSchemaProcNode = new IndexSchemaProcNode(columnList, null);
        ProcResult procResult = indexSchemaProcNode.fetchResult();
        Assert.assertEquals(2, procResult.getRows().size());
        Assert.assertTrue(procResult.getRows().get(1).contains(column2.getDisplayName()));
        Assert.assertFalse(procResult.getRows().get(1).contains(column2.getName()));

    }
}
