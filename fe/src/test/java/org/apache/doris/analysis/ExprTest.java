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

import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.jmockit.Deencapsulation;

import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;

public class ExprTest {

    @Test
    public void testGetTableNameToColumnNames(@Mocked Analyzer analyzer,
                                              @Injectable SlotDescriptor slotDesc1,
                                              @Injectable SlotDescriptor slotDesc2,
                                              @Injectable TupleDescriptor tupleDescriptor1,
                                              @Injectable TupleDescriptor tupleDescriptor2,
                                              @Injectable Table tableA,
                                              @Injectable Table tableB) throws AnalysisException {
        TableName tableAName = new TableName("test", "tableA");
        TableName tableBName = new TableName("test", "tableB");
        SlotRef tableAColumn1 = new SlotRef(tableAName, "c1");
        SlotRef tableBColumn1 = new SlotRef(tableBName, "c1");
        Expr whereExpr = new BinaryPredicate(BinaryPredicate.Operator.EQ, tableAColumn1, tableBColumn1);
        Deencapsulation.setField(tableAColumn1, "desc", slotDesc1);
        Deencapsulation.setField(tableBColumn1, "desc", slotDesc2);
        new Expectations() {
            {
                slotDesc1.isMaterialized();
                result = true;
                slotDesc2.isMaterialized();
                result = true;
                slotDesc1.getColumn().getName();
                result = "c1";
                slotDesc2.getColumn().getName();
                result = "c1";
                slotDesc1.getParent();
                result = tupleDescriptor1;
                slotDesc2.getParent();
                result = tupleDescriptor2;
                tupleDescriptor1.getTable();
                result = tableA;
                tupleDescriptor2.getTable();
                result = tableB;
                tableA.getName();
                result = "tableA";
                tableB.getName();
                result = "tableB";

            }
        };

        Map<String, Set<String>> tableNameToColumnNames = Maps.newHashMap();
        whereExpr.getTableNameToColumnNames(tableNameToColumnNames);
        Assert.assertEquals(tableNameToColumnNames.size(), 2);
        Set<String> tableAColumns = tableNameToColumnNames.get("tableA");
        Assert.assertNotEquals(tableAColumns, null);
        Assert.assertTrue(tableAColumns.contains("c1"));
        Set<String> tableBColumns = tableNameToColumnNames.get("tableB");
        Assert.assertNotEquals(tableBColumns, null);
        Assert.assertTrue(tableBColumns.contains("c1"));
    }
}
