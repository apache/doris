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

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;

import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExprTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;

    @Test
    public void testGetTableNameToColumnNames(@Mocked Analyzer analyzer,
                                              @Injectable SlotDescriptor slotDesc1,
                                              @Injectable SlotDescriptor slotDesc2,
                                              @Injectable TupleDescriptor tupleDescriptor1,
                                              @Injectable TupleDescriptor tupleDescriptor2,
                                              @Injectable Table tableA,
                                              @Injectable Table tableB) throws AnalysisException {
        TableName tableAName = new TableName(internalCtl, "test", "tableA");
        TableName tableBName = new TableName(internalCtl, "test", "tableB");
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
                tableA.getId();
                result = 1;
                tableB.getId();
                result = 2;

            }
        };

        Map<Long, Set<String>> tableNameToColumnNames = Maps.newHashMap();
        whereExpr.getTableIdToColumnNames(tableNameToColumnNames);
        Assert.assertEquals(tableNameToColumnNames.size(), 2);
        Set<String> tableAColumns = tableNameToColumnNames.get(new Long(1));
        Assert.assertNotEquals(tableAColumns, null);
        Assert.assertTrue(tableAColumns.contains("c1"));
        Set<String> tableBColumns = tableNameToColumnNames.get(new Long(2));
        Assert.assertNotEquals(tableBColumns, null);
        Assert.assertTrue(tableBColumns.contains("c1"));
    }

    @Test
    public void testUncheckedCastTo() throws AnalysisException {
        // uncheckedCastTo should return new object

        // date
        DateLiteral dateLiteral = new DateLiteral(2020, 4, 5, 12, 0, 5, Type.DATETIME);
        Assert.assertTrue(dateLiteral.getType().equals(Type.DATETIME));
        DateLiteral castLiteral = (DateLiteral) dateLiteral.uncheckedCastTo(Type.DATE);
        Assert.assertFalse(dateLiteral == castLiteral);
        Assert.assertTrue(dateLiteral.getType().equals(Type.DATETIME));
        Assert.assertTrue(castLiteral.getType().equals(Type.DATE));

        Assert.assertEquals(0, castLiteral.getHour());
        Assert.assertEquals(0, castLiteral.getMinute());
        Assert.assertEquals(0, castLiteral.getSecond());

        Assert.assertEquals(12, dateLiteral.getHour());
        Assert.assertEquals(0, dateLiteral.getMinute());
        Assert.assertEquals(5, dateLiteral.getSecond());

        DateLiteral dateLiteral2 = new DateLiteral(2020, 4, 5, Type.DATE);
        Assert.assertTrue(dateLiteral2.getType().equals(Type.DATE));
        castLiteral = (DateLiteral) dateLiteral2.uncheckedCastTo(Type.DATETIME);
        Assert.assertFalse(dateLiteral2 == castLiteral);
        Assert.assertTrue(dateLiteral2.getType().equals(Type.DATE));
        Assert.assertTrue(castLiteral.getType().equals(Type.DATETIME));

        Assert.assertEquals(0, castLiteral.getHour());
        Assert.assertEquals(0, castLiteral.getMinute());
        Assert.assertEquals(0, castLiteral.getSecond());

        // float
        FloatLiteral floatLiteral = new FloatLiteral(0.1, Type.FLOAT);
        Assert.assertTrue(floatLiteral.getType().equals(Type.FLOAT));
        FloatLiteral castFloatLiteral = (FloatLiteral) floatLiteral.uncheckedCastTo(Type.DOUBLE);
        Assert.assertTrue(floatLiteral.getType().equals(Type.FLOAT));
        Assert.assertTrue(castFloatLiteral.getType().equals(Type.DOUBLE));
        Assert.assertFalse(floatLiteral == castFloatLiteral);
        FloatLiteral castFloatLiteral2 = (FloatLiteral) floatLiteral.uncheckedCastTo(Type.FLOAT);
        Assert.assertTrue(floatLiteral == castFloatLiteral2);

        // int
        IntLiteral intLiteral = new IntLiteral(200);
        Assert.assertTrue(intLiteral.getType().equals(Type.SMALLINT));
        IntLiteral castIntLiteral = (IntLiteral) intLiteral.uncheckedCastTo(Type.INT);
        Assert.assertTrue(intLiteral.getType().equals(Type.SMALLINT));
        Assert.assertTrue(castIntLiteral.getType().equals(Type.INT));
        Assert.assertFalse(intLiteral == castIntLiteral);
        IntLiteral castIntLiteral2 = (IntLiteral) intLiteral.uncheckedCastTo(Type.SMALLINT);
        Assert.assertTrue(intLiteral == castIntLiteral2);

        // null
        NullLiteral nullLiteral = NullLiteral.create(Type.DATE);
        Assert.assertTrue(nullLiteral.getType().equals(Type.DATE));
        NullLiteral castNullLiteral = (NullLiteral) nullLiteral.uncheckedCastTo(Type.DATETIME);
        Assert.assertTrue(nullLiteral.getType().equals(Type.DATE));
        Assert.assertTrue(castNullLiteral.getType().equals(Type.DATETIME));
        Assert.assertFalse(nullLiteral == castNullLiteral);
        NullLiteral castNullLiteral2 = (NullLiteral) nullLiteral.uncheckedCastTo(Type.DATE);
        Assert.assertTrue(nullLiteral == castNullLiteral2);

        // string
        StringLiteral stringLiteral = new StringLiteral("abc");
        Assert.assertTrue(stringLiteral.getType().equals(Type.VARCHAR));
        StringLiteral castStringLiteral = (StringLiteral) stringLiteral.uncheckedCastTo(Type.CHAR);
        Assert.assertTrue(stringLiteral.getType().equals(Type.VARCHAR));
        Assert.assertTrue(castStringLiteral.getType().equals(Type.CHAR));
        Assert.assertFalse(stringLiteral == castStringLiteral);
        StringLiteral castStringLiteral2 = (StringLiteral) stringLiteral.uncheckedCastTo(Type.VARCHAR);
        Assert.assertTrue(stringLiteral == castStringLiteral2);
    }

    @Test
    public void testEqualSets() {
        Expr r1 = new DateLiteral(2020, 10, 20);
        Expr r2 = new DateLiteral(2020, 10, 21);
        Expr r3 = new DateLiteral(2020, 10, 22);
        Expr r4 = new DateLiteral(2020, 10, 23);
        Expr r5 = new DateLiteral(2020, 10, 23, Type.DATEV2);
        Expr r6 = new DateLiteral(2020, 10, 23, 0, 0, 0, Type.DATETIME);
        Expr r7 = new DateLiteral(2020, 10, 23, 0, 0, 0, Type.DATETIMEV2);
        Expr r8 = new DateLiteral(2020, 10, 23, 0, 0, 0, ScalarType.createDatetimeV2Type(3));

        //list1 equal list2
        List<Expr> list1 = new ArrayList<>();
        List<Expr> list2 = new ArrayList<>();
        list1.add(r1);
        list1.add(r2);
        list1.add(r3);
        list1.add(r5);
        list1.add(r6);
        list1.add(r7);
        list1.add(r8);
        list2.add(r1);
        list2.add(r2);
        list2.add(r3);
        list2.add(r5);
        list2.add(r6);
        list2.add(r7);
        list2.add(r8);
        Assert.assertTrue(Expr.equalSets(list1, list2));

        //list3 not equal list4
        list2.add(r4);
        Assert.assertFalse(Expr.equalSets(list1, list2));
    }

    @Test
    public void testSrcSlotRef(@Injectable SlotDescriptor slotDescriptor) {
        TableName tableName = new TableName(internalCtl, "db1", "table1");
        SlotRef slotRef = new SlotRef(tableName, "c1");
        slotRef.setDesc(slotDescriptor);
        Deencapsulation.setField(slotRef, "isAnalyzed", true);
        Expr castExpr = new CastExpr(new TypeDef(Type.INT), slotRef);
        new Expectations() {
            {
                slotDescriptor.getSourceExprs();
                result = null;
            }
        };

        SlotRef srcSlotRef = castExpr.getSrcSlotRef();
        Assert.assertTrue(srcSlotRef != null);
        Assert.assertTrue(srcSlotRef == slotRef);
    }
}
