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

package org.apache.doris.datasource.hive;

import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Type;

import org.junit.Assert;
import org.junit.Test;

public class HiveMetaStoreClientHelperTest {

    @Test
    public void testConvertDorisExprToSlotRefRejectsCast() {
        // Test that convertDorisExprToSlotRef returns null for any CAST expression

        SlotRef stringColumn = new SlotRef(new TableName(), "c_str");
        stringColumn.setType(Type.STRING);

        // Test 1: Direct SlotRef should work
        SlotRef result = HiveMetaStoreClientHelper.convertDorisExprToSlotRef(stringColumn);
        Assert.assertNotNull("Direct SlotRef should be returned", result);
        Assert.assertEquals("Should return the same SlotRef", stringColumn, result);

        // Test 2: CAST expression should return null
        CastExpr castExpr = new CastExpr(Type.INT, stringColumn);
        result = HiveMetaStoreClientHelper.convertDorisExprToSlotRef(castExpr);
        Assert.assertNull("CAST expression should return null", result);

        // Test 3: Nested CAST should also return null
        CastExpr nestedCast = new CastExpr(Type.BIGINT, castExpr);
        result = HiveMetaStoreClientHelper.convertDorisExprToSlotRef(nestedCast);
        Assert.assertNull("Nested CAST expression should return null", result);

        // Test 4: Different CAST types should all return null
        SlotRef intColumn = new SlotRef(new TableName(), "c_int");
        intColumn.setType(Type.INT);

        // int to string
        CastExpr intToString = new CastExpr(Type.STRING, intColumn);
        result = HiveMetaStoreClientHelper.convertDorisExprToSlotRef(intToString);
        Assert.assertNull("CAST(int AS string) should return null", result);

        // string to datetime
        CastExpr stringToDatetime = new CastExpr(Type.DATETIMEV2, stringColumn);
        result = HiveMetaStoreClientHelper.convertDorisExprToSlotRef(stringToDatetime);
        Assert.assertNull("CAST(string AS datetime) should return null", result);

        // string to date
        CastExpr stringToDate = new CastExpr(Type.DATEV2, stringColumn);
        result = HiveMetaStoreClientHelper.convertDorisExprToSlotRef(stringToDate);
        Assert.assertNull("CAST(string AS date) should return null", result);

        // float to int
        SlotRef floatColumn = new SlotRef(new TableName(), "c_float");
        floatColumn.setType(Type.FLOAT);
        CastExpr floatToInt = new CastExpr(Type.INT, floatColumn);
        result = HiveMetaStoreClientHelper.convertDorisExprToSlotRef(floatToInt);
        Assert.assertNull("CAST(float AS int) should return null", result);

        // decimal to string
        SlotRef decimalColumn = new SlotRef(new TableName(), "c_decimal");
        decimalColumn.setType(Type.DECIMALV2);
        CastExpr decimalToString = new CastExpr(Type.STRING, decimalColumn);
        result = HiveMetaStoreClientHelper.convertDorisExprToSlotRef(decimalToString);
        Assert.assertNull("CAST(decimal AS string) should return null", result);
    }
}
