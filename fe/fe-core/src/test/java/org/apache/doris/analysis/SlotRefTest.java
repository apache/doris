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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;

import mockit.Expectations;
import mockit.Injectable;
import org.junit.Assert;
import org.junit.Test;

public class SlotRefTest {

    @Test
    public void testColumnEqual(@Injectable SlotDescriptor slotDescriptor,
                                @Injectable TupleDescriptor tupleDescriptor,
                                @Injectable TableRef tableRef,
                                @Injectable Column column) {
        TableName tableName1 = new TableName("db1", "table1");
        SlotRef slotRef1 = new SlotRef(tableName1, "c1");
        TableName tableName2 = new TableName("db1", "table1");
        new Expectations() {
            {
                slotDescriptor.getType();
                result = Type.INT;
                slotDescriptor.getParent();
                result = tupleDescriptor;
                tupleDescriptor.getRef();
                result = tableRef;
                tableRef.getName();
                result = tableName2;
                slotDescriptor.getColumn();
                result = column;
                column.getName();
                result = "C1";
            }
        };
        SlotRef slotRef2 = new SlotRef(slotDescriptor);
        Assert.assertTrue(slotRef1.columnEqual(slotRef2));
        Assert.assertTrue(slotRef2.columnEqual(slotRef1));

    }
}
