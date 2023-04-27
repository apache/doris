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

package org.apache.doris.qe;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;

import org.junit.Assert;
import org.junit.Test;

public class ShowResultSetMetaDataTest {
    @Test
    public void testNormal() {
        ShowResultSetMetaData metaData = ShowResultSetMetaData.builder().build();
        Assert.assertEquals(0, metaData.getColumnCount());

        metaData = ShowResultSetMetaData.builder()
                .addColumn(new Column("col1", ScalarType.createType(PrimitiveType.INT)))
                .addColumn(new Column("col2", ScalarType.createType(PrimitiveType.INT)))
                .build();

        Assert.assertEquals(2, metaData.getColumnCount());
        Assert.assertEquals("col1", metaData.getColumn(0).getName());
        Assert.assertEquals("col2", metaData.getColumn(1).getName());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testOutBound() {
        ShowResultSetMetaData metaData = ShowResultSetMetaData.builder().build();
        metaData.getColumn(1);
        Assert.fail("No exception throws.");
    }
}
