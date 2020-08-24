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

package org.apache.doris.alter;

import org.apache.doris.analysis.ColumnPosition;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.jmockit.Deencapsulation;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;

import mockit.Expectations;
import mockit.Injectable;

public class SchemaChangeHandlerTest {

    @Test
    public void testAddValueColumnOnAggMV(@Injectable OlapTable olapTable, @Injectable Column newColumn,
                                          @Injectable ColumnPosition columnPosition) {
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();
        new Expectations() {
            {
                olapTable.getKeysType();
                result = KeysType.DUP_KEYS;
                newColumn.getAggregationType();
                result = null;
                olapTable.getIndexMetaByIndexId(2).getKeysType();
                result = KeysType.AGG_KEYS;
                newColumn.isKey();
                result = false;
            }
        };

        try {
            Deencapsulation.invoke(schemaChangeHandler, "addColumnInternal", olapTable, newColumn, columnPosition,
                                   new Long(2), new Long(1),
                                   Maps.newHashMap(), Sets.newHashSet());
            Assert.fail();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }
}
