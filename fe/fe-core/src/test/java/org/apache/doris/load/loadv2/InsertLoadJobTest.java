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

package org.apache.doris.load.loadv2;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.MetaNotFoundException;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;
import java.util.Set;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;

public class InsertLoadJobTest {

    @Test
    public void testGetTableNames(@Mocked Catalog catalog,
                                  @Injectable Database database,
                                  @Injectable Table table) throws MetaNotFoundException {
        InsertLoadJob insertLoadJob = new InsertLoadJob("label", 1L, 1L, 1L, 1000, "", "");
        String tableName = "table1";
        new Expectations() {
            {
                catalog.getDb(anyLong);
                result = Optional.of(database);
                database.getTable(anyLong);
                result = Optional.of(table);
                table.getName();
                result = tableName;
            }
        };
        Set<String> tableNames = insertLoadJob.getTableNamesForShow();
        Assert.assertEquals(1, tableNames.size());
        Assert.assertTrue(tableNames.contains(tableName));
        Assert.assertEquals(JobState.FINISHED, insertLoadJob.getState());
        Assert.assertEquals(Integer.valueOf(100), Deencapsulation.getField(insertLoadJob, "progress"));

    }
}
