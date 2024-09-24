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

package org.apache.doris.insertoverwrite;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class InsertOverwriteManagerTest {
    @Mocked
    private DatabaseIf db;

    @Mocked
    private TableIf table;

    @Before
    public void setUp()
            throws NoSuchMethodException, SecurityException, AnalysisException, DdlException, MetaNotFoundException {

        new Expectations() {
            {
                db.getId();
                minTimes = 0;
                result = 1L;

                db.getFullName();
                minTimes = 0;
                result = "db1";

                table.getId();
                minTimes = 0;
                result = 2L;

                table.getName();
                minTimes = 0;
                result = "table1";
            }
        };
    }

    @Test
    public void testParallel() {
        InsertOverwriteManager manager = new InsertOverwriteManager();
        manager.recordRunningTableOrException(db, table);
        try {
            manager.recordRunningTableOrException(db, table);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Not allowed"));
        }
        manager.dropRunningRecord(db.getId(), table.getId());
        manager.recordRunningTableOrException(db, table);
    }

}
