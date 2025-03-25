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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.datasource.hive.HMSExternalTable;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class InsertOverwriteManagerTest {
    @Mocked
    private DatabaseIf db;

    @Mocked
    private OlapTable table;

    @Mocked
    private HMSExternalTable hmsExternalTable;

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

                hmsExternalTable.getId();
                minTimes = 0;
                result = 3L;

                hmsExternalTable.getName();
                minTimes = 0;
                result = "hmsTable";
            }
        };
    }

    @Test
    public void testParallel() {
        InsertOverwriteManager manager = new InsertOverwriteManager();
        manager.recordRunningTableOrException(db, table);
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> manager.recordRunningTableOrException(db, table));
        manager.dropRunningRecord(db.getId(), table.getId());
        Assertions.assertDoesNotThrow(() -> manager.recordRunningTableOrException(db, table));
    }

    @Test
    public void testHmsTableParallel() {
        InsertOverwriteManager manager = new InsertOverwriteManager();
        manager.recordRunningTableOrException(db, hmsExternalTable);
        Assertions.assertDoesNotThrow(() -> manager.recordRunningTableOrException(db, hmsExternalTable));
        manager.dropRunningRecord(db.getId(), hmsExternalTable.getId());
    }
}
