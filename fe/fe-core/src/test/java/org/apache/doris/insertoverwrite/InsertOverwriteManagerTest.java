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
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.datasource.hive.HMSExternalTable;

import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

public class InsertOverwriteManagerTest {
    private DatabaseIf db = Mockito.mock(DatabaseIf.class);

    private OlapTable table = Mockito.mock(OlapTable.class);

    private HMSExternalTable hmsExternalTable = Mockito.mock(HMSExternalTable.class);

    private MTMV mtmv = Mockito.mock(MTMV.class);

    @Before
    public void setUp()
            throws NoSuchMethodException, SecurityException, AnalysisException, DdlException, MetaNotFoundException {

        Mockito.when(db.getId()).thenReturn(1L);
        Mockito.when(db.getFullName()).thenReturn("db1");
        Mockito.when(table.getId()).thenReturn(2L);
        Mockito.when(table.getName()).thenReturn("table1");
        Mockito.when(hmsExternalTable.getId()).thenReturn(3L);
        Mockito.when(hmsExternalTable.getName()).thenReturn("hmsTable");
        Mockito.when(mtmv.getId()).thenReturn(4L);
        Mockito.when(mtmv.getName()).thenReturn("mtmv1");
    }

    @Test
    public void testMTMVParallel() {
        InsertOverwriteManager manager = new InsertOverwriteManager();
        manager.recordRunningTableOrException(db, mtmv);
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> manager.recordRunningTableOrException(db, mtmv));
        manager.dropRunningRecord(db.getId(), mtmv.getId());
        Assertions.assertDoesNotThrow(() -> manager.recordRunningTableOrException(db, mtmv));
    }

    @Test
    public void testHmsTableParallel() {
        InsertOverwriteManager manager = new InsertOverwriteManager();
        manager.recordRunningTableOrException(db, hmsExternalTable);
        Assertions.assertDoesNotThrow(() -> manager.recordRunningTableOrException(db, hmsExternalTable));
        manager.dropRunningRecord(db.getId(), hmsExternalTable.getId());
    }

    @Test
    public void testOlapTableParallel() {
        InsertOverwriteManager manager = new InsertOverwriteManager();
        manager.recordRunningTableOrException(db, table);
        Assertions.assertDoesNotThrow(() -> manager.recordRunningTableOrException(db, table));
        manager.dropRunningRecord(db.getId(), table.getId());
    }
}
