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

package org.apache.doris.datasource.maxcompute;

import org.apache.doris.common.UserException;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Optional;

public class MCTransactionTest {
    @Test
    public void testBeginInsertRejectsOdpsExternalTable() {
        assertBeginInsertRejectsUnsupportedOdpsTable("mc_external_table");
    }

    @Test
    public void testBeginInsertRejectsOdpsLogicalView() {
        assertBeginInsertRejectsUnsupportedOdpsTable("mc_logical_view");
    }

    private void assertBeginInsertRejectsUnsupportedOdpsTable(String tableName) {
        MaxComputeExternalCatalog catalog = Mockito.mock(MaxComputeExternalCatalog.class);
        MaxComputeExternalTable table = Mockito.mock(MaxComputeExternalTable.class);
        Mockito.when(table.isUnsupportedOdpsTable()).thenReturn(true);
        Mockito.when(table.getDbName()).thenReturn("default");
        Mockito.when(table.getName()).thenReturn(tableName);

        MCTransaction transaction = new MCTransaction(catalog);

        UserException exception = Assert.assertThrows(UserException.class,
                () -> transaction.beginInsert(table, Optional.empty()));
        Assert.assertTrue(exception.getMessage().contains(
                "Writing MaxCompute external table or logical view is not supported: default." + tableName));
        Mockito.verify(catalog, Mockito.never()).getOdpsTableIdentifier(Mockito.anyString(), Mockito.anyString());
    }
}
