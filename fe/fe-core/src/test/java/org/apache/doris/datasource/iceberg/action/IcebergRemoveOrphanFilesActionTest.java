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

package org.apache.doris.datasource.iceberg.action;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;

import org.apache.iceberg.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class IcebergRemoveOrphanFilesActionTest {
    private static class TestRemoveOrphanFilesAction extends IcebergRemoveOrphanFilesAction {
        TestRemoveOrphanFilesAction(Map<String, String> properties) throws UserException {
            super(properties, Optional.empty(), Optional.empty());
            namedArguments.validate(properties);
        }

        List<String> run(IcebergExternalTable table) throws UserException {
            return executeAction(table);
        }
    }

    @Test
    public void testCanonicalFileIdentityAndCollisionSafety() {
        Assertions.assertTrue(IcebergRemoveOrphanFilesAction.sameFileIdentity(
                "s3a://BUCKET/table/data.parquet", "s3://bucket/table/data.parquet"));
        Assertions.assertFalse(IcebergRemoveOrphanFilesAction.sameFileIdentity(
                "s3://bucket-a/table/data.parquet", "s3://bucket-b/table/data.parquet"));
        Assertions.assertThrows(UserException.class,
                () -> IcebergRemoveOrphanFilesAction.verifyReachableIndexBudget(
                        new HashSet<>(Arrays.asList("s3://bucket-a/table/data.parquet",
                                "s3://bucket-b/table/data.parquet")), 2048));
    }

    @Test
    public void testReachableIndexIsBounded() {
        Assertions.assertThrows(UserException.class,
                () -> IcebergRemoveOrphanFilesAction.verifyReachableIndexBudget(
                        new HashSet<>(Arrays.asList("s3://bucket/table/a", "s3://bucket/table/b")), 1));
    }

    @Test
    public void testReachableIndexRejectsOversizedPathBeforeEntryLimit() {
        String oversizedLocation = "s3://bucket/table/"
                + new String(new char[512]).replace('\0', 'a');
        Assertions.assertDoesNotThrow(
                () -> IcebergRemoveOrphanFilesAction.verifyReachableIndexBudget(
                        new HashSet<>(Arrays.asList("s3://bucket/table/data.parquet")), 512));
        Assertions.assertThrows(UserException.class,
                () -> IcebergRemoveOrphanFilesAction.verifyReachableIndexBudget(
                        new HashSet<>(Arrays.asList(oversizedLocation)), 512));
    }

    @Test
    public void testDryRunRefreshesBeforeBuildingReachableIndex() throws Exception {
        long olderThan = System.currentTimeMillis() - 48L * 60 * 60 * 1000;
        Map<String, String> properties = new HashMap<>();
        properties.put(IcebergRemoveOrphanFilesAction.OLDER_THAN, String.valueOf(olderThan));
        properties.put(IcebergRemoveOrphanFilesAction.DRY_RUN, "true");
        TestRemoveOrphanFilesAction action = new TestRemoveOrphanFilesAction(properties);
        IcebergExternalTable externalTable = Mockito.mock(IcebergExternalTable.class);
        Table table = Mockito.mock(Table.class);
        Mockito.when(externalTable.getIcebergTable()).thenReturn(table);

        Assertions.assertThrows(UserException.class, () -> action.run(externalTable));

        Mockito.verify(table).refresh();
    }
}
