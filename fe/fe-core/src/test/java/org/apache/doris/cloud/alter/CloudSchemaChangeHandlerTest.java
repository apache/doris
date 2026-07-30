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

package org.apache.doris.cloud.alter;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.DynamicPartitionUtil;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

public class CloudSchemaChangeHandlerTest {
    @Test
    public void testUpdatePartitionInvertedIndexStorageFormatDoesNotScanPartitions() throws Exception {
        Database database = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Env env = Mockito.mock(Env.class);
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_PARTITION_INVERTED_INDEX_STORAGE_FORMAT, "V3");

        Mockito.when(database.getTableOrMetaException("tbl", Table.TableType.OLAP)).thenReturn(table);

        try (MockedStatic<Config> config = Mockito.mockStatic(Config.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<DynamicPartitionUtil> dynamicPartitionUtil =
                        Mockito.mockStatic(DynamicPartitionUtil.class)) {
            config.when(Config::isCloudMode).thenReturn(true);
            envStatic.when(Env::getCurrentEnv).thenReturn(env);

            new CloudSchemaChangeHandler().updateTableProperties(database, "tbl", properties);

            Mockito.verify(env).modifyTableProperties(database, table, properties);
            Mockito.verify(table, Mockito.never()).getAllPartitions();
            Mockito.verify(table, Mockito.never()).getPartitionInfo();
        }
    }

    @Test
    public void testUpdatePartitionInvertedIndexStorageFormatReturnsWhenFormatUnchanged() throws Exception {
        Database database = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Env env = Mockito.mock(Env.class);
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_PARTITION_INVERTED_INDEX_STORAGE_FORMAT, "v3");

        Mockito.when(database.getTableOrMetaException("tbl", Table.TableType.OLAP)).thenReturn(table);
        Mockito.when(table.getPartitionInvertedIndexFileStorageFormat())
                .thenReturn(TInvertedIndexFileStorageFormat.V3);

        try (MockedStatic<Config> config = Mockito.mockStatic(Config.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            config.when(Config::isCloudMode).thenReturn(true);
            envStatic.when(Env::getCurrentEnv).thenReturn(env);

            new CloudSchemaChangeHandler().updateTableProperties(database, "tbl", properties);

            Mockito.verify(env, Mockito.never()).modifyTableProperties(database, table, properties);
            Mockito.verify(table).readLock();
            Mockito.verify(table).readUnlock();
            Assert.assertEquals("v3", properties.get(
                    PropertyAnalyzer.PROPERTIES_PARTITION_INVERTED_INDEX_STORAGE_FORMAT));
        }
    }
}
