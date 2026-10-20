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

package org.apache.doris.datasource;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalDatabase;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.property.fileformat.FileFormatProperties;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.NotSupportedException;

import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class UnsupportedExternalReadsTest {
    @Test
    public void testAvroFileFormatIsRejected() {
        for (String format : new String[] {"avro", "AVRO"}) {
            AnalysisException error = Assert.assertThrows(AnalysisException.class,
                    () -> FileFormatProperties.createFileFormatProperties(format));
            Assert.assertTrue(error.getMessage().contains("Avro file reads are no longer supported"));
        }
    }

    @Test
    public void testHiveTransactionalTablesAreRejected() {
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        HMSExternalDatabase database = new HMSExternalDatabase(catalog, 1L, "db", "db");
        HMSExternalTable table = new HMSExternalTable(2L, "tbl", "tbl", catalog, database);
        Table remote = new Table();
        StorageDescriptor storage = new StorageDescriptor();
        storage.setInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
        remote.setSd(storage);
        Deencapsulation.setField(table, "remoteTable", remote);
        for (String mode : new String[] {"default", "insert_only"}) {
            Map<String, String> properties = new HashMap<>();
            properties.put("transactional", "true");
            properties.put("transactional_properties", mode);
            remote.setParameters(properties);
            NotSupportedException error = Assert.assertThrows(NotSupportedException.class,
                    () -> Deencapsulation.invoke(table, "supportedHiveTable"));
            Assert.assertTrue(error.getMessage().contains("Hive transactional table reads are no longer supported"));
        }
        remote.setParameters(Collections.emptyMap());
        Assert.assertEquals(Boolean.TRUE, Deencapsulation.invoke(table, "supportedHiveTable"));
    }

    @Test
    public void testLakeSoulCatalogIsRejected() {
        CatalogLog log = new CatalogLog();
        log.setCatalogId(1L);
        log.setCatalogName("removed_catalog");
        log.setProps(Collections.singletonMap("type", "lakesoul"));
        DdlException error = Assert.assertThrows(DdlException.class, () -> CatalogFactory.createFromLog(log));
        Assert.assertTrue(error.getMessage().contains("Lakesoul catalog is no longer supported"));
    }
}
