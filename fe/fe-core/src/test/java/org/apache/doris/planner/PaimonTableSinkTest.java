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

package org.apache.doris.planner;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.nereids.trees.plans.commands.insert.BaseExternalTableInsertCommandContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TPaimonTableSink;

import org.apache.paimon.fs.Path;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class PaimonTableSinkTest {

    @Test
    public void testBindDataSinkBuildsPaimonThriftPayload() throws UserException {
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();
        try {
            PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class);
            CatalogProperty catalogProperty = Mockito.mock(CatalogProperty.class);
            PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
            FileStoreTable fileStoreTable = Mockito.mock(FileStoreTable.class,
                    Mockito.withSettings().serializable());
            TableSchema schema = Mockito.mock(TableSchema.class, Mockito.withSettings().serializable());

            Map<String, String> paimonCatalogOptions = new HashMap<>();
            paimonCatalogOptions.put(CatalogOptions.WAREHOUSE.key(), "hdfs://nn:8020/warehouse");
            Mockito.when(catalog.getPaimonOptionsMap()).thenReturn(paimonCatalogOptions);
            Mockito.when(catalog.getCatalogProperty()).thenReturn(catalogProperty);
            Mockito.when(catalogProperty.getHadoopProperties()).thenReturn(Collections.emptyMap());

            Mockito.when(table.getCatalog()).thenReturn(catalog);
            Mockito.when(table.getDbName()).thenReturn("db1");
            Mockito.when(table.getName()).thenReturn("tbl1");
            Mockito.when(table.getPaimonTable(Mockito.any(Optional.class))).thenReturn(fileStoreTable);

            Mockito.when(fileStoreTable.location())
                    .thenReturn(new Path("hdfs://nn:8020/warehouse/db1.db/tbl1"));
            Mockito.when(fileStoreTable.schema()).thenReturn(schema);
            Map<String, String> tableOptions = new HashMap<>();
            tableOptions.put("file.format", "orc");
            tableOptions.put("manifest.format", "avro");
            Mockito.when(fileStoreTable.options()).thenReturn(tableOptions);
            Mockito.when(schema.numBuckets()).thenReturn(8);

            BaseExternalTableInsertCommandContext insertCtx = new BaseExternalTableInsertCommandContext();
            insertCtx.setTxnId(42);
            insertCtx.setCommitUser("doris_txn_42");

            PaimonTableSink sink = new PaimonTableSink(table);
            sink.setCols(Arrays.asList(
                    new Column("k", PrimitiveType.INT),
                    new Column("v", PrimitiveType.STRING)));
            sink.bindDataSink(Optional.of(insertCtx));

            TPaimonTableSink thrift = sink.tDataSink.getPaimonTableSink();
            Assert.assertEquals("db1", thrift.getDbName());
            Assert.assertEquals("tbl1", thrift.getTbName());
            Assert.assertEquals("hdfs://nn:8020/warehouse/db1.db/tbl1", thrift.getTableLocation());
            Assert.assertEquals(8, thrift.getBucketNum());
            Assert.assertEquals(Arrays.asList("k", "v"), thrift.getColumnNames());
            Assert.assertEquals("42", thrift.getPaimonOptions().get("doris.commit_identifier"));
            Assert.assertEquals("doris_txn_42", thrift.getPaimonOptions().get("doris.commit_user"));
            Assert.assertEquals("orc", thrift.getPaimonOptions().get("file.format"));
            Assert.assertEquals("avro", thrift.getPaimonOptions().get("manifest.format"));
            Assert.assertEquals("hadoop", thrift.getHadoopConfig().get("hadoop.user.name"));
            Assert.assertEquals("hadoop", thrift.getHadoopConfig().get("hadoop.username"));
            Assert.assertTrue(thrift.isSetSerializedTable());
            Assert.assertFalse(thrift.getSerializedTable().isEmpty());
        } finally {
            ConnectContext.remove();
        }
    }
}
