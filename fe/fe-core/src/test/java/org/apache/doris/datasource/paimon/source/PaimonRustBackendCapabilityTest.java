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

package org.apache.doris.datasource.paimon.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.datasource.ExternalScanNode;
import org.apache.doris.datasource.FederationBackendPolicy;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TPaimonFileDesc;
import org.apache.doris.thrift.TPaimonReaderType;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.utils.InstantiationUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

public class PaimonRustBackendCapabilityTest {
    @Test
    public void testUnknownAndMixedBackendsUseJni() throws Exception {
        Backend oldBackend = new Backend(1, "127.0.0.1", 9050);
        Backend newBackend = capableBackend(2);
        Assert.assertEquals(TPaimonReaderType.PAIMON_JNI, reader(Collections.emptyList(), true));
        Assert.assertEquals(TPaimonReaderType.PAIMON_JNI, reader(Collections.singletonList(oldBackend), true));
        Assert.assertEquals(TPaimonReaderType.PAIMON_JNI, reader(Arrays.asList(newBackend, oldBackend), true));
        Assert.assertEquals(TPaimonReaderType.PAIMON_RUST, reader(Collections.singletonList(newBackend), true));
        Assert.assertEquals(TPaimonReaderType.PAIMON_RUST, reader(Arrays.asList(newBackend, capableBackend(3)), true));
        Assert.assertEquals(TPaimonReaderType.PAIMON_JNI, reader(Collections.singletonList(newBackend), false));
    }

    private Backend capableBackend(long id) {
        // Exercise the same persisted capability used when a follower plans the scan.
        return GsonUtils.GSON.fromJson("{\"id\":" + id + ",\"supportsPaimonRustReader\":true}", Backend.class);
    }

    private TPaimonReaderType reader(List<Backend> backends, boolean enabled) throws Exception {
        SessionVariable vars = new SessionVariable();
        vars.setEnablePaimonRustReader(enabled);
        vars.enableFileScannerV2 = true;
        PaimonScanNode node = new PaimonScanNode(new PlanNodeId(0), new TupleDescriptor(new TupleId(0)),
                false, vars, ScanContext.EMPTY);
        Field policyField = ExternalScanNode.class.getDeclaredField("backendPolicy");
        policyField.setAccessible(true);
        ((FederationBackendPolicy) policyField.get(node)).replaceBackendOrder(backends);

        PaimonSource source = Mockito.mock(PaimonSource.class);
        FileStoreTable table = Mockito.mock(FileStoreTable.class);
        Mockito.when(table.schema()).thenReturn(new TableSchema(1,
                Collections.singletonList(new DataField(0, "id", new IntType())), 0,
                Collections.emptyList(), Collections.emptyList(), Collections.emptyMap(), null));
        PaimonExternalTable external = Mockito.mock(PaimonExternalTable.class);
        Mockito.when(source.getExternalTable()).thenReturn(external);
        Mockito.when(external.getDbName()).thenReturn("db");
        Mockito.when(external.getName()).thenReturn("t");
        Mockito.when(source.getTableLocation()).thenReturn("file:///warehouse/db/t");
        node.setSource(source);
        Field tableField = PaimonScanNode.class.getDeclaredField("processedTable");
        tableField.setAccessible(true);
        tableField.set(node, table);

        DataFileMeta file = DataFileMeta.forAppend("data.parquet", 1024, 1, SimpleStats.EMPTY_STATS,
                1, 1, 1, Collections.emptyList(), null, FileSource.APPEND,
                Collections.emptyList(), null, null, Collections.emptyList());
        DataSplit split = DataSplit.builder().rawConvertible(true).withPartition(BinaryRow.EMPTY_ROW)
                .withBucket(0).withBucketPath("file:///warehouse/db/t/bucket-0")
                .withDataFiles(Collections.singletonList(file)).build();
        TFileRangeDesc range = new TFileRangeDesc();
        Method setParams = PaimonScanNode.class.getDeclaredMethod("setPaimonParams",
                TFileRangeDesc.class, PaimonSplit.class);
        setParams.setAccessible(true);
        setParams.invoke(node, range, new PaimonSplit(split));
        TPaimonFileDesc fileDesc = range.getTableFormatParams().getPaimonParams();
        if (fileDesc.getReaderType() == TPaimonReaderType.PAIMON_JNI) {
            // Legacy BEs require the Java object encoding as well as the JNI reader enum.
            DataSplit decoded = InstantiationUtil.deserializeObject(
                    Base64.getUrlDecoder().decode(fileDesc.getPaimonSplit()), getClass().getClassLoader());
            Assert.assertEquals("data.parquet", decoded.dataFiles().get(0).fileName());
        }
        return fileDesc.getReaderType();
    }
}
