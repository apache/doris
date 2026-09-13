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

package org.apache.doris.datasource.scan;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.connector.DefaultConnectorContext;
import org.apache.doris.connector.spi.ConnectorStorageAccess;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.datasource.split.PluginDrivenSplit;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.kerberos.ExecutionAuthenticator;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TScanRangeLocations;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Real provider -> engine resolver -> split -> node/range Thrift, without opening any remote file. */
class FileQueryScanNodeStorageAccessTest {
    @ParameterizedTest
    @ValueSource(strings = {
            "abfss://container@account.dfs.core.windows.net/table/data.parquet",
            "https://account.blob.core.windows.net/container/table/data.parquet"
    })
    void azureSasProducesNativeRangeAndNodeParameters(String uri) throws Exception {
        DefaultConnectorContext context = new DefaultConnectorContext("c", 1L);
        ConnectorStorageAccess access = context.newStorageAccessResolver(Map.of(
                "adls.sas-token.account.dfs.core.windows.net", "sig=temporary&se=2100-01-01T00:00:00Z"))
                .apply(uri);
        TestScanNode node = new TestScanNode();

        TFileRangeDesc range = node.translate(access);

        Assertions.assertEquals(TFileType.FILE_S3, range.getFileType());
        Assertions.assertEquals(uri, range.getPath());
        Assertions.assertEquals(16, range.getStartOffset());
        Assertions.assertEquals(32, range.getSize());
        Assertions.assertEquals(128, range.getFileSize());
        Assertions.assertFalse(range.isSetFsName());
        Assertions.assertFalse(node.getFileScanRangeParams().isSetHdfsParams());
        Assertions.assertEquals(TFileType.FILE_S3, node.getFileScanRangeParams().getFileType());
        Assertions.assertEquals(access.getBackendProperties(), node.getFileScanRangeParams().getProperties());
        Assertions.assertEquals("azure", node.getFileScanRangeParams().getProperties().get("provider"));
        Assertions.assertTrue(node.getFileScanRangeParams().getProperties().keySet().stream()
                .allMatch(key -> key.equals("provider") || key.startsWith("AZURE_")));
    }

    @Test
    void genuineHdfsKeepsItsHadoopChannel() throws Exception {
        StorageAdapter adapter = StorageAdapter.ofProvider("HDFS", Map.of("fs.defaultFS", "hdfs://namenode:8020",
                "hadoop.username", "reader"));
        ConnectorStorageAccess access = new DefaultConnectorContext("c", 1L,
                () -> new ExecutionAuthenticator() {}, () -> Map.of(adapter.getType(), adapter))
                .newStorageAccessResolver(Collections.emptyMap()).apply("hdfs://namenode:8020/table/data.parquet");
        TestScanNode node = new TestScanNode();

        TFileRangeDesc range = node.translate(access);

        Assertions.assertEquals(TFileType.FILE_HDFS, range.getFileType());
        Assertions.assertEquals("hdfs://namenode:8020", range.getFsName());
        Assertions.assertTrue(node.getFileScanRangeParams().isSetHdfsParams());
        Assertions.assertFalse(node.getFileScanRangeParams().isSetProperties());
    }

    @ParameterizedTest
    @CsvSource({"https://example.org/data.parquet,FILE_HTTP", "s3://bucket/data.parquet,FILE_S3",
            "hdfs://namenode/data.parquet,FILE_HDFS", "file:///tmp/data.parquet,FILE_LOCAL"})
    void connectorsWithoutStorageAccessRetainSchemeRouting(String path, String expectedFileType) {
        ConnectorScanRange range = new ConnectorScanRange() {
            @Override
            public Optional<String> getPath() {
                return Optional.of(path);
            }

            @Override
            public Map<String, String> getProperties() {
                return Collections.emptyMap();
            }
        };

        Assertions.assertEquals(TFileType.valueOf(expectedFileType), new PluginDrivenSplit(range).getLocationType());
    }

    private static final class TestScanNode extends FileQueryScanNode {
        TestScanNode() {
            super(new PlanNodeId(0), new TupleDescriptor(new TupleId(0)), "storage-contract",
                    ScanContext.EMPTY, false, new SessionVariable());
            params = new TFileScanRangeParams();
            params.setFormatType(TFileFormatType.FORMAT_PARQUET);
        }

        TFileRangeDesc translate(ConnectorStorageAccess access) throws Exception {
            ConnectorScanRange range = new ConnectorScanRange() {
                @Override
                public Optional<String> getPath() {
                    return Optional.of(access.getNormalizedUri());
                }

                @Override
                public Optional<String> getBackendFileType() {
                    return Optional.of(access.getBackendFileType());
                }

                @Override
                public long getStart() {
                    return 16;
                }

                @Override
                public long getLength() {
                    return 32;
                }

                @Override
                public long getFileSize() {
                    return 128;
                }

                @Override
                public boolean isPartitionBearing() {
                    return true;
                }

                @Override
                public Map<String, String> getProperties() {
                    return Collections.emptyMap();
                }
            };
            Method translate = FileQueryScanNode.class.getDeclaredMethod("splitToScanRange", Backend.class,
                    Map.class, Split.class, List.class, boolean.class);
            translate.setAccessible(true);
            Backend backend = new Backend(1L, "127.0.0.1", 9050);
            TScanRangeLocations locations = (TScanRangeLocations) translate.invoke(this, backend,
                    access.getBackendProperties(), new PluginDrivenSplit(range), Collections.emptyList(), true);
            return locations.getScanRange().getExtScanRange().getFileScanRange().getRanges().get(0);
        }

        @Override
        protected TFileFormatType getFileFormatType() {
            return TFileFormatType.FORMAT_PARQUET;
        }

        @Override
        protected List<String> getPathPartitionKeys() {
            return Collections.emptyList();
        }

        @Override
        protected TableIf getTargetTable() {
            throw new UnsupportedOperationException("Range transport does not access table metadata");
        }

        @Override
        protected Map<String, String> getLocationProperties() {
            throw new UnsupportedOperationException("The resolver supplies the selected location parameters");
        }
    }
}
