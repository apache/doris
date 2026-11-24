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

package org.apache.doris.datasource.split;

import org.apache.doris.thrift.TExternalScanRange;
import org.apache.doris.thrift.TFetchSplitBatchResult;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.THudiFileDesc;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TLanceFileDesc;
import org.apache.doris.thrift.TMaxComputeFileDesc;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaimonDeletionFileDesc;
import org.apache.doris.thrift.TPaimonFileDesc;
import org.apache.doris.thrift.TPaimonReaderType;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TRemoteDorisFileDesc;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TScanRangeParams;
import org.apache.doris.thrift.TSplitSource;
import org.apache.doris.thrift.TTableFormatFileDesc;
import org.apache.doris.thrift.TTransactionalHiveDeleteDeltaDesc;
import org.apache.doris.thrift.TTransactionalHiveDesc;
import org.apache.doris.thrift.TTrinoConnectorFileDesc;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FileScanRangeCompactorTest {
    @Test
    void preservesEveryConnectorDescriptorThroughSplitBatchSerialization() throws Exception {
        List<TFileRangeDesc> expected = connectorRanges();
        List<TScanRangeLocations> locations = new ArrayList<>();
        for (TFileRangeDesc file : expected) {
            locations.add(new TScanRangeLocations().setScanRange(scanRange(file))
                    .setLocations(List.of(new TScanRangeLocation().setBackendId(1)
                            .setServer(new TNetworkAddress("backend", 9060)))));
        }
        TFetchSplitBatchResult result = new TFetchSplitBatchResult()
                .setSplits(FileScanRangeCompactor.compactSplitBatch(locations));
        TFetchSplitBatchResult decoded = new TFetchSplitBatchResult();
        new TDeserializer().deserialize(decoded, new TSerializer().serialize(result));

        Assertions.assertEquals(1, decoded.getSplitsSize());
        Assertions.assertEquals(expected, decoded.getSplits().get(0).getScanRange()
                .getExtScanRange().getFileScanRange().getRanges());
        for (TScanRangeLocations original : locations) {
            Assertions.assertEquals(1, original.getScanRange().getExtScanRange().getFileScanRange().getRangesSize());
        }
    }

    @Test
    void compactsAssignedFileRangesWithoutReducingScannerConcurrency() {
        List<TScanRangeParams> original = ranges(1000);
        Map<Integer, List<TScanRangeParams>> input = Map.of(1, original);
        TQueryOptions options = new TQueryOptions().setMaxFileScannersConcurrency(16).setParallelInstance(7);

        List<TScanRangeParams> compacted = FileScanRangeCompactor.compactInstanceScanRanges(
                input, Map.of(1, new TFileScanRangeParams()), options).get(1);

        Assertions.assertTrue(compacted.size() >= 112);
        Assertions.assertTrue(compacted.size() < original.size());
        Assertions.assertEquals(flatten(original), flatten(compacted));
        Assertions.assertEquals(1000, input.get(1).size());
        Assertions.assertTrue(original.stream().allMatch(r -> files(r).getRangesSize() == 1));
        // Whatever the BE thread-pool/operator limit is, its final scanner count remains identical.
        for (int backendLimit = 1; backendLimit <= 112; backendLimit++) {
            Assertions.assertEquals(Math.min(backendLimit, original.size()),
                    Math.min(backendLimit, compacted.size()));
        }
    }

    @Test
    void usesBackendDefaultScannerLimitForUnsetOrNonPositiveSessionValue() {
        for (int configured : List.of(0, -1)) {
            List<TScanRangeParams> original = ranges(100);
            List<TScanRangeParams> compacted = FileScanRangeCompactor.compactInstanceScanRanges(
                    Map.of(1, original), Map.of(1, new TFileScanRangeParams()),
                    new TQueryOptions().setMaxFileScannersConcurrency(configured).setParallelInstance(1)).get(1);
            Assertions.assertTrue(compacted.size() >= 16);
            Assertions.assertTrue(compacted.size() < 100);
            Assertions.assertEquals(flatten(original), flatten(compacted));
        }
    }

    @Test
    void leavesSmallFileScansNonFileScansAndBatchSourceReferencesUnchanged() {
        List<TScanRangeParams> small = ranges(3);
        List<TScanRangeParams> nativeRanges = List.of(new TScanRangeParams().setScanRange(new TScanRange()));
        TFileScanRange source = new TFileScanRange().setSplitSource(new TSplitSource().setSplitSourceId(99));
        List<TScanRangeParams> batch = List.of(new TScanRangeParams().setScanRange(
                new TScanRange().setExtScanRange(new TExternalScanRange().setFileScanRange(source))));
        Map<Integer, List<TScanRangeParams>> result = FileScanRangeCompactor.compactInstanceScanRanges(
                Map.of(1, small, 2, nativeRanges, 3, batch),
                Map.of(1, new TFileScanRangeParams(), 3, new TFileScanRangeParams()), new TQueryOptions());

        Assertions.assertSame(small, result.get(1));
        Assertions.assertSame(nativeRanges, result.get(2));
        Assertions.assertSame(batch, result.get(3));
    }

    @Test
    void skipsQueriesWithoutFileScans() {
        Map<Integer, List<TScanRangeParams>> nativeScans = Map.of(1,
                List.of(new TScanRangeParams().setScanRange(new TScanRange())));
        Assertions.assertSame(nativeScans, FileScanRangeCompactor.compactInstanceScanRanges(
                nativeScans, Map.of(), new TQueryOptions()));
    }

    @Test
    void neverCombinesIndependentSplitSourceStreams() {
        List<TScanRangeParams> sources = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            sources.add(new TScanRangeParams().setScanRange(new TScanRange().setExtScanRange(
                    new TExternalScanRange().setFileScanRange(new TFileScanRange()
                            .setSplitSource(new TSplitSource().setSplitSourceId(i))))));
        }
        List<TScanRangeParams> result = FileScanRangeCompactor.compactInstanceScanRanges(
                Map.of(1, sources), Map.of(1, new TFileScanRangeParams()),
                new TQueryOptions().setMaxFileScannersConcurrency(1).setParallelInstance(1)).get(1);
        Assertions.assertEquals(sources, result);
    }

    @Test
    void preservesVolumeIdsParameterBoundariesAndInputObjects() {
        List<TScanRangeParams> original = ranges(20);
        for (int i = 0; i < 20; i++) {
            original.get(i).setVolumeId(i < 10 ? 1 : 2);
            files(original.get(i)).setParams(new TFileScanRangeParams()
                    .setFormatType(i < 5 ? TFileFormatType.FORMAT_PARQUET : TFileFormatType.FORMAT_JNI));
        }
        List<TScanRangeParams> compacted = FileScanRangeCompactor.compactInstanceScanRanges(
                Map.of(1, original), Map.of(1, new TFileScanRangeParams()),
                new TQueryOptions().setMaxFileScannersConcurrency(1).setParallelInstance(1)).get(1);

        Assertions.assertEquals(3, compacted.size());
        Assertions.assertEquals(List.of(5, 5, 10), compacted.stream()
                .map(r -> files(r).getRangesSize()).collect(Collectors.toList()));
        Assertions.assertEquals(List.of(1, 1, 2), compacted.stream()
                .map(TScanRangeParams::getVolumeId).collect(Collectors.toList()));
        Assertions.assertEquals(flatten(original), flatten(compacted));
        Assertions.assertTrue(original.stream().allMatch(r -> files(r).getRangesSize() == 1));
    }

    @Test
    void keepsConnectorFieldsAndShrinksSynchronousSerializedRequests() throws Exception {
        List<TFileRangeDesc> expected = connectorRanges();
        List<TScanRangeParams> original = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            for (TFileRangeDesc file : expected) {
                original.add(new TScanRangeParams().setScanRange(scanRange(file)));
            }
        }
        List<TScanRangeParams> compacted = FileScanRangeCompactor.compactInstanceScanRanges(
                Map.of(1, original), Map.of(1, new TFileScanRangeParams()), new TQueryOptions()).get(1);
        TSerializer serializer = new TSerializer();
        int beforeBytes = 0;
        int afterBytes = 0;
        List<TScanRangeParams> decoded = new ArrayList<>();
        for (TScanRangeParams range : original) {
            beforeBytes += serializer.serialize(range).length;
        }
        for (TScanRangeParams range : compacted) {
            byte[] bytes = serializer.serialize(range);
            afterBytes += bytes.length;
            TScanRangeParams copy = new TScanRangeParams();
            new TDeserializer().deserialize(copy, bytes);
            decoded.add(copy);
        }
        Assertions.assertEquals(flatten(original), flatten(decoded));
        Assertions.assertTrue(afterBytes < beforeBytes);
    }

    private static List<TScanRangeParams> ranges(int count) {
        List<TScanRangeParams> ranges = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ranges.add(new TScanRangeParams().setScanRange(scanRange(
                    new TFileRangeDesc().setPath("file" + i).setStartOffset(i).setSize(10))));
        }
        return ranges;
    }

    private static TScanRange scanRange(TFileRangeDesc file) {
        return new TScanRange().setExtScanRange(new TExternalScanRange()
                .setFileScanRange(new TFileScanRange().setRanges(new ArrayList<>(List.of(file)))));
    }

    private static TFileScanRange files(TScanRangeParams range) {
        return range.getScanRange().getExtScanRange().getFileScanRange();
    }

    private static List<TFileRangeDesc> flatten(List<TScanRangeParams> ranges) {
        return ranges.stream().flatMap(r -> files(r).getRanges().stream()).collect(Collectors.toList());
    }

    private static List<TFileRangeDesc> connectorRanges() {
        List<TTableFormatFileDesc> formats = List.of(
                new TTableFormatFileDesc().setTableFormatType("hive").setTransactionalHiveParams(
                        new TTransactionalHiveDesc().setPartition("p=1").setDeleteDeltas(List.of(
                                new TTransactionalHiveDeleteDeltaDesc().setDirectoryLocation("delete_delta")
                                        .setFileNames(List.of("bucket_00000"))))),
                new TTableFormatFileDesc().setTableFormatType("iceberg").setIcebergParams(
                        new TIcebergFileDesc().setFormatVersion(3).setPartitionSpecId(7)
                                .setPartitionDataJson("{partition:1}").setFirstRowId(100).setSerializedSplit("split")
                                .setDeleteFiles(List.of(new TIcebergDeleteFileDesc().setPath("position-delete")
                                        .setContent(1), new TIcebergDeleteFileDesc().setPath("equality-delete")
                                        .setContent(2).setFieldIds(List.of(1))))),
                new TTableFormatFileDesc().setTableFormatType("paimon").setPaimonParams(
                        new TPaimonFileDesc().setReaderType(TPaimonReaderType.PAIMON_NATIVE).setSchemaId(8)
                                .setDeletionFile(new TPaimonDeletionFileDesc().setPath("deletion-vector")
                                        .setOffset(9).setLength(10))),
                new TTableFormatFileDesc().setTableFormatType("paimon").setPaimonParams(
                        new TPaimonFileDesc().setReaderType(TPaimonReaderType.PAIMON_JNI).setPaimonSplit("jni-split")),
                new TTableFormatFileDesc().setTableFormatType("hudi").setHudiParams(
                        new THudiFileDesc().setInstantTime("20261013").setDataFilePath("base.parquet")
                                .setDeltaLogs(List.of("log.1", "log.2")).setSchemaId(11)),
                new TTableFormatFileDesc().setTableFormatType("max_compute").setMaxComputeParams(
                        new TMaxComputeFileDesc().setSessionId("session").setTableBatchReadSession("read-session")),
                new TTableFormatFileDesc().setTableFormatType("trino_connector").setTrinoConnectorParams(
                        new TTrinoConnectorFileDesc().setTrinoConnectorSplit("trino-split")),
                new TTableFormatFileDesc().setTableFormatType("jdbc").setJdbcParams(Map.of("query", "SELECT 1")),
                new TTableFormatFileDesc().setTableFormatType("es").setEsParams(Map.of("shard_id", "2")),
                new TTableFormatFileDesc().setTableFormatType("adbc").setAdbcParams(Map.of("partition_b64", "AAAA")),
                new TTableFormatFileDesc().setTableFormatType("lance").setLanceParams(
                        new TLanceFileDesc().setDatasetUri("dataset").setFragmentIds(List.of(1L, 2L)).setVersion(3)),
                new TTableFormatFileDesc().setTableFormatType("fluss").setFlussParams(
                        Map.of("range_type", "LOG", "start_offset", "5")),
                new TTableFormatFileDesc().setTableFormatType("remote_doris").setRemoteDorisParams(
                        new TRemoteDorisFileDesc().setTicket(new byte[] {1, 2, 3}).setLocationUri("grpc://backend")));
        List<TFileRangeDesc> result = new ArrayList<>();
        for (int i = 0; i < formats.size(); i++) {
            result.add(new TFileRangeDesc().setPath("file" + i).setStartOffset(i).setSize(10 + i)
                    .setFileSize(100 + i).setModificationTime(1000 + i).setFsName("filesystem" + i)
                    .setFormatType(i % 2 == 0 ? TFileFormatType.FORMAT_PARQUET : TFileFormatType.FORMAT_JNI)
                    .setSelfSplitWeight(i + 1).setFileCacheAdmission(i % 2 == 0)
                    .setColumnsFromPath(List.of("partition" + i)).setColumnsFromPathKeys(List.of("p"))
                    .setColumnsFromPathIsNull(List.of(i % 2 == 0)).setTableFormatParams(formats.get(i)));
        }
        return result;
    }
}
