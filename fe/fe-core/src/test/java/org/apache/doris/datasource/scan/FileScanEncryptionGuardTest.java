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

import org.apache.doris.common.Config;
import org.apache.doris.thrift.TExternalScanRange;
import org.apache.doris.thrift.TFileEncryptionInfo;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineFragmentParamsList;
import org.apache.doris.thrift.TPipelineInstanceParams;
import org.apache.doris.thrift.TPlan;
import org.apache.doris.thrift.TPlanFragment;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TScanRangeParams;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

class FileScanEncryptionGuardTest {
    @Test
    void plaintextScansRemainAvailable() {
        FileScanEncryptionGuard.checkBatch(List.of(location(new TIcebergFileDesc())));
        FileScanEncryptionGuard.checkFragments(new TPipelineFragmentParamsList());
    }

    @Test
    void dataAndEachDeleteKindAreRejectedBeforeDispatchOrLazyResponse() {
        for (int content : List.of(0, 1, 2, 3)) {
            TIcebergFileDesc iceberg = content == 0 ? new TIcebergFileDesc().setEncryptionInfo(info())
                    : new TIcebergFileDesc().setDeleteFiles(List.of(new TIcebergDeleteFileDesc()
                            .setContent(content).setEncryptionInfo(info())));
            TScanRangeLocations location = location(iceberg);
            assertUnavailable(() -> FileScanEncryptionGuard.checkBatch(List.of(location)));
            TPipelineFragmentParamsList fragments = fragments(location);
            assertUnavailable(() -> FileScanEncryptionGuard.checkFragments(fragments));
            // A repeated dispatch must not bypass the missing native format or capability.
            assertUnavailable(() -> FileScanEncryptionGuard.checkFragments(fragments));
        }
    }

    @Test
    void mixedOlapAndEncryptedScansCheckOnlyTheirRecipientsMarkers() {
        Map<Integer, TFileScanRangeParams> shared = Map.of(1,
                new TFileScanRangeParams().setFormatType(TFileFormatType.FORMAT_PARQUET_PME));
        TPipelineFragmentParams olap = new TPipelineFragmentParams().setFileScanParams(shared)
                .setFragment(plan(2, TPlanNodeType.OLAP_SCAN_NODE))
                .setLocalParams(List.of(new TPipelineInstanceParams().setPerNodeScanRanges(Map.of(2,
                        List.of(new TScanRangeParams().setScanRange(new TScanRange()))))));
        Assertions.assertFalse(FileScanEncryptionGuard.checkFragments(
                new TPipelineFragmentParamsList().setParamsList(List.of(olap)), () -> false));
        TScanRangeLocations encrypted = location(new TIcebergFileDesc().setEncryptionInfo(info()));
        encrypted.getScanRange().getExtScanRange().getFileScanRange().getRanges().get(0)
                .setFormatType(TFileFormatType.FORMAT_PARQUET_PME);
        TPipelineFragmentParamsList scan = fragments(encrypted);
        scan.getParamsList().get(0).setFileScanParams(shared).setFragment(plan(1, TPlanNodeType.FILE_SCAN_NODE));
        Assertions.assertTrue(FileScanEncryptionGuard.checkFragments(scan, () -> true));
        assertUnavailable(() -> FileScanEncryptionGuard.checkFragments(scan, () -> false));
    }

    @Test
    void markerOnlyFilePlanStillRequiresCapabilityWithoutInitialRanges() {
        TPipelineFragmentParams scan = new TPipelineFragmentParams()
                .setFragment(plan(1, TPlanNodeType.FILE_SCAN_NODE)).setFileScanParams(Map.of(1,
                        new TFileScanRangeParams().setFormatType(TFileFormatType.FORMAT_PARQUET_PME)));
        TPipelineFragmentParamsList request = new TPipelineFragmentParamsList().setParamsList(List.of(scan));
        Assertions.assertTrue(FileScanEncryptionGuard.checkFragments(request, () -> true));
        assertUnavailable(() -> FileScanEncryptionGuard.checkFragments(request, () -> false));
        TPipelineFragmentParams owner = new TPipelineFragmentParams().setFileScanParams(scan.getFileScanParams())
                .setFragment(plan(2, TPlanNodeType.OLAP_SCAN_NODE));
        scan.unsetFileScanParams();
        scan.setIsSimplifiedParam(true);
        request.setParamsList(List.of(owner, scan));
        Assertions.assertTrue(FileScanEncryptionGuard.checkFragments(request, () -> true));
        assertUnavailable(() -> FileScanEncryptionGuard.checkFragments(request, () -> false));
        scan.unsetFragment();
        scan.setLocalParams(List.of(new TPipelineInstanceParams().setPerNodeScanRanges(Map.of(1, List.of()))));
        Assertions.assertTrue(FileScanEncryptionGuard.checkFragments(request, () -> true));
        assertUnavailable(() -> FileScanEncryptionGuard.checkFragments(request, () -> false));
    }

    @Test
    void unassignedSharedParametersStillValidateAndProtectActualKeys() {
        TFileScanRangeParams unused = new TFileScanRangeParams().setFormatType(TFileFormatType.FORMAT_PARQUET_PME)
                .setTableFormatParams(new TTableFormatFileDesc().setIcebergParams(
                        new TIcebergFileDesc().setEncryptionInfo(info())));
        TPipelineFragmentParamsList request = new TPipelineFragmentParamsList().setParamsList(List.of(
                new TPipelineFragmentParams().setFileScanParams(Map.of(1, unused))
                        .setFragment(plan(2, TPlanNodeType.OLAP_SCAN_NODE))));
        assertUnavailable(() -> FileScanEncryptionGuard.checkFragments(request, () -> false));
        Assertions.assertTrue(FileScanEncryptionGuard.checkFragments(request, () -> true));
        unused.getTableFormatParams().getIcebergParams().getEncryptionInfo().setVersion(9);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> FileScanEncryptionGuard.checkFragments(request, () -> true));
    }

    @Test
    void queryLevelFileParamsCannotBypassTheCheck() {
        TPipelineFragmentParams fragment = new TPipelineFragmentParams().setFileScanParams(Map.of(1,
                new TFileScanRangeParams().setTableFormatParams(new TTableFormatFileDesc()
                        .setIcebergParams(new TIcebergFileDesc().setEncryptionInfo(info())))));
        assertUnavailable(() -> FileScanEncryptionGuard.checkFragments(
                new TPipelineFragmentParamsList().setParamsList(List.of(fragment))));
    }

    @Test
    void externalChannelsRejectKeysEvenWhenLocalPrivateTlsIsEnabled() {
        boolean enabled = Config.enable_tls;
        try {
            Config.enable_tls = true;
            FileScanEncryptionGuard.checkFragments(fragments(location(new TIcebergFileDesc())), false);
            for (int content : List.of(0, 1, 2, 3)) {
                TIcebergFileDesc iceberg = content == 0 ? new TIcebergFileDesc().setEncryptionInfo(info())
                        : new TIcebergFileDesc().setDeleteFiles(List.of(new TIcebergDeleteFileDesc()
                                .setContent(content).setEncryptionInfo(info())));
                IllegalStateException error = Assertions.assertThrows(IllegalStateException.class,
                        () -> FileScanEncryptionGuard.checkFragments(fragments(location(iceberg)), false));
                Assertions.assertEquals("File encryption [KEY_TRANSPORT_UNAVAILABLE]", error.getMessage());
            }
            TPipelineFragmentParams shared = new TPipelineFragmentParams().setFileScanParams(Map.of(1,
                    new TFileScanRangeParams().setTableFormatParams(new TTableFormatFileDesc()
                            .setIcebergParams(new TIcebergFileDesc().setEncryptionInfo(info())))));
            IllegalStateException error = Assertions.assertThrows(IllegalStateException.class,
                    () -> FileScanEncryptionGuard.checkFragments(
                            new TPipelineFragmentParamsList().setParamsList(List.of(shared)), false));
            Assertions.assertEquals("File encryption [KEY_TRANSPORT_UNAVAILABLE]", error.getMessage());
        } finally {
            Config.enable_tls = enabled;
        }
    }

    @Test
    void remoteMarkerCannotUseALocalCapabilityProof() {
        TPipelineFragmentParams fragment = new TPipelineFragmentParams().setFragment(plan(1, TPlanNodeType.FILE_SCAN_NODE))
                .setFileScanParams(Map.of(1,
                        new TFileScanRangeParams().setFormatType(TFileFormatType.FORMAT_PARQUET_PME)));
        TPipelineFragmentParamsList fragments = new TPipelineFragmentParamsList().setParamsList(List.of(fragment));
        IllegalStateException error = Assertions.assertThrows(IllegalStateException.class,
                () -> FileScanEncryptionGuard.checkFragments(fragments, () -> true, false));
        Assertions.assertEquals("File encryption [KEY_TRANSPORT_UNAVAILABLE]", error.getMessage());
        Assertions.assertTrue(FileScanEncryptionGuard.checkFragments(fragments, () -> true, true));
    }

    @Test
    void malformedAndFutureDescriptorsDoNotBecomePlaintext() {
        for (TFileEncryptionInfo value : List.of(new TFileEncryptionInfo(), info().setVersion(9))) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> FileScanEncryptionGuard.checkBatch(
                    List.of(location(new TIcebergFileDesc().setEncryptionInfo(value)))));
        }
    }

    @Test
    void markedNativeKeysRequireAVerifiedTarget() {
        for (int content : List.of(0, 1, 2)) {
            TIcebergFileDesc iceberg = content == 0 ? new TIcebergFileDesc().setEncryptionInfo(info())
                    : new TIcebergFileDesc().setDeleteFiles(List.of(new TIcebergDeleteFileDesc()
                            .setContent(content).setEncryptionInfo(info())));
            TScanRangeLocations location = location(iceberg);
            TFileRangeDesc file = location.getScanRange().getExtScanRange().getFileScanRange().getRanges().get(0);
            file.setFormatType(TFileFormatType.FORMAT_PARQUET_PME);
            Assertions.assertTrue(FileScanEncryptionGuard.checkBatch(List.of(location), () -> true));
            assertUnavailable(() -> FileScanEncryptionGuard.checkBatch(List.of(location), () -> false));
            file.setFormatType(TFileFormatType.FORMAT_PARQUET);
            assertUnavailable(() -> FileScanEncryptionGuard.checkBatch(List.of(location), () -> true));
        }
    }

    @Test
    void markerOnlyFragmentsRecheckCapabilityBeforeEverySerialization() {
        TPipelineFragmentParams fragment = new TPipelineFragmentParams().setFragment(plan(1, TPlanNodeType.FILE_SCAN_NODE))
                .setFileScanParams(Map.of(1,
                        new TFileScanRangeParams().setFormatType(TFileFormatType.FORMAT_PARQUET_PME)));
        TPipelineFragmentParamsList fragments = new TPipelineFragmentParamsList().setParamsList(List.of(fragment));
        AtomicBoolean supported = new AtomicBoolean(true);
        Assertions.assertTrue(FileScanEncryptionGuard.checkFragments(fragments, supported::get));
        supported.set(false);
        assertUnavailable(() -> FileScanEncryptionGuard.checkFragments(fragments, supported::get));
    }

    @Test
    void plaintextBatchDoesNotRequireEncryptionCapability() {
        Assertions.assertFalse(FileScanEncryptionGuard.checkBatch(
                List.of(location(new TIcebergFileDesc())), () -> false));
    }

    @Test
    void initialFragmentsMustMarkSharedParamsForPhysicalCountHandling() {
        TScanRangeLocations location = location(new TIcebergFileDesc().setEncryptionInfo(info()));
        location.getScanRange().getExtScanRange().getFileScanRange().getRanges().get(0)
                .setFormatType(TFileFormatType.FORMAT_PARQUET_PME);
        TPipelineFragmentParamsList fragments = fragments(location);
        fragments.getParamsList().get(0).setFileScanParams(Map.of(1,
                new TFileScanRangeParams().setFormatType(TFileFormatType.FORMAT_PARQUET)));
        assertUnavailable(() -> FileScanEncryptionGuard.checkFragments(fragments, () -> true));
        fragments.getParamsList().get(0).getFileScanParams().get(1).setFormatType(TFileFormatType.FORMAT_PARQUET_PME);
        Assertions.assertTrue(FileScanEncryptionGuard.checkFragments(fragments, () -> true));
    }

    @Test
    void simplifiedFragmentsInheritTheFirstFragmentsSharedScanParams() {
        TScanRangeLocations location = location(new TIcebergFileDesc().setEncryptionInfo(info()));
        location.getScanRange().getExtScanRange().getFileScanRange().getRanges().get(0)
                .setFormatType(TFileFormatType.FORMAT_PARQUET_PME);
        TPipelineFragmentParams owner = new TPipelineFragmentParams().setFragment(plan(1, TPlanNodeType.FILE_SCAN_NODE))
                .setFileScanParams(Map.of(1,
                        new TFileScanRangeParams().setFormatType(TFileFormatType.FORMAT_PARQUET_PME)));
        TPipelineFragmentParams simplified = fragments(location).getParamsList().get(0).setIsSimplifiedParam(true);
        TPipelineFragmentParamsList request = new TPipelineFragmentParamsList()
                .setParamsList(List.of(owner, simplified));
        Assertions.assertTrue(FileScanEncryptionGuard.checkFragments(request, () -> true));
        assertUnavailable(() -> FileScanEncryptionGuard.checkFragments(request, () -> false));
        owner.getFileScanParams().get(1).setFormatType(TFileFormatType.FORMAT_PARQUET);
        assertUnavailable(() -> FileScanEncryptionGuard.checkFragments(request, () -> true));
    }

    private static TPlanFragment plan(int nodeId, TPlanNodeType type) {
        return new TPlanFragment().setPlan(new TPlan().setNodes(List.of(
                new TPlanNode().setNodeId(nodeId).setNodeType(type))));
    }

    private static TFileEncryptionInfo info() {
        return new TFileEncryptionInfo().setVersion(1).setKey(new byte[16]).setAadPrefix(new byte[] {0, (byte) 255});
    }

    private static TScanRangeLocations location(TIcebergFileDesc iceberg) {
        return new TScanRangeLocations().setScanRange(new TScanRange().setExtScanRange(new TExternalScanRange()
                .setFileScanRange(new TFileScanRange().setRanges(List.of(new TFileRangeDesc()
                        .setTableFormatParams(new TTableFormatFileDesc().setIcebergParams(iceberg)))))));
    }

    private static TPipelineFragmentParamsList fragments(TScanRangeLocations location) {
        return new TPipelineFragmentParamsList().setParamsList(List.of(new TPipelineFragmentParams()
                .setLocalParams(List.of(new TPipelineInstanceParams().setPerNodeScanRanges(Map.of(1,
                        List.of(new TScanRangeParams().setScanRange(location.getScanRange()))))))));
    }

    private static void assertUnavailable(Runnable action) {
        IllegalStateException error = Assertions.assertThrows(IllegalStateException.class, action::run);
        Assertions.assertEquals("File encryption [READ_UNAVAILABLE]", error.getMessage());
    }
}
