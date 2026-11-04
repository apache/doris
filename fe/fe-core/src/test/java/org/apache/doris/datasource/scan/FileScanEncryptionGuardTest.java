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

import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TExternalScanRange;
import org.apache.doris.thrift.TFileEncryptionInfo;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineFragmentParamsList;
import org.apache.doris.thrift.TPipelineInstanceParams;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TScanRangeParams;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class FileScanEncryptionGuardTest {
    @Test
    void legacyPlaintextScansNeedNoNewCapabilities() {
        FileScanEncryptionGuard.checkBatch(List.of(location(new TIcebergFileDesc())), null);
        FileScanEncryptionGuard.checkFragments(new TPipelineFragmentParamsList(), null);
    }

    @Test
    void dataAndEachDeleteKindAreRejectedBeforeDispatchOrLazyResponse() {
        for (int content : List.of(0, 1, 2, 3)) {
            TIcebergFileDesc iceberg = content == 0 ? new TIcebergFileDesc().setEncryptionInfo(info())
                    : new TIcebergFileDesc().setDeleteFiles(List.of(new TIcebergDeleteFileDesc()
                            .setContent(content).setEncryptionInfo(info())));
            TScanRangeLocations location = location(iceberg);
            assertUnavailable(() -> FileScanEncryptionGuard.checkBatch(List.of(location), null));
            TPipelineFragmentParamsList fragments = fragments(location);
            assertUnavailable(() -> FileScanEncryptionGuard.checkFragments(fragments, new Backend()));
            // Reassigning exactly the same ranges to another legacy backend must recheck the receiver.
            assertUnavailable(() -> FileScanEncryptionGuard.checkFragments(fragments, new Backend()));
        }
    }

    @Test
    void queryLevelFileParamsCannotBypassTheCheck() {
        TPipelineFragmentParams fragment = new TPipelineFragmentParams().setFileScanParams(Map.of(1,
                new TFileScanRangeParams().setTableFormatParams(new TTableFormatFileDesc()
                        .setIcebergParams(new TIcebergFileDesc().setEncryptionInfo(info())))));
        assertUnavailable(() -> FileScanEncryptionGuard.checkFragments(
                new TPipelineFragmentParamsList().setParamsList(List.of(fragment)), null));
    }

    @Test
    void readerCapabilityDoesNotAuthorizeUnprotectedKeyTransport() {
        Backend capable = GsonUtils.GSON.fromJson(
                "{\"fileEncryptionCapabilities\":{\"version\":1,\"readerSupported\":true}}", Backend.class);
        IllegalStateException error = Assertions.assertThrows(IllegalStateException.class,
                () -> FileScanEncryptionGuard.checkFragments(fragments(location(
                        new TIcebergFileDesc().setEncryptionInfo(info()))), capable));
        Assertions.assertEquals("File encryption [RPC_PROTECTION_UNAVAILABLE]", error.getMessage());
    }

    @Test
    void malformedAndFutureDescriptorsDoNotBecomePlaintext() {
        for (TFileEncryptionInfo value : List.of(new TFileEncryptionInfo(), info().setVersion(9))) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> FileScanEncryptionGuard.checkBatch(
                    List.of(location(new TIcebergFileDesc().setEncryptionInfo(value))), null));
        }
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
        Assertions.assertEquals("File encryption [BACKEND_CAPABILITY_UNAVAILABLE]", error.getMessage());
    }
}
