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

import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.DebugPointUtil.DebugPoint;
import org.apache.doris.rpc.FileScanEncryption;
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
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TScanRangeParams;
import org.apache.doris.thrift.TTableFormatFileDesc;

import java.util.List;
import java.util.Map;
import java.util.function.IntSupplier;

/** Last checks before serializing file keys, including lazy split responses and replanned retries. */
public final class FileScanEncryptionGuard {
    private FileScanEncryptionGuard() {
    }

    public static void checkFragments(TPipelineFragmentParamsList fragments) {
        checkFragments(fragments, () -> 0);
    }

    public static boolean checkFragments(TPipelineFragmentParamsList fragments, IntSupplier verifiedVersion) {
        Checker checker = new Checker(verifiedVersion, FileScanEncryption.VERSION, true);
        if (fragments.isSetParamsList()) {
            Map<Integer, TFileScanRangeParams> paramsByNode = Map.of();
            for (TPipelineFragmentParams fragment : fragments.getParamsList()) {
                // Later simplified fragments reuse the query-wide params sent in the first fragment.
                if (fragment.isSetFileScanParams()) {
                    paramsByNode = fragment.getFileScanParams();
                    for (TFileScanRangeParams params : paramsByNode.values()) {
                        checker.checkParams(params);
                    }
                }
                if (fragment.isSetLocalParams()) {
                    for (TPipelineInstanceParams instance : fragment.getLocalParams()) {
                        if (instance.isSetPerNodeScanRanges()) {
                            for (Map.Entry<Integer, List<TScanRangeParams>> node
                                    : instance.getPerNodeScanRanges().entrySet()) {
                                for (TScanRangeParams range : node.getValue()) {
                                    checker.checkRange(range.getScanRange(), paramsByNode.get(node.getKey()));
                                }
                            }
                        }
                    }
                }
            }
        }
        return checker.needsCapability;
    }

    public static void checkBatch(List<TScanRangeLocations> ranges) {
        checkBatch(ranges, () -> 0, 0);
    }

    public static boolean checkBatch(List<TScanRangeLocations> ranges, IntSupplier verifiedVersion, int callerVersion) {
        Checker checker = new Checker(verifiedVersion, callerVersion, false);
        for (TScanRangeLocations range : ranges) {
            checker.checkRange(range.getScanRange(), null);
        }
        return checker.needsCapability;
    }

    private static final class Checker {
        private final IntSupplier verifiedVersion;
        private final int callerVersion;
        private final boolean initialDispatch;
        private boolean needsCapability;

        private Checker(IntSupplier verifiedVersion, int callerVersion, boolean initialDispatch) {
            this.initialDispatch = initialDispatch;
            this.verifiedVersion = verifiedVersion;
            this.callerVersion = callerVersion;
        }

        private void checkDescriptor(TFileFormatType format, TTableFormatFileDesc table) {
            boolean encrypted = table != null && FileScanEncryption.validate(table);
            if (encrypted && format != TFileFormatType.FORMAT_PARQUET_PME) {
                throw new IllegalStateException("File encryption [READ_UNAVAILABLE]");
            }
            if (format == TFileFormatType.FORMAT_PARQUET_PME && !needsCapability) {
                // Even a marker-only first batch must exclude old readers before sending any fragment.
                if (callerVersion < FileScanEncryption.VERSION
                        || verifiedVersion.getAsInt() < FileScanEncryption.VERSION) {
                    throw new IllegalStateException("File encryption [READ_UNAVAILABLE]");
                }
                needsCapability = true;
            }
        }

        private void checkParams(TFileScanRangeParams params) {
            checkDescriptor(params.getFormatType(), params.getTableFormatParams());
        }

        private void checkRange(TScanRange range, TFileScanRangeParams inheritedParams) {
            if (!range.isSetExtScanRange() || !range.getExtScanRange().isSetFileScanRange()) {
                return;
            }
            TFileScanRange fileScan = range.getExtScanRange().getFileScanRange();
            TFileScanRangeParams params = fileScan.isSetParams() ? fileScan.getParams() : inheritedParams;
            if (params != null) {
                checkParams(params);
            }
            if (fileScan.isSetRanges()) {
                DebugPoint injection = DebugPointUtil.getDebugPoint("FileScanEncryptionGuard.inject");
                for (TFileRangeDesc file : fileScan.getRanges()) {
                    TTableFormatFileDesc table = file.getTableFormatParams();
                    if (injection != null && file.isSetPath()
                            && file.getPath().contains(
                                    injection.param("path_contains", "iceberg_encryption_wire_db"))) {
                        table = injectedDescriptor(injection);
                    }
                    TFileFormatType format = file.isSetFormatType() ? file.getFormatType()
                            : (params == null ? null : params.getFormatType());
                    if (initialDispatch && format == TFileFormatType.FORMAT_PARQUET_PME
                            && (params == null || params.getFormatType() != TFileFormatType.FORMAT_PARQUET_PME)) {
                        // An old reader may ignore range overrides and choose its reader from shared params alone.
                        throw new IllegalStateException("File encryption [READ_UNAVAILABLE]");
                    }
                    checkDescriptor(format, table);
                }
            }
        }
    }

    private static TTableFormatFileDesc injectedDescriptor(DebugPoint point) {
        TFileEncryptionInfo info = new TFileEncryptionInfo().setVersion(point.param("version", 1))
                .setKey(new byte[point.param("invalid_key", false) ? 0 : 16]).setAadPrefix(new byte[] {0, (byte) 255});
        TIcebergFileDesc file = new TIcebergFileDesc();
        if (point.param("delete", false)) {
            file.setDeleteFiles(List.of(new TIcebergDeleteFileDesc().setEncryptionInfo(info)));
        } else {
            file.setEncryptionInfo(info);
        }
        return new TTableFormatFileDesc().setIcebergParams(file);
    }
}
