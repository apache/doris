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

/** Last checks before serializing file keys, including lazy split responses and replanned retries. */
public final class FileScanEncryptionGuard {
    private FileScanEncryptionGuard() {
    }

    private static void rejectEncryptedRead() {
        // Native decryption and protected transport for file keys are not implemented yet.
        throw new IllegalStateException("File encryption [READ_UNAVAILABLE]");
    }

    public static void checkFragments(TPipelineFragmentParamsList fragments) {
        if (!fragments.isSetParamsList()) {
            return;
        }
        for (TPipelineFragmentParams fragment : fragments.getParamsList()) {
            if (fragment.isSetFileScanParams()) {
                for (TFileScanRangeParams params : fragment.getFileScanParams().values()) {
                    checkParams(params);
                }
            }
            if (fragment.isSetLocalParams()) {
                for (TPipelineInstanceParams instance : fragment.getLocalParams()) {
                    if (instance.isSetPerNodeScanRanges()) {
                        for (List<TScanRangeParams> ranges : instance.getPerNodeScanRanges().values()) {
                            for (TScanRangeParams range : ranges) {
                                checkRange(range.getScanRange());
                            }
                        }
                    }
                }
            }
        }
    }

    public static void checkBatch(List<TScanRangeLocations> ranges) {
        for (TScanRangeLocations range : ranges) {
            checkRange(range.getScanRange());
        }
    }

    private static void checkParams(TFileScanRangeParams params) {
        if (params.isSetTableFormatParams() && FileScanEncryption.validate(params.getTableFormatParams())) {
            rejectEncryptedRead();
        }
    }

    private static void checkRange(TScanRange range) {
        if (!range.isSetExtScanRange() || !range.getExtScanRange().isSetFileScanRange()) {
            return;
        }
        TFileScanRange fileScan = range.getExtScanRange().getFileScanRange();
        if (fileScan.isSetParams()) {
            checkParams(fileScan.getParams());
        }
        if (fileScan.isSetRanges()) {
            DebugPoint injection = DebugPointUtil.getDebugPoint("FileScanEncryptionGuard.inject");
            for (TFileRangeDesc file : fileScan.getRanges()) {
                TTableFormatFileDesc table = file.getTableFormatParams();
                if (injection != null && file.isSetPath()
                        && file.getPath().contains(injection.param("path_contains", "iceberg_encryption_wire_db"))) {
                    // Synthetic descriptors let SQL regressions exercise dispatch without opening encrypted reads.
                    table = injectedDescriptor(injection);
                }
                if (table != null && FileScanEncryption.validate(table)) {
                    rejectEncryptedRead();
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
