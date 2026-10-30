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

package org.apache.doris.connector.deltalake;

import org.apache.doris.thrift.TDeltaLakeDeletionVectorDesc;

import io.delta.kernel.Snapshot;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.tablefeatures.TableFeature;
import io.delta.kernel.statistics.DataFileStatistics;
import io.delta.kernel.utils.FileStatus;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

/** The only production class coupled to Kernel internal APIs; pinned to Kernel 4.4.1. */
final class DeltaKernelCompat {
    private DeltaKernelCompat() {
    }

    static String tableId(Snapshot snapshot) {
        return snapshot.getMetadata().getId();
    }

    static FileStatus fileStatus(Row file) {
        return InternalScanFileUtils.getAddFileStatus(file);
    }

    static Map<String, String> partitionValues(Row file) {
        return InternalScanFileUtils.getPartitionValues(file);
    }

    static OptionalLong numRecords(Row file) {
        Row add = file.getStruct(InternalScanFileUtils.ADD_FILE_ORDINAL);
        int ordinal = InternalScanFileUtils.ADD_FILE_STATS_ORDINAL;
        if (add.isNullAt(ordinal)) {
            return OptionalLong.empty();
        }
        Optional<Long> count = DataFileStatistics.getNumRecords(add.getString(ordinal));
        return count.isPresent() ? OptionalLong.of(count.get()) : OptionalLong.empty();
    }

    static Optional<TDeltaLakeDeletionVectorDesc> deletionVector(Row file, String tablePath) {
        DeletionVectorDescriptor dv = InternalScanFileUtils.getDeletionVectorDescriptorFromRow(file);
        if (dv == null || dv.getCardinality() == 0) {
            return Optional.empty();
        }
        TDeltaLakeDeletionVectorDesc result = new TDeltaLakeDeletionVectorDesc();
        result.setSizeInBytes(dv.getSizeInBytes());
        result.setCardinality(dv.getCardinality());
        if (dv.isInline()) {
            result.setInlineData(dv.inlineData());
        } else {
            result.setPath(dv.getAbsolutePath(tablePath));
            result.setOffset(dv.getOffset().orElse(0));
        }
        return Optional.of(result);
    }

    static Set<String> readerFeatures(Snapshot snapshot) {
        return readerFeatures(snapshot.getProtocol());
    }

    static Set<String> readerFeatures(Protocol protocol) {
        // Kernel 4.4.1's legacy-protocol branch also selects writer-only features (reader version 0).
        // Those do not require reader support and must not reject an otherwise readable table.
        return protocol.getImplicitlyAndExplicitlySupportedReaderWriterFeatures().stream()
                .filter(feature -> feature.minReaderVersion() > 0)
                .map(TableFeature::featureName).collect(Collectors.toSet());
    }
}
