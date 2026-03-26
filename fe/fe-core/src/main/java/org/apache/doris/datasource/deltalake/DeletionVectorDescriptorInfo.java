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

package org.apache.doris.datasource.deltalake;

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.InternalScanFileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Holds the Deletion Vector descriptor information for a Delta Lake data file.
 * This is extracted from the scan file Row provided by Delta Kernel.
 *
 * Delta Lake DV types:
 *   "u" - UUID-based file: deletion_vector_{uuid}.bin
 *   "p" - Absolute path
 *   "i" - Inline (Base85-encoded)
 *
 * The FE resolves the storageType + pathOrInlineDv into a final absolute
 * file path and offset, so BE can directly reuse the Iceberg DV reading logic.
 */
public class DeletionVectorDescriptorInfo {
    private static final Logger LOG = LogManager.getLogger(DeletionVectorDescriptorInfo.class);

    private final String storageType;
    private final String pathOrInlineDv;
    private final int offset;
    private final int sizeInBytes;
    private final long cardinality;

    // Resolved absolute path for BE (resolved from storageType + pathOrInlineDv + tableLocation)
    private String resolvedPath;
    private int resolvedOffset;

    public DeletionVectorDescriptorInfo(String storageType, String pathOrInlineDv,
            int offset, int sizeInBytes, long cardinality) {
        this.storageType = storageType;
        this.pathOrInlineDv = pathOrInlineDv;
        this.offset = offset;
        this.sizeInBytes = sizeInBytes;
        this.cardinality = cardinality;
    }

    /**
     * Resolve the DV file path from storageType and pathOrInlineDv.
     * After resolution, BE can directly use resolvedPath + resolvedOffset
     * to read the DV blob, identical to Iceberg DV reading.
     */
    public void resolveFilePath(String tableLocation) {
        switch (storageType) {
            case "u":
                // UUID-based: tableLocation + "/deletion_vector_" + uuid + ".bin"
                resolvedPath = tableLocation + "/deletion_vector_" + pathOrInlineDv + ".bin";
                resolvedOffset = offset;
                break;
            case "p":
                // Absolute path
                resolvedPath = pathOrInlineDv;
                resolvedOffset = offset;
                break;
            case "i":
                // Inline DV (Base85 encoded) — not a file path
                // This case is handled differently: the DV data is inline
                resolvedPath = null;
                resolvedOffset = 0;
                break;
            default:
                LOG.warn("Unknown DV storage type: {}", storageType);
                resolvedPath = null;
                resolvedOffset = 0;
        }
    }

    /**
     * Extract DeletionVectorDescriptor from a Delta Kernel scan file Row.
     * Returns null if there is no DV for this file.
     */
    public static DeletionVectorDescriptorInfo fromScanFileRow(Row scanFileRow, String tableLocation) {
        try {
            // Check if deletion vector info exists in the scan file row
            // In Delta Kernel API, DV info is part of the add file action
            io.delta.kernel.internal.actions.DeletionVectorDescriptor dvDescriptor =
                    InternalScanFileUtils.getDeletionVectorDescriptorFromRow(scanFileRow);
            if (dvDescriptor == null) {
                return null;
            }

            String storageType = dvDescriptor.getStorageType();
            String pathOrInlineDv = dvDescriptor.getPathOrInlineDv();
            int dvOffset = dvDescriptor.getOffset().orElse(0);
            int sizeInBytes = dvDescriptor.getSizeInBytes();
            long cardinality = dvDescriptor.getCardinality();

            DeletionVectorDescriptorInfo info = new DeletionVectorDescriptorInfo(
                    storageType, pathOrInlineDv, dvOffset, sizeInBytes, cardinality);
            info.resolveFilePath(tableLocation);
            return info;
        } catch (Exception e) {
            LOG.debug("No deletion vector for scan file row", e);
            return null;
        }
    }

    public String getStorageType() {
        return storageType;
    }

    public String getPathOrInlineDv() {
        return pathOrInlineDv;
    }

    public int getOffset() {
        return offset;
    }

    public int getSizeInBytes() {
        return sizeInBytes;
    }

    public long getCardinality() {
        return cardinality;
    }

    public String getResolvedPath() {
        return resolvedPath;
    }

    public int getResolvedOffset() {
        return resolvedOffset;
    }

    public boolean isInline() {
        return "i".equals(storageType);
    }
}
