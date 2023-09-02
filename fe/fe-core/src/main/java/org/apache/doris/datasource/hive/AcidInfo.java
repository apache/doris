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

package org.apache.doris.datasource.hive;

import java.util.List;
import java.util.Objects;

/**
 * Stores information about Acid properties of a partition.
 */
public class AcidInfo {
    private final String partitionLocation;
    private final List<DeleteDeltaInfo> deleteDeltas;

    public AcidInfo(String partitionLocation, List<DeleteDeltaInfo> deleteDeltas) {
        this.partitionLocation = partitionLocation;
        this.deleteDeltas = deleteDeltas;
    }

    public String getPartitionLocation() {
        return partitionLocation;
    }

    public List<DeleteDeltaInfo> getDeleteDeltas() {
        return deleteDeltas;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AcidInfo acidInfo = (AcidInfo) o;
        return Objects.equals(partitionLocation, acidInfo.partitionLocation) && Objects.equals(
                deleteDeltas, acidInfo.deleteDeltas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionLocation, deleteDeltas);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AcidInfo{");
        sb.append("partitionLocation='").append(partitionLocation).append('\'');
        sb.append(", deleteDeltas=").append(deleteDeltas);
        sb.append('}');
        return sb.toString();
    }

    public static class DeleteDeltaInfo {
        private String directoryLocation;
        private List<String> fileNames;

        public DeleteDeltaInfo(String location, List<String> filenames) {
            this.directoryLocation = location;
            this.fileNames = filenames;
        }

        public String getDirectoryLocation() {
            return directoryLocation;
        }

        public List<String> getFileNames() {
            return fileNames;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DeleteDeltaInfo that = (DeleteDeltaInfo) o;
            return Objects.equals(directoryLocation, that.directoryLocation)
                    && Objects.equals(fileNames, that.fileNames);
        }

        @Override
        public int hashCode() {
            return Objects.hash(directoryLocation, fileNames);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("DeleteDeltaInfo{");
            sb.append("directoryLocation='").append(directoryLocation).append('\'');
            sb.append(", fileNames=").append(fileNames);
            sb.append('}');
            return sb.toString();
        }
    }
}
