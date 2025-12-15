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

package org.apache.doris.datasource.iceberg.cache;

import org.apache.iceberg.FileContent;

import java.util.Objects;
import java.util.OptionalLong;

/**
 * Cache key for a single Iceberg manifest file.
 */
public class ManifestCacheKey {
    private final String path;
    private final long length;
    private final OptionalLong lastModified;
    private final long sequenceNumber;
    private final long snapshotId;
    private final FileContent content;

    public ManifestCacheKey(String path, long length, OptionalLong lastModified,
            long sequenceNumber, long snapshotId, FileContent content) {
        this.path = path;
        this.length = length;
        this.lastModified = lastModified;
        this.sequenceNumber = sequenceNumber;
        this.snapshotId = snapshotId;
        this.content = content;
    }

    public String getPath() {
        return path;
    }

    public FileContent getContent() {
        return content;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ManifestCacheKey)) {
            return false;
        }
        ManifestCacheKey that = (ManifestCacheKey) o;
        return length == that.length
                && sequenceNumber == that.sequenceNumber
                && snapshotId == that.snapshotId
                && Objects.equals(path, that.path)
                && Objects.equals(lastModified, that.lastModified)
                && content == that.content;
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, length, lastModified, sequenceNumber, snapshotId, content);
    }

    @Override
    public String toString() {
        return "ManifestCacheKey{"
                + "path='" + path + '\''
                + ", length=" + length
                + ", lastModified=" + lastModified
                + ", sequenceNumber=" + sequenceNumber
                + ", snapshotId=" + snapshotId
                + ", content=" + content
                + '}';
    }
}
