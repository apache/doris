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

package org.apache.doris.datasource.iceberg;

import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;

import java.util.Objects;

/**
 * Cache key for one iceberg manifest entry.
 *
 * <p>This key only contains stable identity dimensions (manifest path + content type).
 * Runtime loader context (manifest instance, table instance) must not be stored here.
 */
public class IcebergManifestEntryKey {
    private final String manifestPath;
    private final ManifestContent content;

    public IcebergManifestEntryKey(String manifestPath, ManifestContent content) {
        this.manifestPath = Objects.requireNonNull(manifestPath, "manifestPath can not be null");
        this.content = Objects.requireNonNull(content, "content can not be null");
    }

    public static IcebergManifestEntryKey of(ManifestFile manifest) {
        return new IcebergManifestEntryKey(manifest.path(), manifest.content());
    }

    public String getManifestPath() {
        return manifestPath;
    }

    public ManifestContent getContent() {
        return content;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IcebergManifestEntryKey)) {
            return false;
        }
        IcebergManifestEntryKey that = (IcebergManifestEntryKey) o;
        return Objects.equals(manifestPath, that.manifestPath)
                && content == that.content;
    }

    @Override
    public int hashCode() {
        return Objects.hash(manifestPath, content);
    }
}
