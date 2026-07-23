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

package org.apache.doris.filesystem.s3;

import org.apache.doris.filesystem.GlobListing;
import org.apache.doris.filesystem.Location;
import org.apache.doris.filesystem.spi.ObjectStorageGlob;
import org.apache.doris.filesystem.spi.S3CompatibleFileSystem;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * S3 filesystem backed by the AWS S3 SDK.
 */
public class S3FileSystem extends S3CompatibleFileSystem {

    private final S3FileSystemProperties properties;

    public S3FileSystem(S3FileSystemProperties properties) {
        this(properties, new S3ObjStorage(properties));
    }

    S3FileSystem(S3FileSystemProperties properties, S3ObjStorage objStorage) {
        super(objStorage, objStorage.isUsePathStyle(), objStorage.getSupportedSchemes());
        this.properties = properties;
    }

    public S3FileSystem(S3ObjStorage objStorage) {
        super(objStorage, objStorage.isUsePathStyle(), objStorage.getSupportedSchemes());
        this.properties = null;
    }

    public Optional<S3FileSystemProperties> properties() {
        return Optional.ofNullable(properties);
    }

    @Override
    public GlobListing globListWithLimit(Location path, String startAfter, long maxBytes,
            long maxFiles) throws IOException {
        if (isDirectoryBucketEndpoint()
                && ((startAfter != null && !startAfter.isEmpty()) || maxBytes > 0 || maxFiles > 0)) {
            throw new IOException("S3 directory bucket glob listing does not support key-based pagination "
                    + "because StartAfter is unsupported and object order is not lexicographical");
        }
        return super.globListWithLimit(path, startAfter, maxBytes, maxFiles);
    }

    @Override
    protected String globListListingPrefix(String globPattern) {
        if (isDirectoryBucketEndpoint()) {
            return slashTerminatedNonGlobPrefix(globPattern);
        }
        return super.globListListingPrefix(globPattern);
    }

    @Override
    protected List<String> globListObjectPrefixes(String globPattern, String listingPrefix) {
        if (isDirectoryBucketEndpoint()) {
            return List.of(listingPrefix);
        }
        return super.globListObjectPrefixes(globPattern, listingPrefix);
    }

    private boolean isDirectoryBucketEndpoint() {
        return properties != null && properties.isDirectoryBucketEndpoint();
    }

    private static String slashTerminatedNonGlobPrefix(String globPattern) {
        String prefix = longestNonGlobPrefix(globPattern);
        if (prefix.isEmpty() || prefix.endsWith("/")) {
            return prefix;
        }
        int slash = prefix.lastIndexOf('/');
        return slash < 0 ? "" : prefix.substring(0, slash + 1);
    }

    protected static boolean isSingleLevelGlob(String pathStr) {
        return S3CompatibleFileSystem.isSingleLevelGlob(pathStr);
    }

    protected static String longestNonGlobPrefix(String globPattern) {
        return S3CompatibleFileSystem.longestNonGlobPrefix(globPattern);
    }

    protected static List<String> expandedGlobListPrefixes(String globPattern) {
        return ObjectStorageGlob.expandedGlobListPrefixes(globPattern);
    }

    protected static String globToRegex(String glob) {
        return S3CompatibleFileSystem.globToRegex(glob);
    }
}
