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

import org.apache.doris.filesystem.spi.S3CompatibleFileSystem;

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
        super(objStorage, objStorage.isUsePathStyle());
        this.properties = properties;
    }

    public S3FileSystem(S3ObjStorage objStorage) {
        super(objStorage, objStorage.isUsePathStyle());
        this.properties = null;
    }

    public Optional<S3FileSystemProperties> properties() {
        return Optional.ofNullable(properties);
    }

    @Override
    protected String globListPrefix(String globPattern) {
        if (isDirectoryBucketEndpoint()) {
            return slashTerminatedNonGlobPrefix(globPattern);
        }
        return super.globListPrefix(globPattern);
    }

    @Override
    protected List<String> globListPrefixes(String globPattern, String listPrefix) {
        if (isDirectoryBucketEndpoint()) {
            return List.of(listPrefix);
        }
        return super.globListPrefixes(globPattern, listPrefix);
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
        return S3CompatibleFileSystem.expandedGlobListPrefixes(globPattern);
    }

    protected static String globToRegex(String glob) {
        return S3CompatibleFileSystem.globToRegex(glob);
    }
}
