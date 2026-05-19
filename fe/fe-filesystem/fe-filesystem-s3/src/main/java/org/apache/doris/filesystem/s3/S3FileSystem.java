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

import org.apache.doris.filesystem.spi.ObjectStorageFileSystem;

import java.util.Optional;

/**
 * S3 filesystem backed by the AWS S3 SDK.
 */
public class S3FileSystem extends ObjectStorageFileSystem {

    private final S3FileSystemProperties properties;

    public S3FileSystem(S3FileSystemProperties properties) {
        this(properties, new S3ObjStorage(properties));
    }

    S3FileSystem(S3FileSystemProperties properties, S3ObjStorage objStorage) {
        super("S3", objStorage, objStorage.isUsePathStyle());
        this.properties = properties;
    }

    public S3FileSystem(S3ObjStorage objStorage) {
        super("S3", objStorage, objStorage.isUsePathStyle());
        this.properties = null;
    }

    public Optional<S3FileSystemProperties> properties() {
        return Optional.ofNullable(properties);
    }

    protected static boolean isSingleLevelGlob(String pathStr) {
        return ObjectStorageFileSystem.isSingleLevelGlob(pathStr);
    }

    protected static String longestNonGlobPrefix(String globPattern) {
        return ObjectStorageFileSystem.longestNonGlobPrefix(globPattern);
    }

    protected static String globToRegex(String glob) {
        return ObjectStorageFileSystem.globToRegex(glob);
    }
}
