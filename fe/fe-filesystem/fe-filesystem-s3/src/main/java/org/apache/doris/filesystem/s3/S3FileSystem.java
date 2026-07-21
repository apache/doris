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

import org.apache.doris.filesystem.S3ExpressUtils;
import org.apache.doris.filesystem.capability.Capability;
import org.apache.doris.filesystem.capability.ReadAccessCheckCapability;
import org.apache.doris.filesystem.spi.S3CompatibleFileSystem;

import java.util.List;
import java.util.Optional;

/**
 * S3 filesystem backed by the AWS S3 SDK.
 */
public class S3FileSystem extends S3CompatibleFileSystem {

    private final S3FileSystemProperties properties;
    private final S3ObjStorage s3ObjStorage;

    public S3FileSystem(S3FileSystemProperties properties) {
        this(properties, new S3ObjStorage(properties));
    }

    S3FileSystem(S3FileSystemProperties properties, S3ObjStorage objStorage) {
        super(objStorage, objStorage.isUsePathStyle(), objStorage.getSupportedSchemes());
        this.properties = properties;
        this.s3ObjStorage = objStorage;
    }

    public S3FileSystem(S3ObjStorage objStorage) {
        super(objStorage, objStorage.isUsePathStyle(), objStorage.getSupportedSchemes());
        this.properties = null;
        this.s3ObjStorage = objStorage;
    }

    public Optional<S3FileSystemProperties> properties() {
        return Optional.ofNullable(properties);
    }

    @Override
    protected GlobListPlan globListPlan(String bucket, String globPattern) {
        if (!s3ObjStorage.usesS3Express(bucket)) {
            return super.globListPlan(bucket, globPattern);
        }
        String directoryPrefix = S3ExpressUtils.directoryPrefix(longestNonGlobPrefix(globPattern));
        return new GlobListPlan(directoryPrefix, List.of(directoryPrefix), false);
    }

    @Override
    public <T extends Capability> Optional<T> capability(Class<T> capabilityType) {
        if (capabilityType == ReadAccessCheckCapability.class) {
            ReadAccessCheckCapability capability = location ->
                    s3ObjStorage.checkReadAccess(location.uri());
            return Optional.of(capabilityType.cast(capability));
        }
        return super.capability(capabilityType);
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
