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

package org.apache.doris.datasource.property.storage;

import org.apache.doris.fs.remote.RemoteFileSystem;
import org.apache.doris.fs.remote.S3FileSystem;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;

import java.util.Arrays;
import java.util.function.Function;

public enum StorageTypeMapper {
    HDFS(HDFSProperties.class, DFSFileSystem::new),
    OSS(OSSProperties.class, S3FileSystem::new),
    OBS(OBSProperties.class, S3FileSystem::new),
    COS(COSProperties.class, S3FileSystem::new),
    S3(S3Properties.class, S3FileSystem::new);

    private final Class<? extends StorageProperties> propClass;
    private final Function<StorageProperties, RemoteFileSystem> factory;

    <T extends StorageProperties> StorageTypeMapper(Class<T> propClass, Function<T, RemoteFileSystem> factory) {
        this.propClass = propClass;
        this.factory = (prop) -> factory.apply(propClass.cast(prop));
    }

    public static RemoteFileSystem create(StorageProperties prop) {
        return Arrays.stream(values())
                .filter(type -> type.propClass.isInstance(prop))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Unknown storage type"))
                .factory.apply(prop);
    }
}

