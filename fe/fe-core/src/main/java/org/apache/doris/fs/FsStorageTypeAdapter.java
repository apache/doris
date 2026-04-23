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

package org.apache.doris.fs;

import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.foundation.fs.FsStorageType;

/**
 * Bidirectional converter between FsStorageType (fe-foundation) and
 * Thrift-generated StorageBackend.StorageType (fe-thrift).
 * Lives in fe-core which can see both types.
 */
public final class FsStorageTypeAdapter {
    private FsStorageTypeAdapter() {}

    public static FsStorageType fromThrift(StorageBackend.StorageType thriftType) {
        switch (thriftType) {
            case S3:     return FsStorageType.S3;
            case HDFS:   return FsStorageType.HDFS;
            case BROKER: return FsStorageType.BROKER;
            case AZURE:  return FsStorageType.AZURE;
            case OFS:    return FsStorageType.OFS;
            case JFS:    return FsStorageType.JFS;
            case LOCAL:  return FsStorageType.LOCAL;
            default:
                throw new IllegalArgumentException("Unknown Thrift StorageType: " + thriftType);
        }
    }

    public static StorageBackend.StorageType toThrift(FsStorageType fsType) {
        switch (fsType) {
            case S3:       return StorageBackend.StorageType.S3;
            case HDFS:     return StorageBackend.StorageType.HDFS;
            case OSS_HDFS: return StorageBackend.StorageType.HDFS;
            case BROKER:   return StorageBackend.StorageType.BROKER;
            case AZURE:    return StorageBackend.StorageType.AZURE;
            case OFS:      return StorageBackend.StorageType.OFS;
            case JFS:      return StorageBackend.StorageType.JFS;
            case LOCAL:    return StorageBackend.StorageType.LOCAL;
            default:
                throw new IllegalArgumentException("Unknown FsStorageType: " + fsType);
        }
    }
}
