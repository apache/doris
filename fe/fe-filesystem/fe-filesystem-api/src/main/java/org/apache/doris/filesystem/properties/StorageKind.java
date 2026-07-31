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

package org.apache.doris.filesystem.properties;

/**
 * High-level storage classification used by the filesystem framework.
 */
public enum StorageKind {
    /**
     * Object storage systems such as S3, OSS, COS, OBS, GCS, and Azure Blob.
     */
    OBJECT_STORAGE,

    /**
     * HDFS-compatible file systems such as HDFS and OSS-HDFS.
     */
    HDFS_COMPATIBLE,

    /**
     * Broker-based file systems.
     */
    BROKER,

    /**
     * Local file system.
     */
    LOCAL,

    /**
     * HTTP-based file systems.
     */
    HTTP
}
