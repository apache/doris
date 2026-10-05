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
 * Backend-facing storage classification used to select FE-to-BE adapters.
 *
 * <p>This is intentionally more specific than {@link StorageKind}. For example,
 * Azure Blob is object storage at the filesystem layer, but it may be sent to BE
 * either through the S3-compatible adapter or through its native AZURE storage type.
 */
public enum BackendStorageKind {
    /**
     * S3-compatible object storage adapter, such as TS3StorageParam.
     */
    S3_COMPATIBLE,

    /**
     * Storage that should keep its own backend storage type, such as AZURE.
     */
    NATIVE,

    /**
     * HDFS or HDFS-compatible storage.
     */
    HDFS,

    /**
     * Broker-based storage.
     */
    BROKER,

    /**
     * Local file system.
     */
    LOCAL
}
