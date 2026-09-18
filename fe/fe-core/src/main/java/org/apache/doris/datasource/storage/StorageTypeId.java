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

package org.apache.doris.datasource.storage;

/**
 * Storage type identifier used by fe-core routing and Type-keyed maps.
 *
 * <p>Successor of {@code StorageProperties.Type} with the identical value set, so migration is a
 * mechanical {@code StorageTypeId.valueOf(type.name())}. Deliberately a plain identifier — all
 * typed storage knowledge (parsing, validation, conversion) lives behind the fe-filesystem SPI;
 * fe-core only routes on this id. JuiceFS has no id of its own: fe-core historically treats
 * {@code jfs://} as HDFS, and the facade maps the SPI's JFS provider back to {@link #HDFS}.</p>
 */
public enum StorageTypeId {
    HDFS,
    S3,
    OSS,
    OBS,
    COS,
    GCS,
    OSS_HDFS,
    MINIO,
    OZONE,
    AZURE,
    BROKER,
    LOCAL,
    HTTP,
    UNKNOWN
}
