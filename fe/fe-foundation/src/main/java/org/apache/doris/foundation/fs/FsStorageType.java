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

package org.apache.doris.foundation.fs;

/**
 * Storage type enum for persistent file system identification.
 * Intentionally in fe-foundation (zero-dependency module) so that
 * fe-filesystem-spi can reference it without depending on fe-thrift.
 */
public enum FsStorageType {
    S3,
    HDFS,
    BROKER,
    AZURE,
    OFS,
    JFS,
    OSS_HDFS,
    LOCAL;

    public String typeName() {
        return name();
    }
}
