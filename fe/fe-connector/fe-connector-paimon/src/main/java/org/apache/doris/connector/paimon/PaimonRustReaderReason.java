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

package org.apache.doris.connector.paimon;

/** First unmet Rust admission condition. Names are stable diagnostics; never include property values. */
enum PaimonRustReaderReason {
    SUPPORTED,
    FORCED_JNI,
    DISABLED,
    BACKEND_CAPABILITY,
    UNSUPPORTED_SPLIT,
    UNSUPPORTED_TABLE,
    METADATA_COLUMNS,
    FALLBACK_READ,
    INCREMENTAL_READ,
    VARIANT_PROJECTION,
    NESTED_PROJECTION,
    QUERY_AUTH,
    REST_TOKEN,
    DELETION_VECTOR_MODE,
    MERGE_SEMANTICS,
    SCAN_MODE,
    STORAGE_SCHEME,
    HDFS_CONFIGURATION,
    STORAGE_CREDENTIALS,
    EXTERNAL_FILE,
    ORC_TIMESTAMP_LTZ,
    SCHEMA_METADATA,
    SCHEMA_VERSION,
    SCHEMA_EVOLUTION,
    TIMESTAMP_PRECISION,
    AGGREGATE_SEMANTICS,
    MERGE_ENGINE_RESOURCE_GUARD,
    MULTI_FILE_RESOURCE_GUARD,
    DELETES_RESOURCE_GUARD,
    DELETE_STATISTICS_UNAVAILABLE
}
