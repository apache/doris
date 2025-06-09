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

// TODO: [FileSystemType Unification]
// There are currently multiple definitions of file system types across the codebase, including but not limited to:
// 1. Backend module (e.g., FileSystemBackendType)
// 2. Location/path parsing logic (e.g., LocationType or string-based tags)
// 3. This enum: FileSystemType (used in the SPI/plugin layer)
//
// Problem:
// - File system type definitions are scattered across different modules with inconsistent naming and granularity
// - Adding a new type requires changes in multiple places, increasing risk of bugs and maintenance overhead
// - Difficult to maintain and error-prone
//
// Refactoring Goal:
// - Consolidate file system type definitions into a single source of truth
// - Clearly define the semantics and usage of each type (e.g., remote vs local, object storage vs file system)
// - All modules should reference the unified definition to avoid duplication and hardcoded strings
//
// Suggested Approach:
// - Create a centralized `FsType` enum/class as the canonical definition
// - Provide mapping or adapter methods where needed (e.g., map LocationType to FsType)
// - Gradually deprecate other definitions and annotate them with @Deprecated, including migration instructions
//
public enum FileSystemType {
    S3,
    HDFS,
    OFS,
    JFS,
    BROKER,
    FILE,
    AZURE
}
