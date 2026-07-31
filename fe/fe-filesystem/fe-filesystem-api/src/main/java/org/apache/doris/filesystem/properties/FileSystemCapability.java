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
 * Capabilities a filesystem provider may support.
 *
 * <p>Declared per provider (the storage type is the source of truth) so that framework code can
 * gate operations before invoking them, instead of calling and catching UnsupportedOperationException.
 */
public enum FileSystemCapability {
    /** Read an object. */
    READ,
    /** Write / upload an object. */
    WRITE,
    /** List / glob a prefix. */
    LIST,
    /** Delete an object. */
    DELETE,
    /**
     * Rename is atomic and cheap. True filesystems (HDFS, local) provide this; object stores
     * emulate rename as copy+delete, which is neither atomic nor cheap, and must not declare it.
     */
    ATOMIC_RENAME,
    /**
     * A real hierarchical namespace with directories (mkdirs / renameDirectory / listDirectories
     * carry meaning). Object stores only emulate this via key prefixes and must not declare it.
     */
    HIERARCHICAL_NAMESPACE,
    /** Append to an existing object. Supported by HDFS; not by object stores. */
    APPEND,
    /**
     * Conditional write such as put-if-absent or if-match (used to build commit locks). Supported
     * by some object stores via conditional puts; true filesystems achieve the same via
     * {@link #ATOMIC_RENAME} instead.
     */
    CONDITIONAL_WRITE
}
