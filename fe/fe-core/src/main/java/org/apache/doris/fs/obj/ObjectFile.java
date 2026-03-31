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

package org.apache.doris.fs.obj;

import lombok.Getter;

/**
 * Immutable descriptor of a single object in an object storage.
 * Replaces {@code org.apache.doris.cloud.storage.ObjectFile}.
 */
public class ObjectFile {
    @Getter
    private final String key;
    @Getter
    private final String relativePath;
    @Getter
    private final String etag;
    @Getter
    private final long size;

    public ObjectFile(String key, String relativePath, String etag, long size) {
        this.key = key;
        this.relativePath = relativePath;
        this.etag = etag;
        this.size = size;
    }

    @Override
    public String toString() {
        return "ObjectFile{key='" + key + "', relativePath='" + relativePath
                + "', etag='" + etag + "', size=" + size + '}';
    }
}
