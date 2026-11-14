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

package org.apache.doris.regression.util

/** Explicit destinations for test-generated objects; public fixture locations stay unchanged. */
class S3TestStorage {
    static String writeKey(Map config, String key) {
        String prefix = config.get("s3WritePrefix")?.toString() ?: ""
        if (!prefix) {
            return key
        }
        if (!key || key.startsWith("/") || key.split("/", -1).any { it == "." || it == ".." }
                || prefix.startsWith("/") || prefix.split("/", -1).any { it == "." || it == ".." }) {
            throw new IllegalArgumentException("S3 write paths must remain relative to the configured prefix")
        }
        return prefix.replaceAll('/+$', '') + "/" + key
    }
}
