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

package org.apache.iceberg.io;

import java.util.Map;

/**
 * A {@link ResolvingFileIO} that preserves the legacy Hadoop GCS path unless SDK storage options exist.
 *
 * <p>Adding iceberg-gcp makes the SDK route these locations to GCSFileIO, which does not read the
 * {@code fs.s3a.*} HMAC settings Doris supplies. Both FE and the BE metadata scanner must load this
 * implementation because its class name is carried inside serialized Iceberg scan tasks. Explicit
 * {@code gcs.*} storage options retain the SDK path, including options supplied after FE selection.
 *
 * <p>The SDK package is required because {@code implFromLocation} is package-private. It also keeps
 * this class and iceberg-core in the same class loader on both sides of a metadata scan.
 */
public class NativeGcsResolvingFileIO extends ResolvingFileIO {
    private static final long serialVersionUID = 1L;

    private static final String GCS_FILE_IO_IMPL = "org.apache.iceberg.gcp.gcs.GCSFileIO";
    private static final String HADOOP_FILE_IO_IMPL = "org.apache.iceberg.hadoop.HadoopFileIO";

    public static boolean hasSdkGcsProperties(Map<String, String> properties) {
        return properties.entrySet().stream()
                .anyMatch(entry -> entry.getKey().startsWith("gcs.") && entry.getValue() != null);
    }

    @Override
    String implFromLocation(String location) {
        String impl = super.implFromLocation(location);
        if (!GCS_FILE_IO_IMPL.equals(impl)) {
            return impl;
        }
        // REST/table options and matching vended credentials can arrive after FE chose this resolver.
        // HadoopFileIO ignores SDK encryption and billing options, so those must keep the native path.
        if (hasSdkGcsProperties(properties())) {
            return GCS_FILE_IO_IMPL;
        }
        StorageCredential matching = null;
        for (StorageCredential credential : credentials()) {
            if (location.startsWith(credential.prefix())
                    && (matching == null || credential.prefix().length() >= matching.prefix().length())) {
                matching = credential;
            }
        }
        // The SDK uses the longest prefix and the last duplicate; broader credentials cannot supply its options.
        return matching != null && hasSdkGcsProperties(matching.config()) ? GCS_FILE_IO_IMPL : HADOOP_FILE_IO_IMPL;
    }
}
