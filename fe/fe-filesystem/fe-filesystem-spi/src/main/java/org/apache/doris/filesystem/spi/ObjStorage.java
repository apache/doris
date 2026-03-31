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

package org.apache.doris.filesystem.spi;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Low-level object storage abstraction (S3-style API).
 * Zero external dependencies - only JDK types.
 *
 * @param <C> the native client type (e.g., S3Client, BlobServiceClient)
 */
public interface ObjStorage<C> extends AutoCloseable {

    C getClient() throws IOException;

    RemoteObjects listObjects(String remotePath, String continuationToken) throws IOException;

    RemoteObject headObject(String remotePath) throws IOException;

    void putObject(String remotePath, RequestBody requestBody) throws IOException;

    void deleteObject(String remotePath) throws IOException;

    void copyObject(String srcPath, String dstPath) throws IOException;

    String initiateMultipartUpload(String remotePath) throws IOException;

    UploadPartResult uploadPart(String remotePath, String uploadId, int partNum,
            RequestBody body) throws IOException;

    void completeMultipartUpload(String remotePath, String uploadId,
            List<UploadPartResult> parts) throws IOException;

    void abortMultipartUpload(String remotePath, String uploadId) throws IOException;

    Map<String, String> getProperties();

    @Override
    void close() throws IOException;
}
