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

package org.apache.doris.fs.remote;

import java.util.Map;

/**
 * Optional capability interface for file systems that support S3-style multipart upload completion.
 *
 * <p>In object storage systems (S3, Azure Blob Storage), large files can be uploaded in multiple
 * parts. After all parts are uploaded, the parts must be explicitly merged via
 * {@link #completeMultipartUpload}.
 *
 * <p>Not all {@link ObjFileSystem} implementations support this protocol; callers must check
 * {@code instanceof MultipartUploadCapable} before use.
 *
 * <p>Example usage:
 * <pre>{@code
 * FileSystem fs = switchingFs.fileSystem(path);
 * if (!(fs instanceof MultipartUploadCapable)) {
 *     throw new IllegalStateException("FileSystem does not support multipart upload: " + path);
 * }
 * ((MultipartUploadCapable) fs).completeMultipartUpload(bucket, key, uploadId, parts);
 * }</pre>
 */
public interface MultipartUploadCapable {

    /**
     * Completes a multipart upload by merging all uploaded parts into a single object.
     *
     * @param bucket   the name of the target bucket
     * @param key      the full object key (path) within the bucket
     * @param uploadId the unique identifier of the multipart upload session
     * @param parts    mapping of part numbers (1-based) to their ETag values,
     *                 used to assemble parts in the correct order
     */
    void completeMultipartUpload(String bucket, String key, String uploadId, Map<Integer, String> parts);
}
