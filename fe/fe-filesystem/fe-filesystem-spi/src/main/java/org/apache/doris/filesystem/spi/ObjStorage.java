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

    // -----------------------------------------------------------------------
    // Cloud-specific extensions (default UnsupportedOperationException).
    // Implementations override these for backends that support the operation.
    // -----------------------------------------------------------------------

    /**
     * Obtains temporary STS credentials by assuming a configured IAM role.
     * Only supported by cloud providers that have an STS service (AWS, Alibaba OSS, etc.).
     *
     * @return STS credentials (access key, secret key, security token)
     * @throws IOException                   if the STS call fails
     * @throws UnsupportedOperationException if this storage backend has no STS support
     */
    default StsCredentials getStsToken() throws IOException {
        throw new UnsupportedOperationException("getStsToken not supported by " + getClass().getSimpleName());
    }

    /**
     * Lists objects whose keys share the given prefix, with pagination support.
     *
     * @param prefix            the stage/root prefix (e.g. {@code "stage/user1/"})
     * @param subPrefix         sub-prefix relative to {@code prefix}; empty string lists all
     * @param continuationToken opaque pagination token from a previous call; {@code null} for first page
     * @return listing result; use {@link RemoteObjects#getContinuationToken()} and
     *         {@link RemoteObjects#isTruncated()} to fetch subsequent pages
     * @throws IOException                   if the listing fails
     * @throws UnsupportedOperationException if this storage backend does not support this operation
     */
    default RemoteObjects listObjectsWithPrefix(String prefix, String subPrefix,
            String continuationToken) throws IOException {
        throw new UnsupportedOperationException(
                "listObjectsWithPrefix not supported by " + getClass().getSimpleName());
    }

    /**
     * Returns metadata for a single object identified by {@code prefix + subKey}.
     * Returns a single-element result if the object exists, or an empty result if not found.
     *
     * @param prefix the stage/root prefix
     * @param subKey the object key relative to {@code prefix}
     * @return object metadata as a {@link RemoteObjects} result (0 or 1 elements)
     * @throws IOException                   if the HEAD request fails for reasons other than not-found
     * @throws UnsupportedOperationException if this storage backend does not support this operation
     */
    default RemoteObjects headObjectWithMeta(String prefix, String subKey) throws IOException {
        throw new UnsupportedOperationException(
                "headObjectWithMeta not supported by " + getClass().getSimpleName());
    }

    /**
     * Generates a pre-signed URL that allows direct HTTP PUT upload of the given object key,
     * without requiring the uploader to hold storage credentials.
     *
     * @param objectKey the full object key (relative to the bucket root)
     * @return a pre-signed URL string valid for a limited time
     * @throws IOException                   if URL generation fails
     * @throws UnsupportedOperationException if this storage backend does not support pre-signed URLs
     */
    default String getPresignedUrl(String objectKey) throws IOException {
        throw new UnsupportedOperationException(
                "getPresignedUrl not supported by " + getClass().getSimpleName());
    }

    /**
     * Deletes multiple objects by their full object keys in a single logical operation.
     * Implementations may issue individual delete requests or a batch API call.
     *
     * @param bucket the bucket name
     * @param keys   list of full object keys to delete (bare keys, no scheme or bucket prefix)
     * @throws IOException                   if any deletion fails
     * @throws UnsupportedOperationException if this storage backend does not support this operation
     */
    default void deleteObjectsByKeys(String bucket, List<String> keys) throws IOException {
        throw new UnsupportedOperationException(
                "deleteObjectsByKeys not supported by " + getClass().getSimpleName());
    }

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
