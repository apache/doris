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

import org.apache.doris.filesystem.UploadPartResult;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

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

    /**
     * Returns the underlying native SDK client, creating it lazily when needed.
     *
     * @return native object storage client owned by this storage instance
     * @throws IOException if the client cannot be initialized
     */
    C getClient() throws IOException;

    /**
     * Lists objects under the path represented by {@code remotePath}.
     *
     * <p>The path must include the provider scheme and bucket, for example
     * {@code s3://bucket/stage/}, {@code cos://bucket/stage/}, or
     * {@code obs://bucket/stage/}.
     *
     * @param remotePath         object storage path containing scheme, bucket, and prefix
     * @param continuationToken  opaque pagination token from a previous call; {@code null}
     *                           for the first page
     * @return listed objects and pagination metadata
     * @throws IOException if the list request fails
     */
    RemoteObjects listObjects(String remotePath, String continuationToken) throws IOException;

    /**
     * Lists objects under the path represented by {@code remotePath} with optional
     * provider-neutral list controls.
     *
     * @param remotePath object storage path containing scheme, bucket, and prefix
     * @param options    list options such as continuation token, start-after key,
     *                   delimiter, or maximum key count
     * @return listed objects and pagination metadata
     * @throws IOException                   if the list request fails
     * @throws UnsupportedOperationException if the implementation does not support
     *                                       the requested options
     */
    default RemoteObjects listObjectsWithOptions(String remotePath, ObjectListOptions options) throws IOException {
        if (options == null) {
            return listObjects(remotePath, (String) null);
        }
        if (hasText(options.startAfter()) || options.maxKeys() > 0 || hasText(options.delimiter())) {
            throw new UnsupportedOperationException(
                    "listObjects with extended options not supported by " + getClass().getSimpleName());
        }
        return listObjects(remotePath, options.continuationToken());
    }

    private static boolean hasText(String value) {
        return value != null && !value.isEmpty();
    }

    /**
     * Returns metadata for a single object.
     *
     * @param remotePath object storage path containing scheme, bucket, and object key
     * @return object metadata
     * @throws IOException if the object does not exist or the HEAD request fails
     */
    RemoteObject headObject(String remotePath) throws IOException;

    /**
     * Uploads one complete object.
     *
     * @param remotePath   destination object storage path
     * @param requestBody  upload body and content metadata
     * @throws IOException if the upload fails
     */
    void putObject(String remotePath, RequestBody requestBody) throws IOException;

    /**
     * Deletes one object.
     *
     * @param remotePath object storage path to delete
     * @throws IOException if the delete request fails
     */
    void deleteObject(String remotePath) throws IOException;

    /**
     * Copies one object inside the same provider.
     *
     * @param srcPath source object storage path
     * @param dstPath destination object storage path
     * @throws IOException if the copy request fails
     */
    void copyObject(String srcPath, String dstPath) throws IOException;

    /**
     * Starts a multipart upload session.
     *
     * @param remotePath destination object storage path
     * @return provider upload ID used by subsequent multipart calls
     * @throws IOException if the multipart upload cannot be initiated
     */
    String initiateMultipartUpload(String remotePath) throws IOException;

    /**
     * Uploads one multipart part.
     *
     * @param remotePath destination object storage path
     * @param uploadId   upload ID returned by {@link #initiateMultipartUpload(String)}
     * @param partNum    one-based part number
     * @param body       part body and content metadata
     * @return provider part result, including the ETag or equivalent checksum token
     * @throws IOException if the part upload fails
     */
    UploadPartResult uploadPart(String remotePath, String uploadId, int partNum,
            RequestBody body) throws IOException;

    /**
     * Completes a multipart upload session.
     *
     * @param remotePath destination object storage path
     * @param uploadId   upload ID returned by {@link #initiateMultipartUpload(String)}
     * @param parts      uploaded parts in completion order
     * @throws IOException if completion fails
     */
    void completeMultipartUpload(String remotePath, String uploadId,
            List<UploadPartResult> parts) throws IOException;

    /**
     * Aborts a multipart upload session and releases provider-side temporary data.
     *
     * @param remotePath destination object storage path
     * @param uploadId   upload ID returned by {@link #initiateMultipartUpload(String)}
     * @throws IOException if aborting the upload fails
     */
    void abortMultipartUpload(String remotePath, String uploadId) throws IOException;

    /**
     * Opens an object input stream starting at {@code fromByte}.
     *
     * @param remotePath object storage path to read
     * @param fromByte   zero-based byte offset
     * @return input stream positioned at the requested offset
     * @throws IOException                   if the read request fails
     * @throws UnsupportedOperationException if range reads are not implemented
     */
    default InputStream openInputStreamAt(String remotePath, long fromByte) throws IOException {
        throw new UnsupportedOperationException(
                "openInputStreamAt not supported by " + getClass().getSimpleName());
    }

    /**
     * Returns the last modified timestamp for a single object.
     *
     * @param remotePath object storage path to inspect
     * @return last modified time in epoch milliseconds
     * @throws IOException if metadata lookup fails
     */
    default long headObjectLastModified(String remotePath) throws IOException {
        return headObject(remotePath).getModificationTime();
    }

    /**
     * Releases any native SDK client and provider resources held by this storage instance.
     *
     * @throws IOException if closing the client fails
     */
    @Override
    void close() throws IOException;
}
