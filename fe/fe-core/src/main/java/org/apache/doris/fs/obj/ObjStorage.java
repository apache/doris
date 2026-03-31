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

import org.apache.doris.backup.Status;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;

import org.apache.commons.lang3.tuple.Triple;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * It is just used for reading remote object storage on cloud.
 *
 * @param <C> cloud SDK Client
 */
public interface ObjStorage<C> extends AutoCloseable {

    // CHUNK_SIZE for multi part upload
    int CHUNK_SIZE = 5 * 1024 * 1024;

    // ==================== Legacy Status-based API (keep for backward compat) ====================

    C getClient() throws UserException;

    Triple<String, String, String> getStsToken() throws DdlException;

    Status headObject(String remotePath);

    Status getObject(String remoteFilePath, File localFile);

    Status putObject(String remotePath, @Nullable InputStream content, long contentLenghth);

    Status deleteObject(String remotePath);

    Status deleteObjects(String remotePath);

    Status copyObject(String origFilePath, String destFilePath);

    RemoteObjects listObjects(String remotePath, String continuationToken) throws DdlException;

    // ==================== IOException-based API (bridge to legacy methods above) ====================

    /** Checks that the remote object exists; throws {@link java.io.FileNotFoundException} if not. */
    default void checkObjectExists(String remotePath) throws IOException {
        ObjStorageStatusAdapter.throwIfFailed(headObject(remotePath), "headObject", remotePath);
    }

    /** Downloads a remote object to a local file; throws {@link IOException} on failure. */
    default void getObjectChecked(String remoteFilePath, File localFile) throws IOException {
        ObjStorageStatusAdapter.throwIfFailed(getObject(remoteFilePath, localFile), "getObject", remoteFilePath);
    }

    /** Uploads content to a remote path; throws {@link IOException} on failure. */
    default void putObjectChecked(String remotePath, @Nullable InputStream content,
            long contentLength) throws IOException {
        ObjStorageStatusAdapter.throwIfFailed(putObject(remotePath, content, contentLength), "putObject", remotePath);
    }

    /** Deletes a remote object; throws {@link IOException} on failure. */
    default void deleteObjectChecked(String remotePath) throws IOException {
        ObjStorageStatusAdapter.throwIfFailed(deleteObject(remotePath), "deleteObject", remotePath);
    }

    /** Deletes remote objects by prefix; throws {@link IOException} on failure. */
    default void deleteObjectsChecked(String remotePath) throws IOException {
        ObjStorageStatusAdapter.throwIfFailed(deleteObjects(remotePath), "deleteObjects", remotePath);
    }

    /** Copies a remote object; throws {@link IOException} on failure. */
    default void copyObjectChecked(String origFilePath, String destFilePath) throws IOException {
        ObjStorageStatusAdapter.throwIfFailed(
                copyObject(origFilePath, destFilePath), "copyObject", origFilePath + " -> " + destFilePath);
    }

    /** Lists objects at a remote path; throws {@link IOException} instead of {@link DdlException}. */
    default RemoteObjects listObjectsChecked(String remotePath, String continuationToken) throws IOException {
        try {
            return listObjects(remotePath, continuationToken);
        } catch (DdlException e) {
            throw new IOException("listObjects failed for path: " + remotePath, e);
        }
    }

    // ==================== Utility default methods ====================

    default String normalizePrefix(String prefix) {
        return prefix.isEmpty() ? "" : (prefix.endsWith("/") ? prefix : String.format("%s/", prefix));
    }

    default String getRelativePath(String prefix, String key) throws DdlException {
        String expectedPrefix = normalizePrefix(prefix);
        if (!key.startsWith(expectedPrefix)) {
            throw new DdlException(
                    "List a object whose key: " + key + " does not start with object prefix: " + expectedPrefix);
        }
        return key.substring(expectedPrefix.length());
    }

    // ==================== New cloud.storage methods (default, opt-in) ====================

    /**
     * Generates a presigned PUT URL for direct client upload.
     * Subclasses override this for supported providers; default throws {@link UnsupportedOperationException}.
     *
     * @param objectKey object key (without bucket)
     * @return presigned URL string
     */
    default String getPresignedUrl(String objectKey) throws IOException {
        throw new UnsupportedOperationException(
                getClass().getSimpleName() + " does not support presigned URL");
    }

    /**
     * Lists objects by prefix with pagination, replacing the cloud.storage
     * {@code listObjects(prefix, subPrefix, token)} semantics.
     *
     * @param prefix            main prefix (stage prefix, normalized to end with '/')
     * @param subPrefix         sub-prefix appended after {@code prefix}; may be empty
     * @param continuationToken pagination token; pass {@code null} or empty on first call
     * @return {@link ListObjectsResult} containing file list, truncation flag, and next token
     */
    default ListObjectsResult listObjectsWithPrefix(
            String prefix, String subPrefix, String continuationToken) throws IOException {
        throw new UnsupportedOperationException(
                getClass().getSimpleName() + " does not support listObjectsWithPrefix");
    }

    /**
     * Retrieves metadata for a single object (HEAD), replacing cloud.storage
     * {@code headObject(subKey)} semantics.
     *
     * @param prefix main prefix (used to compute relativePath)
     * @param subKey sub-key appended after {@code prefix}
     * @return {@link ListObjectsResult} containing 1 entry if found, empty list if not found
     */
    default ListObjectsResult headObjectWithMeta(String prefix, String subKey) throws IOException {
        throw new UnsupportedOperationException(
                getClass().getSimpleName() + " does not support headObjectWithMeta");
    }
}
