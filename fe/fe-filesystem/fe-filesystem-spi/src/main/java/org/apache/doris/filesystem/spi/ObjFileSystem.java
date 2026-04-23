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

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.Location;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Abstract base class for object-storage-backed FileSystems.
 * Delegates existence checks and cloud-specific operations to the underlying ObjStorage instance.
 */
public abstract class ObjFileSystem implements FileSystem {

    protected final ObjStorage<?> objStorage;
    protected final String name;

    protected ObjFileSystem(String name, ObjStorage<?> objStorage) {
        this.name = name;
        this.objStorage = objStorage;
    }

    public ObjStorage<?> getObjStorage() {
        return objStorage;
    }

    @Override
    public boolean exists(Location location) throws IOException {
        try {
            objStorage.headObject(location.uri());
            return true;
        } catch (IOException e) {
            if (isNotFoundError(e)) {
                return false;
            }
            throw e;
        }
    }

    /**
     * Returns true if {@code e} represents a "not found" condition (HTTP 404 equivalent).
     *
     * <p>All ObjStorage implementations are required to throw {@link java.io.FileNotFoundException}
     * for missing keys. Subclasses may override to add additional SDK-specific detection.
     */
    protected boolean isNotFoundError(IOException e) {
        return e instanceof java.io.FileNotFoundException;
    }

    // -----------------------------------------------------------------------
    // Cloud-specific delegates - forward to ObjStorage
    // -----------------------------------------------------------------------

    /** @see ObjStorage#getStsToken() */
    public StsCredentials getStsToken() throws IOException {
        return objStorage.getStsToken();
    }

    /** @see ObjStorage#listObjectsWithPrefix(String, String, String) */
    public RemoteObjects listObjectsWithPrefix(String prefix, String subPrefix,
            String continuationToken) throws IOException {
        return objStorage.listObjectsWithPrefix(prefix, subPrefix, continuationToken);
    }

    /** @see ObjStorage#headObjectWithMeta(String, String) */
    public RemoteObjects headObjectWithMeta(String prefix, String subKey) throws IOException {
        return objStorage.headObjectWithMeta(prefix, subKey);
    }

    /** @see ObjStorage#getPresignedUrl(String) */
    public String getPresignedUrl(String objectKey) throws IOException {
        return objStorage.getPresignedUrl(objectKey);
    }

    /** @see ObjStorage#deleteObjectsByKeys(String, List) */
    public void deleteObjectsByKeys(String bucket, List<String> keys) throws IOException {
        objStorage.deleteObjectsByKeys(bucket, keys);
    }

    /**
     * Convenience overload for completing a multipart upload when part ETags are provided
     * as a {@code Map<partNumber, etag>} (as returned by the Thrift/Backend protocol).
     *
     * <p>Converts the map to a sorted {@link UploadPartResult} list before delegating to
     * {@link ObjStorage#completeMultipartUpload(String, String, List)}.
     *
     * @param remotePath the full object URI (e.g. {@code s3://bucket/key})
     * @param uploadId   the multipart upload session ID
     * @param etags      mapping of 1-based part numbers to their ETags
     */
    public void completeMultipartUpload(String remotePath, String uploadId,
            Map<Integer, String> etags) throws IOException {
        List<UploadPartResult> parts = etags.entrySet().stream()
                .map(e -> new UploadPartResult(e.getKey(), e.getValue()))
                .sorted(Comparator.comparingInt(UploadPartResult::partNumber))
                .collect(Collectors.toList());
        objStorage.completeMultipartUpload(remotePath, uploadId, parts);
    }

    @Override
    public void close() throws IOException {
        objStorage.close();
    }
}
