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
            objStorage.headObject(location.withoutScheme());
            return true;
        } catch (IOException e) {
            if (isNotFoundError(e)) {
                return false;
            }
            throw e;
        }
    }

    /**
     * Subclasses may override to detect vendor-specific "not found" errors.
     */
    protected boolean isNotFoundError(IOException e) {
        return e.getMessage() != null && e.getMessage().contains("404");
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

    @Override
    public void close() throws IOException {
        // default no-op; subclasses may override
    }
}
