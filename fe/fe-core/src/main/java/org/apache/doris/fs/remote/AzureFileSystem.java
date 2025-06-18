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

import org.apache.doris.analysis.StorageBackend.StorageType;
import org.apache.doris.backup.Status;
import org.apache.doris.datasource.property.storage.AzureProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.obj.AzureObjStorage;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class AzureFileSystem extends ObjFileSystem {

    private final AzureProperties azureProperties;

    public AzureFileSystem(AzureProperties azureProperties) {
        super(StorageType.AZURE.name(), StorageType.AZURE, new AzureObjStorage(azureProperties));
        this.azureProperties = azureProperties;
        this.properties.putAll(azureProperties.getOrigProps());
    }

    @Override
    public Status renameDir(String origFilePath, String destFilePath) {
        throw new UnsupportedOperationException("Renaming directories is not supported in Azure File System.");
    }

    @Override
    public Status listFiles(String remotePath, boolean recursive, List<RemoteFile> result) {
        throw new UnsupportedOperationException("Listing files is not supported in Azure File System.");
    }

    @Override
    public Status globList(String remotePath, List<RemoteFile> result, boolean fileNameOnly) {
        AzureObjStorage azureObjStorage = (AzureObjStorage) getObjStorage();
        return azureObjStorage.globList(remotePath, result, fileNameOnly);
    }

    @Override
    public Status listDirectories(String remotePath, Set<String> result) {
        throw new UnsupportedOperationException("Listing directories is not supported in Azure File System.");
    }

    @Override
    public StorageProperties getStorageProperties() {
        return azureProperties;
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            try {
                objStorage.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
