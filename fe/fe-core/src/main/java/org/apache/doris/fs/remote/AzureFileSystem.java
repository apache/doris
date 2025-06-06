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
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.AzureProperties;
import org.apache.doris.fs.obj.AzureObjStorage;

import org.apache.hadoop.fs.FileSystem;

import java.util.List;

public class AzureFileSystem extends ObjFileSystem {

    public AzureFileSystem(AzureProperties azureProperties) {
        super(StorageType.AZURE.name(), StorageType.S3, new AzureObjStorage(azureProperties));
        this.storageProperties = azureProperties;
        this.properties.putAll(storageProperties.getOrigProps());
    }

    @Override
    protected FileSystem nativeFileSystem(String remotePath) throws UserException {
        return null;
    }

    @Override
    public Status globList(String remotePath, List<RemoteFile> result, boolean fileNameOnly) {
        AzureObjStorage azureObjStorage = (AzureObjStorage) getObjStorage();
        return azureObjStorage.globList(remotePath, result, fileNameOnly);
    }
}
