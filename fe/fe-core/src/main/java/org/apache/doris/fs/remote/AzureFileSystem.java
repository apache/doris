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

import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.analysis.StorageBackend.StorageType;
import org.apache.doris.backup.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.fs.obj.AzureObjStorage;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class AzureFileSystem extends ObjFileSystem {
    private static final Logger LOG = LogManager.getLogger(AzureFileSystem.class);

    public AzureFileSystem(Map<String, String> properties) {
        super(StorageType.AZURE.name(), StorageType.AZURE, new AzureObjStorage(properties));
        initFsProperties();
    }

    @VisibleForTesting
    public AzureFileSystem(AzureObjStorage storage) {
        super(StorageBackend.StorageType.AZURE.name(), StorageBackend.StorageType.AZURE, storage);
        initFsProperties();
    }

    private void initFsProperties() {
        this.properties.putAll(((AzureObjStorage) objStorage).getProperties());
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
