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

package org.apache.doris.fs;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.remote.RemoteFileSystem;

import java.util.List;
import java.util.Map;

public class FileSystemFactory {

    public static RemoteFileSystem get(StorageProperties storageProperties) {
        return StorageTypeMapper.create(storageProperties);
    }

    //todo remove when catalog use storage properties
    public static RemoteFileSystem get(FileSystemType fileSystemType, Map<String, String> properties)
            throws UserException {
        List<StorageProperties> storagePropertiesList = StorageProperties.createAll(properties);

        for (StorageProperties storageProperties : storagePropertiesList) {
            if (storageProperties.getStorageName().equalsIgnoreCase(fileSystemType.name())) {
                return StorageTypeMapper.create(storageProperties);
            }
        }
        throw new RuntimeException("Unsupported file system type: " + fileSystemType);
    }
}
