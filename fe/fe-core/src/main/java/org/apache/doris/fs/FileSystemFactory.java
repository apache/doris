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

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.remote.BrokerFileSystem;
import org.apache.doris.fs.remote.RemoteFileSystem;

import java.util.List;
import java.util.Map;

public class FileSystemFactory {

    public static RemoteFileSystem get(Map<String, String> properties) throws UserException {
        StorageProperties storageProperties = StorageProperties.createPrimary(properties);
        return get(storageProperties);
    }

    public static RemoteFileSystem get(StorageBackend.StorageType storageType, String bindBreakName,
                                       Map<String, String> properties) {
        if (storageType.equals(StorageBackend.StorageType.BROKER)) {
            return new BrokerFileSystem(bindBreakName, properties);
        }
        StorageProperties storageProperties = StorageProperties.createPrimary(properties);
        return get(storageProperties);
    }

    public static RemoteFileSystem get(StorageProperties storageProperties) {
        return StorageTypeMapper.create(storageProperties);
    }

    // This method is a temporary workaround for handling properties.
    // It will be removed when broker properties are officially supported.
    public static RemoteFileSystem get(String name, Map<String, String> properties) {
        return new BrokerFileSystem(name, properties);
    }

    public static RemoteFileSystem get(FileSystemType fileSystemType, Map<String, String> properties,
                                       String bindBrokerName)
            throws UserException {
        if (fileSystemType == FileSystemType.BROKER) {
            return new BrokerFileSystem(bindBrokerName, properties);
        }
        List<StorageProperties> storagePropertiesList = StorageProperties.createAll(properties);

        for (StorageProperties storageProperties : storagePropertiesList) {
            if (storageProperties.getStorageName().equalsIgnoreCase(fileSystemType.name())) {
                return StorageTypeMapper.create(storageProperties);
            }
        }
        throw new RuntimeException("Unsupported file system type: " + fileSystemType);
    }

    public static RemoteFileSystem get(BrokerDesc brokerDesc) {
        if (null != brokerDesc.getStorageProperties()) {
            return get(brokerDesc.getStorageProperties());
        }
        if (null != brokerDesc.getStorageType()
                && brokerDesc.getStorageType().equals(StorageBackend.StorageType.BROKER)) {
            return new BrokerFileSystem(brokerDesc.getName(), brokerDesc.getProperties());
        }
        throw new RuntimeException("Unexpected storage type: " + brokerDesc.getStorageType());
    }
}
