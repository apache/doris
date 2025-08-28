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

import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.remote.SwitchingFileSystem;

import java.util.Map;

public class FileSystemProviderImpl implements FileSystemProvider {
    private ExternalMetaCacheMgr extMetaCacheMgr;

    private Map<StorageProperties.Type, StorageProperties> storagePropertiesMap;

    public FileSystemProviderImpl(ExternalMetaCacheMgr extMetaCacheMgr,
                                  Map<StorageProperties.Type, StorageProperties> storagePropertiesMap) {
        this.extMetaCacheMgr = extMetaCacheMgr;
        this.storagePropertiesMap = storagePropertiesMap;
    }

    @Override
    public FileSystem get(SessionContext ctx) {
        return new SwitchingFileSystem(extMetaCacheMgr, storagePropertiesMap);
    }
}
