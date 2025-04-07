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
import org.apache.doris.backup.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.HadoopAuthenticator;
import org.apache.doris.datasource.property.storage.AbstractObjectStorageProperties;
import org.apache.doris.fs.obj.S3ObjStorage;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class S3FileSystem extends ObjFileSystem {

    private static final Logger LOG = LogManager.getLogger(S3FileSystem.class);
    private HadoopAuthenticator authenticator = null;
    private AbstractObjectStorageProperties s3Properties;


    public S3FileSystem(AbstractObjectStorageProperties s3Properties) {

        super(StorageBackend.StorageType.S3.name(), StorageBackend.StorageType.S3,
                new S3ObjStorage(s3Properties));
        this.s3Properties = s3Properties;
        this.storageProperties = s3Properties;
        initFsProperties();

    }

    @VisibleForTesting
    public S3FileSystem(S3ObjStorage storage) {
        super(StorageBackend.StorageType.S3.name(), StorageBackend.StorageType.S3, storage);
        initFsProperties();
    }

    private void initFsProperties() {
        this.properties.putAll(storageProperties.getOrigProps());
    }


    @Override
    protected FileSystem nativeFileSystem(String remotePath) throws UserException {
        return null;

    }

    // broker file pattern glob is too complex, so we use hadoop directly
    @Override
    public Status globList(String remotePath, List<RemoteFile> result, boolean fileNameOnly) {
        S3ObjStorage objStorage = (S3ObjStorage) this.objStorage;
        return objStorage.globList(remotePath, result, fileNameOnly);
    }

    @Override
    public boolean connectivityTest() throws UserException {
        S3ObjStorage objStorage = (S3ObjStorage) this.objStorage;
        try {
            objStorage.getClient().listBuckets();
            return true;
        } catch (Exception e) {
            LOG.error("S3 connectivityTest error", e);
        }
        return false;

    }

    @VisibleForTesting
    public HadoopAuthenticator getAuthenticator() {
        return authenticator;
    }
}
