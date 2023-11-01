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
import software.amazon.awssdk.core.sync.RequestBody;

import java.io.File;

/**
 * It is just used for reading remote object storage on cloud.
 * @param <C> cloud SDK Client
 */
public interface ObjStorage<C> {
    C getClient(String bucket) throws UserException;

    Triple<String, String, String> getStsToken() throws DdlException;

    Status headObject(String remotePath);

    Status getObject(String remoteFilePath, File localFile);

    Status putObject(String remotePath, @Nullable RequestBody requestBody);

    Status deleteObject(String remotePath);

    Status deleteObjects(String remotePath);

    Status copyObject(String origFilePath, String destFilePath);

    RemoteObjects listObjects(String remotePath, String continuationToken) throws DdlException;

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
}
