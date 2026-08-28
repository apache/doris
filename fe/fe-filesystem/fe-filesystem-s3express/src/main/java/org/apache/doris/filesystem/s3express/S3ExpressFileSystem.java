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

package org.apache.doris.filesystem.s3express;

import org.apache.doris.filesystem.s3.S3FileSystem;
import org.apache.doris.filesystem.s3.S3ObjStorage;

import java.util.List;

/** Amazon S3 Express filesystem implementing S3 Express glob planning. */
public final class S3ExpressFileSystem extends S3FileSystem {

    public S3ExpressFileSystem(S3ExpressFileSystemProperties properties) {
        super(properties, new S3ExpressObjStorage(properties));
    }

    S3ExpressFileSystem(S3ExpressFileSystemProperties properties, S3ObjStorage objStorage) {
        super(properties, objStorage);
    }

    @Override
    protected String globListPrefix(String globPattern) {
        String prefix = longestNonGlobPrefix(globPattern);
        if (prefix.isEmpty() || prefix.endsWith("/")) {
            return prefix;
        }
        int slash = prefix.lastIndexOf('/');
        return slash < 0 ? "" : prefix.substring(0, slash + 1);
    }

    @Override
    protected List<String> globListPrefixes(String globPattern, String listPrefix) {
        return List.of(listPrefix);
    }
}
