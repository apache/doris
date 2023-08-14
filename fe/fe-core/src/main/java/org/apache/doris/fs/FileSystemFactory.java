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

import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.S3Util;
import org.apache.doris.fs.remote.BrokerFileSystem;
import org.apache.doris.fs.remote.RemoteFileSystem;
import org.apache.doris.fs.remote.S3FileSystem;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;
import org.apache.doris.fs.remote.dfs.JFSFileSystem;
import org.apache.doris.fs.remote.dfs.OFSFileSystem;

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class FileSystemFactory {

    public static RemoteFileSystem get(String name, StorageBackend.StorageType type, Map<String, String> properties) {
        // TODO: rename StorageBackend.StorageType
        if (type == StorageBackend.StorageType.S3) {
            return new S3FileSystem(properties);
        } else if (type == StorageBackend.StorageType.HDFS || type == StorageBackend.StorageType.GFS) {
            return new DFSFileSystem(properties);
        } else if (type == StorageBackend.StorageType.OFS) {
            return new OFSFileSystem(properties);
        } else if (type == StorageBackend.StorageType.JFS) {
            return new JFSFileSystem(properties);
        } else if (type == StorageBackend.StorageType.BROKER) {
            return new BrokerFileSystem(name, properties);
        } else {
            throw new UnsupportedOperationException(type.toString() + "backend is not implemented");
        }
    }

    public static Pair<FileSystemType, String> getFSIdentity(String location, String bindBrokerName) {
        FileSystemType fsType;
        if (bindBrokerName != null) {
            fsType = FileSystemType.BROKER;
        } else if (S3Util.isObjStorage(location)) {
            if (S3Util.isHdfsOnOssEndpoint(location)) {
                // if hdfs service is enabled on oss, use hdfs lib to access oss.
                fsType = FileSystemType.DFS;
            } else {
                fsType = FileSystemType.S3;
            }
        } else if (location.startsWith(FeConstants.FS_PREFIX_HDFS) || location.startsWith(FeConstants.FS_PREFIX_GFS)
                 || location.startsWith(FeConstants.FS_PREFIX_VIEWFS)) {
            fsType = FileSystemType.DFS;
        } else if (location.startsWith(FeConstants.FS_PREFIX_OFS) || location.startsWith(FeConstants.FS_PREFIX_COSN)) {
            // ofs:// and cosn:// use the same underlying file system: Tencent Cloud HDFS, aka CHDFS)) {
            fsType = FileSystemType.OFS;
        } else if (location.startsWith(FeConstants.FS_PREFIX_JFS)) {
            fsType = FileSystemType.JFS;
        } else {
            throw new UnsupportedOperationException("Unknown file system for location: " + location);
        }

        Path path = new Path(location);
        URI uri = path.toUri();
        String fsIdent = Strings.nullToEmpty(uri.getScheme()) + "://" + Strings.nullToEmpty(uri.getAuthority());
        return Pair.of(fsType, fsIdent);
    }

    public static RemoteFileSystem getRemoteFileSystem(FileSystemType type, Configuration conf,
                                                       String bindBrokerName) {
        Map<String, String> properties = new HashMap<>();
        conf.iterator().forEachRemaining(e -> properties.put(e.getKey(), e.getValue()));
        switch (type) {
            case S3:
                return new S3FileSystem(properties);
            case DFS:
                return new DFSFileSystem(properties);
            case OFS:
                return new OFSFileSystem(properties);
            case JFS:
                return new JFSFileSystem(properties);
            case BROKER:
                return new BrokerFileSystem(bindBrokerName, properties);
            default:
                throw new IllegalStateException("Not supported file system type: " + type);
        }
    }

    public static RemoteFileSystem getS3FileSystem(Map<String, String> properties) {
        // use for test
        return get(StorageBackend.StorageType.S3.name(), StorageBackend.StorageType.S3, properties);
    }

    public static org.apache.hadoop.fs.FileSystem getNativeByPath(Path path, Configuration conf) throws IOException {
        return path.getFileSystem(conf);
    }
}
