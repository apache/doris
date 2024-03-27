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

package org.apache.doris.external.iceberg;

import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;

import com.google.common.collect.Maps;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Ignore;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

public class IcebergHadoopCatalogTest {
    FileSystem nativeFs;
    private static final PathFilter TABLE_FILTER = (path) -> path.getName().endsWith(".metadata.json");

    @Ignore
    // This logic is same as HadoopCatalog.listNamespaces in Iceberg 1.4.3.
    // So that we can use this to test the behavior.
    // Set correct properties to test.
    public void testHadoopCatalogListNamespaces() throws UserException, IOException {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("cos.access_key", "xxx");
        properties.put("cos.secret_key", "yyy");
        properties.put("cos.endpoint", "cos.ap-beijing.myqcloud.com");
        properties.put("cos.region", "ap-beijing");
        Map<String, String> hadoopProps = PropertyConverter.convertToHadoopFSProperties(properties);
        String pathStr = "cosn://bucket1/namespace";
        DFSFileSystem fs = (DFSFileSystem) FileSystemFactory.get("", StorageBackend.StorageType.HDFS, hadoopProps);
        nativeFs = fs.nativeFileSystem(pathStr);

        RemoteIterator<FileStatus> it = nativeFs.listStatusIterator(new Path(pathStr));
        while (it.hasNext()) {
            Path path = (it.next()).getPath();
            if (isNamespace(path)) {
                System.out.println(path);
            }
        }
    }

    private boolean isNamespace(Path path) throws IOException {
        return this.isDirectory(path) && !this.isTableDir(path);
    }

    private boolean isDirectory(Path path) throws IOException {
        try {
            FileStatus fileStatus = this.nativeFs.getFileStatus(path);
            return fileStatus.isDirectory();
        } catch (FileNotFoundException var3) {
            return false;
        } catch (IOException var4) {
            throw var4;
        }
    }

    private boolean isTableDir(Path path) throws IOException {
        Path metadataPath = new Path(path, "metadata");

        try {
            return this.nativeFs.listStatus(metadataPath, TABLE_FILTER).length >= 1;
        } catch (FileNotFoundException var4) {
            return false;
        } catch (IOException var5) {
            throw var5;
        }
    }
}
