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

package org.apache.doris.regression.util

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assert;

import java.io.FileOutputStream;
import java.io.IOException;

// upload file from  "regression-test/data/hdfs"  to  "hdfs://xx/user/${userName}/groovy/"
// download file from "hdfs://xx/user/${userName}/groovy/" to  "regression-test/data/hdfs"
class Hdfs {
    FileSystem fs = null;
    String uri;
    String userName;
    String testRemoteDir;
    String localDataDir;

    Hdfs(String uri, String username, String localDataDir) {
        Configuration conf = new Configuration();
        conf.setStrings("fs.default.name", uri);
        this.uri = uri;
        this.userName = username;
        System.setProperty("HADOOP_USER_NAME", username);
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        this.testRemoteDir = genRemoteDataDir();
        this.localDataDir = localDataDir;
        try {
            fs = FileSystem.get(conf);
            Path remoteDirPath = new Path(testRemoteDir);
            if (!fs.exists(remoteDirPath)) {
                fs.mkdirs(remoteDirPath);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    List<String> downLoad(String prefix) {
        List<String> files = new ArrayList<>();
        try {
            String filepath = this.testRemoteDir + prefix + "*";
            FileStatus[] fileStatusArray = fs.globStatus(new Path(filepath + "*"));
            for (FileStatus fileStatus : fileStatusArray) {
                Path path = fileStatus.getPath();
                FSDataInputStream fsDataInputStream = fs.open(path);
                String localFilePath = getAbsoluteLocalPath(prefix.split('/')[0] + path.getName())
                FileOutputStream fileOutputStream = new FileOutputStream(localFilePath);
                IOUtils.copy(fsDataInputStream, fileOutputStream);
                files.add(localFilePath);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return files;
    }

    String getAbsoluteLocalPath(String file_name) {
        String localAbsolutePath = this.localDataDir + "/" + file_name;
        return localAbsolutePath;
    }

    String genRemoteDataDir() {
        return this.uri + "/user/" + this.userName + "/groovy/";
    }

    String getAbsoluteRemotePath(String file_name) {
        String remoteAbsolutePath = genRemoteDataDir() + file_name;
        return remoteAbsolutePath;
    }

    String upload(String local_file) {
        try {
            Path src = new Path(local_file);
            String remote_file = getAbsoluteRemotePath(src.getName());
            Path dst = new Path(remote_file);
            fs.copyFromLocalFile(src, dst);
            return remote_file;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }
}