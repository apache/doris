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

package org.apache.doris.backup;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

// represent a file or a dir in remote storage
public class RemoteFile {
    // Only file name, not full path
    private String name;
    private boolean isFile;
    private long size;
    // Block size of underlying file system. e.g. HDFS and S3.
    // A large file will split into multiple blocks. The blocks are transparent to the user.
    // Default block size for HDFS 2.x is 128M.
    private long blockSize;

    public RemoteFile(String name, boolean isFile, long size, long blockSize) {
        Preconditions.checkState(!Strings.isNullOrEmpty(name));
        this.name = name;
        this.isFile = isFile;
        this.size = size;
        this.blockSize = blockSize;
    }

    public String getName() {
        return name;
    }

    public boolean isFile() {
        return isFile;
    }

    public long getSize() {
        return size;
    }

    public long getBlockSize() {
        return blockSize;
    }

    @Override
    public String toString() {
        return "[name: " + name + ", is file: " + isFile + "]";
    }
}
