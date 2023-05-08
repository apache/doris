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

package org.apache.doris.fs.operations;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

public class HDFSOpParams extends OpParams {
    private String remotePath;
    private long startOffset;
    private FSDataOutputStream fsDataOutputStream;
    private FSDataInputStream fsDataInputStream;

    protected HDFSOpParams(FSDataInputStream fsDataInputStream) {
        this(null, 0, fsDataInputStream, null);
    }

    protected HDFSOpParams(FSDataOutputStream fsDataOutputStream) {
        this(null, 0, null, fsDataOutputStream);
    }

    protected HDFSOpParams(String remotePath, long startOffset) {
        this(remotePath, startOffset, null, null);
    }

    protected HDFSOpParams(String remotePath, long startOffset,
                           FSDataInputStream fsDataInputStream, FSDataOutputStream fsDataOutputStream) {
        this.remotePath = remotePath;
        this.startOffset = startOffset;
        this.fsDataInputStream = fsDataInputStream;
        this.fsDataOutputStream = fsDataOutputStream;
    }

    public String remotePath() {
        return remotePath;
    }

    public long startOffset() {
        return startOffset;
    }

    public FSDataOutputStream fsDataOutputStream() {
        return fsDataOutputStream;
    }

    public FSDataInputStream fsDataInputStream() {
        return fsDataInputStream;
    }

    public void withFsDataOutputStream(FSDataOutputStream fsDataOutputStream) {
        this.fsDataOutputStream = fsDataOutputStream;
    }

    public void withFsDataInputStream(FSDataInputStream fsDataInputStream) {
        this.fsDataInputStream = fsDataInputStream;
    }
}
