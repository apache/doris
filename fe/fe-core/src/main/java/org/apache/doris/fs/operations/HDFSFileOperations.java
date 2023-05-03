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

import org.apache.doris.backup.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.URI;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class HDFSFileOperations implements FileOperations {
    private static final Logger LOG = LogManager.getLogger(HDFSFileOperations.class);
    public static final int READ_BUFFER_SIZE = 128 << 10; // 128k
    public static final int WRITE_BUFFER_SIZE = 128 << 10; // 128k

    private final FileSystem hdfsClient;

    public HDFSFileOperations(FileSystem hdfsClient) {
        this.hdfsClient = hdfsClient;
    }


    /**
     * open remotePath for read.
     *
     * @param opParams hdfsOpParams.remotePath:  hdfs://namenode:port/path.
     *                 hdfsOpParams.startOffset: the offset to read.
     * @return Status.OK and set fsDataInputStream if success
     */
    public Status openReader(OpParams opParams) {
        HDFSOpParams hdfsOpParams = (HDFSOpParams) opParams;
        try {
            URI pathUri = URI.create(hdfsOpParams.remotePath());
            Path inputFilePath = new Path(pathUri.getPath());
            FSDataInputStream fsDataInputStream = hdfsClient.open(inputFilePath, READ_BUFFER_SIZE);
            fsDataInputStream.seek(hdfsOpParams.startOffset());
            hdfsOpParams.withFsDataInputStream(fsDataInputStream);
            return Status.OK;
        } catch (IOException e) {
            LOG.error("errors while open path", e);
            return new Status(Status.ErrCode.COMMON_ERROR, "failed to open reader, msg:" + e.getMessage());
        } catch (UserException ex) {
            LOG.error("errors while get filesystem", ex);
            return new Status(Status.ErrCode.COMMON_ERROR, "failed to get filesystem, msg:" + ex.getMessage());
        }
    }

    /**
     * close for read.
     *
     * @param opParams hdfsOpParams.fsDataInputStream: the input stream.
     * @return Status.OK if success.
     */
    public Status closeReader(OpParams opParams) {
        HDFSOpParams hdfsOpParams = (HDFSOpParams) opParams;
        FSDataInputStream fsDataInputStream = hdfsOpParams.fsDataInputStream();
        synchronized (this) {
            try {
                fsDataInputStream.close();
            } catch (IOException e) {
                LOG.error("errors while close file input stream", e);
                return new Status(Status.ErrCode.COMMON_ERROR,
                        "errors while close file input stream, msg: " + e.getMessage());
            }
        }
        return Status.OK;
    }


    /**
     * open remotePath for write.
     *
     * @param opParams hdfsOpParams.remotePath: hdfs://namenode:port/path.
     * @return Status.OK and set FSDataOutputStream if success
     */
    public Status openWriter(OpParams opParams) {
        HDFSOpParams hdfsOpParams = (HDFSOpParams) opParams;
        try {
            URI pathUri = URI.create(hdfsOpParams.remotePath());
            Path inputFilePath = new Path(pathUri.getPath());
            hdfsOpParams.withFsDataOutputStream(hdfsClient.create(inputFilePath, true, WRITE_BUFFER_SIZE));
            return Status.OK;
        } catch (IOException e) {
            LOG.error("errors while open path", e);
            return new Status(Status.ErrCode.COMMON_ERROR, "failed to open writer, msg:" + e.getMessage());
        } catch (UserException ex) {
            LOG.error("errors while get filesystem", ex);
            return new Status(Status.ErrCode.COMMON_ERROR, "failed to get filesystem, msg:" + ex.getMessage());
        }
    }

    /**
     * close for write.
     *
     * @param opParams hdfsOpParams.fsDataOutputStream: output stream.
     * @return Status.OK if success.
     */
    public Status closeWriter(OpParams opParams) {
        HDFSOpParams hdfsOpParams = (HDFSOpParams) opParams;
        FSDataOutputStream fsDataOutputStream = hdfsOpParams.fsDataOutputStream();
        synchronized (this) {
            try {
                fsDataOutputStream.flush();
                fsDataOutputStream.close();
                LOG.info("finished to close writer");
            } catch (IOException e) {
                LOG.error("errors while close file output stream", e);
                return new Status(Status.ErrCode.COMMON_ERROR, "failed to close writer, msg:" + e.getMessage());
            }
        }
        return Status.OK;
    }
}
