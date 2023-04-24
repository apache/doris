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

public class HDFSFileOperations {
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
     * @param remotePath  hdfs://namenode:port/path.
     * @param startOffset the offset to read.
     * @return FSDataInputStream if success.
     * @throws UserException when get filesystem failed.
     * @throws IOException   when open file error.
     */
    public FSDataInputStream openReader(String remotePath, long startOffset) throws UserException, IOException {
        URI pathUri = URI.create(remotePath);
        Path inputFilePath = new Path(pathUri.getPath());
        try {
            FSDataInputStream fsDataInputStream = hdfsClient.open(inputFilePath, READ_BUFFER_SIZE);
            fsDataInputStream.seek(startOffset);
            return fsDataInputStream;
        } catch (IOException e) {
            LOG.error("errors while open path", e);
            throw new IOException(e.getMessage());
        }
    }

    /**
     * close for read.
     *
     * @param fsDataInputStream the input stream.
     * @return Status.OK if success.
     */
    public Status closeReader(FSDataInputStream fsDataInputStream) {
        synchronized (fsDataInputStream) {
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
     * @param remotePath hdfs://namenode:port/path.
     * @return FSDataOutputStream
     * @throws UserException when get filesystem failed.
     * @throws IOException   when open path error.
     */
    public FSDataOutputStream openWriter(String remotePath) throws UserException, IOException {
        URI pathUri = URI.create(remotePath);
        Path inputFilePath = new Path(pathUri.getPath());
        try {
            return hdfsClient.create(inputFilePath, true, WRITE_BUFFER_SIZE);
        } catch (IOException e) {
            LOG.error("errors while open path", e);
            throw new IOException(e.getMessage());
        }
    }

    /**
     * close for write.
     *
     * @param fsDataOutputStream output stream.
     * @return Status.OK if success.
     */

    public Status closeWriter(FSDataOutputStream fsDataOutputStream) {
        synchronized (fsDataOutputStream) {
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
