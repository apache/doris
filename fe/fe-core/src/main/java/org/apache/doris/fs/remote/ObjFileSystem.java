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
import org.apache.doris.fs.obj.ObjStorage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;

public abstract class ObjFileSystem extends RemoteFileSystem {
    private static final Logger LOG = LogManager.getLogger(ObjFileSystem.class);

    protected final ObjStorage<?> objStorage;

    public ObjFileSystem(String name, StorageBackend.StorageType type, ObjStorage<?> objStorage) {
        super(name, type);
        this.objStorage = objStorage;
    }

    public ObjStorage<?> getObjStorage() {
        return objStorage;
    }

    @Override
    public Status exists(String remotePath) {
        return objStorage.headObject(remotePath);
    }

    @Override
    public Status directoryExists(String dir) {
        return listFiles(dir, false, new ArrayList<>());
    }

    /**
     * download data from remote file and check data size with expected file size.
     *
     * @param remoteFilePath remote file path
     * @param localFilePath  local file path
     * @param fileSize       download data size
     * @return
     */
    @Override
    public Status downloadWithFileSize(String remoteFilePath, String localFilePath, long fileSize) {
        long start = System.currentTimeMillis();
        // Write the data to a local file
        File localFile = new File(localFilePath);
        if (localFile.exists()) {
            try {
                Files.walk(Paths.get(localFilePath), FileVisitOption.FOLLOW_LINKS)
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            } catch (IOException e) {
                return new Status(
                        Status.ErrCode.COMMON_ERROR, "failed to delete exist local file: " + localFilePath);
            }
        }
        Status st = objStorage.getObject(remoteFilePath, localFile);
        if (st != Status.OK) {
            return st;
        }
        if (localFile.length() == fileSize) {
            LOG.info(
                    "finished to get file from {} to {} with size: {}. cost {} ms",
                    remoteFilePath,
                    localFile.toPath(),
                    fileSize,
                    (System.currentTimeMillis() - start));
            return Status.OK;
        } else {
            return new Status(Status.ErrCode.COMMON_ERROR, localFile.toString());
        }
    }

    @Override
    public Status directUpload(String content, String remoteFile) {
        Status st = objStorage.putObject(remoteFile, new ByteArrayInputStream(content.getBytes()), content.length());
        if (st != Status.OK) {
            return st;
        }
        LOG.info("upload content success.");
        return Status.OK;
    }

    @Override
    public Status upload(String localPath, String remotePath) {
        File localFile = new File(localPath);
        Status st = null;
        try {
            st = objStorage.putObject(remotePath, new FileInputStream(localFile), localFile.length());
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        if (st != Status.OK) {
            return st;
        }
        LOG.info("upload file " + localPath + " success.");
        return Status.OK;
    }

    @Override
    public Status makeDir(String remotePath) {
        if (!remotePath.endsWith("/")) {
            remotePath += "/";
        }
        Status st = objStorage.putObject(remotePath, new ByteArrayInputStream(new byte[0]), 0);
        if (st != Status.OK) {
            return st;
        }
        LOG.info("makeDir success.");
        return Status.OK;
    }

    @Override
    public Status rename(String origFilePath, String destFilePath) {
        Status status = objStorage.copyObject(origFilePath, destFilePath);
        if (status.ok()) {
            return delete(origFilePath);
        } else {
            return status;
        }
    }

    public Status copy(String origFilePath, String destFilePath) {
        return objStorage.copyObject(origFilePath, destFilePath);
    }

    @Override
    public Status delete(String remotePath) {
        return objStorage.deleteObject(remotePath);
    }

    @Override
    public Status deleteDirectory(String absolutePath) {
        return objStorage.deleteObjects(absolutePath);
    }


    /**
     * Completes a multipart upload operation.
     *
     * <p>In object storage systems, large files are often uploaded in multiple parts.
     * Once all parts have been successfully uploaded, this method is called to merge
     * them into a single finalized object.
     *
     * <p>The main purpose of this method is to notify the underlying storage service
     * to perform the final merge and make the object available for normal access.
     *
     * @param bucket   The name of the target bucket.
     * @param key      The full object key (path) within the bucket.
     * @param uploadId The unique identifier of the multipart upload session.
     * @param parts    A mapping of part numbers to their corresponding ETag values,
     *                 used to assemble the parts in the correct order.
     */
    public abstract void completeMultipartUpload(String bucket, String key,
                                                 String uploadId, Map<Integer, String> parts);
}
