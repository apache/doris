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

import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.backup.RemoteFile;
import org.apache.doris.backup.Status;
import org.apache.doris.catalog.AuthType;
import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HdfsStorage encapsulate interfaces accessing HDFS directly.
 * @see org.apache.doris.fs.remote.dfs.DFSFileSystem
 */
@Deprecated
public class HdfsStorage extends BlobStorage {
    private static final Logger LOG = LogManager.getLogger(HdfsStorage.class);
    private final Map<String, String> hdfsProperties;

    private final int readBufferSize = 128 << 10; // 128k
    private final int writeBufferSize = 128 << 10; // 128k

    private FileSystem dfsFileSystem = null;

    /**
     * init HdfsStorage with properties.
     *
     * @param properties parameters to access HDFS.
     */
    public HdfsStorage(Map<String, String> properties) {
        hdfsProperties = new HashMap<>();
        setProperties(properties);
        setType(StorageBackend.StorageType.HDFS);
        setName(StorageBackend.StorageType.HDFS.name());
    }

    @Override
    public FileSystem getFileSystem(String remotePath) throws UserException {
        if (dfsFileSystem != null) {
            return dfsFileSystem;
        }
        String username = hdfsProperties.get(HdfsResource.HADOOP_USER_NAME);
        Configuration conf = new HdfsConfiguration();
        boolean isSecurityEnabled = false;
        for (Map.Entry<String, String> propEntry : hdfsProperties.entrySet()) {
            conf.set(propEntry.getKey(), propEntry.getValue());
            if (propEntry.getKey().equals(HdfsResource.HADOOP_SECURITY_AUTHENTICATION)
                    && propEntry.getValue().equals(AuthType.KERBEROS.getDesc())) {
                isSecurityEnabled = true;
            }
        }
        try {
            if (isSecurityEnabled) {
                UserGroupInformation.setConfiguration(conf);
                UserGroupInformation.loginUserFromKeytab(
                        hdfsProperties.get(HdfsResource.HADOOP_KERBEROS_PRINCIPAL),
                        hdfsProperties.get(HdfsResource.HADOOP_KERBEROS_KEYTAB));
            }
            if (username == null) {
                dfsFileSystem = FileSystem.get(java.net.URI.create(remotePath), conf);
            } else {
                dfsFileSystem = FileSystem.get(java.net.URI.create(remotePath), conf, username);
            }
        } catch (Exception e) {
            LOG.error("errors while connect to " + remotePath, e);
            throw new UserException("errors while connect to " + remotePath, e);
        }
        return dfsFileSystem;
    }

    @Override
    public void setProperties(Map<String, String> properties) {
        super.setProperties(properties);
        hdfsProperties.putAll(properties);
    }

    @Override
    public Status downloadWithFileSize(String remoteFilePath, String localFilePath, long fileSize) {
        LOG.debug("download from {} to {}, file size: {}.", remoteFilePath, localFilePath, fileSize);
        final long start = System.currentTimeMillis();
        FSDataInputStream fsDataInputStream = null;
        try {
            fsDataInputStream = openReader(remoteFilePath, 0);
        } catch (Exception e) {
            return new Status(Status.ErrCode.COMMON_ERROR, e.getMessage());
        }
        LOG.info("finished to open reader. download {} to {}.", remoteFilePath, localFilePath);

        // delete local file if exist
        File localFile = new File(localFilePath);
        if (localFile.exists()) {
            try {
                Files.walk(Paths.get(localFilePath), FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder())
                        .map(java.nio.file.Path::toFile).forEach(File::delete);
            } catch (IOException e) {
                return new Status(Status.ErrCode.COMMON_ERROR,
                        "failed to delete exist local file: " + localFilePath + ", msg: " + e.getMessage());
            }
        }
        // create local file
        try {
            if (!localFile.createNewFile()) {
                return new Status(Status.ErrCode.COMMON_ERROR, "failed to create local file: " + localFilePath);
            }
        } catch (IOException e) {
            return new Status(Status.ErrCode.COMMON_ERROR,
                    "failed to create local file: " + localFilePath + ", msg: " + e.getMessage());
        }

        String lastErrMsg = null;
        Status status = Status.OK;
        try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(localFile))) {
            final long bufSize = 1024 * 1024; // 1MB
            long leftSize = fileSize;
            long readOffset = 0;
            while (leftSize > 0) {
                long readLen = Math.min(leftSize, bufSize);
                try {
                    ByteBuffer data = pread(fsDataInputStream, readOffset, readLen);
                    if (readLen != data.array().length) {
                        LOG.warn(
                                "the actual read length does not equal to "
                                        + "the expected read length: {} vs. {}, file: {}",
                                data.array().length, readLen, remoteFilePath);
                    }
                    // write local file
                    out.write(data.array());
                    readOffset += data.array().length;
                    leftSize -= data.array().length;
                } catch (Exception e) {
                    lastErrMsg = String.format(
                            "failed to read. " + "current read offset: %d, read length: %d,"
                                    + " file size: %d, file: %s. msg: %s",
                            readOffset, readLen, fileSize, remoteFilePath, e.getMessage());
                    LOG.warn(lastErrMsg);
                    status = new Status(Status.ErrCode.COMMON_ERROR, lastErrMsg);
                    break;
                }
            }
        } catch (IOException e) {
            return new Status(Status.ErrCode.COMMON_ERROR, "Got exception: " + e.getMessage());
        } finally {
            Status closeStatus = closeReader(fsDataInputStream);
            if (!closeStatus.ok()) {
                LOG.warn(closeStatus.getErrMsg());
                if (status.ok()) {
                    // we return close write error only if no other error has been encountered.
                    status = closeStatus;
                }
            }
        }

        LOG.info("finished to download from {} to {} with size: {}. cost {} ms", remoteFilePath, localFilePath,
                fileSize, (System.currentTimeMillis() - start));
        return status;
    }

    private int readBytesFully(FSDataInputStream is, byte[] dest) throws IOException {
        int readLength = 0;
        while (readLength < dest.length) {
            int availableReadLength = dest.length - readLength;
            int n = is.read(dest, readLength, availableReadLength);
            if (n <= 0) {
                break;
            }
            readLength += n;
        }
        return readLength;
    }

    /**
     * read data from fsDataInputStream.
     *
     * @param fsDataInputStream input stream for read.
     * @param readOffset        read offset.
     * @param length            read length.
     * @return ByteBuffer
     * @throws IOException when read data error.
     */
    public ByteBuffer pread(FSDataInputStream fsDataInputStream, long readOffset, long length) throws IOException {
        synchronized (fsDataInputStream) {
            long currentStreamOffset;
            try {
                currentStreamOffset = fsDataInputStream.getPos();
            } catch (IOException e) {
                LOG.error("errors while get file pos from output stream", e);
                throw new IOException("errors while get file pos from output stream", e);
            }
            if (currentStreamOffset != readOffset) {
                // it's ok, when reading some format like parquet, it is not a sequential read
                LOG.debug("invalid offset, current read offset is " + currentStreamOffset
                        + " is not equal to request offset " + readOffset + " seek to it");
                try {
                    fsDataInputStream.seek(readOffset);
                } catch (IOException e) {
                    throw new IOException(String.format(
                            "current read offset %d is not equal to %d, and could not seek to it, msg: %s",
                            currentStreamOffset, readOffset, e.getMessage()));
                }
            }
            // Avoid using the ByteBuffer based read for Hadoop because some
            // FSDataInputStream
            // implementations are not ByteBufferReadable,
            // See https://issues.apache.org/jira/browse/HADOOP-14603
            byte[] buf;
            if (length > readBufferSize) {
                buf = new byte[readBufferSize];
            } else {
                buf = new byte[(int) length];
            }
            try {
                int readLength = readBytesFully(fsDataInputStream, buf);
                if (readLength < 0) {
                    throw new IOException("end of file reached");
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "read buffer from input stream, buffer size:" + buf.length + ", read length:" + readLength);
                }
                return ByteBuffer.wrap(buf, 0, readLength);
            } catch (IOException e) {
                LOG.error("errors while read data from stream", e);
                throw new IOException("errors while read data from stream " + e.getMessage());
            }
        }
    }

    @Override
    public Status directUpload(String content, String remoteFile) {
        FSDataOutputStream fsDataOutputStream = null;
        try {
            fsDataOutputStream = openWriter(remoteFile);
        } catch (Exception e) {
            return new Status(Status.ErrCode.COMMON_ERROR, e.getMessage());
        }
        LOG.info("finished to open writer. directly upload to remote path {}.", remoteFile);

        Status status = Status.OK;
        try {
            fsDataOutputStream.writeBytes(content);
        } catch (IOException e) {
            LOG.error("errors while write data to output stream", e);
            status = new Status(Status.ErrCode.COMMON_ERROR, "write exception: " + e.getMessage());
        } finally {
            Status closeStatus = closeWriter(fsDataOutputStream);
            if (!closeStatus.ok()) {
                LOG.warn(closeStatus.getErrMsg());
                if (status.ok()) {
                    status = closeStatus;
                }
            }
        }
        return status;
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
        FileSystem fileSystem = getFileSystem(remotePath);
        try {
            return fileSystem.create(inputFilePath, true, writeBufferSize);
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
        FileSystem fileSystem = getFileSystem(remotePath);
        try {
            FSDataInputStream fsDataInputStream = fileSystem.open(inputFilePath, readBufferSize);
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

    @Override
    public Status upload(String localPath, String remotePath) {
        long start = System.currentTimeMillis();
        LOG.debug("local path {}, remote path {}", localPath, remotePath);
        FSDataOutputStream fsDataOutputStream = null;
        try {
            fsDataOutputStream = openWriter(remotePath);
        } catch (Exception e) {
            return new Status(Status.ErrCode.COMMON_ERROR, e.getMessage());
        }

        LOG.info("finished to open writer. directly upload to remote path {}.", remotePath);
        // read local file and write remote
        File localFile = new File(localPath);
        long fileLength = localFile.length();
        byte[] readBuf = new byte[1024];
        Status status = new Status(Status.ErrCode.OK, "");
        try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(localFile))) {
            // save the last err msg
            String lastErrMsg = null;
            // save the current write offset of remote file
            long writeOffset = 0;
            // read local file, 1MB at a time
            int bytesRead;
            while ((bytesRead = in.read(readBuf)) != -1) {
                try {
                    fsDataOutputStream.write(readBuf, 0, bytesRead);
                } catch (IOException e) {
                    LOG.error("errors while write data to output stream", e);
                    lastErrMsg = String.format(
                            "failed to write hdfs. current write offset: %d, write length: %d, "
                                    + "file length: %d, file: %s, msg: errors while write data to output stream",
                            writeOffset, bytesRead, fileLength, remotePath);
                    status = new Status(Status.ErrCode.COMMON_ERROR, lastErrMsg);
                    break;
                }

                // write succeed, update current write offset
                writeOffset += bytesRead;
            } // end of read local file loop
        } catch (FileNotFoundException e1) {
            return new Status(Status.ErrCode.COMMON_ERROR, "encounter file not found exception: " + e1.getMessage());
        } catch (IOException e1) {
            return new Status(Status.ErrCode.COMMON_ERROR, "encounter io exception: " + e1.getMessage());
        } finally {
            Status closeStatus = closeWriter(fsDataOutputStream);
            if (!closeStatus.ok()) {
                LOG.warn(closeStatus.getErrMsg());
                if (status.ok()) {
                    // we return close write error only if no other error has been encountered.
                    status = closeStatus;
                }
            }
        }

        if (status.ok()) {
            LOG.info("finished to upload {} to remote path {}. cost: {} ms", localPath, remotePath,
                    (System.currentTimeMillis() - start));
        }
        return status;
    }

    @Override
    public Status rename(String srcPath, String destPath) {
        long start = System.currentTimeMillis();
        try {
            URI srcPathUri = URI.create(srcPath);
            URI destPathUri = URI.create(destPath);
            if (!srcPathUri.getAuthority().trim().equals(destPathUri.getAuthority().trim())) {
                return new Status(Status.ErrCode.COMMON_ERROR, "only allow rename in same file system");
            }
            FileSystem fileSystem = getFileSystem(destPath);
            Path srcfilePath = new Path(srcPathUri.getPath());
            Path destfilePath = new Path(destPathUri.getPath());
            boolean isRenameSuccess = fileSystem.rename(srcfilePath, destfilePath);
            if (!isRenameSuccess) {
                return new Status(Status.ErrCode.COMMON_ERROR, "failed to rename " + srcPath + " to " + destPath);
            }
        } catch (UserException e) {
            return new Status(Status.ErrCode.COMMON_ERROR, e.getMessage());
        } catch (IOException e) {
            LOG.error("errors while rename path from " + srcPath + " to " + destPath);
            return new Status(Status.ErrCode.COMMON_ERROR,
                    "failed to rename remote " + srcPath + " to " + destPath + ", msg: " + e.getMessage());
        }
        LOG.info("finished to rename {} to  {}. cost: {} ms", srcPath, destPath, (System.currentTimeMillis() - start));
        return Status.OK;
    }

    @Override
    public Status delete(String remotePath) {
        try {
            URI pathUri = URI.create(remotePath);
            Path inputFilePath = new Path(pathUri.getPath());
            FileSystem fileSystem = getFileSystem(remotePath);
            fileSystem.delete(inputFilePath, true);
        } catch (UserException e) {
            return new Status(Status.ErrCode.COMMON_ERROR, e.getMessage());
        } catch (IOException e) {
            LOG.error("errors while delete path " + remotePath);
            return new Status(Status.ErrCode.COMMON_ERROR,
                    "failed to delete remote path: " + remotePath + ", msg: " + e.getMessage());
        }
        LOG.info("finished to delete remote path {}.", remotePath);
        return Status.OK;
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(String remotePath) throws UserException {
        FileSystem fileSystem = getFileSystem(remotePath);
        try {
            return fileSystem.listLocatedStatus(new Path(remotePath));
        } catch (IOException e) {
            throw new UserException("Failed to list located status for path: " + remotePath, e);
        }
    }

    @Override
    public Status list(String remotePath, List<RemoteFile> result) {
        return list(remotePath, result, true);
    }

    /**
     * get files in remotePath of HDFS.
     *
     * @param remotePath   hdfs://namenode:port/path.
     * @param result       files in remotePath.
     * @param fileNameOnly means get file only in remotePath if true.
     * @return Status.OK if success.
     */
    @Override
    public Status list(String remotePath, List<RemoteFile> result, boolean fileNameOnly) {
        try {
            URI pathUri = URI.create(remotePath);
            FileSystem fileSystem = getFileSystem(remotePath);
            Path pathPattern = new Path(pathUri.getPath());
            FileStatus[] files = fileSystem.globStatus(pathPattern);
            if (files == null) {
                LOG.info("no files in path " + remotePath);
                return Status.OK;
            }
            for (FileStatus fileStatus : files) {
                RemoteFile remoteFile = new RemoteFile(
                        fileNameOnly ? fileStatus.getPath().getName() : fileStatus.getPath().toString(),
                        !fileStatus.isDirectory(), fileStatus.isDirectory() ? -1 : fileStatus.getLen(),
                        fileStatus.getBlockSize());
                result.add(remoteFile);
            }
        } catch (FileNotFoundException e) {
            LOG.info("file not found: " + e.getMessage());
            return new Status(Status.ErrCode.NOT_FOUND, "file not found: " + e.getMessage());
        } catch (Exception e) {
            LOG.error("errors while get file status ", e);
            return new Status(Status.ErrCode.COMMON_ERROR, "errors while get file status " + e.getMessage());
        }
        LOG.info("finish list path {}", remotePath);
        return Status.OK;
    }

    @Override
    public Status makeDir(String remotePath) {
        return new Status(Status.ErrCode.COMMON_ERROR, "mkdir is not implemented.");
    }

    @Override
    public Status checkPathExist(String remotePath) {
        try {
            URI pathUri = URI.create(remotePath);
            Path inputFilePath = new Path(pathUri.getPath());
            FileSystem fileSystem = getFileSystem(remotePath);
            boolean isPathExist = fileSystem.exists(inputFilePath);
            if (!isPathExist) {
                return new Status(Status.ErrCode.NOT_FOUND, "remote path does not exist: " + remotePath);
            }
            return Status.OK;
        } catch (Exception e) {
            LOG.error("errors while check path exist " + remotePath, e);
            return new Status(Status.ErrCode.COMMON_ERROR,
                    "failed to check remote path exist: " + remotePath + ". msg: " + e.getMessage());
        }
    }

    @Override
    public StorageBackend.StorageType getStorageType() {
        return this.getType();
    }
}
