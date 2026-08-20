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

package org.apache.doris.fs.remote.dfs;

import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.backup.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.HadoopAuthenticator;
import org.apache.doris.common.util.S3Util;
import org.apache.doris.common.util.URI;
import org.apache.doris.datasource.property.storage.HdfsCompatibleProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.io.DorisInputFile;
import org.apache.doris.fs.io.DorisOutputFile;
import org.apache.doris.fs.io.ParsedPath;
import org.apache.doris.fs.io.hdfs.HdfsInputFile;
import org.apache.doris.fs.io.hdfs.HdfsOutputFile;
import org.apache.doris.fs.operations.HDFSFileOperations;
import org.apache.doris.fs.operations.HDFSOpParams;
import org.apache.doris.fs.operations.OpParams;
import org.apache.doris.fs.remote.RemoteFile;
import org.apache.doris.fs.remote.RemoteFileSystem;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

public class DFSFileSystem extends RemoteFileSystem {

    public static final String PROP_ALLOW_FALLBACK_TO_SIMPLE_AUTH = "ipc.client.fallback-to-simple-auth-allowed";
    private static final Logger LOG = LogManager.getLogger(DFSFileSystem.class);
    private final HdfsCompatibleProperties hdfsProperties;
    private volatile DFSFileSystemResource resource = null;

    public DFSFileSystem(HdfsCompatibleProperties hdfsProperties) {
        super(StorageBackend.StorageType.HDFS.name(), StorageBackend.StorageType.HDFS);
        this.properties.putAll(hdfsProperties.getOrigProps());
        this.hdfsProperties = hdfsProperties;
    }

    @Override
    public StorageProperties getStorageProperties() {
        return hdfsProperties;
    }

    @Override
    public Status listFiles(String remotePath, boolean recursive, List<RemoteFile> result) {
        try {
            Path locatedPath = new Path(remotePath);
            try (FileSystemLease lease = acquireFileSystemLease(locatedPath)) {
                org.apache.hadoop.fs.FileSystem fileSystem = lease.fileSystem();
                RemoteIterator<LocatedFileStatus> locatedFiles = getLocatedFiles(recursive, fileSystem, locatedPath);
                while (locatedFiles.hasNext()) {
                    LocatedFileStatus fileStatus = locatedFiles.next();
                    RemoteFile location = new RemoteFile(
                            fileStatus.getPath(), fileStatus.isDirectory(), fileStatus.getLen(),
                            fileStatus.getBlockSize(), fileStatus.getModificationTime(),
                            fileStatus.getBlockLocations());
                    result.add(location);
                }
            }
        } catch (FileNotFoundException e) {
            return new Status(Status.ErrCode.NOT_FOUND, e.getMessage());
        } catch (Exception e) {
            return new Status(Status.ErrCode.COMMON_ERROR, e.getMessage());
        }
        return Status.OK;
    }


    @Override
    public Status listDirectories(String remotePath, Set<String> result) {
        try {
            Path locatedPath = new Path(remotePath);
            try (FileSystemLease lease = acquireFileSystemLease(locatedPath)) {
                FileSystem fileSystem = lease.fileSystem();
                FileStatus[] fileStatuses = getFileStatuses(locatedPath, fileSystem);
                result.addAll(
                        Arrays.stream(fileStatuses)
                                .filter(FileStatus::isDirectory)
                                .map(file -> file.getPath().toString() + "/")
                                .collect(ImmutableSet.toImmutableSet()));
            }
        } catch (Exception e) {
            return new Status(Status.ErrCode.COMMON_ERROR, e.getMessage());
        }
        return Status.OK;
    }

    public DFSFileSystem(HdfsCompatibleProperties hdfsProperties, StorageBackend.StorageType storageType) {
        super(storageType.name(), storageType);
        this.properties.putAll(hdfsProperties.getOrigProps());
        this.hdfsProperties = hdfsProperties;
    }

    public synchronized FileSystemLease acquireFileSystemLease(Path remotePath) throws IOException {
        if (closed.get()) {
            throw new IOException("FileSystem is closed.");
        }
        if (resource == null) {
            DFSFileSystemResource newResource = createResource(remotePath);
            RemoteFSPhantomManager.registerPhantomReference(this, newResource);
            resource = newResource;
        }
        return resource.acquire();
    }

    private DFSFileSystemResource createResource(Path remotePath) throws IOException {
        try {
            FileSystem fileSystem = hdfsProperties.getHadoopAuthenticator().doAs(() -> {
                try {
                    Configuration originalConf = hdfsProperties.getHadoopStorageConfig();
                    // Create a copy of the original configuration to avoid modifying global settings
                    Configuration confCopy = new Configuration(originalConf);
                    // Disable FileSystem caching to ensure a new instance is created every time
                    // Reason: We manage the lifecycle of FileSystem instances manually.
                    // Even if the caller doesn't explicitly close the instance, we will do so when needed.
                    // However, since Hadoop caches FileSystem instances by default,
                    // other parts of the system may still be using the same instance.
                    // If we close the shared instance here, it could break those other users.
                    // Therefore, we disable the cache to ensure isolated, non-shared instances.
                    confCopy.setBoolean("fs.hdfs.impl.disable.cache", true);
                    return FileSystem.get(remotePath.toUri(), confCopy);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            return new DFSFileSystemResource(fileSystem);
        } catch (Exception e) {
            throw new IOException("Failed to get dfs FileSystem for " + e.getMessage(), e);
        }
    }

    public interface FileSystemLease extends AutoCloseable {
        FileSystem fileSystem();

        HDFSFileOperations operations();

        @Override
        void close();
    }

    protected RemoteIterator<LocatedFileStatus> getLocatedFiles(boolean recursive,
            FileSystem fileSystem, Path locatedPath) throws IOException {
        return hdfsProperties.getHadoopAuthenticator().doAs(() -> fileSystem.listFiles(locatedPath, recursive));
    }

    protected FileStatus[] getFileStatuses(Path remotePath, FileSystem fileSystem) throws IOException {
        return hdfsProperties.getHadoopAuthenticator().doAs(() -> fileSystem.listStatus(remotePath));
    }

    public static Configuration getHdfsConf(boolean fallbackToSimpleAuth) {
        Configuration hdfsConf = new HdfsConfiguration();
        if (fallbackToSimpleAuth) {
            // need support fallback to simple if the cluster is a mixture of  kerberos and simple auth.
            hdfsConf.set(PROP_ALLOW_FALLBACK_TO_SIMPLE_AUTH, "true");
        }
        return hdfsConf;
    }

    @Override
    public Status downloadWithFileSize(String remoteFilePath, String localFilePath, long fileSize) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("download from {} to {}, file size: {}.", remoteFilePath, localFilePath, fileSize);
        }
        final long start = System.currentTimeMillis();
        try (FileSystemLease lease = acquireFileSystemLease(new Path(remoteFilePath))) {
            HDFSFileOperations operations = lease.operations();
            HDFSOpParams hdfsOpParams = OpParams.of(remoteFilePath);
            Status st = operations.openReader(hdfsOpParams);
            if (st != Status.OK) {
                return st;
            }
            FSDataInputStream fsDataInputStream = hdfsOpParams.fsDataInputStream();
            LOG.info("finished to open reader. download {} to {}.", remoteFilePath, localFilePath);

            Status status = Status.OK;
            try {
                // delete local file if exist
                File localFile = new File(localFilePath);
                if (localFile.exists()) {
                    try {
                        Files.walk(Paths.get(localFilePath), FileVisitOption.FOLLOW_LINKS)
                                .sorted(Comparator.reverseOrder())
                                .map(java.nio.file.Path::toFile)
                                .forEach(File::delete);
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

                String lastErrMsg;
                try (BufferedOutputStream out = new BufferedOutputStream(Files.newOutputStream(localFile.toPath()))) {
                    final long bufSize = 1024 * 1024; // 1MB
                    long leftSize = fileSize;
                    long readOffset = 0;
                    while (leftSize > 0) {
                        long readLen = Math.min(leftSize, bufSize);
                        try {
                            ByteBuffer data = readStreamBuffer(fsDataInputStream, readOffset, readLen);
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
                }
            } catch (IOException e) {
                return new Status(Status.ErrCode.COMMON_ERROR, "Got exception: " + e.getMessage());
            } finally {
                Status closeStatus = operations.closeReader(OpParams.of(fsDataInputStream));
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
        } catch (IOException e) {
            LOG.warn("errors while download from {} to {}", remoteFilePath, localFilePath, e);
            return new Status(Status.ErrCode.COMMON_ERROR,
                    "failed to download remote file: " + remoteFilePath + ", msg: " + e.getMessage());
        }
    }

    /**
     * read data from fsDataInputStream.
     *
     * @param fsDataInputStream input stream for read.
     * @param readOffset read offset.
     * @param length read length.
     * @return ByteBuffer
     * @throws IOException when read data error.
     */
    private static ByteBuffer readStreamBuffer(FSDataInputStream fsDataInputStream, long readOffset, long length)
            throws IOException {
        synchronized (fsDataInputStream) {
            long currentStreamOffset;
            try {
                currentStreamOffset = fsDataInputStream.getPos();
            } catch (IOException e) {
                LOG.warn("errors while get file pos from output stream", e);
                throw new IOException("errors while get file pos from output stream", e);
            }
            if (currentStreamOffset != readOffset) {
                // it's ok, when reading some format like parquet, it is not a sequential read
                if (LOG.isDebugEnabled()) {
                    LOG.debug("invalid offset, current read offset is " + currentStreamOffset
                            + " is not equal to request offset " + readOffset + " seek to it");
                }
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
            if (length > HDFSFileOperations.READ_BUFFER_SIZE) {
                buf = new byte[HDFSFileOperations.READ_BUFFER_SIZE];
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
                LOG.warn("errors while read data from stream", e);
                throw new IOException("errors while read data from stream " + e.getMessage());
            }
        }
    }

    private static int readBytesFully(FSDataInputStream is, byte[] dest) throws IOException {
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

    @Override
    public Status exists(String remotePath) {
        try {
            URI pathUri = URI.create(remotePath);
            Path inputFilePath = new Path(pathUri.getLocation());
            try (FileSystemLease lease = acquireFileSystemLease(inputFilePath)) {
                FileSystem fileSystem = lease.fileSystem();
                boolean isPathExist = hdfsProperties.getHadoopAuthenticator().doAs(
                        () -> fileSystem.exists(inputFilePath));
                if (!isPathExist) {
                    return new Status(Status.ErrCode.NOT_FOUND, "remote path does not exist: " + remotePath);
                }
            }
            return Status.OK;
        } catch (Exception e) {
            LOG.warn("errors while check path exist " + remotePath, e);
            return new Status(Status.ErrCode.COMMON_ERROR,
                    "failed to check remote path exist: " + remotePath + ". msg: " + e.getMessage());
        }
    }

    @Override
    public Status directUpload(String content, String remoteFile) {
        try (FileSystemLease lease = acquireFileSystemLease(new Path(remoteFile))) {
            HDFSFileOperations operations = lease.operations();
            HDFSOpParams hdfsOpParams = OpParams.of(remoteFile);
            Status wst = operations.openWriter(hdfsOpParams);
            if (wst != Status.OK) {
                return wst;
            }
            FSDataOutputStream fsDataOutputStream = hdfsOpParams.fsDataOutputStream();
            LOG.info("finished to open writer. directly upload to remote path {}.", remoteFile);

            Status status = Status.OK;
            try {
                fsDataOutputStream.writeBytes(content);
            } catch (IOException e) {
                LOG.warn("errors while write data to output stream", e);
                status = new Status(Status.ErrCode.COMMON_ERROR, "write exception: " + e.getMessage());
            } finally {
                Status closeStatus = operations.closeWriter(OpParams.of(fsDataOutputStream));
                if (!closeStatus.ok()) {
                    LOG.warn(closeStatus.getErrMsg());
                    if (status.ok()) {
                        status = closeStatus;
                    }
                }
            }
            return status;
        } catch (IOException e) {
            LOG.warn("errors while direct upload to {}", remoteFile, e);
            return new Status(Status.ErrCode.COMMON_ERROR,
                    "failed to direct upload remote file: " + remoteFile + ", msg: " + e.getMessage());
        }
    }

    @Override
    public Status upload(String localPath, String remotePath) {
        long start = System.currentTimeMillis();
        if (LOG.isDebugEnabled()) {
            LOG.debug("local path {}, remote path {}", localPath, remotePath);
        }
        try (FileSystemLease lease = acquireFileSystemLease(new Path(remotePath))) {
            HDFSFileOperations operations = lease.operations();
            HDFSOpParams hdfsOpParams = OpParams.of(remotePath);
            Status wst = operations.openWriter(hdfsOpParams);
            if (wst != Status.OK) {
                return wst;
            }
            FSDataOutputStream fsDataOutputStream = hdfsOpParams.fsDataOutputStream();
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
                        LOG.warn("errors while write data to output stream", e);
                        lastErrMsg = String.format(
                                "failed to write hdfs. current write offset: %d, write length: %d, "
                                        + "file length: %d, file: %s, "
                                        + "msg: errors while write data to output stream",
                                writeOffset, bytesRead, fileLength, remotePath);
                        status = new Status(Status.ErrCode.COMMON_ERROR, lastErrMsg);
                        break;
                    }

                    // write succeed, update current write offset
                    writeOffset += bytesRead;
                } // end of read local file loop
            } catch (FileNotFoundException e1) {
                return new Status(Status.ErrCode.COMMON_ERROR,
                        "encounter file not found exception: " + e1.getMessage());
            } catch (IOException e1) {
                return new Status(Status.ErrCode.COMMON_ERROR, "encounter io exception: " + e1.getMessage());
            } finally {
                Status closeStatus = operations.closeWriter(OpParams.of(fsDataOutputStream));
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
        } catch (IOException e) {
            LOG.warn("errors while upload {} to {}", localPath, remotePath, e);
            return new Status(Status.ErrCode.COMMON_ERROR,
                    "failed to upload remote file: " + remotePath + ", msg: " + e.getMessage());
        }
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
            try (FileSystemLease lease = acquireFileSystemLease(new Path(destPath))) {
                FileSystem fileSystem = lease.fileSystem();
                Path srcfilePath = new Path(srcPathUri.getPath());
                Path destfilePath = new Path(destPathUri.getPath());
                boolean isRenameSuccess = hdfsProperties.getHadoopAuthenticator().doAs(()
                        -> fileSystem.rename(srcfilePath, destfilePath));
                if (!isRenameSuccess) {
                    return new Status(Status.ErrCode.COMMON_ERROR, "failed to rename " + srcPath + " to " + destPath);
                }
            }
        } catch (UserException e) {
            return new Status(Status.ErrCode.COMMON_ERROR, e.getMessage());
        } catch (IOException e) {
            LOG.warn("errors while rename path from " + srcPath + " to " + destPath);
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
            Path inputFilePath = new Path(pathUri.getLocation());
            try (FileSystemLease lease = acquireFileSystemLease(inputFilePath)) {
                FileSystem fileSystem = lease.fileSystem();
                hdfsProperties.getHadoopAuthenticator().doAs(() -> fileSystem.delete(inputFilePath, true));
            }
        } catch (UserException e) {
            return new Status(Status.ErrCode.COMMON_ERROR, e.getMessage());
        } catch (IOException e) {
            LOG.warn("errors while delete path " + remotePath);
            return new Status(Status.ErrCode.COMMON_ERROR,
                    "failed to delete remote path: " + remotePath + ", msg: " + e.getMessage());
        }
        LOG.info("finished to delete remote path {}.", remotePath);
        return Status.OK;
    }

    /**
     * get files in remotePath of HDFS.
     *
     * @param remotePath hdfs://namenode:port/path.
     * @param result files in remotePath.
     * @param fileNameOnly means get file only in remotePath if true.
     * @return Status.OK if success.
     */
    @Override
    public Status globList(String remotePath, List<RemoteFile> result, boolean fileNameOnly) {
        try {
            URI pathUri = URI.create(remotePath);
            Path pathPattern = new Path(S3Util.extendGlobs(pathUri.getLocation()));
            try (FileSystemLease lease = acquireFileSystemLease(pathPattern)) {
                FileSystem fileSystem = lease.fileSystem();
                FileStatus[] files = hdfsProperties.getHadoopAuthenticator().doAs(
                        () -> fileSystem.globStatus(pathPattern));
                if (files == null) {
                    LOG.info("no files in path " + remotePath);
                    return Status.OK;
                }
                for (FileStatus fileStatus : files) {
                    RemoteFile remoteFile = new RemoteFile(
                            fileNameOnly ? fileStatus.getPath().getName() : fileStatus.getPath().toString(),
                            !fileStatus.isDirectory(), fileStatus.isDirectory() ? -1 : fileStatus.getLen(),
                            fileStatus.getBlockSize(), fileStatus.getModificationTime());
                    result.add(remoteFile);
                }
            }
        } catch (FileNotFoundException e) {
            LOG.info("file not found: " + e.getMessage());
            return new Status(Status.ErrCode.NOT_FOUND, "file not found: " + e.getMessage());
        } catch (Exception e) {
            LOG.warn("errors while get file status ", e);
            return new Status(Status.ErrCode.COMMON_ERROR, "errors while get file status " + e.getMessage());
        }
        LOG.info("finish list path {}", remotePath);
        return Status.OK;
    }

    @Override
    public Status makeDir(String remotePath) {
        try {
            Path locatedPath = new Path(remotePath);
            try (FileSystemLease lease = acquireFileSystemLease(locatedPath)) {
                FileSystem fileSystem = lease.fileSystem();
                if (!hdfsProperties.getHadoopAuthenticator().doAs(() -> fileSystem.mkdirs(locatedPath))) {
                    LOG.warn("failed to make dir for " + remotePath);
                    return new Status(Status.ErrCode.COMMON_ERROR, "failed to make dir for " + remotePath);
                }
            }
        } catch (Exception e) {
            LOG.warn("failed to make dir for {}, exception:", remotePath, e);
            return new Status(Status.ErrCode.COMMON_ERROR, e.getMessage());
        }
        return Status.OK;
    }

    @VisibleForTesting
    public HadoopAuthenticator getAuthenticator() {
        return hdfsProperties.getHadoopAuthenticator();
    }

    @Override
    public void close() throws IOException {
        DFSFileSystemResource current;
        synchronized (this) {
            if (closed.get()) {
                return;
            }
            closed.set(true);
            current = resource;
        }
        // Close outside the DFSFileSystem lock because closing the Hadoop FileSystem can be slow.
        if (current != null) {
            current.requestClose();
        }
    }

    public FileStatus getFileStatus(Path path) throws IOException {
        try (FileSystemLease lease = acquireFileSystemLease(path)) {
            FileSystem fileSystem = lease.fileSystem();
            return hdfsProperties.getHadoopAuthenticator().doAs(() -> fileSystem.getFileStatus(path));
        }
    }

    public FSDataInputStream openFile(Path path, FileSystemLease lease) throws IOException {
        FileSystem fileSystem = lease.fileSystem();
        return hdfsProperties.getHadoopAuthenticator().doAs(() -> fileSystem.open(path));
    }

    public FSDataOutputStream createFile(Path path, boolean overwrite, FileSystemLease lease) throws IOException {
        FileSystem fileSystem = lease.fileSystem();
        return hdfsProperties.getHadoopAuthenticator().doAs(() -> fileSystem.create(path, overwrite));
    }

    @Override
    public DorisOutputFile newOutputFile(ParsedPath path) {
        return new HdfsOutputFile(path, this);
    }

    @Override
    public DorisInputFile newInputFile(ParsedPath path, long length) {
        return new HdfsInputFile(path, length, this);
    }
}
