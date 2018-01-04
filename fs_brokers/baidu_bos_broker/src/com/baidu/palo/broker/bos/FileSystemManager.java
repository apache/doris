// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.broker.bos;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.baidu.palo.thrift.TBrokerFD;
import com.baidu.palo.thrift.TBrokerFileStatus;
import com.baidu.palo.thrift.TBrokerOperationStatusCode;

public class FileSystemManager {

    private static Logger logger = LogManager
            .getLogger(FileSystemManager.class.getName());
    public static final String BOS_ENDPOINT = "bos_endpoint";
    public static final String BOS_ACCESS_KEY = "bos_accesskey";
    public static final String BOS_SECRET_ACCESS_KEY = "bos_secret_accesskey";
    
    private ScheduledExecutorService handleManagementPool = Executors.newScheduledThreadPool(2);
    
    private int readBufferSize = 128 << 10; // 128k
    private int writeBufferSize = 128 << 10; // 128k
    
    private ConcurrentHashMap<FileSystemIdentity, BrokerFileSystem> cachedFileSystem;
    private ClientContextManager clientContextManager;
    
    public FileSystemManager() {
        cachedFileSystem = new ConcurrentHashMap<>();
        clientContextManager = new ClientContextManager(handleManagementPool);
        readBufferSize = BrokerConfig.hdfs_read_buffer_size_kb << 10;
        writeBufferSize = BrokerConfig.hdfs_write_buffer_size_kb << 10;
        handleManagementPool.schedule(new FileSystemExpirationChecker(), 0, TimeUnit.SECONDS);
    }
    
    /**
     * visible for test
     * 
     * file system handle is cached, the identity is host + username_password
     * it will have safety problem if only hostname is used because one user may specify username and password
     * and then access hdfs, another user may not specify username and password but could also access data
     * @param path
     * @param properties
     * @return
     * @throws URISyntaxException 
     * @throws Exception 
     */
    public BrokerFileSystem getFileSystem(String path, Map<String, String> properties) {
        URI pathUri;
        try {
            pathUri = new URI(path);
        } catch (URISyntaxException e) {
            throw new BrokerException(TBrokerOperationStatusCode.INVALID_INPUT_FILE_PATH, e);
        }
        if (!properties.containsKey(BOS_ENDPOINT) || properties.get(BOS_ENDPOINT).trim().equals("")) {
            throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT, " bos_endpoint is not specified");
        }
        String accessKey = properties.containsKey(BOS_ACCESS_KEY) ? properties.get(BOS_ACCESS_KEY).trim() : "";
        String secretKey = properties.containsKey(BOS_SECRET_ACCESS_KEY) 
                ? properties.get(BOS_SECRET_ACCESS_KEY).trim() : "";
        String bosEndpoint = properties.get(BOS_ENDPOINT).trim();
        // using bce endpint + bucket name + aksk as unique key
        FileSystemIdentity fileSystemIdentity = new FileSystemIdentity(bosEndpoint + "/" 
                + pathUri.getAuthority(), accessKey + "_" + secretKey);
        BrokerFileSystem fileSystem = null;
        cachedFileSystem.putIfAbsent(fileSystemIdentity, new BrokerFileSystem(fileSystemIdentity));
        fileSystem = cachedFileSystem.get(fileSystemIdentity);
        if (fileSystem == null) {
            // it means it is removed concurrently by checker thread
            return null;
        }
        fileSystem.getLock().lock();
        try {
            if (!cachedFileSystem.containsKey(fileSystemIdentity)) {
                // this means the file system is closed by file system checker thread
                // it is a corner case
                return null;
            }
            if (fileSystem.getDFSFileSystem() == null) {
                // create a new filesystem
                Configuration conf = new Configuration();
                conf.set("fs.bos.impl", "com.baidu.palo.broker.bos.BaiduBosFileSystem");
                conf.set("fs.bos.access.key", accessKey);
                conf.set("fs.bos.secret.access.key", secretKey);
                conf.set("fs.bos.endpoint", bosEndpoint);
                FileSystem dfsFileSystem = FileSystem.get(URI.create(path), conf);
                fileSystem.setFileSystem(dfsFileSystem);
            }
            return fileSystem;
        } catch (Exception e) {
            logger.error("errors while connect to " + path, e);
            throw new BrokerException(TBrokerOperationStatusCode.NOT_AUTHORIZED, e);
        } finally {
            fileSystem.getLock().unlock();
        }
    }
    
    public List<TBrokerFileStatus> listPath(String path, Map<String, String> properties) {
        List<TBrokerFileStatus> resultFileStatus = null;
        URI pathUri = getUriFromPath(path);
        BrokerFileSystem fileSystem = getFileSystem(path, properties);
        Path pathPattern = new Path(pathUri.getPath());
        try {
            FileStatus[] files = fileSystem.getDFSFileSystem().globStatus(pathPattern);
            if (files == null) {
                resultFileStatus = new ArrayList<>(0);
                return resultFileStatus;
            }
            resultFileStatus = new ArrayList<>(files.length);
            for (FileStatus fileStatus : files) {
                TBrokerFileStatus brokerFileStatus = new TBrokerFileStatus();
                brokerFileStatus.setIsDir(fileStatus.isDir());
                if (fileStatus.isDir()) {
                    brokerFileStatus.setIsSplitable(false);
                    brokerFileStatus.setSize(-1);
                } else {
                    brokerFileStatus.setSize(fileStatus.getLen());
                    brokerFileStatus.setIsSplitable(true);
                }
                brokerFileStatus.setPath(fileStatus.getPath().toString());
                resultFileStatus.add(brokerFileStatus);
            }
        } catch (Exception e) {
            logger.error("errors while get file status ", e);
            fileSystem.closeFileSystem();
            throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR, 
                    e, "unknown error when get file status");
        }
        return resultFileStatus;
    }
    
    public void deletePath(String path, Map<String, String> properties) {
        URI pathUri = getUriFromPath(path);
        BrokerFileSystem fileSystem = getFileSystem(path, properties);
        Path filePath = new Path(pathUri.getPath());
        try {
            fileSystem.getDFSFileSystem().delete(filePath, true);
        } catch (IOException e) {
            logger.error("errors while delete path " + path);
            fileSystem.closeFileSystem();
            throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR, 
                    e, "delete path {} error", path);
        }
    }
    
    public void renamePath(String srcPath, String destPath, Map<String, String> properties) {
        URI srcPathUri = getUriFromPath(srcPath);
        URI destPathUri = getUriFromPath(destPath);
        if (!srcPathUri.getAuthority().trim().equals(destPathUri.getAuthority().trim())) {
            throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR, 
                    "only allow rename in same file system");
        }
        BrokerFileSystem fileSystem = getFileSystem(srcPath, properties);
        Path srcfilePath = new Path(srcPathUri.getPath());
        Path destfilePath = new Path(destPathUri.getPath());
        try {
            boolean isRenameSuccess = fileSystem.getDFSFileSystem().rename(srcfilePath, destfilePath);
            if (!isRenameSuccess) {
                throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR, 
                        "failed to rename path from {} to {}", srcPath, destPath); 
            }
        } catch (IOException e) {
            logger.error("errors while rename path from " + srcPath + " to " + destPath);
            fileSystem.closeFileSystem();
            throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR, 
                    e, "errors while rename {} to {}", srcPath, destPath);
        }
    }
    
    public boolean checkPathExist(String path, Map<String, String> properties) {
        URI pathUri = getUriFromPath(path);
        BrokerFileSystem fileSystem = getFileSystem(path, properties);
        Path filePath = new Path(pathUri.getPath());
        try {
            boolean isPathExist = fileSystem.getDFSFileSystem().exists(filePath);
            return isPathExist;
        } catch (IOException e) {
            logger.error("errors while check path exist: " + path);
            fileSystem.closeFileSystem();
            throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR, 
                    e, "errors while check if path {} exist", path);
        }
    }
    
    public TBrokerFD openReader(String clientId, String path, long startOffset, Map<String, String> properties) {
        URI pathUri = getUriFromPath(path);
        Path inputFilePath = new Path(pathUri.getPath());
        BrokerFileSystem fileSystem = getFileSystem(path, properties);
        try {
            FSDataInputStream fsDataInputStream = fileSystem.getDFSFileSystem().open(inputFilePath, readBufferSize);
            fsDataInputStream.seek(startOffset);
            UUID uuid = UUID.randomUUID();
            TBrokerFD fd = parseUUIDToFD(uuid);
            clientContextManager.putNewInputStream(clientId, fd, fsDataInputStream, fileSystem);
            return fd;
        } catch (IOException e) {
            logger.error("errors while open path", e);
            fileSystem.closeFileSystem();
            throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR, 
                    e, "could not open file {}", path);
        }
    }
    
    public ByteBuffer pread(TBrokerFD fd, long offset, long length) {
        FSDataInputStream fsDataInputStream = clientContextManager.getFsDataInputStream(fd);
        synchronized (fsDataInputStream) {
            long currentStreamOffset;
            try {
                currentStreamOffset = fsDataInputStream.getPos();
            } catch (IOException e) {
                logger.error("errors while get file pos from output stream", e);
                throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR, 
                        "errors while get file pos from output stream");
            }
            if (currentStreamOffset != offset) {
                logger.warn("invalid offset, current read offset is " 
                        + currentStreamOffset + " is not equal to request offset " 
                        + offset + " seek to it");
                try {
                    fsDataInputStream.seek(offset);
                } catch (IOException e) {
                    throw new BrokerException(TBrokerOperationStatusCode.INVALID_INPUT_OFFSET, 
                            e, "current read offset {} is not equal to {}, and could not seek to it", 
                            currentStreamOffset, offset);
                }
            }
            byte[] buf;
            if (length > readBufferSize) {
                buf = new byte[readBufferSize];
            } else {
                buf = new byte[(int) length];
            }
            try {
                int readLength = fsDataInputStream.read(buf);
                if (readLength < 0) {
                    throw new BrokerException(TBrokerOperationStatusCode.END_OF_FILE, 
                            "end of file reached");
                }
                return ByteBuffer.wrap(buf, 0, readLength);
            } catch (IOException e) {
                logger.error("errors while read data from stream", e);
                throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR, 
                        e, "errors while write data to output stream");
            }
        }
    }
    
    public void seek(TBrokerFD fd, long offset) {
        throw new BrokerException(TBrokerOperationStatusCode.OPERATION_NOT_SUPPORTED, 
                "seek this method is not supported");
    }
    
    public void closeReader(TBrokerFD fd) {
        FSDataInputStream fsDataInputStream = clientContextManager.getFsDataInputStream(fd);
        synchronized (fsDataInputStream) {
            try {
                fsDataInputStream.close();
            } catch (IOException e) {
                logger.error("errors while close file input stream", e);
            } finally {
                clientContextManager.removeInputStream(fd);
            }
        }
    }
    
    public TBrokerFD openWriter(String clientId, String path, Map<String, String> properties) {
        URI pathUri = getUriFromPath(path);
        Path inputFilePath = new Path(pathUri.getPath());
        BrokerFileSystem fileSystem = getFileSystem(path, properties);
        try {
            FSDataOutputStream fsDataOutputStream = fileSystem.getDFSFileSystem().create(inputFilePath, 
                    true, writeBufferSize);
            UUID uuid = UUID.randomUUID();
            TBrokerFD fd = parseUUIDToFD(uuid);
            clientContextManager.putNewOutputStream(clientId, fd, fsDataOutputStream, fileSystem);
            return fd;
        } catch (IOException e) {
            logger.error("errors while open path", e);
            fileSystem.closeFileSystem();
            throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR, 
                    e, "could not open file {}", path);
        }
    }
    
    public void pwrite(TBrokerFD fd, long offset, byte[] data) {
        FSDataOutputStream fsDataOutputStream = clientContextManager.getFsDataOutputStream(fd);
        synchronized (fsDataOutputStream) {
            long currentStreamOffset;
            try {
                currentStreamOffset = fsDataOutputStream.getPos();
            } catch (IOException e) {
                logger.error("errors while get file pos from output stream", e);
                throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR, 
                        "errors while get file pos from output stream");
            }
            if (currentStreamOffset != offset) {
                throw new BrokerException(TBrokerOperationStatusCode.INVALID_INPUT_OFFSET, 
                        "current outputstream offset is {} not equal to request {}", 
                        currentStreamOffset, offset);
            }
            try {
                fsDataOutputStream.write(data);
            } catch (IOException e) {
                logger.error("errors while write data to output stream", e);
                throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR, 
                        e, "errors while write data to output stream");
            }
        }
    }
    
    public void closeWriter(TBrokerFD fd) {
        FSDataOutputStream fsDataOutputStream = clientContextManager.getFsDataOutputStream(fd);
        synchronized (fsDataOutputStream) {
            try {
                fsDataOutputStream.close();
            } catch (IOException e) {
                logger.error("errors while close file output stream", e);
            } finally {
                clientContextManager.removeOutputStream(fd);
            }
        }
    }
    
    public void ping(String clientId) {
        clientContextManager.onPing(clientId);
    }
    
    private URI getUriFromPath(String path) {
        URI pathUri;
        try {
            pathUri = new URI(path);
        } catch (URISyntaxException e) {
            logger.error("invalid input path " + path);
            throw new BrokerException(TBrokerOperationStatusCode.INVALID_INPUT_FILE_PATH, 
                    e, "invalid input path {} ", path);
        }
        return pathUri;
    }
    
    private static TBrokerFD parseUUIDToFD(UUID uuid) {
        return new TBrokerFD(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }
    
    class FileSystemExpirationChecker implements Runnable {
        @Override
        public void run() {
            try {
                for (BrokerFileSystem fileSystem : cachedFileSystem.values()) {
                    if (fileSystem.isExpired(BrokerConfig.client_expire_seconds)) {
                        logger.info("file system " + fileSystem + " is expired, close and remove it");
                        fileSystem.getLock().lock();
                        try {
                            fileSystem.closeFileSystem();
                        } catch (Throwable t) {
                            logger.error("errors while close file system", t);
                        } finally {
                            cachedFileSystem.remove(fileSystem.getIdentity());
                            fileSystem.getLock().unlock();
                        }
                    }
                }
            } finally {
                FileSystemManager.this.handleManagementPool.schedule(this, 60, TimeUnit.SECONDS);
            }
        }
        
    }
}
