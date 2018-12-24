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

package org.apache.doris.broker.hdfs;

import org.apache.doris.thrift.TBrokerFD;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TBrokerOperationStatusCode;

import com.google.common.base.Strings;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FileSystemManager {

    private static Logger logger = Logger
            .getLogger(FileSystemManager.class.getName());
    private static final String HDFS_SCHEME = "hdfs://";
    private static final String HDFS_UGI_CONF = "hadoop.job.ugi";
    private static final String USER_NAME_KEY = "username";
    private static final String PASSWORD_KEY = "password";
    private static final String AUTHENTICATION_SIMPLE = "simple";
    private static final String AUTHENTICATION_KERBEROS = "kerberos";
    private static final String KERBEROS_PRINCIPAL = "kerberos_principal";
    private static final String KERBEROS_KEYTAB = "kerberos_keytab";
    private static final String KERBEROS_KEYTAB_CONTENT = "kerberos_keytab_content";

    // arguments for ha hdfs
    private static final String DFS_NAMESERVICES_KEY = "dfs.nameservices";
    private static final String DFS_HA_NAMENODES_PREFIX = "dfs.ha.namenodes.";
    private static final String DFS_HA_NAMENODE_RPC_ADDRESS_PREFIX = "dfs.namenode.rpc-address.";
    private static final String DFS_CLIENT_FAILOVER_PROXY_PROVIDER_PREFIX =
            "dfs.client.failover.proxy.provider.";
    private static final String DEFAULT_DFS_CLIENT_FAILOVER_PROXY_PROVIDER =
            "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider";
    private static final String FS_DEFAULTFS_KEY = "fs.defaultFS";
    
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

    private static String preparePrincipal(String originalPrincipal) throws UnknownHostException {
        String finalPrincipal = originalPrincipal;
        String[] components = originalPrincipal.split("[/@]");
        if (components != null && components.length == 3) {
            if (components[1].equals("_HOST")) {
                // Convert hostname(fqdn) to lower case according to SecurityUtil.getServerPrincipal
                finalPrincipal = components[0] + "/" +
                        StringUtils.toLowerCase(InetAddress.getLocalHost().getCanonicalHostName())
                        + "@" + components[2];
            } else if (components[1].equals("_IP")) {
                finalPrincipal = components[0] + "/" +
                        InetAddress.getByName(InetAddress.getLocalHost().getCanonicalHostName()).getHostAddress()
                        + "@" + components[2];
            }
        }

        return finalPrincipal;
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
        String host = HDFS_SCHEME + pathUri.getAuthority();
        if (Strings.isNullOrEmpty(pathUri.getAuthority())) {
            if (properties.containsKey(FS_DEFAULTFS_KEY)) {
                host = properties.get(FS_DEFAULTFS_KEY);
                logger.info("no schema and authority in path. use fs.defaultFs");
            } else {
                logger.warn("invalid hdfs path. authority is null,path:" + path);
                throw  new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                        "invalid hdfs path. authority is null");
            }
        }
        String username = properties.containsKey(USER_NAME_KEY) ? properties.get(USER_NAME_KEY) : "";
        String password = properties.containsKey(PASSWORD_KEY) ? properties.get(PASSWORD_KEY) : "";
        String dfsNameServices =
                properties.containsKey(DFS_NAMESERVICES_KEY) ? properties.get(DFS_NAMESERVICES_KEY) : "";
        String authentication = AUTHENTICATION_SIMPLE;
        if (properties.containsKey(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION)) {
            authentication = properties.get(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION);
            if (Strings.isNullOrEmpty(authentication)
                    || (!authentication.equals(AUTHENTICATION_SIMPLE)
                    && !authentication.equals(AUTHENTICATION_KERBEROS))) {
                logger.warn("invalid authentication:" + authentication);
                throw  new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                        "invalid authentication:" + authentication);
            }
        }

        String hdfsUgi = username + "," + password;
        FileSystemIdentity fileSystemIdentity = null;
        BrokerFileSystem fileSystem = null;
        if (authentication.equals(AUTHENTICATION_SIMPLE)) {
            fileSystemIdentity = new FileSystemIdentity(host, hdfsUgi);
        } else {
            // for kerberos, use host + principal + keytab as filesystemindentity
            String kerberosContent = "";
            if (properties.containsKey(KERBEROS_KEYTAB)) {
                kerberosContent = properties.get(KERBEROS_KEYTAB);
            } else if (properties.containsKey(KERBEROS_KEYTAB_CONTENT)) {
                kerberosContent = properties.get(KERBEROS_KEYTAB_CONTENT);
            } else {
                throw  new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                        "keytab is required for kerberos authentication");
            }
            if (!properties.containsKey(KERBEROS_PRINCIPAL)) {
                throw  new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                        "principal is required for kerberos authentication");
            } else {
                kerberosContent = kerberosContent + properties.get(KERBEROS_PRINCIPAL);
            }
            try {
                MessageDigest digest = MessageDigest.getInstance("md5");
                byte[] result = digest.digest(kerberosContent.getBytes());
                String kerberosUgi = new String(result);
                fileSystemIdentity = new FileSystemIdentity(host, kerberosUgi);
            } catch (NoSuchAlgorithmException e) {
                throw  new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                        e.getMessage());
            }
        }
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
                logger.info("could not find file system for path " + path + " create a new one");
                // create a new filesystem
                Configuration conf = new Configuration();
                // TODO get this param from properties
                // conf.set("dfs.replication", "2");
                String tmpFilePath = null;
                if (authentication.equals(AUTHENTICATION_SIMPLE)) {
                    conf.set(HDFS_UGI_CONF, hdfsUgi);
                } else if (authentication.equals(AUTHENTICATION_KERBEROS)){
                    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
                            AUTHENTICATION_KERBEROS);

                    String principal = preparePrincipal(properties.get(KERBEROS_PRINCIPAL));
                    String keytab = "";
                    if (properties.containsKey(KERBEROS_KEYTAB)) {
                        keytab = properties.get(KERBEROS_KEYTAB);
                    } else if (properties.containsKey(KERBEROS_KEYTAB_CONTENT)) {
                        // pass kerberos keytab content use base64 encoding
                        // so decode it and write it to tmp path under /tmp
                        // because ugi api only accept a local path as argument
                        String keytab_content = properties.get(KERBEROS_KEYTAB_CONTENT);
                        byte[] base64decodedBytes = Base64.getDecoder().decode(keytab_content);
                        long currentTime = System.currentTimeMillis();
                        Random random = new Random(currentTime);
                        int randNumber = random.nextInt(10000);
                        tmpFilePath = "/tmp/." + Long.toString(currentTime) + "_" + Integer.toString(randNumber);
                        FileOutputStream fileOutputStream = new FileOutputStream(tmpFilePath);
                        fileOutputStream.write(base64decodedBytes);
                        fileOutputStream.close();
                        keytab = tmpFilePath;
                    } else {
                        throw  new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                                "keytab is required for kerberos authentication");
                    }
                    UserGroupInformation.setConfiguration(conf);
                    UserGroupInformation.loginUserFromKeytab(principal, keytab);
                    if (properties.containsKey(KERBEROS_KEYTAB_CONTENT)) {
                        try {
                            File file = new File(tmpFilePath);
                            if(!file.delete()){
                                logger.warn("delete tmp file:" +  tmpFilePath + " failed");
                            }
                        } catch (Exception e) {
                            throw new  BrokerException(TBrokerOperationStatusCode.FILE_NOT_FOUND,
                                    e.getMessage());
                        }
                    }
                } else {
                    throw  new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                            "invalid authentication.");
                }
                if (!Strings.isNullOrEmpty(dfsNameServices)) {
                    // ha hdfs arguments
                    final String dfsHaNameNodesKey = DFS_HA_NAMENODES_PREFIX + dfsNameServices;
                    if (!properties.containsKey(dfsHaNameNodesKey)) {
                        throw  new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                                "load request missed necessary arguments for ha mode");
                    }
                    String dfsHaNameNodes =  properties.get(dfsHaNameNodesKey);
                    conf.set(DFS_NAMESERVICES_KEY, dfsNameServices);
                    conf.set(dfsHaNameNodesKey, dfsHaNameNodes);
                    String[] nameNodes = dfsHaNameNodes.split(",");
                    if (nameNodes == null) {
                        throw  new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                                "invalid " + dfsHaNameNodesKey + " configuration");
                    } else {
                        for (String nameNode : nameNodes) {
                            String nameNodeRpcAddress =
                                    DFS_HA_NAMENODE_RPC_ADDRESS_PREFIX + dfsNameServices + "." + nameNode;
                            if (!properties.containsKey(nameNodeRpcAddress)) {
                                throw  new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                                        "missed " + nameNodeRpcAddress + " configuration");
                            } else {
                                conf.set(nameNodeRpcAddress, properties.get(nameNodeRpcAddress));
                            }
                        }
                    }

                    final String dfsClientFailoverProxyProviderKey =
                            DFS_CLIENT_FAILOVER_PROXY_PROVIDER_PREFIX + dfsNameServices;
                    if (properties.containsKey(dfsClientFailoverProxyProviderKey)) {
                        conf.set(dfsClientFailoverProxyProviderKey,
                                properties.get(dfsClientFailoverProxyProviderKey));
                    } else {
                        conf.set(dfsClientFailoverProxyProviderKey,
                                DEFAULT_DFS_CLIENT_FAILOVER_PROXY_PROVIDER);
                    }
                    if (properties.containsKey(FS_DEFAULTFS_KEY)) {
                        conf.set(FS_DEFAULTFS_KEY, properties.get(FS_DEFAULTFS_KEY));
                    }
                }

                FileSystem dfsFileSystem = FileSystem.get(URI.create(host), conf);
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
    
    public List<TBrokerFileStatus> listPath(String path, boolean fileNameOnly, Map<String, String> properties) {
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
                if (fileNameOnly) {
                    // return like this: file.txt
                    brokerFileStatus.setPath(fileStatus.getPath().getName());
                } else {
                    // return like this: //path/to/your/file.txt
                    brokerFileStatus.setPath(fileStatus.getPath().toString());
                }
                resultFileStatus.add(brokerFileStatus);
            }
        } catch (FileNotFoundException e) {
            logger.info("file not found: " + e.getMessage());
            throw new BrokerException(TBrokerOperationStatusCode.FILE_NOT_FOUND,
                    e, "file not found");
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
            pathUri = pathUri.normalize();
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
