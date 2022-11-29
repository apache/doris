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

package org.apache.doris.common.util;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.backup.RemoteFile;
import org.apache.doris.backup.S3Storage;
import org.apache.doris.backup.Status;
import org.apache.doris.catalog.AuthType;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TBrokerCheckPathExistRequest;
import org.apache.doris.thrift.TBrokerCheckPathExistResponse;
import org.apache.doris.thrift.TBrokerCloseReaderRequest;
import org.apache.doris.thrift.TBrokerCloseWriterRequest;
import org.apache.doris.thrift.TBrokerDeletePathRequest;
import org.apache.doris.thrift.TBrokerFD;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TBrokerListPathRequest;
import org.apache.doris.thrift.TBrokerListResponse;
import org.apache.doris.thrift.TBrokerOpenMode;
import org.apache.doris.thrift.TBrokerOpenReaderRequest;
import org.apache.doris.thrift.TBrokerOpenReaderResponse;
import org.apache.doris.thrift.TBrokerOpenWriterRequest;
import org.apache.doris.thrift.TBrokerOpenWriterResponse;
import org.apache.doris.thrift.TBrokerOperationStatus;
import org.apache.doris.thrift.TBrokerOperationStatusCode;
import org.apache.doris.thrift.TBrokerPReadRequest;
import org.apache.doris.thrift.TBrokerPWriteRequest;
import org.apache.doris.thrift.TBrokerReadResponse;
import org.apache.doris.thrift.TBrokerRenamePathRequest;
import org.apache.doris.thrift.TBrokerVersion;
import org.apache.doris.thrift.THdfsConf;
import org.apache.doris.thrift.THdfsParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class BrokerUtil {
    private static final Logger LOG = LogManager.getLogger(BrokerUtil.class);

    private static final int READ_BUFFER_SIZE_B = 1024 * 1024;
    public static String HADOOP_FS_NAME = "fs.defaultFS";
    // simple or kerberos
    public static String HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";
    public static String HADOOP_USER_NAME = "hadoop.username";
    public static String HADOOP_KERBEROS_PRINCIPAL = "hadoop.kerberos.principal";
    public static String HADOOP_KERBEROS_KEYTAB = "hadoop.kerberos.keytab";
    public static String HADOOP_SHORT_CIRCUIT = "dfs.client.read.shortcircuit";
    public static String HADOOP_SOCKET_PATH = "dfs.domain.socket.path";

    public static THdfsParams generateHdfsParam(Map<String, String> properties) {
        THdfsParams tHdfsParams = new THdfsParams();
        tHdfsParams.setHdfsConf(new ArrayList<>());
        for (Map.Entry<String, String> property : properties.entrySet()) {
            if (property.getKey().equalsIgnoreCase(HADOOP_FS_NAME)) {
                tHdfsParams.setFsName(property.getValue());
            } else if (property.getKey().equalsIgnoreCase(HADOOP_USER_NAME)) {
                tHdfsParams.setUser(property.getValue());
            } else if (property.getKey().equalsIgnoreCase(HADOOP_KERBEROS_PRINCIPAL)) {
                tHdfsParams.setHdfsKerberosPrincipal(property.getValue());
            } else if (property.getKey().equalsIgnoreCase(HADOOP_KERBEROS_KEYTAB)) {
                tHdfsParams.setHdfsKerberosKeytab(property.getValue());
            } else {
                THdfsConf hdfsConf = new THdfsConf();
                hdfsConf.setKey(property.getKey());
                hdfsConf.setValue(property.getValue());
                tHdfsParams.hdfs_conf.add(hdfsConf);
            }
        }
        // `dfs.client.read.shortcircuit` and `dfs.domain.socket.path` should be both set to enable short circuit read.
        // We should disable short circuit read if they are not both set because it will cause performance down.
        if (!properties.containsKey(HADOOP_SHORT_CIRCUIT) || !properties.containsKey(HADOOP_SOCKET_PATH)) {
            tHdfsParams.addToHdfsConf(new THdfsConf(HADOOP_SHORT_CIRCUIT, "false"));
        }
        return tHdfsParams;
    }

    /**
     * Parse file status in path with broker, except directory
     * @param path
     * @param brokerDesc
     * @param fileStatuses: file path, size, isDir, isSplittable
     * @throws UserException if broker op failed
     */
    public static void parseFile(String path, BrokerDesc brokerDesc, List<TBrokerFileStatus> fileStatuses)
            throws UserException {
        if (brokerDesc.getStorageType() == StorageBackend.StorageType.BROKER) {
            TNetworkAddress address = getAddress(brokerDesc);
            TPaloBrokerService.Client client = borrowClient(address);
            boolean failed = true;
            try {
                TBrokerListPathRequest request = new TBrokerListPathRequest(
                        TBrokerVersion.VERSION_ONE, path, false, brokerDesc.getProperties());
                TBrokerListResponse tBrokerListResponse = null;
                try {
                    tBrokerListResponse = client.listPath(request);
                } catch (TException e) {
                    reopenClient(client);
                    tBrokerListResponse = client.listPath(request);
                }
                if (tBrokerListResponse.getOpStatus().getStatusCode() != TBrokerOperationStatusCode.OK) {
                    throw new UserException("Broker list path failed. path=" + path
                        + ",broker=" + address + ",msg=" + tBrokerListResponse.getOpStatus().getMessage());
                }
                failed = false;
                for (TBrokerFileStatus tBrokerFileStatus : tBrokerListResponse.getFiles()) {
                    if (tBrokerFileStatus.isDir) {
                        continue;
                    }
                    fileStatuses.add(tBrokerFileStatus);
                }
            } catch (TException e) {
                LOG.warn("Broker list path exception, path={}, address={}", path, address, e);
                throw new UserException("Broker list path exception. path=" + path + ", broker=" + address);
            } finally {
                returnClient(client, address, failed);
            }
        } else if (brokerDesc.getStorageType() == StorageBackend.StorageType.S3) {
            S3Storage s3 = new S3Storage(brokerDesc.getProperties());
            List<RemoteFile> rfiles = new ArrayList<>();
            try {
                Status st = s3.list(path, rfiles, false);
                if (!st.ok()) {
                    throw new UserException("S3 list path failed. path=" + path
                            + ",msg=" + st.getErrMsg());
                }
            } catch (Exception e) {
                LOG.warn("s3 list path exception, path={}", path, e);
                throw new UserException("s3 list path exception. path=" + path + ", err: " + e.getMessage());
            }
            for (RemoteFile r : rfiles) {
                if (r.isFile()) {
                    fileStatuses.add(new TBrokerFileStatus(r.getName(), !r.isFile(), r.getSize(), r.isFile()));
                }
            }
        } else if (brokerDesc.getStorageType() == StorageBackend.StorageType.HDFS) {
            if (!brokerDesc.getProperties().containsKey(HADOOP_FS_NAME)
                    || !brokerDesc.getProperties().containsKey(HADOOP_USER_NAME)) {
                throw new UserException(String.format(
                    "The properties of hdfs is invalid. %s and %s are needed", HADOOP_FS_NAME, HADOOP_USER_NAME));
            }
            String fsName = brokerDesc.getProperties().get(HADOOP_FS_NAME);
            String userName = brokerDesc.getProperties().get(HADOOP_USER_NAME);
            Configuration conf = new HdfsConfiguration();
            boolean isSecurityEnabled = false;
            for (Map.Entry<String, String> propEntry : brokerDesc.getProperties().entrySet()) {
                conf.set(propEntry.getKey(), propEntry.getValue());
                if (propEntry.getKey().equals(BrokerUtil.HADOOP_SECURITY_AUTHENTICATION)
                        && propEntry.getValue().equals(AuthType.KERBEROS.getDesc())) {
                    isSecurityEnabled = true;
                }
            }
            try {
                if (isSecurityEnabled) {
                    UserGroupInformation.setConfiguration(conf);
                    UserGroupInformation.loginUserFromKeytab(
                            brokerDesc.getProperties().get(BrokerUtil.HADOOP_KERBEROS_PRINCIPAL),
                            brokerDesc.getProperties().get(BrokerUtil.HADOOP_KERBEROS_KEYTAB));
                }
                FileSystem fs = FileSystem.get(new URI(fsName), conf, userName);
                FileStatus[] statusList = fs.globStatus(new Path(path));
                if (statusList == null) {
                    throw new UserException("failed to get files from path: " + path);
                }
                for (FileStatus status : statusList) {
                    if (status.isFile()) {
                        fileStatuses.add(new TBrokerFileStatus(status.getPath().toUri().getPath(),
                                status.isDirectory(), status.getLen(), status.isFile()));
                    }
                }
            } catch (IOException | InterruptedException | URISyntaxException e) {
                LOG.warn("hdfs check error: ", e);
                throw new UserException(e.getMessage());
            }
        }
    }

    public static String printBroker(String brokerName, TNetworkAddress address) {
        return brokerName + "[" + address.toString() + "]";
    }

    public static List<String> parseColumnsFromPath(String filePath, List<String> columnsFromPath)
            throws UserException {
        return parseColumnsFromPath(filePath, columnsFromPath, true);
    }

    public static List<String> parseColumnsFromPath(
            String filePath,
            List<String> columnsFromPath,
            boolean caseSensitive)
            throws UserException {
        if (columnsFromPath == null || columnsFromPath.isEmpty()) {
            return Collections.emptyList();
        }
        String[] strings = filePath.split("/");
        if (strings.length < 2) {
            throw new UserException("Fail to parse columnsFromPath, expected: "
                    + columnsFromPath + ", filePath: " + filePath);
        }
        String[] columns = new String[columnsFromPath.size()];
        int size = 0;
        for (int i = strings.length - 2; i >= 0; i--) {
            String str = strings[i];
            if (str != null && str.isEmpty()) {
                continue;
            }
            if (str == null || !str.contains("=")) {
                throw new UserException("Fail to parse columnsFromPath, expected: "
                        + columnsFromPath + ", filePath: " + filePath);
            }
            String[] pair = str.split("=", 2);
            if (pair.length != 2) {
                throw new UserException("Fail to parse columnsFromPath, expected: "
                        + columnsFromPath + ", filePath: " + filePath);
            }
            String parsedColumnName = caseSensitive ? pair[0] : pair[0].toLowerCase();
            int index = columnsFromPath.indexOf(parsedColumnName);
            if (index == -1) {
                continue;
            }
            columns[index] = pair[1];
            size++;
            if (size >= columnsFromPath.size()) {
                break;
            }
        }
        if (size != columnsFromPath.size()) {
            throw new UserException("Fail to parse columnsFromPath, expected: "
                    + columnsFromPath + ", filePath: " + filePath);
        }
        return Lists.newArrayList(columns);
    }

    /**
     * Read binary data from path with broker
     * @param path
     * @param brokerDesc
     * @return byte[]
     * @throws UserException if broker op failed or not only one file
     */
    public static byte[] readFile(String path, BrokerDesc brokerDesc, long maxReadLen) throws UserException {
        TNetworkAddress address = getAddress(brokerDesc);
        TPaloBrokerService.Client client = borrowClient(address);
        boolean failed = true;
        TBrokerFD fd = null;
        try {
            // get file size
            TBrokerListPathRequest request = new TBrokerListPathRequest(
                    TBrokerVersion.VERSION_ONE, path, false, brokerDesc.getProperties());
            TBrokerListResponse tBrokerListResponse = null;
            try {
                tBrokerListResponse = client.listPath(request);
            } catch (TException e) {
                reopenClient(client);
                tBrokerListResponse = client.listPath(request);
            }
            if (tBrokerListResponse.getOpStatus().getStatusCode() != TBrokerOperationStatusCode.OK) {
                throw new UserException("Broker list path failed. path=" + path + ", broker=" + address
                                                + ",msg=" + tBrokerListResponse.getOpStatus().getMessage());
            }
            List<TBrokerFileStatus> fileStatuses = tBrokerListResponse.getFiles();
            if (fileStatuses.size() != 1) {
                throw new UserException("Broker files num error. path=" + path + ", broker=" + address
                                                + ", files num: " + fileStatuses.size());
            }

            Preconditions.checkState(!fileStatuses.get(0).isIsDir());
            long fileSize = fileStatuses.get(0).getSize();

            // open reader
            String clientId = FrontendOptions.getLocalHostAddress() + ":" + Config.rpc_port;
            TBrokerOpenReaderRequest tOpenReaderRequest = new TBrokerOpenReaderRequest(
                    TBrokerVersion.VERSION_ONE, path, 0, clientId, brokerDesc.getProperties());
            TBrokerOpenReaderResponse tOpenReaderResponse = null;
            try {
                tOpenReaderResponse = client.openReader(tOpenReaderRequest);
            } catch (TException e) {
                reopenClient(client);
                tOpenReaderResponse = client.openReader(tOpenReaderRequest);
            }
            if (tOpenReaderResponse.getOpStatus().getStatusCode() != TBrokerOperationStatusCode.OK) {
                throw new UserException("Broker open reader failed. path=" + path + ", broker=" + address
                                                + ", msg=" + tOpenReaderResponse.getOpStatus().getMessage());
            }
            fd = tOpenReaderResponse.getFd();

            // read
            long readLen = fileSize;
            if (maxReadLen > 0 && maxReadLen < fileSize) {
                readLen = maxReadLen;
            }
            TBrokerPReadRequest tPReadRequest = new TBrokerPReadRequest(
                    TBrokerVersion.VERSION_ONE, fd, 0, readLen);
            TBrokerReadResponse tReadResponse = null;
            try {
                tReadResponse = client.pread(tPReadRequest);
            } catch (TException e) {
                reopenClient(client);
                tReadResponse = client.pread(tPReadRequest);
            }
            if (tReadResponse.getOpStatus().getStatusCode() != TBrokerOperationStatusCode.OK) {
                throw new UserException("Broker read failed. path=" + path + ", broker=" + address
                                                + ", msg=" + tReadResponse.getOpStatus().getMessage());
            }
            failed = false;
            return tReadResponse.getData();
        } catch (TException e) {
            String failMsg = "Broker read file exception. path=" + path + ", broker=" + address;
            LOG.warn(failMsg, e);
            throw new UserException(failMsg);
        } finally {
            // close reader
            if (fd != null) {
                failed = true;
                TBrokerCloseReaderRequest tCloseReaderRequest = new TBrokerCloseReaderRequest(
                        TBrokerVersion.VERSION_ONE, fd);
                TBrokerOperationStatus tOperationStatus = null;
                try {
                    tOperationStatus = client.closeReader(tCloseReaderRequest);
                } catch (TException e) {
                    reopenClient(client);
                    try {
                        tOperationStatus = client.closeReader(tCloseReaderRequest);
                    } catch (TException ex) {
                        LOG.warn("Broker close reader failed. path={}, address={}", path, address, ex);
                    }
                }
                if (tOperationStatus == null || tOperationStatus.getStatusCode() != TBrokerOperationStatusCode.OK) {
                    LOG.warn("Broker close reader failed. path={}, address={}, error={}", path, address,
                             tOperationStatus.getMessage());
                } else {
                    failed = false;
                }
            }

            // return client
            returnClient(client, address, failed);
        }
    }

    /**
     * Write binary data to destFilePath with broker
     * @param data
     * @param destFilePath
     * @param brokerDesc
     * @throws UserException if broker op failed
     */
    public static void writeFile(byte[] data, String destFilePath, BrokerDesc brokerDesc) throws UserException {
        BrokerWriter writer = new BrokerWriter(destFilePath, brokerDesc);
        try {
            writer.open();
            ByteBuffer byteBuffer = ByteBuffer.wrap(data);
            writer.write(byteBuffer, data.length);
        } finally {
            writer.close();
        }
    }

    /**
     * Write srcFilePath file to destFilePath with broker
     * @param srcFilePath
     * @param destFilePath
     * @param brokerDesc
     * @throws UserException if broker op failed
     */
    public static void writeFile(String srcFilePath, String destFilePath,
                                 BrokerDesc brokerDesc) throws UserException {
        FileInputStream fis = null;
        FileChannel channel = null;
        BrokerWriter writer = new BrokerWriter(destFilePath, brokerDesc);
        ByteBuffer byteBuffer = ByteBuffer.allocate(READ_BUFFER_SIZE_B);
        try {
            writer.open();
            fis = new FileInputStream(srcFilePath);
            channel = fis.getChannel();
            while (true) {
                int readSize = channel.read(byteBuffer);
                if (readSize == -1) {
                    break;
                }

                byteBuffer.flip();
                writer.write(byteBuffer, readSize);
                byteBuffer.clear();
            }
        } catch (IOException e) {
            String failMsg = "Read file exception. filePath=" + srcFilePath;
            LOG.warn(failMsg, e);
            throw new UserException(failMsg);
        } finally {
            // close broker file writer and local file input stream
            writer.close();
            try {
                if (channel != null) {
                    channel.close();
                }
                if (fis != null) {
                    fis.close();
                }
            } catch (IOException e) {
                LOG.warn("Close local file failed. srcPath={}", srcFilePath, e);
            }
        }
    }

    /**
     * Delete path with broker
     * @param path
     * @param brokerDesc
     * @throws UserException if broker op failed
     */
    public static void deletePath(String path, BrokerDesc brokerDesc) throws UserException {
        TNetworkAddress address = getAddress(brokerDesc);
        TPaloBrokerService.Client client = borrowClient(address);
        boolean failed = true;
        try {
            TBrokerDeletePathRequest tDeletePathRequest = new TBrokerDeletePathRequest(
                    TBrokerVersion.VERSION_ONE, path, brokerDesc.getProperties());
            TBrokerOperationStatus tOperationStatus = null;
            try {
                tOperationStatus = client.deletePath(tDeletePathRequest);
            } catch (TException e) {
                reopenClient(client);
                tOperationStatus = client.deletePath(tDeletePathRequest);
            }
            if (tOperationStatus.getStatusCode() != TBrokerOperationStatusCode.OK) {
                throw new UserException("Broker delete path failed. path=" + path + ", broker=" + address
                                                + ", msg=" + tOperationStatus.getMessage());
            }
            failed = false;
        } catch (TException e) {
            LOG.warn("Broker read path exception, path={}, address={}, exception={}", path, address, e);
            throw new UserException("Broker read path exception. path=" + path + ",broker=" + address);
        } finally {
            returnClient(client, address, failed);
        }
    }

    public static boolean checkPathExist(String remotePath, BrokerDesc brokerDesc) throws UserException {
        Pair<TPaloBrokerService.Client, TNetworkAddress> pair = getBrokerAddressAndClient(brokerDesc);
        TPaloBrokerService.Client client = pair.first;
        TNetworkAddress address = pair.second;
        boolean failed = true;
        try {
            TBrokerCheckPathExistRequest req = new TBrokerCheckPathExistRequest(TBrokerVersion.VERSION_ONE,
                    remotePath, brokerDesc.getProperties());
            TBrokerCheckPathExistResponse rep = client.checkPathExist(req);
            if (rep.getOpStatus().getStatusCode() != TBrokerOperationStatusCode.OK) {
                throw new UserException("Broker check path exist failed. path=" + remotePath + ", broker=" + address
                        + ", msg=" + rep.getOpStatus().getMessage());
            }
            failed = false;
            return rep.isPathExist;
        } catch (TException e) {
            LOG.warn("Broker check path exist failed, path={}, address={}, exception={}", remotePath, address, e);
            throw new UserException("Broker check path exist exception. path=" + remotePath + ",broker=" + address);
        } finally {
            returnClient(client, address, failed);
        }
    }

    public static void rename(String origFilePath, String destFilePath, BrokerDesc brokerDesc) throws UserException {
        Pair<TPaloBrokerService.Client, TNetworkAddress> pair = getBrokerAddressAndClient(brokerDesc);
        TPaloBrokerService.Client client = pair.first;
        TNetworkAddress address = pair.second;
        boolean failed = true;
        try {
            TBrokerRenamePathRequest req = new TBrokerRenamePathRequest(TBrokerVersion.VERSION_ONE, origFilePath,
                    destFilePath, brokerDesc.getProperties());
            TBrokerOperationStatus rep = client.renamePath(req);
            if (rep.getStatusCode() != TBrokerOperationStatusCode.OK) {
                throw new UserException("failed to rename " + origFilePath + " to " + destFilePath
                        + ", msg: " + rep.getMessage() + ", broker: " + address);
            }
            failed = false;
        } catch (TException e) {
            LOG.warn("Broker rename file failed, origin path={}, dest path={}, address={}, exception={}",
                    origFilePath, destFilePath, address, e);
            throw new UserException("Broker rename file exception. origin path=" + origFilePath
                    + ", dest path=" + destFilePath + ", broker=" + address);
        } finally {
            returnClient(client, address, failed);
        }
    }

    public static Pair<TPaloBrokerService.Client, TNetworkAddress> getBrokerAddressAndClient(BrokerDesc brokerDesc)
            throws UserException {
        Pair<TPaloBrokerService.Client, TNetworkAddress> pair = Pair.of(null, null);
        TNetworkAddress address = getAddress(brokerDesc);
        TPaloBrokerService.Client client = borrowClient(address);
        pair.first = client;
        pair.second = address;
        return pair;
    }

    public static TNetworkAddress getAddress(BrokerDesc brokerDesc) throws UserException {
        FsBroker broker = null;
        try {
            String localIP = FrontendOptions.getLocalHostAddress();
            broker = Env.getCurrentEnv().getBrokerMgr().getBroker(brokerDesc.getName(), localIP);
        } catch (AnalysisException e) {
            throw new UserException(e.getMessage());
        }
        return new TNetworkAddress(broker.ip, broker.port);
    }

    public static TPaloBrokerService.Client borrowClient(TNetworkAddress address) throws UserException {
        TPaloBrokerService.Client client = null;
        try {
            client = ClientPool.brokerPool.borrowObject(address);
        } catch (Exception e) {
            try {
                client = ClientPool.brokerPool.borrowObject(address);
            } catch (Exception e1) {
                throw new UserException("Create connection to broker(" + address + ") failed.");
            }
        }
        return client;
    }

    private static void returnClient(TPaloBrokerService.Client client, TNetworkAddress address, boolean failed) {
        if (failed) {
            ClientPool.brokerPool.invalidateObject(address, client);
        } else {
            ClientPool.brokerPool.returnObject(address, client);
        }
    }

    private static void reopenClient(TPaloBrokerService.Client client) {
        ClientPool.brokerPool.reopen(client);
    }

    private static class BrokerWriter {
        private String brokerFilePath;
        private BrokerDesc brokerDesc;
        private TPaloBrokerService.Client client;
        private TNetworkAddress address;
        private TBrokerFD fd;
        private long currentOffset;
        private boolean isReady;
        private boolean failed;

        public BrokerWriter(String brokerFilePath, BrokerDesc brokerDesc) {
            this.brokerFilePath = brokerFilePath;
            this.brokerDesc = brokerDesc;
            this.isReady = false;
            this.failed = true;
        }

        public void open() throws UserException {
            failed = true;
            address = BrokerUtil.getAddress(brokerDesc);
            client = BrokerUtil.borrowClient(address);
            try {
                String clientId = FrontendOptions.getLocalHostAddress() + ":" + Config.rpc_port;
                TBrokerOpenWriterRequest tOpenWriterRequest = new TBrokerOpenWriterRequest(
                        TBrokerVersion.VERSION_ONE, brokerFilePath, TBrokerOpenMode.APPEND,
                        clientId, brokerDesc.getProperties());
                TBrokerOpenWriterResponse tOpenWriterResponse = null;
                try {
                    tOpenWriterResponse = client.openWriter(tOpenWriterRequest);
                } catch (TException e) {
                    reopenClient(client);
                    tOpenWriterResponse = client.openWriter(tOpenWriterRequest);
                }
                if (tOpenWriterResponse.getOpStatus().getStatusCode() != TBrokerOperationStatusCode.OK) {
                    throw new UserException("Broker open writer failed. destPath=" + brokerFilePath
                                                    + ", broker=" + address
                                                    + ", msg=" + tOpenWriterResponse.getOpStatus().getMessage());
                }
                failed = false;
                fd = tOpenWriterResponse.getFd();
                currentOffset = 0L;
                isReady = true;
            } catch (TException e) {
                String failMsg = "Broker open writer exception. filePath=" + brokerFilePath + ", broker=" + address;
                LOG.warn(failMsg, e);
                throw new UserException(failMsg);
            }
        }

        public void write(ByteBuffer byteBuffer, long bufferSize) throws UserException {
            if (!isReady) {
                throw new UserException("Broker writer is not ready. filePath="
                        + brokerFilePath + ", broker=" + address);
            }

            failed = true;
            TBrokerOperationStatus tOperationStatus = null;
            TBrokerPWriteRequest tPWriteRequest = new TBrokerPWriteRequest(
                    TBrokerVersion.VERSION_ONE, fd, currentOffset, byteBuffer);
            try {
                try {
                    tOperationStatus = client.pwrite(tPWriteRequest);
                } catch (TException e) {
                    reopenClient(client);
                    tOperationStatus = client.pwrite(tPWriteRequest);
                }
                if (tOperationStatus.getStatusCode() != TBrokerOperationStatusCode.OK) {
                    throw new UserException("Broker write failed. filePath=" + brokerFilePath + ", broker=" + address
                                                    + ", msg=" + tOperationStatus.getMessage());
                }
                failed = false;
                currentOffset += bufferSize;
            } catch (TException e) {
                String failMsg = "Broker write exception. filePath=" + brokerFilePath + ", broker=" + address;
                LOG.warn(failMsg, e);
                throw new UserException(failMsg);
            }
        }

        public void close() {
            // close broker writer
            failed = true;
            TBrokerOperationStatus tOperationStatus = null;
            if (fd != null) {
                TBrokerCloseWriterRequest tCloseWriterRequest = new TBrokerCloseWriterRequest(
                        TBrokerVersion.VERSION_ONE, fd);
                try {
                    tOperationStatus = client.closeWriter(tCloseWriterRequest);
                } catch (TException e) {
                    reopenClient(client);
                    try {
                        tOperationStatus = client.closeWriter(tCloseWriterRequest);
                    } catch (TException ex) {
                        LOG.warn("Broker close writer failed. filePath={}, address={}", brokerFilePath, address, ex);
                    }
                }
                if (tOperationStatus == null || tOperationStatus.getStatusCode() != TBrokerOperationStatusCode.OK) {
                    LOG.warn("Broker close writer failed. filePath={}, address={}, error={}", brokerFilePath,
                             address, tOperationStatus.getMessage());
                } else {
                    failed = false;
                }
            }

            // return client
            returnClient(client, address, failed);
            isReady = false;
        }

    }
}
