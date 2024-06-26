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
import org.apache.doris.backup.Status;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.fs.remote.RemoteFile;
import org.apache.doris.fs.remote.RemoteFileSystem;
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
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BrokerUtil {
    private static final Logger LOG = LogManager.getLogger(BrokerUtil.class);

    private static final int READ_BUFFER_SIZE_B = 1024 * 1024;

    /**
     * Parse file status in path with broker, except directory
     * @param path
     * @param brokerDesc
     * @param fileStatuses: file path, size, isDir, isSplittable
     * @throws UserException if broker op failed
     */
    public static void parseFile(String path, BrokerDesc brokerDesc, List<TBrokerFileStatus> fileStatuses)
            throws UserException {
        List<RemoteFile> rfiles = new ArrayList<>();
        try {
            RemoteFileSystem fileSystem = FileSystemFactory.get(
                    brokerDesc.getName(), brokerDesc.getStorageType(), brokerDesc.getProperties());
            Status st = fileSystem.globList(path, rfiles, false);
            if (!st.ok()) {
                throw new UserException(st.getErrMsg());
            }
        } catch (Exception e) {
            LOG.warn("{} list path exception, path={}", brokerDesc.getName(), path, e);
            throw new UserException(brokerDesc.getName() +  " list path exception. path="
                    + path + ", err: " + e.getMessage());
        }
        for (RemoteFile r : rfiles) {
            if (r.isFile()) {
                TBrokerFileStatus status = new TBrokerFileStatus(r.getName(), !r.isFile(), r.getSize(), r.isFile());
                status.setBlockSize(r.getBlockSize());
                status.setModificationTime(r.getModificationTime());
                fileStatuses.add(status);
            }
        }
    }

    public static String printBroker(String brokerName, TNetworkAddress address) {
        return brokerName + "[" + address.toString() + "]";
    }

    public static List<String> parseColumnsFromPath(String filePath, List<String> columnsFromPath)
            throws UserException {
        return parseColumnsFromPath(filePath, columnsFromPath, true, false);
    }

    public static List<String> parseColumnsFromPath(
            String filePath,
            List<String> columnsFromPath,
            boolean caseSensitive,
            boolean isACID)
            throws UserException {
        if (columnsFromPath == null || columnsFromPath.isEmpty()) {
            return Collections.emptyList();
        }
        // if it is ACID, the path count is 3. The hdfs path is hdfs://xxx/table_name/par=xxx/delta(or base)_xxx/.
        int pathCount = isACID ? 3 : 2;
        if (!caseSensitive) {
            for (int i = 0; i < columnsFromPath.size(); i++) {
                String path = columnsFromPath.remove(i);
                columnsFromPath.add(i, path.toLowerCase());
            }
        }
        String[] strings = filePath.split("/");
        if (strings.length < 2) {
            throw new UserException("Fail to parse columnsFromPath, expected: "
                    + columnsFromPath + ", filePath: " + filePath);
        }
        String[] columns = new String[columnsFromPath.size()];
        int size = 0;
        boolean skipOnce = true;
        for (int i = strings.length - pathCount; i >= 0; i--) {
            String str = strings[i];
            if (str != null && str.isEmpty()) {
                continue;
            }
            if (str == null || !str.contains("=")) {
                if (!isACID && skipOnce) {
                    skipOnce = false;
                    continue;
                }
                throw new UserException("Fail to parse columnsFromPath, expected: "
                        + columnsFromPath + ", filePath: " + filePath);
            }
            skipOnce = false;
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
            columns[index] = HiveMetaStoreCache.HIVE_DEFAULT_PARTITION.equals(pair[1])
                ? FeConstants.null_string : pair[1];
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
            String clientId = NetUtils
                    .getHostPortInAccessibleFormat(FrontendOptions.getLocalHostAddress(), Config.rpc_port);
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
                if (tOperationStatus.getStatusCode() != TBrokerOperationStatusCode.OK) {
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
        return new TNetworkAddress(broker.host, broker.port);
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
                String clientId = NetUtils
                        .getHostPortInAccessibleFormat(FrontendOptions.getLocalHostAddress(), Config.rpc_port);
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
                if (tOperationStatus == null) {
                    LOG.warn("Broker close reader failed. fd={}, address={}", fd.toString(), address);
                } else if (tOperationStatus.getStatusCode() != TBrokerOperationStatusCode.OK) {
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
