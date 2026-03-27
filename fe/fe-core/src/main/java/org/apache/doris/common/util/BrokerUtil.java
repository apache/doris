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
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.hive.HiveExternalMetaCache;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.fs.remote.RemoteFile;
import org.apache.doris.fs.remote.RemoteFileSystem;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TBrokerCloseReaderRequest;
import org.apache.doris.thrift.TBrokerFD;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TBrokerListPathRequest;
import org.apache.doris.thrift.TBrokerListResponse;
import org.apache.doris.thrift.TBrokerOpenReaderRequest;
import org.apache.doris.thrift.TBrokerOpenReaderResponse;
import org.apache.doris.thrift.TBrokerOperationStatus;
import org.apache.doris.thrift.TBrokerOperationStatusCode;
import org.apache.doris.thrift.TBrokerPReadRequest;
import org.apache.doris.thrift.TBrokerReadResponse;
import org.apache.doris.thrift.TBrokerVersion;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BrokerUtil {
    private static final Logger LOG = LogManager.getLogger(BrokerUtil.class);

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
        try (RemoteFileSystem fileSystem = FileSystemFactory.get(brokerDesc.getStorageProperties())) {
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

    public static void deleteDirectoryWithFileSystem(String path, BrokerDesc brokerDesc) throws UserException {
        try (RemoteFileSystem fileSystem = FileSystemFactory.get(brokerDesc.getStorageProperties())) {
            Status st = fileSystem.deleteDirectory(path);
            if (!st.ok()) {
                throw new UserException(brokerDesc.getName() +  " delete directory exception. path="
                        + path + ", err: " + st.getErrMsg());
            }
        } catch (Exception e) {
            LOG.warn("{} delete directory exception, path={}", brokerDesc.getName(), path, e);
            throw new UserException(brokerDesc.getName() +  " delete directory exception. path="
                    + path + ", err: " + e.getMessage());
        }
    }

    public static void deleteParentDirectoryWithFileSystem(String path, BrokerDesc brokerDesc) throws UserException {
        deleteDirectoryWithFileSystem(extractParentDirectory(path), brokerDesc);
    }

    public static String extractParentDirectory(String path) {
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash >= 0) {
            return path.substring(0, lastSlash + 1);
        }
        return path;
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
            columns[index] = HiveExternalMetaCache.HIVE_DEFAULT_PARTITION.equals(pair[1])
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
                    TBrokerVersion.VERSION_ONE, path, false, brokerDesc.getBackendConfigProperties());
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
                    TBrokerVersion.VERSION_ONE, path, 0, clientId, brokerDesc.getBackendConfigProperties());
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
}
