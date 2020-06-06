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

package org.apache.doris.backup;

import org.apache.doris.backup.Status.ErrCode;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.BrokerUtil;
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
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class BlobStorage implements Writable {
    private static final Logger LOG = LogManager.getLogger(BlobStorage.class);

    private String brokerName;
    private Map<String, String> properties = Maps.newHashMap();

    private BlobStorage() {
        // for persist
    }

    public BlobStorage(String brokerName, Map<String, String> properties) {
        this.brokerName = brokerName;
        this.properties = properties;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public Status downloadWithFileSize(String remoteFilePath, String localFilePath, long fileSize) {
        LOG.debug("download from {} to {}, file size: {}.",
                  remoteFilePath, localFilePath, fileSize);

        long start = System.currentTimeMillis();

        // 1. get a proper broker
        Pair<TPaloBrokerService.Client, TNetworkAddress> pair = new Pair<TPaloBrokerService.Client, TNetworkAddress>(
                null, null);
        Status st = getBroker(pair);
        if (!st.ok()) {
            return st;
        }
        TPaloBrokerService.Client client = pair.first;
        TNetworkAddress address = pair.second;

        // 2. open file reader with broker
        TBrokerFD fd = null;
        try {
            TBrokerOpenReaderRequest req = new TBrokerOpenReaderRequest(TBrokerVersion.VERSION_ONE, remoteFilePath,
                    0, clientId(), properties);
            TBrokerOpenReaderResponse rep = client.openReader(req);
            TBrokerOperationStatus opst = rep.getOpStatus();
            if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                return new Status(ErrCode.COMMON_ERROR,
                        "failed to open reader on broker " + BrokerUtil.printBroker(brokerName, address)
                                + " for file: " + remoteFilePath + ". msg: " + opst.getMessage());
            }

            fd = rep.getFd();
            LOG.info("finished to open reader. fd: {}. download {} to {}.",
                     fd, remoteFilePath, localFilePath);
        } catch (TException e) {
            return new Status(ErrCode.COMMON_ERROR,
                    "failed to open reader on broker " + BrokerUtil.printBroker(brokerName, address)
                            + " for file: " + remoteFilePath + ". msg: " + e.getMessage());
        }
        Preconditions.checkNotNull(fd);

        // 3. delete local file if exist
        File localFile = new File(localFilePath);
        if (localFile.exists()) {
            try {
                Files.walk(Paths.get(localFilePath),
                           FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            } catch (IOException e) {
                return new Status(ErrCode.COMMON_ERROR, "failed to delete exist local file: " + localFilePath);
            }
        }

        // 4. create local file
        Status status = Status.OK;
        try {
            if (!localFile.createNewFile()) {
                return new Status(ErrCode.COMMON_ERROR, "failed to create local file: " + localFilePath);
            }
        } catch (IOException e) {
            return new Status(ErrCode.COMMON_ERROR, "failed to create local file: "
                    + localFilePath + ", msg: " + e.getMessage());
        }

        // 5. read remote file with broker and write to local
        String lastErrMsg = null;
        try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(localFile))) {
            final long bufSize = 1024 * 1024; // 1MB
            long leftSize = fileSize;
            long readOffset = 0;
            while (leftSize > 0) {
                long readLen = leftSize > bufSize ? bufSize : leftSize;
                TBrokerReadResponse rep = null;
                // We only retry if we encounter a timeout thrift exception.
                int tryTimes = 0;
                while (tryTimes < 3) {
                    try {
                        TBrokerPReadRequest req = new TBrokerPReadRequest(TBrokerVersion.VERSION_ONE,
                                fd, readOffset, readLen);
                        rep = client.pread(req);
                        if (rep.getOpStatus().getStatusCode() != TBrokerOperationStatusCode.OK) {
                            // pread return failure.
                            lastErrMsg = String.format("failed to read via broker %s. "
                                    + "current read offset: %d, read length: %d,"
                                    + " file size: %d, file: %s, err code: %d, msg: %s",
                                    BrokerUtil.printBroker(brokerName, address),
                                    readOffset, readLen, fileSize,
                                    remoteFilePath, rep.getOpStatus().getStatusCode(), 
                                    rep.getOpStatus().getMessage());
                            LOG.warn(lastErrMsg);
                            status = new Status(ErrCode.COMMON_ERROR, lastErrMsg);
                        }
                        LOG.debug("download. readLen: {}, read data len: {}, left size:{}. total size: {}",
                                  readLen, rep.getData().length, leftSize, fileSize);
                        break;
                    } catch (TTransportException e) {
                        if (e.getType() == TTransportException.TIMED_OUT) {
                            // we only retry when we encounter timeout exception.
                            lastErrMsg = String.format("failed to read via broker %s. "
                                    + "current read offset: %d, read length: %d,"
                                    + " file size: %d, file: %s, timeout.",
                                    BrokerUtil.printBroker(brokerName, address),
                                    readOffset, readLen, fileSize,
                                    remoteFilePath);
                            tryTimes++;
                            continue;
                        }

                        lastErrMsg = String.format("failed to read via broker %s. "
                                + "current read offset: %d, read length: %d,"
                                + " file size: %d, file: %s. msg: %s",
                                BrokerUtil.printBroker(brokerName, address),
                                readOffset, readLen, fileSize,
                                remoteFilePath, e.getMessage());
                        LOG.warn(lastErrMsg);
                        status = new Status(ErrCode.COMMON_ERROR, lastErrMsg);
                        break;
                    } catch (TException e) {
                        lastErrMsg = String.format("failed to read via broker %s. "
                                + "current read offset: %d, read length: %d,"
                                + " file size: %d, file: %s. msg: %s",
                                BrokerUtil.printBroker(brokerName, address),
                                readOffset, readLen, fileSize,
                                remoteFilePath, e.getMessage());
                        LOG.warn(lastErrMsg);
                        status = new Status(ErrCode.COMMON_ERROR, lastErrMsg);
                        break;
                    }
                } // end of retry loop

                if (status.ok() && tryTimes < 3) {
                    // read succeed, write to local file
                    Preconditions.checkNotNull(rep);
                    // NOTICE(cmy): Sometimes the actual read length does not equal to the expected read length,
                    // even if the broker's read buffer size is large enough.
                    // I don't know why, but have to adapt to it.
                    if (rep.getData().length != readLen) {
                        LOG.warn("the actual read length does not equal to "
                                + "the expected read length: {} vs. {}, file: {}, broker: {}",
                                 rep.getData().length, readLen, remoteFilePath,
                                BrokerUtil.printBroker(brokerName, address));
                    }

                    out.write(rep.getData());
                    readOffset += rep.getData().length;
                    leftSize -= rep.getData().length;
                } else {
                    status = new Status(ErrCode.COMMON_ERROR, lastErrMsg);
                    break;
                }
            } // end of reading remote file
        } catch (IOException e) {
            return new Status(ErrCode.COMMON_ERROR, "Got exception: " + e.getMessage() + ", broker: " +
                    BrokerUtil.printBroker(brokerName, address))  ;
        } finally {
            // close broker reader
            Status closeStatus = closeReader(client, address, fd);
            if (!closeStatus.ok()) {
                LOG.warn(closeStatus.getErrMsg());
                if (status.ok()) {
                    // we return close write error only if no other error has been encountered.
                    status = closeStatus;
                }
                ClientPool.brokerPool.invalidateObject(address, client);
            } else {
                ClientPool.brokerPool.returnObject(address, client);
            }
        }

        LOG.info("finished to download from {} to {} with size: {}. cost {} ms", remoteFilePath, localFilePath,
                 fileSize, (System.currentTimeMillis() - start));
        return status;
    }

    // directly upload the content to remote file
    public Status directUpload(String content, String remoteFile) {
        Status status = Status.OK;

        // 1. get a proper broker
        Pair<TPaloBrokerService.Client, TNetworkAddress> pair = new Pair<TPaloBrokerService.Client, TNetworkAddress>(
                null, null);
        status = getBroker(pair);
        if (!status.ok()) {
            return status;
        }
        TPaloBrokerService.Client client = pair.first;
        TNetworkAddress address = pair.second;

        TBrokerFD fd = new TBrokerFD();
        try {
            // 2. open file writer with broker
            status = openWriter(client, address, remoteFile, fd);
            if (!status.ok()) {
                return status;
            }

            // 3. write content
            try {
                ByteBuffer bb = ByteBuffer.wrap(content.getBytes("UTF-8"));
                TBrokerPWriteRequest req = new TBrokerPWriteRequest(TBrokerVersion.VERSION_ONE, fd, 0, bb);
                TBrokerOperationStatus opst = client.pwrite(req);
                if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                    // pwrite return failure.
                    status = new Status(ErrCode.COMMON_ERROR, "write failed: " + opst.getMessage()
                            + ", broker: " + BrokerUtil.printBroker(brokerName, address));
                }
            } catch (TException e) {
                status = new Status(ErrCode.BAD_CONNECTION, "write exception: " + e.getMessage()
                        + ", broker: " + BrokerUtil.printBroker(brokerName, address));
            } catch (UnsupportedEncodingException e) {
                status = new Status(ErrCode.COMMON_ERROR, "unsupported encoding: " + e.getMessage());
            }
        } finally {
            Status closeStatus = closeWriter(client, address, fd);
            if (closeStatus.getErrCode() == ErrCode.BAD_CONNECTION || status.getErrCode() == ErrCode.BAD_CONNECTION) {
                ClientPool.brokerPool.invalidateObject(address, client);
            } else {
                ClientPool.brokerPool.returnObject(address, client);
            }
        }

        return status;
    }

    public Status upload(String localPath, String remotePath) {
        long start = System.currentTimeMillis();

        Status status = Status.OK;

        // 1. get a proper broker
        Pair<TPaloBrokerService.Client, TNetworkAddress> pair = new Pair<TPaloBrokerService.Client, TNetworkAddress>(null, null);
        status = getBroker(pair);
        if (!status.ok()) {
            return status;
        }
        TPaloBrokerService.Client client = pair.first;
        TNetworkAddress address = pair.second;

        // 2. open file write with broker
        TBrokerFD fd = new TBrokerFD();
        status = openWriter(client, address, remotePath, fd);
        if (!status.ok()) {
            return status;
        }

        // 3. read local file and write to remote with broker
        File localFile = new File(localPath);
        long fileLength = localFile.length();
        byte[] readBuf = new byte[1024];
        try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(localFile))) {
            // save the last err msg
            String lastErrMsg = null;
            // save the current write offset of remote file
            long writeOffset = 0;
            // read local file, 1MB at a time
            int bytesRead = 0;
            while ((bytesRead = in.read(readBuf)) != -1) {
                ByteBuffer bb = ByteBuffer.wrap(readBuf, 0, bytesRead);
                
                // We only retry if we encounter a timeout thrift exception.
                int tryTimes = 0;
                while (tryTimes < 3) {
                    try {
                        TBrokerPWriteRequest req = new TBrokerPWriteRequest(TBrokerVersion.VERSION_ONE, fd, writeOffset, bb);
                        TBrokerOperationStatus opst = client.pwrite(req);
                        if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                            // pwrite return failure.
                            lastErrMsg = String.format("failed to write via broker %s. "
                                    + "current write offset: %d, write length: %d,"
                                    + " file length: %d, file: %s, err code: %d, msg: %s",
                                    BrokerUtil.printBroker(brokerName, address),
                                    writeOffset, bytesRead, fileLength,
                                    remotePath, opst.getStatusCode(), opst.getMessage());
                            LOG.warn(lastErrMsg);
                            status = new Status(ErrCode.COMMON_ERROR, lastErrMsg);
                        }
                        break;
                    } catch (TTransportException e) {
                        if (e.getType() == TTransportException.TIMED_OUT) {
                            // we only retry when we encounter timeout exception.
                            lastErrMsg = String.format("failed to write via broker %s. "
                                    + "current write offset: %d, write length: %d,"
                                    + " file length: %d, file: %s. timeout",
                                    BrokerUtil.printBroker(brokerName, address),
                                    writeOffset, bytesRead, fileLength,
                                    remotePath);
                            tryTimes++;
                            continue;
                        }
                        
                        lastErrMsg = String.format("failed to write via broker %s. "
                                + "current write offset: %d, write length: %d,"
                                + " file length: %d, file: %s. encounter TTransportException: %s",
                                BrokerUtil.printBroker(brokerName, address),
                                writeOffset, bytesRead, fileLength,
                                remotePath, e.getMessage());
                        LOG.warn(lastErrMsg, e);
                        status = new Status(ErrCode.COMMON_ERROR, lastErrMsg);
                        break;
                    } catch (TException e) {
                        lastErrMsg = String.format("failed to write via broker %s. "
                                + "current write offset: %d, write length: %d,"
                                + " file length: %d, file: %s. encounter TException: %s",
                                BrokerUtil.printBroker(brokerName, address),
                                writeOffset, bytesRead, fileLength,
                                remotePath, e.getMessage());
                        LOG.warn(lastErrMsg, e);
                        status = new Status(ErrCode.COMMON_ERROR, lastErrMsg);
                        break;
                    }
                }
                
                if (status.ok() && tryTimes < 3) {
                    // write succeed, update current write offset
                    writeOffset += bytesRead;
                } else {
                    status = new Status(ErrCode.COMMON_ERROR, lastErrMsg);
                    break;
                }
            } // end of read local file loop
        } catch (FileNotFoundException e1) {
            return new Status(ErrCode.COMMON_ERROR, "encounter file not found exception: " + e1.getMessage()
                    + ", broker: " + BrokerUtil.printBroker(brokerName, address));
        } catch (IOException e1) {
            return new Status(ErrCode.COMMON_ERROR, "encounter io exception: " + e1.getMessage()
                    + ", broker: " + BrokerUtil.printBroker(brokerName, address));
        } finally {
            // close write
            Status closeStatus = closeWriter(client, address, fd);
            if (!closeStatus.ok()) {
                LOG.warn(closeStatus.getErrMsg());
                if (status.ok()) {
                    // we return close write error only if no other error has been encountered.
                    status = closeStatus;
                }
                ClientPool.brokerPool.invalidateObject(address, client);
            } else {
                ClientPool.brokerPool.returnObject(address, client);
            }
        }

        if (status.ok()) {
            LOG.info("finished to upload {} to remote path {}. cost: {} ms",
                     localPath, remotePath, (System.currentTimeMillis() - start));
        }
        return status;
    }

    public Status rename(String origFilePath, String destFilePath) {
        long start = System.currentTimeMillis();
        Status status = Status.OK;

        // 1. get a proper broker
        Pair<TPaloBrokerService.Client, TNetworkAddress> pair = new Pair<TPaloBrokerService.Client, TNetworkAddress>(
                null, null);
        status = getBroker(pair);
        if (!status.ok()) {
            return status;
        }
        TPaloBrokerService.Client client = pair.first;
        TNetworkAddress address = pair.second;

        // 2. rename
        boolean needReturn = true;
        try {
            TBrokerRenamePathRequest req = new TBrokerRenamePathRequest(TBrokerVersion.VERSION_ONE, origFilePath,
                    destFilePath, properties);
            TBrokerOperationStatus ost = client.renamePath(req);
            if (ost.getStatusCode() != TBrokerOperationStatusCode.OK) {
                return new Status(ErrCode.COMMON_ERROR,
                        "failed to rename " + origFilePath + " to " + destFilePath + ", msg: " + ost.getMessage()
                                + ", broker: " + BrokerUtil.printBroker(brokerName, address));
            }
        } catch (TException e) {
            needReturn = false;
            return new Status(ErrCode.COMMON_ERROR,
                    "failed to rename " + origFilePath + " to " + destFilePath + ", msg: " + e.getMessage()
                    + ", broker: " + BrokerUtil.printBroker(brokerName, address));
        } finally {
            if (needReturn) {
                ClientPool.brokerPool.returnObject(address, client);
            } else {
                ClientPool.brokerPool.invalidateObject(address, client);
            }
        }

        LOG.info("finished to rename {} to  {}. cost: {} ms",
                 origFilePath, destFilePath, (System.currentTimeMillis() - start));
        return Status.OK;
    }

    public Status delete(String remotePath) {
        // get a proper broker
        Pair<TPaloBrokerService.Client, TNetworkAddress> pair = new Pair<TPaloBrokerService.Client, TNetworkAddress>(
                null, null);
        Status st = getBroker(pair);
        if (!st.ok()) {
            return st;
        }
        TPaloBrokerService.Client client = pair.first;
        TNetworkAddress address = pair.second;

        // delete
        boolean needReturn = true;
        try {
            TBrokerDeletePathRequest req = new TBrokerDeletePathRequest(TBrokerVersion.VERSION_ONE, remotePath,
                    properties);
            TBrokerOperationStatus opst = client.deletePath(req);
            if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                return new Status(ErrCode.COMMON_ERROR,
                        "failed to delete remote path: " + remotePath + ". msg: " + opst.getMessage()
                                + ", broker: " + BrokerUtil.printBroker(brokerName, address));
            }

            LOG.info("finished to delete remote path {}.", remotePath);
        } catch (TException e) {
            needReturn = false;
            return new Status(ErrCode.COMMON_ERROR,
                    "failed to delete remote path: " + remotePath + ". msg: " + e.getMessage()
                            + ", broker: " + BrokerUtil.printBroker(brokerName, address));
        } finally {
            if (needReturn) {
                ClientPool.brokerPool.returnObject(address, client);
            } else {
                ClientPool.brokerPool.invalidateObject(address, client);
            }
        }

        return Status.OK;
    }

    // List files in remotePath
    // The remote file name will only contains file name only(Not full path)
    public Status list(String remotePath, List<RemoteFile> result) {
        // get a proper broker
        Pair<TPaloBrokerService.Client, TNetworkAddress> pair = new Pair<TPaloBrokerService.Client, TNetworkAddress>(null, null);
        Status st = getBroker(pair);
        if (!st.ok()) {
            return st;
        }
        TPaloBrokerService.Client client = pair.first;
        TNetworkAddress address = pair.second;

        // list
        boolean needReturn = true;
        try {
            TBrokerListPathRequest req = new TBrokerListPathRequest(TBrokerVersion.VERSION_ONE, remotePath,
                    false /* not recursive */, properties);
            req.setFileNameOnly(true);
            TBrokerListResponse rep = client.listPath(req);
            TBrokerOperationStatus opst = rep.getOpStatus();
            if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                return new Status(ErrCode.COMMON_ERROR,
                        "failed to list remote path: " + remotePath + ". msg: " + opst.getMessage()
                                + ", broker: " + BrokerUtil.printBroker(brokerName, address));
            }

            List<TBrokerFileStatus> fileStatus = rep.getFiles();
            for (TBrokerFileStatus tFile : fileStatus) {
                RemoteFile file = new RemoteFile(tFile.path, !tFile.isDir, tFile.size);
                result.add(file);
            }
            LOG.info("finished to list remote path {}. get files: {}", remotePath, result);
        } catch (TException e) {
            needReturn = false;
            return new Status(ErrCode.COMMON_ERROR,
                    "failed to list remote path: " + remotePath + ". msg: " + e.getMessage()
                            + ", broker: " + BrokerUtil.printBroker(brokerName, address));
        } finally {
            if (needReturn) {
                ClientPool.brokerPool.returnObject(address, client);
            } else {
                ClientPool.brokerPool.invalidateObject(address, client);
            }
        }

        return Status.OK;
    }

    public Status makeDir(String remotePath) {
        // 1. get a proper broker
        Pair<TPaloBrokerService.Client, TNetworkAddress> pair = new Pair<TPaloBrokerService.Client, TNetworkAddress>(
                null, null);
        Status st = getBroker(pair);
        if (!st.ok()) {
            return st;
        }
        TPaloBrokerService.Client client = pair.first;
        TNetworkAddress address = pair.second;

        // TODO: mkdir
        return Status.OK;
    }

    public Status checkPathExist(String remotePath) {
        // 1. get a proper broker
        Pair<TPaloBrokerService.Client, TNetworkAddress> pair = new Pair<TPaloBrokerService.Client, TNetworkAddress>(
                null, null);
        Status st = getBroker(pair);
        if (!st.ok()) {
            return st;
        }
        TPaloBrokerService.Client client = pair.first;
        TNetworkAddress address = pair.second;

        // check path
        boolean needReturn = true;
        try {
            TBrokerCheckPathExistRequest req = new TBrokerCheckPathExistRequest(TBrokerVersion.VERSION_ONE,
                    remotePath, properties);
            TBrokerCheckPathExistResponse rep = client.checkPathExist(req);
            TBrokerOperationStatus opst = rep.getOpStatus();
            if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                return new Status(ErrCode.COMMON_ERROR,
                        "failed to check remote path exist: " + remotePath
                                + ", broker: " + BrokerUtil.printBroker(brokerName, address)
                                + ". msg: " + opst.getMessage());
            }

            if (!rep.isIsPathExist()) {
                return new Status(ErrCode.NOT_FOUND, "remote path does not exist: " + remotePath);
            }

            return Status.OK;
        } catch (TException e) {
            needReturn = false;
            return new Status(ErrCode.COMMON_ERROR,
                    "failed to check remote path exist: " + remotePath
                            + ", broker: " + BrokerUtil.printBroker(brokerName, address)
                            + ". msg: " + e.getMessage());
        } finally {
            if (needReturn) {
                ClientPool.brokerPool.returnObject(address, client);
            } else {
                ClientPool.brokerPool.invalidateObject(address, client);
            }
        }
    }

    public static String clientId() {
        return FrontendOptions.getLocalHostAddress() + ":" + Config.edit_log_port;
    }

    private Status getBroker(Pair<TPaloBrokerService.Client, TNetworkAddress> result) {
        FsBroker broker = null;
        try {
            String localIP = FrontendOptions.getLocalHostAddress();
            broker = Catalog.getCurrentCatalog().getBrokerMgr().getBroker(brokerName, localIP);
        } catch (AnalysisException e) {
            return new Status(ErrCode.COMMON_ERROR, "failed to get a broker address: " + e.getMessage());
        }
        TNetworkAddress address = new TNetworkAddress(broker.ip, broker.port);
        TPaloBrokerService.Client client = null;
        try {
            client = ClientPool.brokerPool.borrowObject(address);
        } catch (Exception e) {
            return new Status(ErrCode.COMMON_ERROR, "failed to get broker client: " + e.getMessage());
        }

        result.first = client;
        result.second = address;
        LOG.info("get broker: {}", BrokerUtil.printBroker(brokerName, address));
        return Status.OK;
    }

    private Status openWriter(TPaloBrokerService.Client client, TNetworkAddress address, String remoteFile,
            TBrokerFD fd) {
        try {
            TBrokerOpenWriterRequest req = new TBrokerOpenWriterRequest(TBrokerVersion.VERSION_ONE,
                    remoteFile, TBrokerOpenMode.APPEND, clientId(), properties);
            TBrokerOpenWriterResponse rep = client.openWriter(req);
            TBrokerOperationStatus opst = rep.getOpStatus();
            if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                return new Status(ErrCode.COMMON_ERROR,
                        "failed to open writer on broker " + BrokerUtil.printBroker(brokerName, address)
                                + " for file: " + remoteFile + ". msg: " + opst.getMessage());
            }

            fd.setHigh(rep.getFd().getHigh());
            fd.setLow(rep.getFd().getLow());
            LOG.info("finished to open writer. fd: {}. directly upload to remote path {}.",
                     fd, remoteFile);
        } catch (TException e) {
            return new Status(ErrCode.BAD_CONNECTION,
                    "failed to open writer on broker " + BrokerUtil.printBroker(brokerName, address)
                            + ", err: " + e.getMessage());
        }

        return Status.OK;
    }

    private Status closeWriter(TPaloBrokerService.Client client, TNetworkAddress address, TBrokerFD fd) {
        try {
            TBrokerCloseWriterRequest req = new TBrokerCloseWriterRequest(TBrokerVersion.VERSION_ONE, fd);
            TBrokerOperationStatus st = client.closeWriter(req);
            if (st.getStatusCode() != TBrokerOperationStatusCode.OK) {
                return new Status(ErrCode.COMMON_ERROR,
                        "failed to close writer on broker " + BrokerUtil.printBroker(brokerName, address)
                                + " for fd: " + fd);
            }

            LOG.info("finished to close writer. fd: {}.", fd);
        } catch (TException e) {
            return new Status(ErrCode.BAD_CONNECTION,
                    "failed to close writer on broker " + BrokerUtil.printBroker(brokerName, address)
                            + ", fd " + fd + ", msg: " + e.getMessage());
        }

        return Status.OK;
    }

    private Status closeReader(TPaloBrokerService.Client client, TNetworkAddress address, TBrokerFD fd) {
        try {
            TBrokerCloseReaderRequest req = new TBrokerCloseReaderRequest(TBrokerVersion.VERSION_ONE, fd);
            TBrokerOperationStatus st = client.closeReader(req);
            if (st.getStatusCode() != TBrokerOperationStatusCode.OK) {
                return new Status(ErrCode.COMMON_ERROR,
                        "failed to close reader on broker " + BrokerUtil.printBroker(brokerName, address)
                                + " for fd: " + fd);
            }

            LOG.info("finished to close reader. fd: {}.", fd);
        } catch (TException e) {
            return new Status(ErrCode.BAD_CONNECTION,
                    "failed to close reader on broker " + BrokerUtil.printBroker(brokerName, address)
                            + ", fd " + fd + ", msg: " + e.getMessage());
        }

        return Status.OK;
    }

    public static BlobStorage read(DataInput in) throws IOException {
        BlobStorage blobStorage = new BlobStorage();
        blobStorage.readFields(in);
        return blobStorage;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // must write type first
        Text.writeString(out, brokerName);

        out.writeInt(properties.size());
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }
    }

    public void readFields(DataInput in) throws IOException {
        brokerName = Text.readString(in);

        // properties
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            properties.put(key, value);
        }
    }
}
