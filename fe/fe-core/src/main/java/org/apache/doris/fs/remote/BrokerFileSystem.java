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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.fs.operations.BrokerFileOperations;
import org.apache.doris.fs.operations.OpParams;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TBrokerCheckPathExistRequest;
import org.apache.doris.thrift.TBrokerCheckPathExistResponse;
import org.apache.doris.thrift.TBrokerDeletePathRequest;
import org.apache.doris.thrift.TBrokerFD;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TBrokerIsSplittableRequest;
import org.apache.doris.thrift.TBrokerIsSplittableResponse;
import org.apache.doris.thrift.TBrokerListPathRequest;
import org.apache.doris.thrift.TBrokerListResponse;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class BrokerFileSystem extends RemoteFileSystem {
    private static final Logger LOG = LogManager.getLogger(BrokerFileSystem.class);
    private final BrokerFileOperations operations;

    public BrokerFileSystem(String name, Map<String, String> properties) {
        super(name, StorageBackend.StorageType.BROKER);
        properties.putAll(PropertyConverter.convertToHadoopFSProperties(properties));
        this.properties = properties;
        this.operations = new BrokerFileOperations(name, properties);
    }

    public Pair<TPaloBrokerService.Client, TNetworkAddress> getBroker() {
        Pair<TPaloBrokerService.Client, TNetworkAddress> result = Pair.of(null, null);
        FsBroker broker;
        try {
            String localIP = FrontendOptions.getLocalHostAddress();
            broker = Env.getCurrentEnv().getBrokerMgr().getBroker(name, localIP);
        } catch (AnalysisException e) {
            LOG.warn("failed to get a broker address: " + e.getMessage());
            return null;
        }
        TNetworkAddress address = new TNetworkAddress(broker.host, broker.port);
        TPaloBrokerService.Client client;
        try {
            client = ClientPool.brokerPool.borrowObject(address);
        } catch (Exception e) {
            LOG.warn("failed to get broker client: " + e.getMessage());
            return null;
        }

        result.first = client;
        result.second = address;
        LOG.info("get broker: {}", BrokerUtil.printBroker(name, address));
        return result;
    }

    @Override
    public Status exists(String remotePath) {
        // 1. get a proper broker
        Pair<TPaloBrokerService.Client, TNetworkAddress> pair = getBroker();
        if (pair == null) {
            return new Status(Status.ErrCode.COMMON_ERROR, "failed to get broker client");
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
                return new Status(Status.ErrCode.COMMON_ERROR,
                        "failed to check remote path exist: " + remotePath
                                + ", broker: " + BrokerUtil.printBroker(name, address)
                                + ". msg: " + opst.getMessage());
            }

            if (!rep.isIsPathExist()) {
                return new Status(Status.ErrCode.NOT_FOUND, "remote path does not exist: " + remotePath);
            }

            return Status.OK;
        } catch (TException e) {
            needReturn = false;
            return new Status(Status.ErrCode.COMMON_ERROR,
                    "failed to check remote path exist: " + remotePath
                            + ", broker: " + BrokerUtil.printBroker(name, address)
                            + ". msg: " + e.getMessage());
        } finally {
            if (needReturn) {
                ClientPool.brokerPool.returnObject(address, client);
            } else {
                ClientPool.brokerPool.invalidateObject(address, client);
            }
        }
    }

    @Override
    public Status downloadWithFileSize(String remoteFilePath, String localFilePath, long fileSize) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("download from {} to {}, file size: {}.", remoteFilePath, localFilePath, fileSize);
        }

        long start = System.currentTimeMillis();

        // 1. get a proper broker
        Pair<TPaloBrokerService.Client, TNetworkAddress> pair = getBroker();
        if (pair == null) {
            return new Status(Status.ErrCode.COMMON_ERROR, "failed to get broker client");
        }
        TPaloBrokerService.Client client = pair.first;
        TNetworkAddress address = pair.second;

        // 2. open file reader with broker
        TBrokerFD fd = new TBrokerFD();
        Status opStatus = operations.openReader(OpParams.of(client, address, remoteFilePath, fd));
        if (!opStatus.ok()) {
            return opStatus;
        }
        LOG.info("finished to open reader. fd: {}. download {} to {}.",
                fd, remoteFilePath, localFilePath);
        Preconditions.checkNotNull(fd);
        // 3. delete local file if exist
        File localFile = new File(localFilePath);
        if (localFile.exists()) {
            try {
                Files.walk(Paths.get(localFilePath), FileVisitOption.FOLLOW_LINKS)
                        .sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            } catch (IOException e) {
                return new Status(Status.ErrCode.COMMON_ERROR, "failed to delete exist local file: " + localFilePath);
            }
        }

        // 4. create local file
        Status status = Status.OK;
        try {
            if (!localFile.createNewFile()) {
                return new Status(Status.ErrCode.COMMON_ERROR, "failed to create local file: " + localFilePath);
            }
        } catch (IOException e) {
            return new Status(Status.ErrCode.COMMON_ERROR, "failed to create local file: "
                    + localFilePath + ", msg: " + e.getMessage());
        }

        // 5. read remote file with broker and write to local
        String lastErrMsg = null;
        try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(localFile))) {
            final long bufSize = 1024 * 1024; // 1MB
            long leftSize = fileSize;
            long readOffset = 0;
            while (leftSize > 0) {
                long readLen = Math.min(leftSize, bufSize);
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
                                    BrokerUtil.printBroker(name, address),
                                    readOffset, readLen, fileSize,
                                    remoteFilePath, rep.getOpStatus().getStatusCode().getValue(),
                                    rep.getOpStatus().getMessage());
                            LOG.warn(lastErrMsg);
                            status = new Status(Status.ErrCode.COMMON_ERROR, lastErrMsg);
                        }
                        if (rep.opStatus.statusCode != TBrokerOperationStatusCode.END_OF_FILE) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("download. readLen: {}, read data len: {}, left size:{}. total size: {}",
                                        readLen, rep.getData().length, leftSize, fileSize);
                            }
                        } else {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("read eof: " + remoteFilePath);
                            }
                        }
                        break;
                    } catch (TTransportException e) {
                        if (e.getType() == TTransportException.TIMED_OUT) {
                            // we only retry when we encounter timeout exception.
                            lastErrMsg = String.format("failed to read via broker %s. "
                                            + "current read offset: %d, read length: %d,"
                                            + " file size: %d, file: %s, timeout.",
                                    BrokerUtil.printBroker(name, address),
                                    readOffset, readLen, fileSize,
                                    remoteFilePath);
                            tryTimes++;
                            continue;
                        }

                        lastErrMsg = String.format("failed to read via broker %s. "
                                        + "current read offset: %d, read length: %d,"
                                        + " file size: %d, file: %s. msg: %s",
                                BrokerUtil.printBroker(name, address),
                                readOffset, readLen, fileSize,
                                remoteFilePath, e.getMessage());
                        LOG.warn(lastErrMsg);
                        status = new Status(Status.ErrCode.COMMON_ERROR, lastErrMsg);
                        break;
                    } catch (TException e) {
                        lastErrMsg = String.format("failed to read via broker %s. "
                                        + "current read offset: %d, read length: %d,"
                                        + " file size: %d, file: %s. msg: %s",
                                BrokerUtil.printBroker(name, address),
                                readOffset, readLen, fileSize,
                                remoteFilePath, e.getMessage());
                        LOG.warn(lastErrMsg);
                        status = new Status(Status.ErrCode.COMMON_ERROR, lastErrMsg);
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
                                BrokerUtil.printBroker(name, address));
                    }

                    out.write(rep.getData());
                    readOffset += rep.getData().length;
                    leftSize -= rep.getData().length;
                } else {
                    status = new Status(Status.ErrCode.COMMON_ERROR, lastErrMsg);
                    break;
                }
            } // end of reading remote file
        } catch (IOException e) {
            return new Status(Status.ErrCode.COMMON_ERROR, "Got exception: " + e.getMessage() + ", broker: "
                    + BrokerUtil.printBroker(name, address));
        } finally {
            // close broker reader
            Status closeStatus = operations.closeReader(OpParams.of(client, address, fd));
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

        LOG.info("finished to download from {} to {} with size: {}. cost {} ms",
                remoteFilePath, localFilePath, fileSize, (System.currentTimeMillis() - start));
        return status;
    }

    // directly upload the content to remote file
    @Override
    public Status directUpload(String content, String remoteFile) {
        // 1. get a proper broker
        Pair<TPaloBrokerService.Client, TNetworkAddress> pair = getBroker();
        if (pair == null) {
            return new Status(Status.ErrCode.COMMON_ERROR, "failed to get broker client");
        }
        TPaloBrokerService.Client client = pair.first;
        TNetworkAddress address = pair.second;

        TBrokerFD fd = new TBrokerFD();
        Status status = Status.OK;
        try {
            // 2. open file writer with broker
            status = operations.openWriter(OpParams.of(client, address, remoteFile, fd));
            if (!status.ok()) {
                return status;
            }

            // 3. write content
            try {
                ByteBuffer bb = ByteBuffer.wrap(content.getBytes(StandardCharsets.UTF_8));
                TBrokerPWriteRequest req = new TBrokerPWriteRequest(TBrokerVersion.VERSION_ONE, fd, 0, bb);
                TBrokerOperationStatus opst = client.pwrite(req);
                if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                    // pwrite return failure.
                    status = new Status(Status.ErrCode.COMMON_ERROR, "write failed: " + opst.getMessage()
                            + ", broker: " + BrokerUtil.printBroker(name, address));
                }
            } catch (TException e) {
                status = new Status(Status.ErrCode.BAD_CONNECTION, "write exception: " + e.getMessage()
                        + ", broker: " + BrokerUtil.printBroker(name, address));
            }
        } finally {
            Status closeStatus = operations.closeWriter(OpParams.of(client, address, fd));
            if (closeStatus.getErrCode() == Status.ErrCode.BAD_CONNECTION
                    || status.getErrCode() == Status.ErrCode.BAD_CONNECTION) {
                ClientPool.brokerPool.invalidateObject(address, client);
            } else {
                ClientPool.brokerPool.returnObject(address, client);
            }
        }

        return status;
    }

    @Override
    public Status upload(String localPath, String remotePath) {
        long start = System.currentTimeMillis();
        // 1. get a proper broker
        Pair<TPaloBrokerService.Client, TNetworkAddress> pair = getBroker();
        if (pair == null) {
            return new Status(Status.ErrCode.COMMON_ERROR, "failed to get broker client");
        }
        TPaloBrokerService.Client client = pair.first;
        TNetworkAddress address = pair.second;

        // 2. open file write with broker
        TBrokerFD fd = new TBrokerFD();
        Status status = operations.openWriter(OpParams.of(client, address, remotePath, fd));
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
            int bytesRead;
            while ((bytesRead = in.read(readBuf)) != -1) {
                ByteBuffer bb = ByteBuffer.wrap(readBuf, 0, bytesRead);

                // We only retry if we encounter a timeout thrift exception.
                int tryTimes = 0;
                while (tryTimes < 3) {
                    try {
                        TBrokerPWriteRequest req
                                = new TBrokerPWriteRequest(TBrokerVersion.VERSION_ONE, fd, writeOffset, bb);
                        TBrokerOperationStatus opst = client.pwrite(req);
                        if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                            // pwrite return failure.
                            lastErrMsg = String.format("failed to write via broker %s. "
                                            + "current write offset: %d, write length: %d,"
                                            + " file length: %d, file: %s, err code: %d, msg: %s",
                                    BrokerUtil.printBroker(name, address),
                                    writeOffset, bytesRead, fileLength,
                                    remotePath, opst.getStatusCode().getValue(), opst.getMessage());
                            LOG.warn(lastErrMsg);
                            status = new Status(Status.ErrCode.COMMON_ERROR, lastErrMsg);
                        }
                        break;
                    } catch (TTransportException e) {
                        if (e.getType() == TTransportException.TIMED_OUT) {
                            // we only retry when we encounter timeout exception.
                            lastErrMsg = String.format("failed to write via broker %s. "
                                            + "current write offset: %d, write length: %d,"
                                            + " file length: %d, file: %s. timeout",
                                    BrokerUtil.printBroker(name, address),
                                    writeOffset, bytesRead, fileLength,
                                    remotePath);
                            tryTimes++;
                            continue;
                        }

                        lastErrMsg = String.format("failed to write via broker %s. "
                                        + "current write offset: %d, write length: %d,"
                                        + " file length: %d, file: %s. encounter TTransportException: %s",
                                BrokerUtil.printBroker(name, address),
                                writeOffset, bytesRead, fileLength,
                                remotePath, e.getMessage());
                        LOG.warn(lastErrMsg, e);
                        status = new Status(Status.ErrCode.COMMON_ERROR, lastErrMsg);
                        break;
                    } catch (TException e) {
                        lastErrMsg = String.format("failed to write via broker %s. "
                                        + "current write offset: %d, write length: %d,"
                                        + " file length: %d, file: %s. encounter TException: %s",
                                BrokerUtil.printBroker(name, address),
                                writeOffset, bytesRead, fileLength,
                                remotePath, e.getMessage());
                        LOG.warn(lastErrMsg, e);
                        status = new Status(Status.ErrCode.COMMON_ERROR, lastErrMsg);
                        break;
                    }
                }

                if (status.ok() && tryTimes < 3) {
                    // write succeed, update current write offset
                    writeOffset += bytesRead;
                } else {
                    status = new Status(Status.ErrCode.COMMON_ERROR, lastErrMsg);
                    break;
                }
            } // end of read local file loop
        } catch (FileNotFoundException e1) {
            return new Status(Status.ErrCode.COMMON_ERROR, "encounter file not found exception: " + e1.getMessage()
                    + ", broker: " + BrokerUtil.printBroker(name, address));
        } catch (IOException e1) {
            return new Status(Status.ErrCode.COMMON_ERROR, "encounter io exception: " + e1.getMessage()
                    + ", broker: " + BrokerUtil.printBroker(name, address));
        } finally {
            // close write
            Status closeStatus = operations.closeWriter(OpParams.of(client, address, fd));
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

    @Override
    public Status rename(String origFilePath, String destFilePath) {
        long start = System.currentTimeMillis();
        // 1. get a proper broker
        Pair<TPaloBrokerService.Client, TNetworkAddress> pair = getBroker();
        if (pair == null) {
            return new Status(Status.ErrCode.COMMON_ERROR, "failed to get broker client");
        }
        TPaloBrokerService.Client client = pair.first;
        TNetworkAddress address = pair.second;

        // 2. rename
        boolean needReturn = true;
        try {
            TBrokerRenamePathRequest req = new TBrokerRenamePathRequest(TBrokerVersion.VERSION_ONE,
                    origFilePath, destFilePath, properties);
            TBrokerOperationStatus ost = client.renamePath(req);
            if (ost.getStatusCode() != TBrokerOperationStatusCode.OK) {
                return new Status(Status.ErrCode.COMMON_ERROR,
                        "failed to rename " + origFilePath + " to " + destFilePath + ", msg: " + ost.getMessage()
                                + ", broker: " + BrokerUtil.printBroker(name, address));
            }
        } catch (TException e) {
            needReturn = false;
            return new Status(Status.ErrCode.COMMON_ERROR,
                    "failed to rename " + origFilePath + " to " + destFilePath + ", msg: " + e.getMessage()
                            + ", broker: " + BrokerUtil.printBroker(name, address));
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

    @Override
    public Status delete(String remotePath) {
        // get a proper broker
        Pair<TPaloBrokerService.Client, TNetworkAddress> pair = getBroker();
        if (pair == null) {
            return new Status(Status.ErrCode.COMMON_ERROR, "failed to get broker client");
        }
        TPaloBrokerService.Client client = pair.first;
        TNetworkAddress address = pair.second;

        // delete
        boolean needReturn = true;
        try {
            TBrokerDeletePathRequest req = new TBrokerDeletePathRequest(TBrokerVersion.VERSION_ONE,
                    remotePath, properties);
            TBrokerOperationStatus opst = client.deletePath(req);
            if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                return new Status(Status.ErrCode.COMMON_ERROR,
                        "failed to delete remote path: " + remotePath + ". msg: " + opst.getMessage()
                                + ", broker: " + BrokerUtil.printBroker(name, address));
            }

            LOG.info("finished to delete remote path {}.", remotePath);
        } catch (TException e) {
            needReturn = false;
            return new Status(Status.ErrCode.COMMON_ERROR,
                    "failed to delete remote path: " + remotePath + ". msg: " + e.getMessage()
                            + ", broker: " + BrokerUtil.printBroker(name, address));
        } finally {
            if (needReturn) {
                ClientPool.brokerPool.returnObject(address, client);
            } else {
                ClientPool.brokerPool.invalidateObject(address, client);
            }
        }

        return Status.OK;
    }

    @Override
    public Status listFiles(String remotePath, boolean recursive, List<RemoteFile> result) {
        // get a proper broker
        Pair<TPaloBrokerService.Client, TNetworkAddress> pair = getBroker();
        if (pair == null) {
            return new Status(Status.ErrCode.BAD_CONNECTION, "failed to get broker client");
        }
        TPaloBrokerService.Client client = pair.first;
        TNetworkAddress address = pair.second;

        // invoke broker 'listLocatedFiles' interface
        boolean needReturn = true;
        try {
            TBrokerListPathRequest req = new TBrokerListPathRequest(TBrokerVersion.VERSION_ONE, remotePath,
                    recursive, properties);
            req.setOnlyFiles(true);
            TBrokerListResponse response = client.listLocatedFiles(req);
            TBrokerOperationStatus operationStatus = response.getOpStatus();
            if (operationStatus.getStatusCode() != TBrokerOperationStatusCode.OK) {
                return new Status(Status.ErrCode.COMMON_ERROR,
                        "failed to listLocatedFiles, remote path: " + remotePath + ". msg: "
                                + operationStatus.getMessage() + ", broker: " + BrokerUtil.printBroker(name, address));
            }
            List<TBrokerFileStatus> fileStatus = response.getFiles();
            for (TBrokerFileStatus tFile : fileStatus) {
                org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(tFile.path);
                RemoteFile file = new RemoteFile(path.getName(), path, !tFile.isDir, tFile.isDir, tFile.size,
                        tFile.getBlockSize(), tFile.getModificationTime(), null /* blockLocations is null*/);
                result.add(file);
            }
            LOG.info("finished to listLocatedFiles, remote path {}. get files: {}", remotePath, result);
            return Status.OK;
        } catch (TException e) {
            needReturn = false;
            return new Status(Status.ErrCode.COMMON_ERROR, "failed to listLocatedFiles, remote path: "
                + remotePath + ". msg: " + e.getMessage() + ", broker: " + BrokerUtil.printBroker(name, address));
        } finally {
            if (needReturn) {
                ClientPool.brokerPool.returnObject(address, client);
            } else {
                ClientPool.brokerPool.invalidateObject(address, client);
            }
        }
    }

    public boolean isSplittable(String remotePath, String inputFormat) throws UserException {
        // get a proper broker
        Pair<TPaloBrokerService.Client, TNetworkAddress> pair = getBroker();
        if (pair == null) {
            throw new UserException("failed to get broker client");
        }
        TPaloBrokerService.Client client = pair.first;
        TNetworkAddress address = pair.second;

        // invoke 'isSplittable' interface
        boolean needReturn = true;
        try {
            TBrokerIsSplittableRequest req = new TBrokerIsSplittableRequest().setVersion(TBrokerVersion.VERSION_ONE)
                    .setPath(remotePath).setInputFormat(inputFormat).setProperties(properties);
            TBrokerIsSplittableResponse response = client.isSplittable(req);
            TBrokerOperationStatus operationStatus = response.getOpStatus();
            if (operationStatus.getStatusCode() != TBrokerOperationStatusCode.OK) {
                throw new UserException("failed to get path isSplittable, remote path: " + remotePath + ". msg: "
                    + operationStatus.getMessage() + ", broker: " + BrokerUtil.printBroker(name, address));
            }
            boolean result = response.isSplittable();
            LOG.info("finished to get path isSplittable, remote path {} with format {}, isSplittable: {}",
                    remotePath, inputFormat, result);
            return result;
        } catch (TException e) {
            needReturn = false;
            throw new UserException("failed to get path isSplittable, remote path: "
                + remotePath + ". msg: " + e.getMessage() + ", broker: " + BrokerUtil.printBroker(name, address));
        } finally {
            if (needReturn) {
                ClientPool.brokerPool.returnObject(address, client);
            } else {
                ClientPool.brokerPool.invalidateObject(address, client);
            }
        }
    }

    // List files in remotePath
    @Override
    public Status globList(String remotePath, List<RemoteFile> result, boolean fileNameOnly) {
        // get a proper broker
        Pair<TPaloBrokerService.Client, TNetworkAddress> pair = getBroker();
        if (pair == null) {
            return new Status(Status.ErrCode.COMMON_ERROR, "failed to get broker client");
        }
        TPaloBrokerService.Client client = pair.first;
        TNetworkAddress address = pair.second;

        // list
        boolean needReturn = true;
        try {
            TBrokerListPathRequest req = new TBrokerListPathRequest(TBrokerVersion.VERSION_ONE, remotePath,
                    false /* not recursive */, properties);
            req.setFileNameOnly(fileNameOnly);
            TBrokerListResponse rep = client.listPath(req);
            TBrokerOperationStatus opst = rep.getOpStatus();
            if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                return new Status(Status.ErrCode.COMMON_ERROR,
                        "failed to list remote path: " + remotePath + ". msg: " + opst.getMessage()
                                + ", broker: " + BrokerUtil.printBroker(name, address));
            }

            List<TBrokerFileStatus> fileStatus = rep.getFiles();
            for (TBrokerFileStatus tFile : fileStatus) {
                RemoteFile file = new RemoteFile(tFile.path, !tFile.isDir, tFile.size, 0, tFile.getModificationTime());
                result.add(file);
            }
            LOG.info("finished to list remote path {}. get files: {}", remotePath, result);
        } catch (TException e) {
            needReturn = false;
            return new Status(Status.ErrCode.COMMON_ERROR,
                    "failed to list remote path: " + remotePath + ". msg: " + e.getMessage()
                            + ", broker: " + BrokerUtil.printBroker(name, address));
        } finally {
            if (needReturn) {
                ClientPool.brokerPool.returnObject(address, client);
            } else {
                ClientPool.brokerPool.invalidateObject(address, client);
            }
        }

        return Status.OK;
    }

    @Override
    public Status makeDir(String remotePath) {
        return new Status(Status.ErrCode.COMMON_ERROR, "mkdir is not implemented.");
    }
}
