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

import com.google.common.base.Stopwatch;
import org.apache.doris.common.BrokerPerfMonitor;
import org.apache.doris.common.HiveUtils;
import org.apache.doris.thrift.TBrokerCheckPathExistRequest;
import org.apache.doris.thrift.TBrokerCheckPathExistResponse;
import org.apache.doris.thrift.TBrokerCloseReaderRequest;
import org.apache.doris.thrift.TBrokerCloseWriterRequest;
import org.apache.doris.thrift.TBrokerDeletePathRequest;
import org.apache.doris.thrift.TBrokerFD;
import org.apache.doris.thrift.TBrokerFileSizeRequest;
import org.apache.doris.thrift.TBrokerFileSizeResponse;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TBrokerIsSplittableResponse;
import org.apache.doris.thrift.TBrokerIsSplittableRequest;
import org.apache.doris.thrift.TBrokerListPathRequest;
import org.apache.doris.thrift.TBrokerListResponse;
import org.apache.doris.thrift.TBrokerOpenReaderRequest;
import org.apache.doris.thrift.TBrokerOpenReaderResponse;
import org.apache.doris.thrift.TBrokerOpenWriterRequest;
import org.apache.doris.thrift.TBrokerOpenWriterResponse;
import org.apache.doris.thrift.TBrokerOperationStatus;
import org.apache.doris.thrift.TBrokerOperationStatusCode;
import org.apache.doris.thrift.TBrokerPReadRequest;
import org.apache.doris.thrift.TBrokerPWriteRequest;
import org.apache.doris.thrift.TBrokerPingBrokerRequest;
import org.apache.doris.thrift.TBrokerReadResponse;
import org.apache.doris.thrift.TBrokerRenamePathRequest;
import org.apache.doris.thrift.TBrokerSeekRequest;
import org.apache.doris.thrift.TPaloBrokerService;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class HDFSBrokerServiceImpl implements TPaloBrokerService.Iface {

    private static Logger logger = Logger.getLogger(HDFSBrokerServiceImpl.class.getName());
    private FileSystemManager fileSystemManager;
    
    public HDFSBrokerServiceImpl() {
        fileSystemManager = new FileSystemManager();
    }
    
    private TBrokerOperationStatus generateOKStatus() {
        return new TBrokerOperationStatus(TBrokerOperationStatusCode.OK);
    }
    
    @Override
    public TBrokerListResponse listPath(TBrokerListPathRequest request)
            throws TException {
        logger.info("received a list path request, request detail: " + request);
        TBrokerListResponse response = new TBrokerListResponse();
        try {
            boolean fileNameOnly = false;
            if (request.isSetFileNameOnly()) {
                fileNameOnly = request.isFileNameOnly();
            }
            List<TBrokerFileStatus> fileStatuses = fileSystemManager.listPath(request.path, fileNameOnly,
                    request.properties);
            response.setOpStatus(generateOKStatus());
            response.setFiles(fileStatuses);
            return response;
        } catch (BrokerException e) {
            logger.warn("failed to list path: " + request.path, e);
            TBrokerOperationStatus errorStatus = e.generateFailedOperationStatus();
            response.setOpStatus(errorStatus);
            return response;
        }
    }

    @Override
    public TBrokerListResponse listLocatedFiles(TBrokerListPathRequest request)
            throws TException {
        logger.info("received a listLocatedFiles request, request detail: " + request);
        TBrokerListResponse response = new TBrokerListResponse();
        try {
            boolean recursive = request.isIsRecursive();
            boolean onlyFiles = false;
            if (request.isSetOnlyFiles()) {
                onlyFiles = request.isOnlyFiles();
            }
            List<TBrokerFileStatus> fileStatuses = fileSystemManager.listLocatedFiles(request.path,
                onlyFiles, recursive, request.properties);
            response.setOpStatus(generateOKStatus());
            response.setFiles(fileStatuses);
            return response;
        } catch (BrokerException e) {
            logger.warn("failed to list path: " + request.path, e);
            TBrokerOperationStatus errorStatus = e.generateFailedOperationStatus();
            response.setOpStatus(errorStatus);
            return response;
        }
    }

    @Override
    public TBrokerIsSplittableResponse isSplittable(TBrokerIsSplittableRequest request) throws TException {
        logger.info("received a isSplittable request, request detail: " + request);
        TBrokerIsSplittableResponse response = new TBrokerIsSplittableResponse();
        try {
            boolean isSplittable = HiveUtils.isSplittable(request.path, request.inputFormat, request.properties);
            response.setOpStatus(generateOKStatus());
            response.setSplittable(isSplittable);
            return response;
        } catch (BrokerException e) {
            logger.warn("failed to get isSplitable with path: " + request.path, e);
            TBrokerOperationStatus errorStatus = e.generateFailedOperationStatus();
            response.setOpStatus(errorStatus);
            return response;
        }
    }

    @Override
    public TBrokerOperationStatus deletePath(TBrokerDeletePathRequest request)
            throws TException {
        logger.info("receive a delete path request, request detail: " + request);
        try {
            fileSystemManager.deletePath(request.path, request.properties);
        } catch (BrokerException e) {
            logger.warn("failed to delete path: " + request.path, e);
            TBrokerOperationStatus errorStatus = e.generateFailedOperationStatus();
            return errorStatus;
        }
        return generateOKStatus();
    }

    @Override
    public TBrokerOperationStatus renamePath(TBrokerRenamePathRequest request)
            throws TException {
        logger.info("receive a rename path request, request detail: " + request);
        try {
            fileSystemManager.renamePath(request.srcPath, request.destPath, request.properties);
        } catch (BrokerException e) {
            logger.warn("failed to rename path: " + request.srcPath + " to " + request.destPath, e);
            TBrokerOperationStatus errorStatus = e.generateFailedOperationStatus();
            return errorStatus;
        }
        return generateOKStatus();
    }

    @Override
    public TBrokerCheckPathExistResponse checkPathExist(
            TBrokerCheckPathExistRequest request) throws TException {
        logger.info("receive a check path request, request detail: " + request);
        TBrokerCheckPathExistResponse response = new TBrokerCheckPathExistResponse();
        try {
            boolean isPathExist = fileSystemManager.checkPathExist(request.path, request.properties);
            response.setIsPathExist(isPathExist);
            response.setOpStatus(generateOKStatus());
        } catch (BrokerException e) {
            logger.warn("failed to check path exist: " + request.path, e);
            TBrokerOperationStatus errorStatus = e.generateFailedOperationStatus();
            response.setOpStatus(errorStatus);
        }
        return response;
    }

    @Override
    public TBrokerOpenReaderResponse openReader(TBrokerOpenReaderRequest request)
            throws TException {
        logger.info("receive a open reader request, request detail: " + request);
        TBrokerOpenReaderResponse response = new TBrokerOpenReaderResponse();
        try {
            TBrokerFD fd = fileSystemManager.openReader(request.clientId, request.path,
                    request.startOffset, request.properties);
            response.setFd(fd);
            response.setOpStatus(generateOKStatus());
        } catch (BrokerException e) {
            logger.warn("failed to open reader for path: " + request.path, e);
            TBrokerOperationStatus errorStatus = e.generateFailedOperationStatus();
            response.setOpStatus(errorStatus);
        }
        return response;
    }

    @Override
    public TBrokerReadResponse pread(TBrokerPReadRequest request)
            throws TException {
        logger.debug("receive a read request, request detail: " + request);
        Stopwatch stopwatch = BrokerPerfMonitor.startWatch();
        TBrokerReadResponse response = new TBrokerReadResponse();
        try {
            ByteBuffer readBuf = fileSystemManager.pread(request.fd, request.offset, request.length);
            response.setData(readBuf);
            response.setOpStatus(generateOKStatus());
        } catch (BrokerException e) {
            logger.warn("failed to pread: " + request.fd, e);
            TBrokerOperationStatus errorStatus = e.generateFailedOperationStatus();
            response.setOpStatus(errorStatus);
            return response;
        } finally {
            stopwatch.stop();
            logger.debug("read request fd: " + request.fd.high + "" 
                    + request.fd.low + " cost " 
                    + stopwatch.elapsed(TimeUnit.MILLISECONDS) + " millis");
        }
        return response;
    }

    @Override
    public TBrokerOperationStatus seek(TBrokerSeekRequest request)
            throws TException {
        logger.debug("receive a seek request, request detail: " + request);
        try {
            fileSystemManager.seek(request.fd, request.offset);
        } catch (BrokerException e) {
            logger.warn("failed to seek: " + request.fd, e);
            TBrokerOperationStatus errorStatus = e.generateFailedOperationStatus();
            return errorStatus;
        }
        return generateOKStatus();
    }

    @Override
    public TBrokerOperationStatus closeReader(TBrokerCloseReaderRequest request)
            throws TException {
        logger.info("receive a close reader request, request detail: " + request);
        try {
            fileSystemManager.closeReader(request.fd);
        } catch (BrokerException e) {
            logger.warn("failed to close reader: " + request.fd, e);
            TBrokerOperationStatus errorStatus = e.generateFailedOperationStatus();
            return errorStatus;
        }
        return generateOKStatus();
    }

    @Override
    public TBrokerOpenWriterResponse openWriter(TBrokerOpenWriterRequest request)
            throws TException {
        logger.info("receive a open writer request, request detail: " + request);
        TBrokerOpenWriterResponse response = new TBrokerOpenWriterResponse();
        try {
            TBrokerFD fd = fileSystemManager.openWriter(request.clientId, request.path, request.properties);
            response.setFd(fd);
            response.setOpStatus(generateOKStatus());
        } catch (BrokerException e) {
            logger.warn("failed to open writer: " + request.path, e);
            TBrokerOperationStatus errorStatus = e.generateFailedOperationStatus();
            response.setOpStatus(errorStatus);
        }
        return response;
    }

    @Override
    public TBrokerOperationStatus pwrite(TBrokerPWriteRequest request)
            throws TException {
        logger.debug("receive a pwrite request, request detail: " + request);
        Stopwatch stopwatch = BrokerPerfMonitor.startWatch();
        try {
            fileSystemManager.pwrite(request.fd, request.offset, request.getData());
        } catch (BrokerException e) {
            logger.warn("failed to pwrite: " + request.fd, e);
            TBrokerOperationStatus errorStatus = e.generateFailedOperationStatus();
            return errorStatus;
        } finally {
            stopwatch.stop();
            logger.debug("write request fd: " + request.fd.high + "" 
                    + request.fd.low + " cost " 
                    + stopwatch.elapsed(TimeUnit.MILLISECONDS) + " millis");
        }
        return generateOKStatus();
    }

    @Override
    public TBrokerOperationStatus closeWriter(TBrokerCloseWriterRequest request)
            throws TException {
        logger.info("receive a close writer request, request detail: " + request);
        try {
            fileSystemManager.closeWriter(request.fd);
        } catch (BrokerException e) {
            logger.warn("failed to close writer: " + request.fd, e);
            TBrokerOperationStatus errorStatus = e.generateFailedOperationStatus();
            return errorStatus;
        }
        return generateOKStatus();
    }

    @Override
    public TBrokerOperationStatus ping(TBrokerPingBrokerRequest request)
            throws TException {
        logger.debug("receive a ping request, request detail: " + request);
        try {
            fileSystemManager.ping(request.clientId);
        } catch (BrokerException e) {
            logger.warn("failed to ping: ", e);
            TBrokerOperationStatus errorStatus = e.generateFailedOperationStatus();
            return errorStatus;
        }
        return generateOKStatus();
    }

    @Override
    public TBrokerFileSizeResponse fileSize(
            TBrokerFileSizeRequest request) throws TException {
        logger.debug("receive a file size request, request detail: " + request);
        TBrokerFileSizeResponse response = new TBrokerFileSizeResponse();
        try {
            long fileSize = fileSystemManager.fileSize(request.path, request.properties);
            response.setFileSize(fileSize);
            response.setOpStatus(generateOKStatus());
        } catch (BrokerException e) {
            logger.warn("failed to get file size: " + request.path, e);
            TBrokerOperationStatus errorStatus = e.generateFailedOperationStatus();
            response.setOpStatus(errorStatus);
        }
        return response;
    }
}
