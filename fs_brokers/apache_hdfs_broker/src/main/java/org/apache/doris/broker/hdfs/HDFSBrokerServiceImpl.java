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

package org.apache.doris.broker.hdfs;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import org.apache.doris.thrift.TBrokerCheckPathExistRequest;
import org.apache.doris.thrift.TBrokerCheckPathExistResponse;
import org.apache.doris.thrift.TBrokerCloseReaderRequest;
import org.apache.doris.thrift.TBrokerCloseWriterRequest;
import org.apache.doris.thrift.TBrokerDeletePathRequest;
import org.apache.doris.thrift.TBrokerFD;
import org.apache.doris.thrift.TBrokerFileStatus;
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
import org.apache.doris.common.BrokerPerfMonitor;
import com.google.common.base.Stopwatch;

public class HDFSBrokerServiceImpl implements TPaloBrokerService.Iface {

    private static Logger logger = Logger
            .getLogger(HDFSBrokerServiceImpl.class.getName());
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
        logger.debug("received a list path request, request detail: " + request);
        TBrokerListResponse response = new TBrokerListResponse();
        try {
            List<TBrokerFileStatus> fileStatuses = fileSystemManager.listPath(request.path, request.properties);
            response.setOpStatus(generateOKStatus());
            response.setFiles(fileStatuses);
            return response;
        } catch (BrokerException e) {
            TBrokerOperationStatus errorStatus = e.generateFailedOperationStatus();
            response.setOpStatus(errorStatus);
            return response;
        }
    }

    @Override
    public TBrokerOperationStatus deletePath(TBrokerDeletePathRequest request)
            throws TException {
        logger.debug("receive a delete path request, request detail: " + request);
        try {
            fileSystemManager.deletePath(request.path, request.properties);
        } catch (BrokerException e) {
            TBrokerOperationStatus errorStatus = e.generateFailedOperationStatus();
            return errorStatus;
        }
        return generateOKStatus();
    }

    @Override
    public TBrokerOperationStatus renamePath(TBrokerRenamePathRequest request)
            throws TException {
        logger.debug("receive a rename path request, request detail: " + request);
        try {
            fileSystemManager.renamePath(request.srcPath, request.destPath, request.properties);
        } catch (BrokerException e) {
            TBrokerOperationStatus errorStatus = e.generateFailedOperationStatus();
            return errorStatus;
        }
        return generateOKStatus();
    }

    @Override
    public TBrokerCheckPathExistResponse checkPathExist(
            TBrokerCheckPathExistRequest request) throws TException {
        TBrokerCheckPathExistResponse response = new TBrokerCheckPathExistResponse();
        try {
            boolean isPathExist = fileSystemManager.checkPathExist(request.path, request.properties);
            response.setIsPathExist(isPathExist);
            response.setOpStatus(generateOKStatus());
        } catch (BrokerException e) {
            TBrokerOperationStatus errorStatus = e.generateFailedOperationStatus();
            response.setOpStatus(errorStatus);
        }
        return response;
    }

    @Override
    public TBrokerOpenReaderResponse openReader(TBrokerOpenReaderRequest request)
            throws TException {
        logger.debug("receive a open reader request, request detail: " + request);
        TBrokerOpenReaderResponse response = new TBrokerOpenReaderResponse();
        try {
            TBrokerFD fd = fileSystemManager.openReader(request.clientId, request.path, 
                    request.startOffset, request.properties);
            response.setFd(fd);
            response.setOpStatus(generateOKStatus());
        } catch (BrokerException e) {
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
            TBrokerOperationStatus errorStatus = e.generateFailedOperationStatus();
            response.setOpStatus(errorStatus);
            return response;
        } finally {
            stopwatch.stop();
            logger.debug("read request fd: " + request.fd.high + "" 
                    + request.fd.low + " cost " 
                    + stopwatch.elapsedTime(TimeUnit.MILLISECONDS) + " millis");
        }
        return response;
    }

    @Override
    public TBrokerOperationStatus seek(TBrokerSeekRequest request)
            throws TException {
        try {
            fileSystemManager.seek(request.fd, request.offset);
        } catch (BrokerException e) {
            TBrokerOperationStatus errorStatus = e.generateFailedOperationStatus();
            return errorStatus;
        }
        return generateOKStatus();
    }

    @Override
    public TBrokerOperationStatus closeReader(TBrokerCloseReaderRequest request)
            throws TException {
        try {
            fileSystemManager.closeReader(request.fd);
        } catch (BrokerException e) {
            TBrokerOperationStatus errorStatus = e.generateFailedOperationStatus();
            return errorStatus;
        }
        return generateOKStatus();
    }

    @Override
    public TBrokerOpenWriterResponse openWriter(TBrokerOpenWriterRequest request)
            throws TException {
        logger.debug("receive a open writer request, request detail: " + request);
        TBrokerOpenWriterResponse response = new TBrokerOpenWriterResponse();
        try {
            TBrokerFD fd = fileSystemManager.openWriter(request.clientId, request.path, request.properties);
            response.setFd(fd);
            response.setOpStatus(generateOKStatus());
        } catch (BrokerException e) {
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
            TBrokerOperationStatus errorStatus = e.generateFailedOperationStatus();
            return errorStatus;
        } finally {
            stopwatch.stop();
            logger.debug("write request fd: " + request.fd.high + "" 
                    + request.fd.low + " cost " 
                    + stopwatch.elapsedTime(TimeUnit.MILLISECONDS) + " millis");
        }
        return generateOKStatus();
    }

    @Override
    public TBrokerOperationStatus closeWriter(TBrokerCloseWriterRequest request)
            throws TException {
        try {
            fileSystemManager.closeWriter(request.fd);
        } catch (BrokerException e) {
            TBrokerOperationStatus errorStatus = e.generateFailedOperationStatus();
            return errorStatus;
        }
        return generateOKStatus();
    }

    @Override
    public TBrokerOperationStatus ping(TBrokerPingBrokerRequest request)
            throws TException {
        try {
            fileSystemManager.ping(request.clientId);
        } catch (BrokerException e) {
            TBrokerOperationStatus errorStatus = e.generateFailedOperationStatus();
            return errorStatus;
        }
        return generateOKStatus();
    }
}
