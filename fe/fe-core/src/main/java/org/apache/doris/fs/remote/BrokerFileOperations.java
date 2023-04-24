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

import org.apache.doris.backup.Status;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.thrift.TBrokerCloseReaderRequest;
import org.apache.doris.thrift.TBrokerCloseWriterRequest;
import org.apache.doris.thrift.TBrokerFD;
import org.apache.doris.thrift.TBrokerOpenMode;
import org.apache.doris.thrift.TBrokerOpenReaderRequest;
import org.apache.doris.thrift.TBrokerOpenReaderResponse;
import org.apache.doris.thrift.TBrokerOpenWriterRequest;
import org.apache.doris.thrift.TBrokerOpenWriterResponse;
import org.apache.doris.thrift.TBrokerOperationStatus;
import org.apache.doris.thrift.TBrokerOperationStatusCode;
import org.apache.doris.thrift.TBrokerVersion;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.Map;

public class BrokerFileOperations {

    private static final Logger LOG = LogManager.getLogger(BrokerFileOperations.class);

    private String name;

    private Map<String, String> properties;

    public BrokerFileOperations(String name, Map<String, String> properties) {
        this.name = name;
        this.properties = properties;
    }


    public Status openReader(TPaloBrokerService.Client client, TNetworkAddress address, String remoteFilePath,
                             TBrokerFD fd) {
        try {
            TBrokerOpenReaderRequest req = new TBrokerOpenReaderRequest(TBrokerVersion.VERSION_ONE, remoteFilePath,
                    0, BrokerFileSystem.clientId(), properties);
            TBrokerOpenReaderResponse rep = client.openReader(req);
            TBrokerOperationStatus opst = rep.getOpStatus();
            if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                return new Status(Status.ErrCode.COMMON_ERROR,
                        "failed to open reader on broker " + BrokerUtil.printBroker(name, address)
                                + " for file: " + remoteFilePath + ". msg: " + opst.getMessage());
            }
            fd.setHigh(rep.getFd().getHigh());
            fd.setLow(rep.getFd().getLow());
        } catch (TException e) {
            return new Status(Status.ErrCode.COMMON_ERROR,
                    "failed to open reader on broker " + BrokerUtil.printBroker(name, address)
                            + " for file: " + remoteFilePath + ". msg: " + e.getMessage());
        }
        return Status.OK;
    }

    public Status closeReader(TPaloBrokerService.Client client, TNetworkAddress address, TBrokerFD fd) {
        try {
            TBrokerCloseReaderRequest req = new TBrokerCloseReaderRequest(TBrokerVersion.VERSION_ONE, fd);
            TBrokerOperationStatus st = client.closeReader(req);
            if (st.getStatusCode() != TBrokerOperationStatusCode.OK) {
                return new Status(Status.ErrCode.COMMON_ERROR,
                        "failed to close reader on broker " + BrokerUtil.printBroker(name, address)
                                + " for fd: " + fd);
            }

            LOG.info("finished to close reader. fd: {}.", fd);
        } catch (TException e) {
            return new Status(Status.ErrCode.BAD_CONNECTION,
                    "failed to close reader on broker " + BrokerUtil.printBroker(name, address)
                            + ", fd " + fd + ", msg: " + e.getMessage());
        }

        return Status.OK;
    }

    public Status openWriter(TPaloBrokerService.Client client, TNetworkAddress address, String remoteFile,
                              TBrokerFD fd) {
        try {
            TBrokerOpenWriterRequest req = new TBrokerOpenWriterRequest(TBrokerVersion.VERSION_ONE,
                    remoteFile, TBrokerOpenMode.APPEND, BrokerFileSystem.clientId(), properties);
            TBrokerOpenWriterResponse rep = client.openWriter(req);
            TBrokerOperationStatus opst = rep.getOpStatus();
            if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                return new Status(Status.ErrCode.COMMON_ERROR,
                        "failed to open writer on broker " + BrokerUtil.printBroker(name, address)
                                + " for file: " + remoteFile + ". msg: " + opst.getMessage());
            }

            fd.setHigh(rep.getFd().getHigh());
            fd.setLow(rep.getFd().getLow());
            LOG.info("finished to open writer. fd: {}. directly upload to remote path {}.", fd, remoteFile);
        } catch (TException e) {
            return new Status(Status.ErrCode.BAD_CONNECTION,
                    "failed to open writer on broker " + BrokerUtil.printBroker(name, address)
                            + ", err: " + e.getMessage());
        }

        return Status.OK;
    }

    public Status closeWriter(TPaloBrokerService.Client client, TNetworkAddress address, TBrokerFD fd) {
        try {
            TBrokerCloseWriterRequest req = new TBrokerCloseWriterRequest(TBrokerVersion.VERSION_ONE, fd);
            TBrokerOperationStatus st = client.closeWriter(req);
            if (st.getStatusCode() != TBrokerOperationStatusCode.OK) {
                return new Status(Status.ErrCode.COMMON_ERROR,
                        "failed to close writer on broker " + BrokerUtil.printBroker(name, address)
                                + " for fd: " + fd);
            }

            LOG.info("finished to close writer. fd: {}.", fd);
        } catch (TException e) {
            return new Status(Status.ErrCode.BAD_CONNECTION,
                    "failed to close writer on broker " + BrokerUtil.printBroker(name, address)
                            + ", fd " + fd + ", msg: " + e.getMessage());
        }

        return Status.OK;
    }
}
