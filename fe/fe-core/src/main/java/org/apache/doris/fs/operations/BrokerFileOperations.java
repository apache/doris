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

package org.apache.doris.fs.operations;

import org.apache.doris.backup.Status;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.service.FrontendOptions;
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.Map;

public class BrokerFileOperations implements FileOperations {

    private static final Logger LOG = LogManager.getLogger(BrokerFileOperations.class);

    private final String name;

    private final Map<String, String> properties;

    public BrokerFileOperations(String name, Map<String, String> properties) {
        this.name = name;
        this.properties = properties;
    }

    public static String clientId() {
        return NetUtils
                .getHostPortInAccessibleFormat(FrontendOptions.getLocalHostAddress(), Config.edit_log_port);
    }

    @Override
    public Status openReader(OpParams opParams) {
        BrokerOpParams brokerOpParams = (BrokerOpParams) opParams;
        String remoteFilePath = brokerOpParams.remotePath();
        try {
            TBrokerOpenReaderRequest req = new TBrokerOpenReaderRequest(TBrokerVersion.VERSION_ONE, remoteFilePath,
                    0, clientId(), properties);
            TBrokerOpenReaderResponse rep = brokerOpParams.client().openReader(req);
            TBrokerOperationStatus opst = rep.getOpStatus();
            if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                return new Status(Status.ErrCode.COMMON_ERROR,
                        "failed to open reader on broker "
                                + BrokerUtil.printBroker(name, brokerOpParams.address())
                                + " for file: " + remoteFilePath + ". msg: " + opst.getMessage());
            }
            brokerOpParams.fd().setHigh(rep.getFd().getHigh());
            brokerOpParams.fd().setLow(rep.getFd().getLow());
        } catch (TException e) {
            return new Status(Status.ErrCode.COMMON_ERROR,
                    "failed to open reader on broker "
                            + BrokerUtil.printBroker(name, brokerOpParams.address())
                            + " for file: " + remoteFilePath + ". msg: " + e.getMessage());
        }
        return Status.OK;
    }

    public Status closeReader(OpParams opParams) {
        BrokerOpParams brokerOpParams = (BrokerOpParams) opParams;
        TBrokerFD fd = brokerOpParams.fd();
        try {
            TBrokerCloseReaderRequest req = new TBrokerCloseReaderRequest(TBrokerVersion.VERSION_ONE, fd);
            TBrokerOperationStatus st = brokerOpParams.client().closeReader(req);
            if (st.getStatusCode() != TBrokerOperationStatusCode.OK) {
                return new Status(Status.ErrCode.COMMON_ERROR,
                        "failed to close reader on broker "
                                + BrokerUtil.printBroker(name, brokerOpParams.address())
                                + " for fd: " + fd);
            }

            LOG.info("finished to close reader. fd: {}.", fd);
        } catch (TException e) {
            return new Status(Status.ErrCode.BAD_CONNECTION,
                    "failed to close reader on broker "
                            + BrokerUtil.printBroker(name, brokerOpParams.address())
                            + ", fd " + fd + ", msg: " + e.getMessage());
        }

        return Status.OK;
    }

    public Status openWriter(OpParams desc) {
        BrokerOpParams brokerOpParams = (BrokerOpParams) desc;
        String remoteFile = brokerOpParams.remotePath();
        TBrokerFD fd = brokerOpParams.fd();
        try {
            TBrokerOpenWriterRequest req = new TBrokerOpenWriterRequest(TBrokerVersion.VERSION_ONE,
                    remoteFile, TBrokerOpenMode.APPEND, clientId(), properties);
            TBrokerOpenWriterResponse rep = brokerOpParams.client().openWriter(req);
            TBrokerOperationStatus opst = rep.getOpStatus();
            if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                return new Status(Status.ErrCode.COMMON_ERROR,
                        "failed to open writer on broker "
                                + BrokerUtil.printBroker(name, brokerOpParams.address())
                                + " for file: " + remoteFile + ". msg: " + opst.getMessage());
            }

            fd.setHigh(rep.getFd().getHigh());
            fd.setLow(rep.getFd().getLow());
            LOG.info("finished to open writer. fd: {}. directly upload to remote path {}.", fd, remoteFile);
        } catch (TException e) {
            return new Status(Status.ErrCode.BAD_CONNECTION,
                    "failed to open writer on broker "
                            + BrokerUtil.printBroker(name, brokerOpParams.address())
                            + ", err: " + e.getMessage());
        }

        return Status.OK;
    }

    public Status closeWriter(OpParams desc) {
        BrokerOpParams brokerOpParams = (BrokerOpParams) desc;
        TBrokerFD fd = brokerOpParams.fd();
        try {
            TBrokerCloseWriterRequest req = new TBrokerCloseWriterRequest(TBrokerVersion.VERSION_ONE, fd);
            TBrokerOperationStatus st = brokerOpParams.client().closeWriter(req);
            if (st.getStatusCode() != TBrokerOperationStatusCode.OK) {
                return new Status(Status.ErrCode.COMMON_ERROR,
                        "failed to close writer on broker "
                                + BrokerUtil.printBroker(name, brokerOpParams.address())
                                + " for fd: " + fd);
            }

            LOG.info("finished to close writer. fd: {}.", fd);
        } catch (TException e) {
            return new Status(Status.ErrCode.BAD_CONNECTION,
                    "failed to close writer on broker "
                            + BrokerUtil.printBroker(name, brokerOpParams.address())
                            + ", fd " + fd + ", msg: " + e.getMessage());
        }

        return Status.OK;
    }
}
