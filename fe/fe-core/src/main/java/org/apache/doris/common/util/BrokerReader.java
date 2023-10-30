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
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;

public class BrokerReader {
    private static final Logger LOG = LogManager.getLogger(BrokerReader.class);

    private BrokerDesc brokerDesc;
    private TNetworkAddress address;
    private TPaloBrokerService.Client client;
    private long currentPos;

    public static BrokerReader create(BrokerDesc brokerDesc) throws IOException {
        try {
            TNetworkAddress address = BrokerUtil.getAddress(brokerDesc);
            return new BrokerReader(address, BrokerUtil.borrowClient(address), brokerDesc);
        } catch (UserException e) {
            throw new IOException(e);
        }
    }

    // For test only
    public static BrokerReader create(BrokerDesc brokerDesc, String ip, int port) throws IOException {
        try {
            TNetworkAddress address = new TNetworkAddress(ip, port);
            return new BrokerReader(address, BrokerUtil.borrowClient(address), brokerDesc);
        } catch (UserException e) {
            throw new IOException(e);
        }
    }

    private BrokerReader(TNetworkAddress address, TPaloBrokerService.Client client, BrokerDesc brokerDesc) {
        this.brokerDesc = brokerDesc;
        this.address = address;
        this.client = client;
    }

    public long getCurrentPos() {
        return currentPos;
    }

    public byte[] pread(TBrokerFD fd, long offset, int length) throws IOException, EOFException {
        TBrokerPReadRequest tPReadRequest = new TBrokerPReadRequest(
                TBrokerVersion.VERSION_ONE, fd, offset, length);
        TBrokerReadResponse tReadResponse = null;
        try {
            tReadResponse = client.pread(tPReadRequest);
        } catch (TException e) {
            throw new IOException(e);
        }
        if (tReadResponse.getOpStatus().getStatusCode() != TBrokerOperationStatusCode.OK) {
            throw new IOException("Broker pread failed. fd=" + fd.toString() + ", broker=" + client
                    + ", msg=" + tReadResponse.getOpStatus().getMessage());
        }

        if (tReadResponse.getOpStatus().getStatusCode() == TBrokerOperationStatusCode.END_OF_FILE) {
            throw new EOFException();
        }
        return tReadResponse.getData();
    }

    public TBrokerFD open(String path) throws IOException {
        String clientId = NetUtils
                .getHostPortInAccessibleFormat(FrontendOptions.getLocalHostAddress(), Config.rpc_port);
        TBrokerOpenReaderRequest tOpenReaderRequest = new TBrokerOpenReaderRequest(
                TBrokerVersion.VERSION_ONE, path, 0, clientId, brokerDesc.getProperties());
        TBrokerOpenReaderResponse tOpenReaderResponse = null;
        try {
            tOpenReaderResponse = client.openReader(tOpenReaderRequest);
        } catch (TException e) {
            throw new IOException(e);
        }
        if (tOpenReaderResponse.getOpStatus().getStatusCode() != TBrokerOperationStatusCode.OK) {
            throw new IOException("Broker open reader failed. path=" + path + ", broker=" + address
                    + ", msg=" + tOpenReaderResponse.getOpStatus().getMessage());
        }
        this.currentPos = 0;
        return tOpenReaderResponse.getFd();
    }

    public void close(TBrokerFD fd) {
        TBrokerCloseReaderRequest tCloseReaderRequest = new TBrokerCloseReaderRequest(
                TBrokerVersion.VERSION_ONE, fd);
        TBrokerOperationStatus tOperationStatus = null;
        try {
            tOperationStatus = client.closeReader(tCloseReaderRequest);
        } catch (TException e) {
            LOG.warn("Broker close reader failed. fd={}, address={}", fd.toString(), address, e);
        }
        if (tOperationStatus == null) {
            LOG.warn("Broker close reader failed. fd={}, address={}", fd.toString(), address);
        } else if (tOperationStatus.getStatusCode() != TBrokerOperationStatusCode.OK) {
            LOG.warn("Broker close reader failed. fd={}, address={}, error={}", fd.toString(), address,
                    tOperationStatus.getMessage());
        }
    }

    public long getFileLength(String path) throws IOException {
        TBrokerListPathRequest request = new TBrokerListPathRequest(
                TBrokerVersion.VERSION_ONE, path, false, brokerDesc.getProperties());
        TBrokerListResponse tBrokerListResponse = null;
        try {
            tBrokerListResponse = client.listPath(request);
        } catch (TException e) {
            throw new IOException(e);
        }
        if (tBrokerListResponse.getOpStatus().getStatusCode() != TBrokerOperationStatusCode.OK) {
            throw new IOException("Broker list path failed. path=" + path
                    + ",broker=" + address + ",msg=" + tBrokerListResponse.getOpStatus().getMessage());
        }

        if (tBrokerListResponse.files.size() != 1) {
            throw new IOException("Match " + tBrokerListResponse.files.size() + " files. Expected: 1");
        }

        TBrokerFileStatus fileStatus = tBrokerListResponse.files.get(0);
        if (fileStatus.isDir) {
            throw new IOException("Meet dir. Expect file");
        }

        return fileStatus.size;
    }

    public static class EOFException extends Exception {
        public EOFException() {
            super();
        }
    }
}
