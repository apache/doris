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

package org.apache.doris.flink.backend;

import org.apache.doris.flink.cfg.ConfigurationOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.ConnectedFailedException;
import org.apache.doris.flink.exception.DorisException;
import org.apache.doris.flink.exception.DorisInternalException;
import org.apache.doris.flink.serialization.Routing;
import org.apache.doris.flink.util.ErrorMessages;
import org.apache.doris.thrift.TDorisExternalService;
import org.apache.doris.thrift.TScanBatchResult;
import org.apache.doris.thrift.TScanCloseParams;
import org.apache.doris.thrift.TScanCloseResult;
import org.apache.doris.thrift.TScanNextBatchParams;
import org.apache.doris.thrift.TScanOpenParams;
import org.apache.doris.thrift.TScanOpenResult;
import org.apache.doris.thrift.TStatusCode;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client to request Doris BE
 */
public class BackendClient {
    private static Logger logger = LoggerFactory.getLogger(BackendClient.class);

    private Routing routing;

    private TDorisExternalService.Client client;
    private TTransport transport;

    private boolean isConnected = false;
    private final int retries;
    private final int socketTimeout;
    private final int connectTimeout;

    public BackendClient(Routing routing, DorisReadOptions readOptions) throws ConnectedFailedException {
        this.routing = routing;
        this.connectTimeout = readOptions.getRequestConnectTimeoutMs() == null ? ConfigurationOptions.DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT : readOptions.getRequestConnectTimeoutMs();
        this.socketTimeout = readOptions.getRequestReadTimeoutMs() == null ? ConfigurationOptions.DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT : readOptions.getRequestReadTimeoutMs();
        this.retries = readOptions.getRequestRetries() == null ? ConfigurationOptions.DORIS_REQUEST_RETRIES_DEFAULT : readOptions.getRequestRetries();
        logger.trace("connect timeout set to '{}'. socket timeout set to '{}'. retries set to '{}'.",
                this.connectTimeout, this.socketTimeout, this.retries);
        open();
    }

    private void open() throws ConnectedFailedException {
        logger.debug("Open client to Doris BE '{}'.", routing);
        TException ex = null;
        for (int attempt = 0; !isConnected && attempt < retries; ++attempt) {
            logger.debug("Attempt {} to connect {}.", attempt, routing);
            TBinaryProtocol.Factory factory = new TBinaryProtocol.Factory();
            transport = new TSocket(routing.getHost(), routing.getPort(), socketTimeout, connectTimeout);
            TProtocol protocol = factory.getProtocol(transport);
            client = new TDorisExternalService.Client(protocol);
            if (isConnected) {
                logger.info("Success connect to {}.", routing);
                return;
            }
            try {
                logger.trace("Connect status before open transport to {} is '{}'.", routing, isConnected);
                if (!transport.isOpen()) {
                    transport.open();
                    isConnected = true;
                }
            } catch (TTransportException e) {
                logger.warn(ErrorMessages.CONNECT_FAILED_MESSAGE, routing, e);
                ex = e;
            }

        }
        if (!isConnected) {
            logger.error(ErrorMessages.CONNECT_FAILED_MESSAGE, routing);
            throw new ConnectedFailedException(routing.toString(), ex);
        }
    }

    private void close() {
        logger.trace("Connect status before close with '{}' is '{}'.", routing, isConnected);
        isConnected = false;
        if ((transport != null) && transport.isOpen()) {
            transport.close();
            logger.info("Closed a connection to {}.", routing);
        }
        if (null != client) {
            client = null;
        }
    }

    /**
     * Open a scanner for reading Doris data.
     * @param openParams thrift struct to required by request
     * @return scan open result
     * @throws ConnectedFailedException throw if cannot connect to Doris BE
     */
    public TScanOpenResult openScanner(TScanOpenParams openParams) throws ConnectedFailedException {
        logger.debug("OpenScanner to '{}', parameter is '{}'.", routing, openParams);
        if (!isConnected) {
            open();
        }
        TException ex = null;
        for (int attempt = 0; attempt < retries; ++attempt) {
            logger.debug("Attempt {} to openScanner {}.", attempt, routing);
            try {
                TScanOpenResult result = client.open_scanner(openParams);
                if (result == null) {
                    logger.warn("Open scanner result from {} is null.", routing);
                    continue;
                }
                if (!TStatusCode.OK.equals(result.getStatus().getStatus_code())) {
                    logger.warn("The status of open scanner result from {} is '{}', error message is: {}.",
                            routing, result.getStatus().getStatus_code(), result.getStatus().getError_msgs());
                    continue;
                }
                return result;
            } catch (TException e) {
                logger.warn("Open scanner from {} failed.", routing, e);
                ex = e;
            }
        }
        logger.error(ErrorMessages.CONNECT_FAILED_MESSAGE, routing);
        throw new ConnectedFailedException(routing.toString(), ex);
    }

    /**
     * get next row batch from Doris BE
     * @param nextBatchParams thrift struct to required by request
     * @return scan batch result
     * @throws ConnectedFailedException throw if cannot connect to Doris BE
     */
    public TScanBatchResult getNext(TScanNextBatchParams nextBatchParams) throws DorisException {
        logger.debug("GetNext to '{}', parameter is '{}'.", routing, nextBatchParams);
        if (!isConnected) {
            open();
        }
        TException ex = null;
        TScanBatchResult result = null;
        for (int attempt = 0; attempt < retries; ++attempt) {
            logger.debug("Attempt {} to getNext {}.", attempt, routing);
            try {
                result  = client.get_next(nextBatchParams);
                if (result == null) {
                    logger.warn("GetNext result from {} is null.", routing);
                    continue;
                }
                if (!TStatusCode.OK.equals(result.getStatus().getStatus_code())) {
                    logger.warn("The status of get next result from {} is '{}', error message is: {}.",
                            routing, result.getStatus().getStatus_code(), result.getStatus().getError_msgs());
                    continue;
                }
                return result;
            } catch (TException e) {
                logger.warn("Get next from {} failed.", routing, e);
                ex = e;
            }
        }
        if (result != null && (TStatusCode.OK != (result.getStatus().getStatus_code()))) {
            logger.error(ErrorMessages.DORIS_INTERNAL_FAIL_MESSAGE, routing, result.getStatus().getStatus_code(),
                    result.getStatus().getError_msgs());
            throw new DorisInternalException(routing.toString(), result.getStatus().getStatus_code(),
                    result.getStatus().getError_msgs());
        }
        logger.error(ErrorMessages.CONNECT_FAILED_MESSAGE, routing);
        throw new ConnectedFailedException(routing.toString(), ex);
    }

    /**
     * close an scanner.
     * @param closeParams thrift struct to required by request
     */
    public void closeScanner(TScanCloseParams closeParams) {
        logger.debug("CloseScanner to '{}', parameter is '{}'.", routing, closeParams);
        for (int attempt = 0; attempt < retries; ++attempt) {
            logger.debug("Attempt {} to closeScanner {}.", attempt, routing);
            try {
                TScanCloseResult result = client.close_scanner(closeParams);
                if (result == null) {
                    logger.warn("CloseScanner result from {} is null.", routing);
                    continue;
                }
                if (!TStatusCode.OK.equals(result.getStatus().getStatus_code())) {
                    logger.warn("The status of get next result from {} is '{}', error message is: {}.",
                            routing, result.getStatus().getStatus_code(), result.getStatus().getError_msgs());
                    continue;
                }
                break;
            } catch (TException e) {
                logger.warn("Close scanner from {} failed.", routing, e);
            }
        }
        logger.info("CloseScanner to Doris BE '{}' success.", routing);
        close();
    }
}
