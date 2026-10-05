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

package org.apache.doris.filesystem.broker;

import org.apache.doris.thrift.TBrokerCloseWriterRequest;
import org.apache.doris.thrift.TBrokerFD;
import org.apache.doris.thrift.TBrokerOperationStatus;
import org.apache.doris.thrift.TBrokerOperationStatusCode;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;

/**
 * Unit tests for {@link BrokerOutputStream#close()}.
 *
 * <p>Covers:
 * <ul>
 *   <li>Non-OK closeWriter status surfaces as {@link IOException} and invalidates the
 *       pooled client (broker-side state unknown).</li>
 *   <li>{@code close()} is idempotent — a second invocation does not re-issue RPCs
 *       and does not re-touch the pool.</li>
 *   <li>Successful close returns the client to the pool.</li>
 * </ul>
 */
class BrokerOutputStreamTest {

    private BrokerClientPool mockPool;
    private TPaloBrokerService.Client mockClient;
    private TNetworkAddress endpoint;
    private TBrokerFD fd;
    private BrokerOutputStream stream;

    @BeforeEach
    void setUp() {
        mockPool = Mockito.mock(BrokerClientPool.class);
        mockClient = Mockito.mock(TPaloBrokerService.Client.class);
        endpoint = new TNetworkAddress("broker-host", 9999);
        fd = new TBrokerFD(1L, 2L);
        stream = new BrokerOutputStream(endpoint, mockPool, mockClient, fd);
    }

    @Test
    void close_throwsOnNonOkStatus() throws Exception {
        TBrokerOperationStatus status = new TBrokerOperationStatus(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR);
        status.setMessage("flush failed");
        Mockito.when(mockClient.closeWriter(ArgumentMatchers.any(TBrokerCloseWriterRequest.class)))
                .thenReturn(status);

        Assertions.assertThrows(IOException.class, stream::close);
        Mockito.verify(mockPool).invalidate(endpoint, mockClient);
        Mockito.verify(mockPool, Mockito.never()).returnGood(endpoint, mockClient);
    }

    @Test
    void close_isIdempotent() throws Exception {
        TBrokerOperationStatus status = new TBrokerOperationStatus(TBrokerOperationStatusCode.OK);
        Mockito.when(mockClient.closeWriter(ArgumentMatchers.any(TBrokerCloseWriterRequest.class)))
                .thenReturn(status);

        stream.close();
        stream.close();

        Mockito.verify(mockClient, Mockito.times(1))
                .closeWriter(ArgumentMatchers.any(TBrokerCloseWriterRequest.class));
        Mockito.verify(mockPool, Mockito.times(1)).returnGood(endpoint, mockClient);
        Mockito.verify(mockPool, Mockito.never()).invalidate(endpoint, mockClient);
    }

    @Test
    void close_returnsClientToPoolOnSuccess() throws Exception {
        TBrokerOperationStatus status = new TBrokerOperationStatus(TBrokerOperationStatusCode.OK);
        Mockito.when(mockClient.closeWriter(ArgumentMatchers.any(TBrokerCloseWriterRequest.class)))
                .thenReturn(status);

        stream.close();

        Mockito.verify(mockPool).returnGood(endpoint, mockClient);
        Mockito.verify(mockPool, Mockito.never()).invalidate(endpoint, mockClient);
    }
}
