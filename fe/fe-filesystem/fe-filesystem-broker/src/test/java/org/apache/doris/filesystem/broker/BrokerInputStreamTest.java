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

import org.apache.doris.thrift.TBrokerCloseReaderRequest;
import org.apache.doris.thrift.TBrokerFD;
import org.apache.doris.thrift.TBrokerOperationStatus;
import org.apache.doris.thrift.TBrokerOperationStatusCode;
import org.apache.doris.thrift.TBrokerPReadRequest;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

import org.apache.thrift.TException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;

/**
 * Unit tests for {@link BrokerInputStream} covering close idempotency and
 * {@link java.io.InputStream#read(byte[], int, int)} bounds-checking contract.
 */
class BrokerInputStreamTest {

    private BrokerClientPool mockPool;
    private TPaloBrokerService.Client mockClient;
    private TNetworkAddress endpoint;
    private TBrokerFD fd;
    private BrokerInputStream stream;

    @BeforeEach
    void setUp() {
        mockPool = Mockito.mock(BrokerClientPool.class);
        mockClient = Mockito.mock(TPaloBrokerService.Client.class);
        endpoint = new TNetworkAddress("broker-host", 9999);
        fd = new TBrokerFD(1L, 2L);
        stream = new BrokerInputStream(endpoint, mockPool, mockClient, fd);
    }

    @Test
    void close_isIdempotent() throws Exception {
        TBrokerOperationStatus ok = new TBrokerOperationStatus(TBrokerOperationStatusCode.OK);
        Mockito.when(mockClient.closeReader(ArgumentMatchers.any(TBrokerCloseReaderRequest.class)))
                .thenReturn(ok);

        stream.close();
        stream.close();

        Mockito.verify(mockClient, Mockito.times(1))
                .closeReader(ArgumentMatchers.any(TBrokerCloseReaderRequest.class));
        Mockito.verify(mockPool, Mockito.times(1)).returnGood(endpoint, mockClient);
    }

    @Test
    void close_keepsClosedFlagAfterFailure() throws Exception {
        Mockito.when(mockClient.closeReader(ArgumentMatchers.any(TBrokerCloseReaderRequest.class)))
                .thenThrow(new TException("transport broken"));

        Assertions.assertThrows(IOException.class, stream::close);
        // Second close must be a no-op even though the first one failed mid-RPC.
        stream.close();

        Mockito.verify(mockClient, Mockito.times(1))
                .closeReader(ArgumentMatchers.any(TBrokerCloseReaderRequest.class));
        Mockito.verify(mockPool, Mockito.times(1)).invalidate(endpoint, mockClient);
        Mockito.verify(mockPool, Mockito.never()).returnGood(endpoint, mockClient);
    }

    @Test
    void read_zeroLengthReturnsZeroWithoutRpc() throws Exception {
        byte[] buf = new byte[8];
        Assertions.assertEquals(0, stream.read(buf, 0, 0));
        Mockito.verify(mockClient, Mockito.never())
                .pread(ArgumentMatchers.any(TBrokerPReadRequest.class));
    }

    @Test
    void read_throwsIoobeWhenOffNegative() {
        byte[] buf = new byte[8];
        Assertions.assertThrows(IndexOutOfBoundsException.class, () -> stream.read(buf, -1, 4));
    }

    @Test
    void read_throwsIoobeWhenLenExceedsBuffer() {
        byte[] buf = new byte[8];
        Assertions.assertThrows(IndexOutOfBoundsException.class, () -> stream.read(buf, 4, 5));
    }

    @Test
    void close_invalidatesClientOnCloseReaderNonOk() throws Exception {
        TBrokerOperationStatus bad = new TBrokerOperationStatus(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR);
        bad.setMessage("broker FD lost");
        Mockito.when(mockClient.closeReader(ArgumentMatchers.any(TBrokerCloseReaderRequest.class)))
                .thenReturn(bad);

        // Non-OK closeReader must NOT throw — close() remains idempotent and best-effort.
        stream.close();
        // Client is invalidated rather than returned to the pool.
        Mockito.verify(mockPool).invalidate(endpoint, mockClient);
        Mockito.verify(mockPool, Mockito.never()).returnGood(endpoint, mockClient);

        // Second close is a no-op (closed flag set on first call).
        stream.close();
        Mockito.verify(mockClient, Mockito.times(1))
                .closeReader(ArgumentMatchers.any(TBrokerCloseReaderRequest.class));
        Mockito.verify(mockPool, Mockito.times(1)).invalidate(endpoint, mockClient);
    }

    @Test
    void read_throwsNpeWhenBufferNull() {
        Assertions.assertThrows(NullPointerException.class, () -> stream.read(null, 0, 1));
    }
}
