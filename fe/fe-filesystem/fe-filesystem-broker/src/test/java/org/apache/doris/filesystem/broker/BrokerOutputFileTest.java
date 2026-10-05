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

import org.apache.doris.filesystem.Location;
import org.apache.doris.thrift.TBrokerCheckPathExistRequest;
import org.apache.doris.thrift.TBrokerCheckPathExistResponse;
import org.apache.doris.thrift.TBrokerCloseWriterRequest;
import org.apache.doris.thrift.TBrokerDeletePathRequest;
import org.apache.doris.thrift.TBrokerFD;
import org.apache.doris.thrift.TBrokerListPathRequest;
import org.apache.doris.thrift.TBrokerListResponse;
import org.apache.doris.thrift.TBrokerOpenMode;
import org.apache.doris.thrift.TBrokerOpenWriterRequest;
import org.apache.doris.thrift.TBrokerOpenWriterResponse;
import org.apache.doris.thrift.TBrokerOperationStatus;
import org.apache.doris.thrift.TBrokerOperationStatusCode;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

import org.apache.thrift.TException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link BrokerOutputFile} covering create / createOrOverwrite semantics.
 *
 * <p>The broker Thrift IDL only exposes {@code APPEND} open mode, so:
 * <ul>
 *   <li>{@code create} must verify the file does not already exist before opening.</li>
 *   <li>{@code createOrOverwrite} must delete any existing file before opening to
 *       emulate truncation.</li>
 * </ul>
 */
class BrokerOutputFileTest {

    private BrokerClientPool mockPool;
    private TPaloBrokerService.Client mockClient;
    private BrokerSpiFileSystem fs;
    private TNetworkAddress endpoint;
    private Location location;

    @BeforeEach
    void setUp() throws IOException {
        mockPool = Mockito.mock(BrokerClientPool.class);
        mockClient = Mockito.mock(TPaloBrokerService.Client.class);
        endpoint = new TNetworkAddress("broker-host", 9999);
        Mockito.when(mockPool.borrow(endpoint)).thenReturn(mockClient);

        Map<String, String> params = new HashMap<>();
        fs = new BrokerSpiFileSystem(endpoint, "fe-client", params, mockPool);
        location = Location.of("hdfs:///out/file.txt");
    }

    private void stubExists(boolean exists) throws Exception {
        TBrokerOperationStatus status = new TBrokerOperationStatus(TBrokerOperationStatusCode.OK);
        TBrokerCheckPathExistResponse resp = new TBrokerCheckPathExistResponse(status, exists);
        Mockito.when(mockClient.checkPathExist(ArgumentMatchers.any(TBrokerCheckPathExistRequest.class)))
                .thenReturn(resp);
    }

    private void stubOpenWriterOk() throws Exception {
        TBrokerOperationStatus status = new TBrokerOperationStatus(TBrokerOperationStatusCode.OK);
        TBrokerOpenWriterResponse resp = new TBrokerOpenWriterResponse(status);
        resp.setFd(new TBrokerFD(1L, 2L));
        Mockito.when(mockClient.openWriter(ArgumentMatchers.any(TBrokerOpenWriterRequest.class)))
                .thenReturn(resp);
        // The output stream is closed via try-with-resources in tests; stub closeWriter
        // so the close() call does not NPE on a null status.
        TBrokerOperationStatus closeStatus = new TBrokerOperationStatus(TBrokerOperationStatusCode.OK);
        Mockito.when(mockClient.closeWriter(ArgumentMatchers.any(TBrokerCloseWriterRequest.class)))
                .thenReturn(closeStatus);
    }

    private void stubDeleteOk() throws Exception {
        TBrokerOperationStatus delStatus = new TBrokerOperationStatus(TBrokerOperationStatusCode.OK);
        Mockito.when(mockClient.deletePath(ArgumentMatchers.any(TBrokerDeletePathRequest.class)))
                .thenReturn(delStatus);
        // delete(non-recursive) probes via listPath; return empty so the probe accepts the call
        TBrokerOperationStatus listStatus = new TBrokerOperationStatus(TBrokerOperationStatusCode.OK);
        TBrokerListResponse listResp = new TBrokerListResponse(listStatus);
        Mockito.when(mockClient.listPath(ArgumentMatchers.any(TBrokerListPathRequest.class)))
                .thenReturn(listResp);
    }

    @Test
    void create_throwsIfFileExists() throws Exception {
        stubExists(true);

        BrokerOutputFile out = (BrokerOutputFile) fs.newOutputFile(location);
        Assertions.assertThrows(IOException.class, out::create);
        Mockito.verify(mockClient, Mockito.never())
                .openWriter(ArgumentMatchers.any(TBrokerOpenWriterRequest.class));
    }

    @Test
    void create_writesWhenMissing() throws Exception {
        stubExists(false);
        stubOpenWriterOk();

        BrokerOutputFile out = (BrokerOutputFile) fs.newOutputFile(location);
        try (OutputStream stream = out.create()) {
            Assertions.assertNotNull(stream);
        }

        ArgumentCaptor<TBrokerOpenWriterRequest> captor =
                ArgumentCaptor.forClass(TBrokerOpenWriterRequest.class);
        Mockito.verify(mockClient).openWriter(captor.capture());
        // Broker IDL only defines APPEND; truncation semantics for create() are guarded by the
        // existence check above, not by an open-mode flag.
        Assertions.assertEquals(TBrokerOpenMode.APPEND, captor.getValue().getOpenMode());
        Assertions.assertEquals(location.uri(), captor.getValue().getPath());
    }

    @Test
    void createOrOverwrite_usesWriteMode() throws Exception {
        stubDeleteOk();
        stubOpenWriterOk();

        BrokerOutputFile out = (BrokerOutputFile) fs.newOutputFile(location);
        try (OutputStream stream = out.createOrOverwrite()) {
            Assertions.assertNotNull(stream);
        }

        // createOrOverwrite must delete any existing file before opening (broker has no WRITE
        // mode that truncates), then open the writer.
        Mockito.verify(mockClient).deletePath(ArgumentMatchers.any(TBrokerDeletePathRequest.class));
        ArgumentCaptor<TBrokerOpenWriterRequest> captor =
                ArgumentCaptor.forClass(TBrokerOpenWriterRequest.class);
        Mockito.verify(mockClient).openWriter(captor.capture());
        Assertions.assertEquals(TBrokerOpenMode.APPEND, captor.getValue().getOpenMode());
    }

    @Test
    void openWriter_invalidatesClientOnTException() throws Exception {
        stubExists(false);
        Mockito.when(mockClient.openWriter(ArgumentMatchers.any(TBrokerOpenWriterRequest.class)))
                .thenThrow(new TException("transport closed"));

        BrokerOutputFile out = (BrokerOutputFile) fs.newOutputFile(location);
        Assertions.assertThrows(IOException.class, out::create);
        // exists() above borrows + returnGoods the same mock client, so we can only assert that
        // the openWriter path invalidated the client (and did not double-return it via returnGood).
        Mockito.verify(mockPool, Mockito.times(1)).invalidate(endpoint, mockClient);
        Mockito.verify(mockPool, Mockito.times(1)).returnGood(endpoint, mockClient);
    }
}
