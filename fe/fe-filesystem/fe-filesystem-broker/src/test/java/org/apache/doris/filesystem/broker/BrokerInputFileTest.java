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
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TBrokerListPathRequest;
import org.apache.doris.thrift.TBrokerListResponse;
import org.apache.doris.thrift.TBrokerOpenReaderRequest;
import org.apache.doris.thrift.TBrokerOperationStatus;
import org.apache.doris.thrift.TBrokerOperationStatusCode;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

import org.apache.thrift.TException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link BrokerInputFile} covering lazy length resolution and
 * pool-poisoning safety on transport failures.
 */
class BrokerInputFileTest {

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
        location = Location.of("hdfs:///in/file.txt");
    }

    private void stubListPath(List<TBrokerFileStatus> files) throws Exception {
        TBrokerOperationStatus status = new TBrokerOperationStatus(TBrokerOperationStatusCode.OK);
        TBrokerListResponse resp = new TBrokerListResponse(status);
        resp.setFiles(files);
        Mockito.when(mockClient.listPath(ArgumentMatchers.any(TBrokerListPathRequest.class)))
                .thenReturn(resp);
    }

    private TBrokerFileStatus fileStatus(String path, long size, boolean isDir) {
        TBrokerFileStatus s = new TBrokerFileStatus();
        s.setPath(path);
        s.setSize(size);
        s.setIsDir(isDir);
        s.setModificationTime(0L);
        return s;
    }

    @Test
    void length_returnsCachedKnownLength() throws Exception {
        BrokerInputFile in = (BrokerInputFile) fs.newInputFile(location, 1234L);
        Assertions.assertEquals(1234L, in.length());
        Mockito.verify(mockClient, Mockito.never())
                .listPath(ArgumentMatchers.any(TBrokerListPathRequest.class));
    }

    @Test
    void length_lazyFetchesViaListPath() throws Exception {
        List<TBrokerFileStatus> files = new ArrayList<>();
        files.add(fileStatus(location.uri(), 4096L, false));
        stubListPath(files);

        BrokerInputFile in = (BrokerInputFile) fs.newInputFile(location);
        Assertions.assertEquals(4096L, in.length());
        Assertions.assertEquals(4096L, in.length());

        Mockito.verify(mockClient, Mockito.times(1))
                .listPath(ArgumentMatchers.any(TBrokerListPathRequest.class));
    }

    @Test
    void length_throwsFileNotFoundWhenMissing() throws Exception {
        stubListPath(new ArrayList<>());
        BrokerInputFile in = (BrokerInputFile) fs.newInputFile(location);
        Assertions.assertThrows(FileNotFoundException.class, in::length);
    }

    @Test
    void length_throwsIOExceptionWhenDirectory() throws Exception {
        List<TBrokerFileStatus> files = new ArrayList<>();
        files.add(fileStatus(location.uri(), 0L, true));
        stubListPath(files);

        BrokerInputFile in = (BrokerInputFile) fs.newInputFile(location);
        IOException ex = Assertions.assertThrows(IOException.class, in::length);
        Assertions.assertFalse(ex instanceof FileNotFoundException);
    }

    @Test
    void newStream_invalidatesClientOnTException() throws Exception {
        Mockito.when(mockClient.openReader(ArgumentMatchers.any(TBrokerOpenReaderRequest.class)))
                .thenThrow(new TException("transport closed"));

        BrokerInputFile in = (BrokerInputFile) fs.newInputFile(location, 0L);
        Assertions.assertThrows(IOException.class, in::newStream);
        Mockito.verify(mockPool).invalidate(endpoint, mockClient);
        Mockito.verify(mockPool, Mockito.never()).returnGood(endpoint, mockClient);
    }
}
