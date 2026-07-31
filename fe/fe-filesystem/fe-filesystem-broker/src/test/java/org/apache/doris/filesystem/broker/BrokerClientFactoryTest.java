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

import org.apache.doris.thrift.TBrokerOperationStatus;
import org.apache.doris.thrift.TBrokerOperationStatusCode;
import org.apache.doris.thrift.TBrokerPingBrokerRequest;
import org.apache.doris.thrift.TPaloBrokerService;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

/**
 * Unit tests for {@link BrokerClientFactory#pingBroker} — the validation hook used by
 * {@link BrokerClientPool} to detect half-broken sockets that still report
 * {@code transport.isOpen()=true}. The {@code create()} path is exercised only by integration
 * tests because it requires a real TCP endpoint.
 */
class BrokerClientFactoryTest {

    @Test
    void pingBroker_returnsTrueOnHealthyClient() throws TException {
        BrokerClientFactory factory = new BrokerClientFactory();
        TPaloBrokerService.Client client = Mockito.mock(TPaloBrokerService.Client.class);
        TTransport transport = Mockito.mock(TTransport.class);
        TBrokerOperationStatus okStatus = new TBrokerOperationStatus(TBrokerOperationStatusCode.OK);
        Mockito.when(client.ping(ArgumentMatchers.any(TBrokerPingBrokerRequest.class))).thenReturn(okStatus);

        Assertions.assertTrue(factory.pingBroker(client, transport));
        Mockito.verify(client).ping(ArgumentMatchers.any(TBrokerPingBrokerRequest.class));
    }

    @Test
    void pingBroker_returnsFalseOnTException() throws TException {
        BrokerClientFactory factory = new BrokerClientFactory();
        TPaloBrokerService.Client client = Mockito.mock(TPaloBrokerService.Client.class);
        TTransport transport = Mockito.mock(TTransport.class);
        Mockito.when(client.ping(ArgumentMatchers.any(TBrokerPingBrokerRequest.class)))
                .thenThrow(new TException("socket reset"));

        Assertions.assertFalse(factory.pingBroker(client, transport));
    }

    @Test
    void pingBroker_returnsFalseOnNonOkStatus() throws TException {
        BrokerClientFactory factory = new BrokerClientFactory();
        TPaloBrokerService.Client client = Mockito.mock(TPaloBrokerService.Client.class);
        TTransport transport = Mockito.mock(TTransport.class);
        TBrokerOperationStatus errStatus = new TBrokerOperationStatus(TBrokerOperationStatusCode.NOT_AUTHORIZED);
        errStatus.setMessage("auth failed");
        Mockito.when(client.ping(ArgumentMatchers.any(TBrokerPingBrokerRequest.class))).thenReturn(errStatus);

        Assertions.assertFalse(factory.pingBroker(client, transport));
    }
}
