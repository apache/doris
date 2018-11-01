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

package org.apache.doris.broker.hdfs;

import org.apache.doris.thrift.TBrokerListPathRequest;
import org.apache.doris.thrift.TBrokerListResponse;
import org.apache.doris.thrift.TBrokerVersion;
import org.apache.doris.thrift.TPaloBrokerService;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

public class TestHDFSBrokerService extends TestCase {

    private final String testHdfsHost = "hdfs://host:port";
    private TPaloBrokerService.Client client;
    
    protected void setUp() throws Exception {
        TTransport transport;
        
        transport = new TSocket("host", 9999);
        transport.open();

        TProtocol protocol = new  TBinaryProtocol(transport);
        client = new TPaloBrokerService.Client(protocol);
    }
    
    @Test
    public void testListPath() throws TException {

        Map<String, String> properties = new HashMap<String, String>();
        properties.put("username", "root");
        properties.put("password", "changeit");
        TBrokerListPathRequest request = new TBrokerListPathRequest();
        request.setIsRecursive(false);
        request.setPath(testHdfsHost + "/app/tez/*");
        request.setProperties(properties);
        request.setVersion(TBrokerVersion.VERSION_ONE);
        TBrokerListResponse response = client.listPath(request);
        System.out.println(response);
    }
}
