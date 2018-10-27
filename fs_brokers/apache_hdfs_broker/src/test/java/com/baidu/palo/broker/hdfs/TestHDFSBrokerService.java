// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.Test;

import com.baidu.palo.thrift.TBrokerListPathRequest;
import com.baidu.palo.thrift.TBrokerListResponse;
import com.baidu.palo.thrift.TBrokerVersion;
import com.baidu.palo.thrift.TPaloBrokerService;

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
