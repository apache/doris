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

import org.apache.doris.thrift.TBrokerFD;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

/**
 * All arguments of the FileOperations implementation class should get from this class
 * @see org.apache.doris.fs.operations.FileOperations
 * Should avoid using this class elsewhere
 */
public class OpParams {

    public static BrokerOpParams of(TPaloBrokerService.Client client, TNetworkAddress address, TBrokerFD fd) {
        return new BrokerOpParams(client, address, fd);
    }

    public static BrokerOpParams of(TPaloBrokerService.Client client, TNetworkAddress address,
                                    String remoteFilePath, TBrokerFD fd) {
        return new BrokerOpParams(client, address, remoteFilePath, fd);
    }

    public static HDFSOpParams of(String remotePath) {
        return new HDFSOpParams(remotePath, 0);
    }

    public static HDFSOpParams of(FSDataOutputStream fsDataOutputStream) {
        return new HDFSOpParams(fsDataOutputStream);
    }

    public static HDFSOpParams of(FSDataInputStream fsDataInputStream) {
        return new HDFSOpParams(fsDataInputStream);
    }
}
