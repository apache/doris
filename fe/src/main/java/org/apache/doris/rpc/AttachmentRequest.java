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

package org.apache.doris.rpc;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

// used to compatible with our older thrift protocol
public class AttachmentRequest {
    protected byte[] serializedRequest;
    protected byte[] serializedResult;

    public void setRequest(TBase request) throws TException {
        TSerializer serializer = new TSerializer();
        serializedRequest = serializer.serialize(request);
    }
    public void setSerializedRequest(byte[] request) {
        this.serializedRequest = request;
    }
    public byte[] getSerializedRequest() {
        return serializedRequest;
    }
    public void setSerializedResult(byte[] result) {
        this.serializedResult = result;
    }
    public byte[] getSerializedResult() {
        return serializedResult;
    }
    public void getResult(TBase result) throws TException {
        TDeserializer deserializer = new TDeserializer();
        deserializer.deserialize(result, serializedResult);
    }
}
