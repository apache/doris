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

package com.baidu.palo.rpc;

import com.baidu.jprotobuf.pbrpc.ClientAttachmentHandler;

public class ThriftClientAttachmentHandler implements ClientAttachmentHandler {
    @Override
    public byte[] handleRequest(String serviceName, String methodName, Object... objects) {
        AttachmentRequest request = (AttachmentRequest) objects[0];
        return request.getSerializedRequest();
    }
    @Override
    public void handleResponse(byte[] bytes, String serviceName, String methodName, Object... objects) {
        AttachmentRequest result = (AttachmentRequest) objects[0];
        result.setSerializedResult(bytes);
    }
}
