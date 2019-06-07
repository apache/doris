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

import org.apache.doris.proto.PUniqueId;

import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;
import com.google.common.collect.Lists;

import java.util.List;

@ProtobufClass
public class PTriggerProfileReportRequest extends AttachmentRequest {

    @Protobuf(fieldType = FieldType.OBJECT, order = 1, required = false)
    List<PUniqueId> instanceIds;

    public PTriggerProfileReportRequest() {
    }

    public PTriggerProfileReportRequest(List<PUniqueId> instanceIds) {
        this.instanceIds = Lists.newArrayList();
        this.instanceIds.addAll(instanceIds);
    }
}
