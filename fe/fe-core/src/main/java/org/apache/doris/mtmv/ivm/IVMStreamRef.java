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

package org.apache.doris.mtmv.ivm;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.util.Map;

/**
 * Thin persistent binding between one base table and its stream.
 */
public class IVMStreamRef {
    @SerializedName("st")
    private StreamType streamType;

    @SerializedName("cid")
    private String consumerId;

    @SerializedName("p")
    private Map<String, String> properties;

    public IVMStreamRef() {
        this.properties = Maps.newHashMap();
    }

    public IVMStreamRef(StreamType streamType, String consumerId, Map<String, String> properties) {
        this.streamType = streamType;
        this.consumerId = consumerId;
        this.properties = properties != null ? properties : Maps.newHashMap();
    }

    public StreamType getStreamType() {
        return streamType;
    }

    public void setStreamType(StreamType streamType) {
        this.streamType = streamType;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "IVMStreamRef{"
                + "streamType=" + streamType
                + ", consumerId='" + consumerId + '\''
                + ", properties=" + properties
                + '}';
    }
}
