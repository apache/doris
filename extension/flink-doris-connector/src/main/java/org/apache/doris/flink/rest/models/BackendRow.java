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

package org.apache.doris.flink.rest.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@Deprecated
@JsonIgnoreProperties(ignoreUnknown = true)
public class BackendRow {

    @JsonProperty(value = "HttpPort")
    private String HttpPort;

    @JsonProperty(value = "IP")
    private String IP;

    @JsonProperty(value = "Alive")
    private Boolean Alive;

    public String getHttpPort() {
        return HttpPort;
    }

    public void setHttpPort(String httpPort) {
        HttpPort = httpPort;
    }

    public String getIP() {
        return IP;
    }

    public void setIP(String IP) {
        this.IP = IP;
    }

    public Boolean getAlive() {
        return Alive;
    }

    public void setAlive(Boolean alive) {
        Alive = alive;
    }

    @Override
    public String toString() {
        return "BackendRow{" +
                "HttpPort='" + HttpPort + '\'' +
                ", IP='" + IP + '\'' +
                ", Alive=" + Alive +
                '}';
    }
}
