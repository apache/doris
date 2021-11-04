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
package org.apache.doris.spark.rest.models;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Be response model
 **/
@JsonIgnoreProperties(ignoreUnknown = true)
public class BackendV2 {

    @JsonProperty(value = "backends")
    private List<BackendRowV2> backends;

    public List<BackendRowV2> getBackends() {
        return backends;
    }

    public void setRows(List<BackendRowV2> rows) {
        this.backends = rows;
    }

    public static class BackendRowV2 {
        @JsonProperty("ip")
        public String ip;
        @JsonProperty("http_port")
        public int httpPort;
        @JsonProperty("is_alive")
        public boolean isAlive;

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public int getHttpPort() {
            return httpPort;
        }

        public void setHttpPort(int httpPort) {
            this.httpPort = httpPort;
        }

        public boolean isAlive() {
            return isAlive;
        }

        public void setAlive(boolean alive) {
            isAlive = alive;
        }
    }
}
