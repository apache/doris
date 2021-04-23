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
package org.apache.doris.flink.cfg;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 *  Doris connection options.
 */
public class DorisConnectionOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final String fenodes;
    protected final String username;
    protected final String password;

    public DorisConnectionOptions(String fenodes, String username, String password) {
        this.fenodes = Preconditions.checkNotNull(fenodes, "fenodes  is empty");
        this.username = username;
        this.password = password;
    }

    public String getFenodes() {
        return fenodes;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    /**
     * Builder for {@link DorisConnectionOptions}.
     */
    public static class DorisConnectionOptionsBuilder {
        private String fenodes;
        private String username;
        private String password;

        public DorisConnectionOptionsBuilder withFenodes(String fenodes) {
            this.fenodes = fenodes;
            return this;
        }

        public DorisConnectionOptionsBuilder withUsername(String username) {
            this.username = username;
            return this;
        }

        public DorisConnectionOptionsBuilder withPassword(String password) {
            this.password = password;
            return this;
        }

        public DorisConnectionOptions build() {
            return new DorisConnectionOptions(fenodes, username, password);
        }
    }

}
