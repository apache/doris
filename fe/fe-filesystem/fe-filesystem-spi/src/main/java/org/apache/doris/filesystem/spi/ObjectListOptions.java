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

package org.apache.doris.filesystem.spi;

/**
 * Optional controls for object listing.
 */
public final class ObjectListOptions {

    private final String continuationToken;
    private final String startAfter;
    private final int maxKeys;
    private final String delimiter;

    private ObjectListOptions(Builder builder) {
        this.continuationToken = builder.continuationToken;
        this.startAfter = builder.startAfter;
        this.maxKeys = builder.maxKeys;
        this.delimiter = builder.delimiter;
    }

    public String continuationToken() {
        return continuationToken;
    }

    public String startAfter() {
        return startAfter;
    }

    public int maxKeys() {
        return maxKeys;
    }

    public String delimiter() {
        return delimiter;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String continuationToken;
        private String startAfter;
        private int maxKeys;
        private String delimiter;

        public Builder continuationToken(String continuationToken) {
            this.continuationToken = continuationToken;
            return this;
        }

        public Builder startAfter(String startAfter) {
            this.startAfter = startAfter;
            return this;
        }

        public Builder maxKeys(int maxKeys) {
            this.maxKeys = maxKeys;
            return this;
        }

        public Builder delimiter(String delimiter) {
            this.delimiter = delimiter;
            return this;
        }

        public ObjectListOptions build() {
            return new ObjectListOptions(this);
        }
    }
}
